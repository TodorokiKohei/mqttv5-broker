package sharedsub

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/rs/zerolog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Manager manage information received from pingreq.
type Manager interface {
	UpdateClient(cl *mqtt.Client)
	UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error
	DeleteClient(cl *mqtt.Client)
	SelectSubscriber(topicFilter string, groupSubs map[string]packets.Subscription, pk packets.Packet) (string, error)

	StartRecording(ctx context.Context, wg *sync.WaitGroup) error
}

const (
	defaultRetainedSize = 10
)

// Payload is message format of pingreq sent by client.
type Payload struct {
	NumberOfMsgsInQueue  int64   `json:"numberOfMsgsInQueue"`
	ProcessingTimePerMsg float64 `json:"processingTimerPerMsg"`
}

type Client struct {
	sync.RWMutex
	receivedPayload         []Payload
	numberOfMsgsInQueue     int64
	avgProcessingTimePerMsg float64
	sentMessageCount        int64
}

func GetCsvHeader() []string {
	return []string{
		"clientID",
		"time",
		"sentMessageCount",
		"numberOfMsgsInQueue",
		"avgProcessingTimePerMsg",
	}
}

func (cl *Client) GetCsvRecord(clientId string) []string {
	return []string{
		clientId,
		strconv.FormatInt(time.Now().Unix(), 10),
		strconv.FormatInt(cl.sentMessageCount, 10),
		strconv.FormatInt(cl.numberOfMsgsInQueue, 10),
		strconv.FormatFloat(cl.avgProcessingTimePerMsg, 'f', -1, 64),
	}
}

func NewClient() *Client {
	return &Client{
		receivedPayload:         make([]Payload, 0, defaultRetainedSize),
		numberOfMsgsInQueue:     0,
		avgProcessingTimePerMsg: 0,
		sentMessageCount:        0,
	}
}

// The StatusManager maintains the number of messages in the queue and the processing time per message for each client.
type StatusManager struct {
	sync.RWMutex
	clients map[string]*Client
	log     *zerolog.Logger
	dirName string
}

func NewStatusManager(log *zerolog.Logger, dirName string) *StatusManager {
	log.Info().Msg("create StatusManager")
	sm := &StatusManager{
		clients: make(map[string]*Client),
		log:     log,
		dirName: dirName,
	}
	return sm
}

func (m *StatusManager) UpdateClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	m.clients[cl.ID] = NewClient()
	m.log.Info().Str("client", cl.ID).Msg("update client")
}

// DeleteClient deletes the client from the status manager.
func (m *StatusManager) DeleteClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	delete(m.clients, cl.ID)
	m.log.Info().Str("client", cl.ID).Msg("delete client")
}

// UpdateClientInfo updates the number of messages in the queue and the processing time per message for each client.
func (m *StatusManager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	m.Lock()
	defer m.Unlock()

	client, ok := m.clients[cl.ID]
	if !ok {
		return errors.New("client is not found")
	}

	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	// Update value using subscriber selection with pingreq.
	totalProcessingTime := client.avgProcessingTimePerMsg * float64(len(client.receivedPayload))
	if len(client.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= client.receivedPayload[0].ProcessingTimePerMsg
		client.receivedPayload = client.receivedPayload[1:]
	}
	client.receivedPayload = append(client.receivedPayload, p)
	client.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
	client.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(client.receivedPayload))
	m.log.Info().Str("client", cl.ID).Msgf("update client info: PINGREQ NumberOfMsgsInQueue=%d, ProcessingTimePerMsg=%f", p.NumberOfMsgsInQueue, p.ProcessingTimePerMsg)
	return nil
}

// SelectSubscriber selects a subscriber based on the number of messages in the queue and the processing time per message.
func (m *StatusManager) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	pk packets.Packet,
) (string, error) {
	var selectedClientId string
	minMsgsInQueue := int64(math.MaxInt64)
	minProcessingTime := math.MaxFloat64

	m.RLock()
	defer m.RUnlock()

	for clientId, _ := range groupSubs {
		cl, ok := m.clients[clientId]
		if !ok {
			continue
		}
		numMsgsInQueue := cl.sentMessageCount
		if len(cl.receivedPayload) != 0 {
			numMsgsInQueue += cl.receivedPayload[len(cl.receivedPayload)-1].NumberOfMsgsInQueue
		}
		processingTime := cl.avgProcessingTimePerMsg

		if numMsgsInQueue < minMsgsInQueue || (numMsgsInQueue == minMsgsInQueue && processingTime < minProcessingTime) {
			selectedClientId = clientId
			minMsgsInQueue = numMsgsInQueue
			minProcessingTime = processingTime
		}
	}

	if selectedClientId == "" {
		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	cl := m.clients[selectedClientId]
	cl.Lock()
	cl.sentMessageCount++
	cl.Unlock()

	m.log.Info().Str("topic", topicFilter).Msgf("select subscriber: %s", selectedClientId)
	return selectedClientId, nil
}

// StartRecording Start starts the status manager.
func (m *StatusManager) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// create csv file to write client status
	fileName := filepath.Join(m.dirName, "client_status.csv")
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		m.log.Fatal().Err(err).Msg("failed to open file")
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write csv header
	if err = writer.Write(GetCsvHeader()); err != nil {
		m.log.Fatal().Err(err).Msg("failed to write csv header")
		return err
	}

	// write client status to csv file every second
	done := false
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case <-time.After(time.Second):
			// write client status to csv file
			for clientId, cl := range m.clients {
				if err := writer.Write(cl.GetCsvRecord(clientId)); err != nil {
					m.log.Fatal().Err(err).Msg("failed to write csv record")
					return err
				}
			}
		}
	}
	return nil
}

type RandomManager struct {
	sync.RWMutex
	clients map[string]*Client
	log     *zerolog.Logger
	dirName string
}

func NewRandomManager(log *zerolog.Logger, dirName string) *RandomManager {
	log.Info().Msg("create RandomManager")
	rm := &RandomManager{
		clients: make(map[string]*Client),
		log:     log,
		dirName: dirName,
	}
	return rm
}

func (m *RandomManager) UpdateClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	m.clients[cl.ID] = NewClient()
	m.log.Info().Str("client", cl.ID).Msg("update client")
}

func (m *RandomManager) DeleteClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	delete(m.clients, cl.ID)
	m.log.Info().Str("client", cl.ID).Msg("delete client")
}

func (m *RandomManager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	m.Lock()
	defer m.Unlock()

	client, ok := m.clients[cl.ID]
	if !ok {
		return errors.New("client is not found")
	}

	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	// Update value using subscriber selection with pingreq.
	totalProcessingTime := client.avgProcessingTimePerMsg * float64(len(client.receivedPayload))
	if len(client.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= client.receivedPayload[0].ProcessingTimePerMsg
		client.receivedPayload = client.receivedPayload[1:]
	}
	client.receivedPayload = append(client.receivedPayload, p)
	client.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
	client.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(client.receivedPayload))
	m.log.Info().Str("client", cl.ID).Msgf("update client info: PINGREQ NumberOfMsgsInQueue=%d, ProcessingTimePerMsg=%f", p.NumberOfMsgsInQueue, p.ProcessingTimePerMsg)
	return nil
}

// SelectSubscriber selects a subscriber based on the number of messages in the queue and the processing time per message.
func (m *RandomManager) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	pk packets.Packet,
) (string, error) {
	var selectedClientId string

	m.RLock()
	defer m.RUnlock()

	for clientId, _ := range groupSubs {
		selectedClientId = clientId
		break
	}

	if selectedClientId == "" {
		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	cl := m.clients[selectedClientId]
	cl.Lock()
	cl.sentMessageCount++
	cl.Unlock()

	m.log.Info().Str("topic", topicFilter).Msgf("select subscriber: %s", selectedClientId)
	return selectedClientId, nil
}

// StartRecording Start starts the status manager.
func (m *RandomManager) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// create csv file to write client status
	fileName := filepath.Join(m.dirName, "client_status.csv")
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		m.log.Fatal().Err(err).Msg("failed to open file")
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write csv header
	if err = writer.Write(GetCsvHeader()); err != nil {
		m.log.Fatal().Err(err).Msg("failed to write csv header")
		return err
	}

	// write client status to csv file every second
	done := false
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case <-time.After(time.Second):
			// write client status to csv file
			for clientId, cl := range m.clients {
				if err := writer.Write(cl.GetCsvRecord(clientId)); err != nil {
					m.log.Fatal().Err(err).Msg("failed to write csv record")
					return err
				}
			}
		}
	}
	return nil
}
