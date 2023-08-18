package sharedsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/rs/zerolog"
	"math"
	"path/filepath"
	"sync"
)

const (
	defaultRetainedSize = 5
)

// Payload is message format of pingreq sent by client.
type Payload struct {
	NumberOfMsgsInQueue  int64   `json:"numberOfMsgsInQueue"`
	ProcessingTimePerMsg float64 `json:"processingTimerPerMsg"`
}

type Selector interface {
	selectClientToSend(string, []*Client) (*Client, error)
}

type Updater interface {
	updateClientInfoWithPayload(*Client, Payload) error
	updateClientInfoAfterSending(*Client) error
}

type Manager struct {
	selector Selector
	updater  Updater
	clients  *Clients
	log      *zerolog.Logger
	dirName  string
}

type Options struct {
	Selector Selector
	Updater  Updater
	Log      *zerolog.Logger
	DirName  string
}

func NewManager(options Options) *Manager {
	m := &Manager{
		selector: options.Selector,
		updater:  options.Updater,
		clients:  NewClients(),
		log:      options.Log,
		dirName:  options.DirName,
	}
	m.log.Info().
		Str("Selector", fmt.Sprintf("%#v\n", options.Selector)).
		Str("Updater", fmt.Sprintf("%#v\n", options.Updater)).
		Msg("NewManager")
	return m
}

func (m *Manager) UpdateClient(cl *mqtt.Client) {
	m.clients.AddClient(cl.ID)
	m.log.Info().Str("client", cl.ID).Msg("update client")
}

// DeleteClient deletes the client from the status manager.
func (m *Manager) DeleteClient(cl *mqtt.Client) {
	m.clients.DeleteClient(cl.ID)
	m.log.Info().Str("client", cl.ID).Msg("delete client")
}

// UpdateClientInfo updates the number of messages in the queue and the processing time per message for each client.
func (m *Manager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	err = m.clients.UpdateClientInfoWithPayload(cl.ID, p, m.updater.updateClientInfoWithPayload)
	if err != nil {
		return err
	}
	m.log.Info().Str("client", cl.ID).Msgf("update client info: PINGREQ NumberOfMsgsInQueue=%d, ProcessingTimePerMsg=%f", p.NumberOfMsgsInQueue, p.ProcessingTimePerMsg)
	return nil
}

// SelectSubscriber selects a subscriber based on the number of messages in the queue and the processing time per message.
func (m *Manager) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	_ packets.Packet,
) (string, error) {

	selectedClientId, err := m.clients.SelectClientToSend(
		topicFilter,
		groupSubs,
		m.selector.selectClientToSend,
		m.updater.updateClientInfoAfterSending,
	)

	if err != nil {
		m.log.Error().Err(err).Msgf("failed to select subscriber: %s", topicFilter)
		return "", err
	}

	m.log.Debug().Str("topic", topicFilter).Msgf("select subscriber: %s", selectedClientId)
	return selectedClientId, nil
}

// StartRecording Start starts the status manager.
func (m *Manager) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// write client status to csv file every second
	fileName := filepath.Join(m.dirName, "client_status.csv")
	if err := m.clients.Recording(ctx, fileName); err != nil {
		m.log.Fatal().Err(err).Msg("failed to write csv")
		return err
	}
	return nil
}

type SimpleSelector struct{}

func (su *SimpleSelector) selectClientToSend(topicFilter string, clients []*Client) (*Client, error) {
	var selectedClient *Client
	minMsgsInQueue := int64(math.MaxInt64)
	minProcessingTime := math.MaxFloat64

	for _, cl := range clients {
		numMsgsInQueue := cl.sentMessageCount + cl.numberOfMsgsInQueue
		processingTime := cl.avgProcessingTimePerMsg

		if numMsgsInQueue < minMsgsInQueue || (numMsgsInQueue == minMsgsInQueue && processingTime < minProcessingTime) {
			selectedClient = cl
			minMsgsInQueue = numMsgsInQueue
			minProcessingTime = processingTime
		}
	}

	if selectedClient == nil {
		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}
	return selectedClient, nil
}

type SimpleUpdater struct {
}

func (su *SimpleUpdater) updateClientInfoWithPayload(cl *Client, p Payload) error {
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	if len(cl.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}
	cl.receivedPayload = append(cl.receivedPayload, p)
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
	cl.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.receivedPayload))
	return nil
}

func (su *SimpleUpdater) updateClientInfoAfterSending(cl *Client) error {
	cl.sentMessageCount++
	return nil
}

type RandomSelector struct {
}

type ClearUpdater struct {
}

//
//type RandomManager struct {
//	sync.RWMutex
//	clients map[string]*Client
//	log     *zerolog.Logger
//	dirName string
//}
//
//func NewRandomManager(log *zerolog.Logger, dirName string) *RandomManager {
//	log.Info().Msg("create RandomManager")
//	rm := &RandomManager{
//		clients: make(map[string]*Client),
//		log:     log,
//		dirName: dirName,
//	}
//	return rm
//}
//
//func (m *RandomManager) UpdateClient(cl *mqtt.Client) {
//	m.Lock()
//	defer m.Unlock()
//	m.clients[cl.ID] = NewClient()
//	m.log.Info().Str("client", cl.ID).Msg("update client")
//}
//
//func (m *RandomManager) DeleteClient(cl *mqtt.Client) {
//	m.Lock()
//	defer m.Unlock()
//	delete(m.clients, cl.ID)
//	m.log.Info().Str("client", cl.ID).Msg("delete client")
//}
//
//func (m *RandomManager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
//	m.Lock()
//	defer m.Unlock()
//
//	client, ok := m.clients[cl.ID]
//	if !ok {
//		return errors.New("client is not found")
//	}
//
//	p := Payload{}
//	err := json.Unmarshal(pk.Payload, &p)
//	if err != nil {
//		return err
//	}
//
//	// Update value using subscriber selection with pingreq.
//	totalProcessingTime := client.avgProcessingTimePerMsg * float64(len(client.receivedPayload))
//	if len(client.receivedPayload) == defaultRetainedSize {
//		// Remove old information.
//		totalProcessingTime -= client.receivedPayload[0].ProcessingTimePerMsg
//		client.receivedPayload = client.receivedPayload[1:]
//	}
//	client.receivedPayload = append(client.receivedPayload, p)
//	client.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
//	client.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(client.receivedPayload))
//	m.log.Info().Str("client", cl.ID).Msgf("update client info: PINGREQ NumberOfMsgsInQueue=%d, ProcessingTimePerMsg=%f", p.NumberOfMsgsInQueue, p.ProcessingTimePerMsg)
//	return nil
//}
//
//// SelectSubscriber selects a subscriber based on the number of messages in the queue and the processing time per message.
//func (m *RandomManager) SelectSubscriber(
//	topicFilter string,
//	groupSubs map[string]packets.Subscription,
//	pk packets.Packet,
//) (string, error) {
//	var selectedClientId string
//
//	m.RLock()
//	defer m.RUnlock()
//
//	for clientId, _ := range groupSubs {
//		selectedClientId = clientId
//		break
//	}
//
//	if selectedClientId == "" {
//		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
//	}
//
//	cl := m.clients[selectedClientId]
//	cl.Lock()
//	cl.sentMessageCount++
//	cl.Unlock()
//
//	m.log.Info().Str("topic", topicFilter).Msgf("select subscriber: %s", selectedClientId)
//	return selectedClientId, nil
//}
//
//// StartRecording Start starts the status manager.
//func (m *RandomManager) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
//	defer wg.Done()
//
//	// create csv file to write client status
//	fileName := filepath.Join(m.dirName, "client_status.csv")
//	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
//	if err != nil {
//		m.log.Fatal().Err(err).Msg("failed to open file")
//		return err
//	}
//	defer file.Close()
//	writer := csv.NewWriter(file)
//	defer writer.Flush()
//
//	// write client status to csv file every second
//	if err := m.clients.Recording(ctx, writer); err != nil {
//		m.log.Fatal().Err(err).Msg("failed to write csv")
//		return err
//	}
//
//	return nil
//}
