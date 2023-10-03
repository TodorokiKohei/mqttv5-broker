package sharedsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"log/slog"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultRetainedSize = 5
)

// Payload is message format of pingreq sent by client.
type Payload struct {
	NumberOfMsgsInQueue  int64   `json:"numberOfMsgsInQueue"`
	ProcessingTimePerMsg float64 `json:"processingTimerPerMsg"` // milliseconds
}

// selector selects a subscriber.
type selector interface {
	selectClientToSend(string, []*Client, float64) (*Client, error)
}

// updater updates a client's information.
type updater interface {
	updateClientInfoWithPayload(*Client, Payload) error
	updateClientInfoAfterSending(*Client) error
}

// Algorithm is a combination of selector and updater.
type Algorithm interface {
	selector
	updater
}

type Manager struct {
	algorithm Algorithm
	clients   *Clients
	log       *slog.Logger
	dirName   string
}

type Options struct {
	Algorithm Algorithm
	Log       *slog.Logger
	DirName   string
}

func NewManager(options Options) *Manager {
	m := &Manager{
		clients:   NewClients(),
		algorithm: options.Algorithm,
		log:       options.Log,
		dirName:   options.DirName,
	}
	m.log.Info("Create Manager", "Algorithm", fmt.Sprintf("%#v\n", options.Algorithm))
	return m
}

// UpdateClient updates the client.
func (m *Manager) UpdateClient(cl *mqtt.Client) {
	m.clients.AddClient(cl.ID)
	m.log.Info("update client", "client", cl.ID)
}

// DeleteClient deletes the client.
func (m *Manager) DeleteClient(cl *mqtt.Client) {
	m.clients.DeleteClient(cl.ID)
	m.log.Info("delete client", "client", cl.ID)
}

// UpdateClientInfo updates a client's information with updater.
func (m *Manager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	err = m.clients.UpdateClientInfoWithPayload(cl.ID, p, m.algorithm.updateClientInfoWithPayload)
	if err != nil {
		return err
	}
	m.log.Info(fmt.Sprintf("update client info: PINGREQ NumberOfMsgsInQueue=%d, ProcessingTimePerMsg=%f", p.NumberOfMsgsInQueue, p.ProcessingTimePerMsg), "client", cl.ID)
	return nil
}

// SelectSubscriber selects a subscriber with selector.
func (m *Manager) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	_ packets.Packet,
) (string, error) {

	selectedClientId, err := m.clients.SelectClientToSend(
		topicFilter,
		groupSubs,
		m.algorithm.selectClientToSend,
		m.algorithm.updateClientInfoAfterSending,
	)

	if err != nil {
		m.log.Error(fmt.Sprintf("failed to select subscriber: %s", topicFilter))
		return "", err
	}

	m.log.Debug(fmt.Sprintf("select subscriber: %s", selectedClientId), "topic", topicFilter)
	return selectedClientId, nil
}

// StartRecording writes client status to csv file every second.
func (m *Manager) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// write client status to csv file every second
	fileName := filepath.Join(m.dirName, "client_status.csv")
	if err := m.clients.Recording(ctx, fileName); err != nil {
		m.log.Error("failed to write csv", err)
		return err
	}
	return nil
}

// SimpleAlgorithm selects a subscriber based on the number of messages in the queue and the processing time per message.
type SimpleAlgorithm struct {
	*simpleSelector
	*simpleUpdater
}

func NewSimpleAlgorithm() *SimpleAlgorithm {
	return &SimpleAlgorithm{}
}

type simpleUpdater struct{}

func (su *simpleUpdater) updateClientInfoWithPayload(cl *Client, p Payload) error {
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	if len(cl.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}

	// update client info
	cl.receivedPayload = append(cl.receivedPayload, p)
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
	cl.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.receivedPayload))
	return nil
}

func (su *simpleUpdater) updateClientInfoAfterSending(cl *Client) error {
	cl.sentMessageCount++
	return nil
}

type simpleSelector struct{}

func (su *simpleSelector) selectClientToSend(topicFilter string, clients []*Client, _ float64) (*Client, error) {
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

// RandomAlgorithm selects a subscriber randomly.
type RandomAlgorithm struct {
	*randomSelector
	*simpleUpdater
}

func NewRandomAlgorithm() *RandomAlgorithm {
	return &RandomAlgorithm{}
}

type randomSelector struct{}

func (rs *randomSelector) selectClientToSend(topicFilter string, clients []*Client, _ float64) (*Client, error) {
	if len(clients) == 0 {
		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}
	return clients[rand.Intn(len(clients))], nil
}

// ScoreAlgorithm selects a subscriber based on the score.
// The score is calculated by the number of messages in the queue and the processing time per message.
type ScoreAlgorithm struct {
	*ScoreSelector
	*ScoreUpdater
}

func NewScoreAlgorithm() *ScoreAlgorithm {
	return &ScoreAlgorithm{}
}

type ScoreUpdater struct{}

func (su *ScoreUpdater) updateClientInfoWithPayload(cl *Client, p Payload) error {
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	if len(cl.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}

	// update client info
	cl.receivedPayload = append(cl.receivedPayload, p)
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue
	cl.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.receivedPayload))

	cl.numberOfMessagesInProgress = 0             // reset number of messages in progress
	cl.lastUpdateTimeNano = time.Now().UnixNano() // update last update time
	return nil
}

func (su *ScoreUpdater) updateClientInfoAfterSending(cl *Client) error {
	cl.sentMessageCount++
	cl.numberOfMessagesInProgress++
	cl.lastSentTimeNano = time.Now().UnixNano() // update last sent time
	return nil
}

type ScoreSelector struct{}

func (ss *ScoreSelector) selectClientToSend(topicFilter string, clients []*Client, groupAvgProcessingTime float64) (*Client, error) {
	var selectedClient *Client
	maxScore := int64(-math.MaxInt64)

	now := time.Now().UnixNano()
	for _, cl := range clients {
		// convert processing time (ms) to processing time (ns)
		var tp int64
		if cl.avgProcessingTimePerMsg == 0 {
			tp = int64(groupAvgProcessingTime * 1000000.0)
		} else {
			tp = int64(cl.avgProcessingTimePerMsg * 1000000.0)
		}
		// calculate score
		t1 := now - cl.lastSentTimeNano                             // time since last sent
		t2 := now - cl.lastUpdateTimeNano                           // time since last update
		m := cl.numberOfMsgsInQueue + cl.numberOfMessagesInProgress // number of messages in client queue
		processingTimeRequired := m*tp - t2
		if processingTimeRequired < 0 {
			processingTimeRequired = 0
		}
		score := t1 - processingTimeRequired
		// select client with the highest score
		if score > maxScore {
			selectedClient = cl
			maxScore = score
		}
	}

	if selectedClient == nil {
		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}
	return selectedClient, nil
}
