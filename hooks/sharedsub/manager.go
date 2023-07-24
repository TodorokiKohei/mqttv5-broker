package sharedsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"
	"math"
	"sync"
)

// Manager manage information received from pingreq.
type Manager interface {
	UpdateClient(cl *mqtt.Client)
	UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error
	DeleteClient(cl *mqtt.Client)
	SelectSubscriber(topicFilter string, groupSubs map[string]packets.Subscription, pk packets.Packet) (string, error)
}

const (
	defaultRetainedSize = 10
)

// The StatusManager maintains the number of messages in the queue and the processing time per message for each client.
type StatusManager struct {
	sync.RWMutex
	clients map[string]*Client
}

// Payload is message format of pingreq sent by client.
type Payload struct {
	NumberOfMsgsInQueue  int64   `json:"numberOfMsgsInQueue"`
	ProcessingTimePerMsg float64 `json:"processingTimerPerMsg"`
}

type Client struct {
	sync.RWMutex
	receivedPayload         []Payload
	avgNumberOfMsgsInQueue  float64
	avgProcessingTimePerMsg float64
	sentMessageCount        int64
}

func NewManager() *StatusManager {
	sm := &StatusManager{
		clients: make(map[string]*Client),
	}
	return sm
}

func NewClient() *Client {
	return &Client{
		receivedPayload:         make([]Payload, 0, defaultRetainedSize),
		avgNumberOfMsgsInQueue:  0,
		avgProcessingTimePerMsg: 0,
		sentMessageCount:        0,
	}
}

func (m *StatusManager) UpdateClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	m.clients[cl.ID] = NewClient()
}

func (m *StatusManager) DeleteClient(cl *mqtt.Client) {
	m.Lock()
	defer m.Unlock()
	delete(m.clients, cl.ID)
}

func (m *StatusManager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	client, ok := m.clients[cl.ID]
	if !ok {
		return errors.New("client is not found")
	}
	client.Lock()
	defer client.Unlock()

	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	// Update value using subscriber selection with pingreq.
	totalMsgInQueue := client.avgNumberOfMsgsInQueue * float64(len(client.receivedPayload))
	totalProcessingTime := client.avgProcessingTimePerMsg * float64(len(client.receivedPayload))
	if len(client.receivedPayload) == defaultRetainedSize {
		// Remove old information.
		totalProcessingTime -= client.receivedPayload[0].ProcessingTimePerMsg
		totalMsgInQueue -= float64(client.receivedPayload[0].NumberOfMsgsInQueue)
		client.receivedPayload = client.receivedPayload[1:]
	}
	client.receivedPayload = append(client.receivedPayload, p)
	client.avgNumberOfMsgsInQueue = (totalMsgInQueue + float64(p.NumberOfMsgsInQueue)) / float64(len(client.receivedPayload))
	client.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(client.receivedPayload))
	client.sentMessageCount = 0
	return nil
}

func (m *StatusManager) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	pk packets.Packet,
) (string, error) {
	var selectedClientId string
	minMsgsInQueue := int64(math.MaxInt64)
	minProcessingTime := math.MaxFloat64

	for clientId, _ := range groupSubs {
		cl, ok := m.clients[clientId]
		if !ok {
			continue
		}
		cl.RLock()
		numMsgsInQueue := cl.sentMessageCount
		if len(cl.receivedPayload) != 0 {
			numMsgsInQueue += cl.receivedPayload[len(cl.receivedPayload)-1].NumberOfMsgsInQueue
		}
		processingTime := cl.avgProcessingTimePerMsg
		cl.RUnlock()

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

	return selectedClientId, nil
}
