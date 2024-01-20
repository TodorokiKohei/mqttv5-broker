package algorithms

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2/packets"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"
)

const (
	defaultRetainedSize = 5
)

type Payload struct {
	NumberOfMsgsInQueue  int64   `json:"numberOfMsgsInQueue"`
	ProcessingTimePerMsg float64 `json:"processingTimerPerMsg"` // milliseconds
}

type ScoreAlgorithm struct {
	sync.RWMutex
	log     *slog.Logger
	clients map[string]*scoreClient
}

type scoreClient struct {
	sync.RWMutex
	id string

	receivedPayload         []Payload
	avgProcessingTimePerMsg float64
	numberOfMsgsInQueue     int64

	numberOfMsgsInProgress int64
	lastUpdateTimeNano     int64
	lastSentTimeNano       int64
	sentMessageCount       int64
}

func NewScoreAlgorithm(log *slog.Logger) *ScoreAlgorithm {
	return &ScoreAlgorithm{
		log:     log,
		clients: make(map[string]*scoreClient),
	}
}

func (sa *ScoreAlgorithm) AddClient(clientID string, _ packets.Packet) {
	sa.Lock()
	defer sa.Unlock()
	sa.clients[clientID] = &scoreClient{
		id:                      clientID,
		receivedPayload:         make([]Payload, 0, defaultRetainedSize),
		avgProcessingTimePerMsg: 0,
		numberOfMsgsInQueue:     0,
		numberOfMsgsInProgress:  0,
		lastUpdateTimeNano:      time.Now().UnixNano(),
		lastSentTimeNano:        time.Now().UnixNano(),
		sentMessageCount:        0,
	}
}

func (sa *ScoreAlgorithm) RemoveClient(clientID string) {
	sa.Lock()
	defer sa.Unlock()
	delete(sa.clients, clientID)
}

func (sa *ScoreAlgorithm) UpdateClientWithPingreq(clientID string, pk packets.Packet) error {
	// decode payload
	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	sa.RLock()
	defer sa.RUnlock()

	cl := sa.clients[clientID]
	cl.Lock()
	defer cl.Unlock()

	// update client with PINGREQ payload.
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	totalProcessingTime += p.ProcessingTimePerMsg
	cl.receivedPayload = append(cl.receivedPayload, p)
	if len(cl.receivedPayload) == defaultRetainedSize+1 {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}
	cl.avgProcessingTimePerMsg = totalProcessingTime / float64(len(cl.receivedPayload))
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue

	cl.numberOfMsgsInProgress = 0                 // reset number of messages in progress
	cl.lastUpdateTimeNano = time.Now().UnixNano() // update last update time
	return nil
}

func (sa *ScoreAlgorithm) SelectClientToSend(topicFilter string, groupSubs map[string]packets.Subscription) (string, error) {
	sa.RLock()
	defer sa.RUnlock()

	// calculate average processing time of the group
	var groupAvgProcessingTime float64
	var targetClients []*scoreClient
	for clientID, _ := range groupSubs {
		cl, ok := sa.clients[clientID]
		if !ok {
			continue
		}
		groupAvgProcessingTime += cl.avgProcessingTimePerMsg
		targetClients = append(targetClients, cl)
	}
	groupAvgProcessingTime /= float64(len(targetClients))

	// select client with the highest score
	var selClient *scoreClient
	maxScore := int64(-math.MaxInt64)
	now := time.Now().UnixNano()
	for _, cl := range targetClients {
		// convert processing time (ms) to processing time (ns)
		var tp int64
		if cl.avgProcessingTimePerMsg == 0 {
			tp = int64(groupAvgProcessingTime * 1000000.0)
		} else {
			tp = int64(cl.avgProcessingTimePerMsg * 1000000.0)
		}
		// calculate score
		t1 := now - cl.lastSentTimeNano                         // time since last sent
		t2 := now - cl.lastUpdateTimeNano                       // time since last update
		m := cl.numberOfMsgsInQueue + cl.numberOfMsgsInProgress // number of messages in client queue
		processingTimeRequired := m*tp - t2
		if processingTimeRequired < 0 {
			processingTimeRequired = 0
		}
		score := t1 - processingTimeRequired

		// select client with the highest score
		if score > maxScore {
			selClient = cl
			maxScore = score
		}

		sa.log.Info(fmt.Sprintf("The %s's score is %d", cl.id, score), "method", "SelectClientToSend")
	}

	if selClient == nil {
		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	// update client
	selClient.Lock()
	defer selClient.Unlock()
	selClient.sentMessageCount++
	selClient.numberOfMsgsInProgress++
	selClient.lastSentTimeNano = time.Now().UnixNano() // update last sent time

	return selClient.id, nil
}

func (sa *ScoreAlgorithm) GetCsvHeader() []string {
	return []string{
		"clientID",
		"time",
		"sentMessageCount",
		"numberOfMsgsInQueue",
		"avgProcessingTimePerMsg",
	}
}

func (sa *ScoreAlgorithm) GetCsvRows() [][]string {
	sa.RLock()
	defer sa.RUnlock()

	var rows [][]string
	for id, cl := range sa.clients {
		rows = append(rows, []string{
			id,
			strconv.FormatInt(time.Now().Unix(), 10),
			strconv.FormatInt(cl.sentMessageCount, 10),
			strconv.FormatInt(cl.numberOfMsgsInQueue, 10),
			strconv.FormatFloat(cl.avgProcessingTimePerMsg, 'f', -1, 64),
		})
	}
	return rows
}
