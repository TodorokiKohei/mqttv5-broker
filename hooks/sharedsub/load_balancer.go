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
	"os"
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

type LoadBalancer struct {
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

func NewLoadBalancer(options Options) *LoadBalancer {
	lb := &LoadBalancer{
		clients:   NewClients(),
		algorithm: options.Algorithm,
		log:       options.Log,
		dirName:   options.DirName,
	}

	if lb.log == nil {
		lb.log = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	lb.log.Info("Create LoadBalancer", "Algorithm", fmt.Sprintf("%#v\n", options.Algorithm))
	return lb
}

// CreateClient creates new client.
func (lb *LoadBalancer) CreateClient(cl *mqtt.Client) {
	lb.clients.AddClient(cl.ID)
	lb.log.Info("create new client", "method", "CreateClient", "clientID", cl.ID)
}

// DeleteClient deletes disconnected client.
func (lb *LoadBalancer) DeleteClient(cl *mqtt.Client) {
	lb.clients.DeleteClient(cl.ID)
	lb.log.Info("delete client", "method", "DeleteClient", "clientID", cl.ID)
}

func (lb *LoadBalancer) UpdateClientWithPingreq(cl *mqtt.Client, pk packets.Packet) error {
	p := Payload{}
	err := json.Unmarshal(pk.Payload, &p)
	if err != nil {
		return err
	}

	err = lb.clients.UpdateClientWithPingreq(cl.ID, p, lb.algorithm)
	if err != nil {
		return err
	}
	lb.log.Info("update client with PINGREQ",
		"method", "UpdateClientWithPingreq", "clientID", cl.ID,
		"NumberOfMsgsInQueue", p.NumberOfMsgsInQueue, "ProcessingTimePerMsg", p.ProcessingTimePerMsg, "now", time.Now().UnixNano())
	return nil
}

func (lb *LoadBalancer) SelectSubscriber(
	topicFilter string,
	groupSubs map[string]packets.Subscription,
	pk packets.Packet,
) (string, error) {

	selectedClientId, err := lb.clients.SelectClientToSend(
		topicFilter,
		groupSubs,
		lb.algorithm,
	)

	if err != nil {
		lb.log.Error(fmt.Sprintf("failed to select subscriber: %s", topicFilter))
		return "", err
	}

	lb.log.Info(fmt.Sprintf("select subscriber: %s", selectedClientId), "topic", topicFilter)
	return selectedClientId, nil
}

// StartRecording writes client status to csv file every second.
func (lb *LoadBalancer) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// write client status to csv file every second
	fileName := filepath.Join(lb.dirName, "client_status.csv")
	if err := lb.clients.Recording(ctx, fileName); err != nil {
		lb.log.Error("failed to write csv", err)
		return err
	}
	return nil
}

// RandomAlgorithm selects a subscriber randomly.
type RandomAlgorithm struct {
	*randomSelector
	*simpleUpdater
}

func NewRandomAlgorithm() *RandomAlgorithm {
	return &RandomAlgorithm{
		randomSelector: &randomSelector{},
		simpleUpdater:  &simpleUpdater{},
	}
}

type simpleUpdater struct{}

func (su *simpleUpdater) updateClientInfoWithPayload(cl *Client, p Payload) error {
	// update client info
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	cl.receivedPayload = append(cl.receivedPayload, p)
	if len(cl.receivedPayload) == defaultRetainedSize+1 {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}
	cl.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.receivedPayload))
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue

	cl.numberOfMessagesInProgress = 0
	cl.lastUpdateTimeNano = time.Now().UnixNano()
	return nil
}

func (su *simpleUpdater) updateClientInfoAfterSending(cl *Client) error {
	cl.sentMessageCount++
	cl.numberOfMessagesInProgress++
	cl.lastSentTimeNano = time.Now().UnixNano()
	return nil
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

func NewScoreAlgorithm(log *slog.Logger) *ScoreAlgorithm {
	return &ScoreAlgorithm{
		ScoreUpdater:  &ScoreUpdater{},
		ScoreSelector: &ScoreSelector{log: log},
	}
}

type ScoreUpdater struct{}

func (su *ScoreUpdater) updateClientInfoWithPayload(cl *Client, p Payload) error {
	// update client info
	totalProcessingTime := cl.avgProcessingTimePerMsg * float64(len(cl.receivedPayload))
	cl.receivedPayload = append(cl.receivedPayload, p)
	if len(cl.receivedPayload) == defaultRetainedSize+1 {
		// Remove old information.
		totalProcessingTime -= cl.receivedPayload[0].ProcessingTimePerMsg
		cl.receivedPayload = cl.receivedPayload[1:]
	}
	cl.avgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.receivedPayload))
	cl.numberOfMsgsInQueue = p.NumberOfMsgsInQueue

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

type ScoreSelector struct {
	log *slog.Logger
}

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
		ss.log.Debug("calculate score", "method", "selectClientToSend", "kind", "calcScore", "clientID", cl.id, "score", score, "now", now,
			"t1", t1, "t2", t2, "m", m, "tp", tp, "m*tp-t2", processingTimeRequired)
	}

	if selectedClient == nil {
		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}
	return selectedClient, nil
}

type RoundRobinAlgorithm struct {
	*simpleUpdater
	*roundRobinSelector
}

func NewRoundRobinAlgorithm() *RoundRobinAlgorithm {
	return &RoundRobinAlgorithm{
		simpleUpdater:      &simpleUpdater{},
		roundRobinSelector: &roundRobinSelector{},
	}
}

type roundRobinSelector struct{}

func (rrs *roundRobinSelector) selectClientToSend(topicFilter string, clients []*Client, _ float64) (*Client, error) {

	// select the client with the longest elapsed time since the message was sent
	var selectedClient *Client
	for _, cl := range clients {
		if selectedClient == nil {
			selectedClient = cl
		} else if cl.lastSentTimeNano < selectedClient.lastSentTimeNano {
			selectedClient = cl
		}
	}
	if selectedClient == nil {
		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	return selectedClient, nil
}
