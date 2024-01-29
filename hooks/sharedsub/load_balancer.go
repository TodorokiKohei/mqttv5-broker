package sharedsub

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Algorithm interface {
	AddClient(string, packets.Packet)
	RemoveClient(string)
	UpdateClientWithPingreq(string, packets.Packet) error
	GetCsvHeader() []string
	GetCsvRows() [][]string

	SelectClientToSend(string, map[string]packets.Subscription) (string, error)
}

type LoadBalancer struct {
	algorithm Algorithm
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

func (lb *LoadBalancer) AddClient(cl *mqtt.Client, pk packets.Packet) {
	// add client if id contains "sub"
	if isSub := strings.Contains(strings.ToUpper(cl.ID), "SUB"); isSub {
		lb.algorithm.AddClient(cl.ID, pk)
		lb.log.Info("create new client", "method", "CreateClient", "clientID", cl.ID)
	}
}

func (lb *LoadBalancer) RemoveClient(cl *mqtt.Client) {
	// remove client if id contains "sub"
	if isSub := strings.Contains(strings.ToUpper(cl.ID), "SUB"); isSub {
		lb.algorithm.RemoveClient(cl.ID)
		lb.log.Info("remove client", "method", "DeleteClient", "clientID", cl.ID)
	}
}

func (lb *LoadBalancer) UpdateClientWithPingreq(cl *mqtt.Client, pk packets.Packet) error {
	err := lb.algorithm.UpdateClientWithPingreq(cl.ID, pk)
	if err != nil {
		return err
	}
	//lb.log.Info("update client with PINGREQ",
	//	"method", "UpdateClientWithPingreq", "clientID", cl.ID,
	//	"NumberOfMsgsInQueue", p.NumberOfMsgsInQueue, "ProcessingTimePerMsg", p.ProcessingTimePerMsg, "now", time.Now().UnixNano())
	return nil
}

func (lb *LoadBalancer) SelectSharedSubscriptionSubscriber(shared map[string]map[string]packets.Subscription, _ packets.Packet) (map[string]packets.Subscription, error) {
	sharedSelected := map[string]packets.Subscription{}

	// Select a subscriber for each shared subscription topic.
	for topicFilter, groupSubs := range shared {

		// Select subscriber by the Load Balancer.
		selClientId, err := lb.algorithm.SelectClientToSend(topicFilter, groupSubs)
		if err != nil {
			lb.log.Error("An error occurred in the algorithm for selecting subscribers for shared subscriptions.",
				"method", "OnSelectSubscribers")
			return nil, err
		}

		// Update subscription.
		// If the same subscriber is matched, the QoS delivered to the subscriber is adjusted to the value
		// of the largest QoS among the matched topics.
		oldSub, ok := sharedSelected[selClientId]
		if !ok {
			oldSub = groupSubs[selClientId]
		}
		sharedSelected[selClientId] = oldSub.Merge(groupSubs[selClientId])
		lb.log.Info("select client", "method", "OnSelectSubscribers", "subscriber", selClientId)
	}

	return sharedSelected, nil
}

// StartRecording writes client status to csv file every second.
func (lb *LoadBalancer) StartRecording(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	// write client status to csv file every second
	fileName := filepath.Join(lb.dirName, "client_status.csv")
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	defer file.Close()
	defer writer.Flush()

	// write header
	err = writer.Write(lb.algorithm.GetCsvHeader())
	if err != nil {
		return err
	}

	done := false
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case <-time.After(time.Second):
			// write client status to csv file
			for _, row := range lb.algorithm.GetCsvRows() {
				err = writer.Write(row)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

//
//// RandomAlgorithm selects a subscriber randomly.
//type RandomAlgorithm struct {
//	*randomSelector
//	*simpleUpdater
//}
//
//func NewRandomAlgorithm() *RandomAlgorithm {
//	return &RandomAlgorithm{
//		randomSelector: &randomSelector{},
//		simpleUpdater:  &simpleUpdater{},
//	}
//}
//
//type simpleUpdater struct{}
//
//func (su *simpleUpdater) UpdateClientWithPingreq(cl *Client, p Payload) error {
//	// update client info
//	totalProcessingTime := cl.AvgProcessingTimePerMsg * float64(len(cl.ReceivedPayload))
//	cl.ReceivedPayload = append(cl.ReceivedPayload, p)
//	if len(cl.ReceivedPayload) == defaultRetainedSize+1 {
//		// Remove old information.
//		totalProcessingTime -= cl.ReceivedPayload[0].ProcessingTimePerMsg
//		cl.ReceivedPayload = cl.ReceivedPayload[1:]
//	}
//	cl.AvgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.ReceivedPayload))
//	cl.NumberOfMsgsInQueue = p.NumberOfMsgsInQueue
//
//	cl.NumberOfMessagesInProgress = 0
//	cl.LastUpdateTimeNano = time.Now().UnixNano()
//	return nil
//}
//
//func (su *simpleUpdater) UpdateClientAfterSending(cl *Client) error {
//	cl.SentMessageCount++
//	cl.NumberOfMessagesInProgress++
//	cl.LastSentTimeNano = time.Now().UnixNano()
//	return nil
//}
//
//type randomSelector struct{}
//
//func (rs *randomSelector) SelectClientToSend(topicFilter string, clients []*Client, _ float64) (*Client, error) {
//	if len(clients) == 0 {
//		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
//	}
//	return clients[rand.Intn(len(clients))], nil
//}
//
//// ScoreAlgorithm selects a subscriber based on the score.
//// The score is calculated by the number of messages in the queue and the processing time per message.
//type ScoreAlgorithm struct {
//	*ScoreSelector
//	*ScoreUpdater
//}
//
//func NewScoreAlgorithm(log *slog.Logger) *ScoreAlgorithm {
//	return &ScoreAlgorithm{
//		ScoreUpdater:  &ScoreUpdater{},
//		ScoreSelector: &ScoreSelector{log: log},
//	}
//}
//
//type ScoreUpdater struct{}
//
//func (su *ScoreUpdater) UpdateClientWithPingreq(cl *Client, p Payload) error {
//	// update client info
//	totalProcessingTime := cl.AvgProcessingTimePerMsg * float64(len(cl.ReceivedPayload))
//	cl.ReceivedPayload = append(cl.ReceivedPayload, p)
//	if len(cl.ReceivedPayload) == defaultRetainedSize+1 {
//		// Remove old information.
//		totalProcessingTime -= cl.ReceivedPayload[0].ProcessingTimePerMsg
//		cl.ReceivedPayload = cl.ReceivedPayload[1:]
//	}
//	cl.AvgProcessingTimePerMsg = (totalProcessingTime + p.ProcessingTimePerMsg) / float64(len(cl.ReceivedPayload))
//	cl.NumberOfMsgsInQueue = p.NumberOfMsgsInQueue
//
//	cl.NumberOfMessagesInProgress = 0             // reset number of messages in progress
//	cl.LastUpdateTimeNano = time.Now().UnixNano() // update last update time
//	return nil
//}
//
//func (su *ScoreUpdater) UpdateClientAfterSending(cl *Client) error {
//	cl.SentMessageCount++
//	cl.NumberOfMessagesInProgress++
//	cl.LastSentTimeNano = time.Now().UnixNano() // update last sent time
//	return nil
//}
//
//type ScoreSelector struct {
//	log *slog.Logger
//}
//
//func (ss *ScoreSelector) SelectClientToSend(topicFilter string, clients []*Client, groupAvgProcessingTime float64) (*Client, error) {
//	var selectedClient *Client
//	maxScore := int64(-math.MaxInt64)
//
//	now := time.Now().UnixNano()
//	for _, cl := range clients {
//		// convert processing time (ms) to processing time (ns)
//		var tp int64
//		if cl.AvgProcessingTimePerMsg == 0 {
//			tp = int64(groupAvgProcessingTime * 1000000.0)
//		} else {
//			tp = int64(cl.AvgProcessingTimePerMsg * 1000000.0)
//		}
//		// calculate score
//		t1 := now - cl.LastSentTimeNano                             // time since last sent
//		t2 := now - cl.LastUpdateTimeNano                           // time since last update
//		m := cl.NumberOfMsgsInQueue + cl.NumberOfMessagesInProgress // number of messages in client queue
//		processingTimeRequired := m*tp - t2
//		if processingTimeRequired < 0 {
//			processingTimeRequired = 0
//		}
//		score := t1 - processingTimeRequired
//		// select client with the highest score
//		if score > maxScore {
//			selectedClient = cl
//			maxScore = score
//		}
//		ss.log.Debug("calculate score", "method", "SelectClientToSend", "kind", "calcScore", "clientID", cl.id, "score", score, "now", now,
//			"t1", t1, "t2", t2, "m", m, "tp", tp, "m*tp-t2", processingTimeRequired)
//	}
//
//	if selectedClient == nil {
//		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
//	}
//	return selectedClient, nil
//}
//
//type RoundRobinAlgorithm struct {
//	*simpleUpdater
//	*roundRobinSelector
//}
//
//func NewRoundRobinAlgorithm() *RoundRobinAlgorithm {
//	return &RoundRobinAlgorithm{
//		simpleUpdater:      &simpleUpdater{},
//		roundRobinSelector: &roundRobinSelector{},
//	}
//}
//
//type roundRobinSelector struct{}
//
//func (rrs *roundRobinSelector) SelectClientToSend(topicFilter string, clients []*Client, _ float64) (*Client, error) {
//
//	// select the client with the longest elapsed time since the message was sent
//	var selectedClient *Client
//	for _, cl := range clients {
//		if selectedClient == nil {
//			selectedClient = cl
//		} else if cl.LastSentTimeNano < selectedClient.LastSentTimeNano {
//			selectedClient = cl
//		}
//	}
//	if selectedClient == nil {
//		return nil, errors.New(fmt.Sprintf("client not found: %s", topicFilter))
//	}
//
//	return selectedClient, nil
//}
