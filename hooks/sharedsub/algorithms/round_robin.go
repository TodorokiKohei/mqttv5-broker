package algorithms

import (
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2/packets"
	"math"
	"strconv"
	"sync"
	"time"
)

type RoundRobinAlgorithm struct {
	sync.RWMutex
	clients map[string]*roundRobinClient
}

type roundRobinClient struct {
	sync.RWMutex
	id string

	lastSentTimeNano int64
}

func NewRoundRobinAlgorithm() *RoundRobinAlgorithm {
	return &RoundRobinAlgorithm{
		clients: make(map[string]*roundRobinClient),
	}
}

func (rra *RoundRobinAlgorithm) AddClient(clientID string, _ packets.Packet) {
	rra.Lock()
	defer rra.Unlock()
	rra.clients[clientID] = &roundRobinClient{
		id:               clientID,
		lastSentTimeNano: time.Now().UnixNano(),
	}
}

func (rra *RoundRobinAlgorithm) RemoveClient(id string) {
	rra.Lock()
	defer rra.Unlock()
	delete(rra.clients, id)
}

func (rra *RoundRobinAlgorithm) UpdateClientWithPingreq(_ string, _ packets.Packet) error {
	return nil
}

func (rra *RoundRobinAlgorithm) SelectClientToSend(topicFilter string, groupSubs map[string]packets.Subscription) (string, error) {
	rra.RLock()
	defer rra.RUnlock()

	var selClient *roundRobinClient
	minTime := int64(math.MaxInt64)
	for clientID, _ := range groupSubs {
		cl, ok := rra.clients[clientID]
		if !ok {
			continue
		}
		if cl.lastSentTimeNano < minTime {
			minTime = cl.lastSentTimeNano
			selClient = cl
		}

	}
	if selClient == nil {
		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	selClient.Lock()
	defer selClient.Unlock()
	selClient.lastSentTimeNano = time.Now().UnixNano()

	return selClient.id, nil
}

func (rra *RoundRobinAlgorithm) GetCsvHeader() []string {
	return []string{
		"clientID",
		"time",
		"sentMessageCount",
		"numberOfMsgsInQueue",
		"avgProcessingTimePerMsg",
	}
}

func (rra *RoundRobinAlgorithm) GetCsvRows() [][]string {
	rra.RLock()
	defer rra.RUnlock()

	var rows [][]string
	for id, _ := range rra.clients {
		rows = append(rows, []string{
			id,
			strconv.FormatInt(time.Now().Unix(), 10),
			"0",
			"0",
			"0",
		})
	}
	return rows
}
