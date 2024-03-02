package algorithms

import (
	"errors"
	"fmt"
	"github.com/mochi-mqtt/server/v2/packets"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// RandomAlgorithm selects a subscriber randomly.
type RandomAlgorithm struct {
	sync.RWMutex
	clients map[string]*randomClient
}

type randomClient struct {
	sync.RWMutex
	id string
}

func NewRandomAlgorithm() *RandomAlgorithm {
	return &RandomAlgorithm{
		clients: make(map[string]*randomClient),
	}
}

func (ra *RandomAlgorithm) AddClient(clientID string, _ packets.Packet) {
	ra.Lock()
	defer ra.Unlock()
	ra.clients[clientID] = &randomClient{
		id: clientID,
	}
}

func (ra *RandomAlgorithm) RemoveClient(id string) {
	ra.Lock()
	defer ra.Unlock()
	delete(ra.clients, id)
}

func (ra *RandomAlgorithm) UpdateClientWithPingreq(_ string, _ packets.Packet) error {
	return nil
}

func (ra *RandomAlgorithm) SelectClientToSend(topicFilter string, groupSubs map[string]packets.Subscription) (string, error) {
	ra.RLock()
	defer ra.RUnlock()

	targetIDs := make([]string, 0)
	for id, _ := range groupSubs {
		targetIDs = append(targetIDs, id)
	}
	if len(targetIDs) == 0 {
		return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
	}

	selID := targetIDs[rand.Intn(len(targetIDs))]
	return selID, nil
}

func (ra *RandomAlgorithm) GetCsvHeader() []string {
	return []string{
		"clientID",
		"time",
		"sentMessageCount",
		"numberOfMsgsInQueue",
		"avgProcessingTimePerMsg",
	}
}

func (ra *RandomAlgorithm) GetCsvRows() [][]string {
	ra.RLock()
	defer ra.RUnlock()

	var rows [][]string
	for id, _ := range ra.clients {
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
