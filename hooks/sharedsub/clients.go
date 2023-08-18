package sharedsub

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/mochi-mqtt/server/v2/packets"
	"os"
	"strconv"
	"sync"
	"time"
)

type selectFn func(string, []*Client, float64) (*Client, error)
type updateFnWithPayload func(*Client, Payload) error
type updateFnAfterSend func(*Client) error

type Clients struct {
	sync.RWMutex
	clients map[string]*Client
}

func NewClients() *Clients {
	return &Clients{
		clients: make(map[string]*Client),
	}
}

// AddClient adds a new client to the map
func (cls *Clients) AddClient(clientId string) {
	cls.Lock()
	defer cls.Unlock()
	cls.clients[clientId] = NewClient(clientId)
}

// DeleteClient deletes a client from the map
func (cls *Clients) DeleteClient(clientId string) {
	cls.Lock()
	defer cls.Unlock()
	delete(cls.clients, clientId)
}

func (cls *Clients) UpdateClientInfoWithPayload(
	clientId string,
	p Payload,
	updateFn updateFnWithPayload,
) error {
	cls.Lock()
	defer cls.Unlock()
	cl, ok := cls.clients[clientId]
	if !ok {
		return NotFountError{msg: fmt.Sprintf("client %s not found", clientId)}
	}

	cl.Lock()
	defer cl.Unlock()
	err := updateFn(cl, p)
	if err != nil {
		return err
	}
	return nil
}

func (cls *Clients) SelectClientToSend(
	topicFiter string,
	groupSubs map[string]packets.Subscription,
	selectFn selectFn,
	updateFn updateFnAfterSend,
) (string, error) {
	cls.RLock()
	defer cls.RUnlock()

	// select clients to send
	groupAvgProcessingTimePerMsg := 0.0 // average processing time per message in the group
	clients := make([]*Client, 0, len(cls.clients))
	for clientId, _ := range groupSubs {
		cl, ok := cls.clients[clientId]
		if !ok {
			continue
		}
		clients = append(clients, cl)
		if cl.avgProcessingTimePerMsg != 0 {
			groupAvgProcessingTimePerMsg += cl.avgProcessingTimePerMsg
		}
	}
	groupAvgProcessingTimePerMsg /= float64(len(clients))
	selectedClient, err := selectFn(topicFiter, clients, groupAvgProcessingTimePerMsg)
	if err != nil {
		return "", err
	}

	// update client info
	selectedClient.Lock()
	defer selectedClient.Unlock()
	err = updateFn(selectedClient)
	if err != nil {
		return "", err
	}

	return selectedClient.id, nil
}

func (cls *Clients) Recording(ctx context.Context, fileName string) error {
	// create csv file to write client status
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write csv header
	if err := writer.Write(GetCsvHeader()); err != nil {
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
			for clientId, cl := range cls.clients {
				if err := writer.Write(cl.GetCsvRecord(clientId)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type Client struct {
	sync.RWMutex
	id string

	receivedPayload            []Payload
	numberOfMsgsInQueue        int64
	avgProcessingTimePerMsg    float64 // milliseconds
	sentMessageCount           int64
	numberOfMessagesInProgress int64
	lastSentTimeNano           int64
	lastUpdateTimeNano         int64
}

func NewClient(clientId string) *Client {
	return &Client{
		id:                      clientId,
		receivedPayload:         make([]Payload, 0, defaultRetainedSize),
		numberOfMsgsInQueue:     0,
		avgProcessingTimePerMsg: 0, // milliseconds
		sentMessageCount:        0,
	}
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

type NotFountError struct {
	msg string
}

func (e NotFountError) Error() string {
	return e.msg
}
