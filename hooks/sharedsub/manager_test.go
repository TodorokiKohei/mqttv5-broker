package sharedsub

import (
	"fmt"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

var logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.Disabled)

func TestUpdateClient(t *testing.T) {
	sm := NewManager(&logger)
	cl := &mqtt.Client{
		ID: "test-client",
	}
	sm.UpdateClient(cl)
	_, ok := sm.clients[cl.ID]
	require.True(t, ok)
}

func TestDeleteClient(t *testing.T) {
	sm := NewManager(&logger)
	cl := &mqtt.Client{
		ID: "test-client",
	}
	sm.UpdateClient(cl)
	_, ok := sm.clients[cl.ID]
	require.True(t, ok)
	sm.DeleteClient(cl)
	_, ok = sm.clients[cl.ID]
	require.False(t, ok)
}

func TestUpdateClientInfoValid(t *testing.T) {
	sm := NewManager(&logger)

	cl := &mqtt.Client{
		ID: "two-message",
	}
	sm.UpdateClient(cl)

	// Receive two pingreq
	pk := createPingreq(t, 1, 0.1)
	err := sm.UpdateClientInfo(cl, pk)
	require.NoError(t, err)
	require.True(t, math.Abs(sm.clients[cl.ID].avgNumberOfMsgsInQueue-1.0) < 0.00001)
	require.True(t, math.Abs(sm.clients[cl.ID].avgProcessingTimePerMsg-0.1) < 0.00001)
	pk = createPingreq(t, 3, 0.3)
	err = sm.UpdateClientInfo(cl, pk)
	require.NoError(t, err)
	require.True(t, math.Abs(sm.clients[cl.ID].avgNumberOfMsgsInQueue-2.0) < 0.00001)
	require.True(t, math.Abs(sm.clients[cl.ID].avgProcessingTimePerMsg-0.2) < 0.00001)

	// Receive pingreq larger than defaultRetainSize
	cl = &mqtt.Client{
		ID: "over-message",
	}
	sm.UpdateClient(cl)
	overSize := 5
	for i := 1; i <= defaultRetainedSize+overSize; i++ {
		err = sm.UpdateClientInfo(cl, createPingreq(t, i, float64(i)*0.1))
		require.NoError(t, err)
	}
	var sumAvgNumberOfMsgsInQueue, sumAvgProcessingTimePerMsg float64
	for i := 1 + overSize; i <= defaultRetainedSize+overSize; i++ {
		sumAvgNumberOfMsgsInQueue += float64(i)
		sumAvgProcessingTimePerMsg += float64(i) * 0.1
	}
	want := sumAvgNumberOfMsgsInQueue / defaultRetainedSize
	require.True(t, math.Abs(sm.clients[cl.ID].avgNumberOfMsgsInQueue-want) < 0.000001)
	want = sumAvgProcessingTimePerMsg / defaultRetainedSize
	require.True(t, math.Abs(sm.clients[cl.ID].avgProcessingTimePerMsg-want) < 0.000001)
}

func TestUpdateClientInfoInvalid(t *testing.T) {
	sm := NewManager(&logger)

	cl := &mqtt.Client{
		ID: "valid-client",
	}
	sm.UpdateClient(cl)

	// Receive invalid payload
	pk := createPingreq(t, 1, 0.1)
	pk.Payload = []byte("Hello World")
	err := sm.UpdateClientInfo(cl, pk)
	require.Error(t, err)

	// Non-existent client
	cl = &mqtt.Client{
		ID: "invalid-client",
	}
	pk = createPingreq(t, 1, 1)
	err = sm.UpdateClientInfo(cl, pk)
	require.Error(t, err)
}

func TestSelectSubscriber(t *testing.T) {
	type sub struct {
		cl *mqtt.Client
		pk packets.Packet
	}
	subs := []sub{
		{
			cl: &mqtt.Client{ID: "client-1"},
			pk: createPingreq(t, 1, 2),
		},
		{
			cl: &mqtt.Client{ID: "client-2"},
			pk: createPingreq(t, 2, 1),
		},
		{
			cl: &mqtt.Client{ID: "client-3"},
			pk: createPingreq(t, 3, 2),
		},
	}
	groupSubs := map[string]packets.Subscription{
		"client-1": {},
		"client-2": {},
		"client-3": {},
	}

	sm := NewManager(&logger)
	for _, s := range subs {
		sm.UpdateClient(s.cl)
		err := sm.UpdateClientInfo(s.cl, s.pk)
		require.NoError(t, err)
	}
	got, err := sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	require.Equal(t, "client-1", got)

	got, err = sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	require.Equal(t, "client-2", got)
}

func TestSubscriberNotUpdate(t *testing.T) {
	type sub struct {
		cl *mqtt.Client
	}
	subs := []sub{
		{
			cl: &mqtt.Client{ID: "client-1"},
		},
		{
			cl: &mqtt.Client{ID: "client-2"},
		},
		{
			cl: &mqtt.Client{ID: "client-3"},
		},
	}
	groupSubs := map[string]packets.Subscription{
		"client-1": {},
		"client-2": {},
		"client-3": {},
	}

	sm := NewManager(&logger)
	for _, s := range subs {
		sm.UpdateClient(s.cl)
	}
	got, err := sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	t.Log(got)

	got, err = sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	t.Log(got)

	got, err = sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	t.Log(got)

	got, err = sm.SelectSubscriber("", groupSubs, packets.Packet{})
	require.Equal(t, nil, err)
	t.Log(got)
}

func createPingreq(t *testing.T, inQueue int, perMessage float64) packets.Packet {
	t.Helper()
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Pingreq,
		},
		Properties: packets.Properties{
			User: []packets.UserProperty{
				{Key: "messageId", Val: "1"},
			},
		},
		Payload: []byte(
			fmt.Sprintf("{\"numberOfMsgsInQueue\": %d, \"processingTimerPerMsg\":%f}",
				inQueue, perMessage)),
	}
	return pk
}
