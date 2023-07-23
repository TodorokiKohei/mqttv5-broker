package sharedsub

import (
	"bytes"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/rs/zerolog"
)

type Hook struct {
	manager Manager
	Log     *zerolog.Logger
	mqtt.HookBase
}

func (h *Hook) ID() string {
	return "pingreq-hook"
}

func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSessionEstablished,
		mqtt.OnPacketRead,
		mqtt.OnSelectSubscribers,
		mqtt.OnDisconnect,
	}, []byte{b})
}

func (h *Hook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.manager.UpdateClient(cl)
}

func (h *Hook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Pingreq && pk.Payload != nil {
		err := h.manager.UpdateClientInfo(cl, pk)
		if err != nil {
			return pk, err
		}
	}
	return pk, nil
}

func (h *Hook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	h.manager.DeleteClient(cl)
}

func (h *Hook) OnSelectSubscribers(subs *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	subs.SharedSelected = map[string]packets.Subscription{} // clientID and Subscription of the client.
	for topicFilter, groupSubs := range subs.Shared {
		// Select subscriber with information retained by the manager.
		selClientId, err := h.manager.SelectSubscriber(topicFilter, groupSubs, pk)

		if err != nil {
			h.Log.Error().Err(err).Str("topic", topicFilter).Msg("An error occurred during subscriber selection.")
			// Use standard function implemented by server.
			subs.SharedSelected = map[string]packets.Subscription{}
			return subs
		}

		// Update subscription.
		// If the same subscriber is matched, the QoS delivered to the subscriber is adjusted to the value
		// of the largest QoS among the matched topics.
		oldSub, ok := subs.SharedSelected[selClientId]
		if !ok {
			oldSub = groupSubs[selClientId]
		}
		subs.SharedSelected[selClientId] = oldSub.Merge(groupSubs[selClientId])
	}
	return subs
}
