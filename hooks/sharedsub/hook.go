package sharedsub

import (
	"bytes"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"log/slog"
	"os"
)

type Hook struct {
	mqtt.HookBase
	lb  *LoadBalancer
	log *slog.Logger
}

func NewHook(lb *LoadBalancer, logger *slog.Logger) *Hook {
	hook := &Hook{
		lb:  lb,
		log: logger,
	}
	if hook.log == nil {
		hook.log = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return hook
}

func (h *Hook) ID() string {
	return "shared-subscription-hook"
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
	h.lb.AddClient(cl, pk)
}

func (h *Hook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Pingreq && pk.Payload != nil {
		err := h.lb.UpdateClientWithPingreq(cl, pk)
		if err != nil {
			return pk, err
		}
	}
	return pk, nil
}

func (h *Hook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	h.lb.RemoveClient(cl)
}

func (h *Hook) OnSelectSubscribers(subs *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	subs.SharedSelected, _ = h.lb.SelectSharedSubscriptionSubscriber(subs.Shared, pk)
	return subs
}
