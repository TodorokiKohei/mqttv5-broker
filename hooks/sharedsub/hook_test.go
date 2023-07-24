package sharedsub

import (
	"errors"
	"fmt"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/stretchr/testify/require"
	"testing"
)

type MockManager struct {
	Clients map[string]bool
}

func (m *MockManager) UpdateClient(cl *mqtt.Client) {
	m.Clients[cl.ID] = true
}

func (m *MockManager) DeleteClient(cl *mqtt.Client) {
}

func (m *MockManager) UpdateClientInfo(cl *mqtt.Client, pk packets.Packet) error {
	if pk.FixedHeader.Type == packets.Pingreq && pk.Payload != nil {
		m.Clients[cl.ID] = true
		return nil
	}
	return errors.New("invalid packet type")
}

func (m *MockManager) SelectSubscriber(topicFilter string, groupSubs map[string]packets.Subscription, pk packets.Packet) (string, error) {
	for clientId, _ := range groupSubs {
		if m.Clients[clientId] {
			return clientId, nil
		}
	}
	return "", errors.New(fmt.Sprintf("client not found: %s", topicFilter))
}

func TestID(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "test Hook ID", want: "pingreq-hook"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := new(Hook)
			require.Equal(t, tt.want, h.ID())
			if got := h.ID(); got != tt.want {
				t.Errorf("ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProvides(t *testing.T) {
	type args struct {
		b byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "OnSessionEstablished", args: args{b: mqtt.OnSessionEstablished}, want: true},
		{name: "OnPacketRead", args: args{b: mqtt.OnSessionEstablished}, want: true},
		{name: "OnSelectSubscribers", args: args{b: mqtt.OnSessionEstablished}, want: true},
		{name: "OnDisconnect", args: args{b: mqtt.OnSessionEstablished}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Hook{}
			require.Equal(t, tt.want, h.Provides(tt.args.b))
		})
	}
}

func TestOnPacketRead(t *testing.T) {
	type fields struct {
		manager Manager
	}
	type args struct {
		cl *mqtt.Client
		pk packets.Packet
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   error
		wantCl bool
	}{
		{
			name: "receive extended pingreq",
			fields: fields{manager: &MockManager{
				Clients: make(map[string]bool),
			}},
			args: args{
				cl: &mqtt.Client{ID: "ok-client"},
				pk: packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
					Payload:     []byte("Hello World"),
				},
			},
			want:   nil,
			wantCl: true,
		},
		{
			name: "receive standard pingreq",
			fields: fields{manager: &MockManager{
				Clients: make(map[string]bool),
			}},
			args: args{
				cl: &mqtt.Client{ID: "ok-client"},
				pk: packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
				},
			},
			want:   nil,
			wantCl: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Hook{
				manager: tt.fields.manager,
			}
			_, err := h.OnPacketRead(tt.args.cl, tt.args.pk)
			require.NoError(t, err)
			require.Equal(t, tt.wantCl, tt.fields.manager.(*MockManager).Clients[tt.args.cl.ID])
		})
	}
}

func TestOnSelectSubscribers(t *testing.T) {
	type fields struct {
		manager Manager
	}
	type client struct {
		ID  string
		sel bool
	}
	type args struct {
		subs *mqtt.Subscribers
		cls  []client
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *mqtt.Subscribers
	}{
		{
			name: "select subscriber",
			fields: fields{manager: &MockManager{
				Clients: make(map[string]bool),
			}},
			args: args{
				subs: &mqtt.Subscribers{
					Shared: map[string]map[string]packets.Subscription{
						"$share/g/t1": {
							"client-1": packets.Subscription{},
							"client-2": packets.Subscription{},
							"client-3": packets.Subscription{},
						},
					},
					SharedSelected: nil,
					Subscriptions:  nil,
				},
				cls: []client{
					{ID: "client-1", sel: false},
					{ID: "client-2", sel: true},
					{ID: "client-3", sel: false},
				},
			},
			want: &mqtt.Subscribers{
				Shared: map[string]map[string]packets.Subscription{
					"$share/g/t1": {
						"client-1": packets.Subscription{},
						"client-2": packets.Subscription{},
						"client-3": packets.Subscription{},
					},
				},
				SharedSelected: map[string]packets.Subscription{
					"client-2": {
						Identifiers: map[string]int{
							"": 0,
						},
					},
				},
				Subscriptions: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Hook{
				manager: tt.fields.manager,
			}
			for _, cl := range tt.args.cls {
				h.manager.(*MockManager).Clients[cl.ID] = cl.sel
			}
			require.Equal(t, tt.want, h.OnSelectSubscribers(tt.args.subs, packets.Packet{}))
		})
	}
}
