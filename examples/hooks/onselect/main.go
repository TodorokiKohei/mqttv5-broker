// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"bytes"
	"flag"
	"github.com/mochi-co/mqtt/v2/packets"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/listeners"
)

func main() {
	tcpAddr := flag.String("tcp", ":1883", "network address for TCP listener")
	wsAddr := flag.String("ws", ":1882", "network address for Websocket listener")
	infoAddr := flag.String("info", ":8080", "network address for web info dashboard listener")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)

	_ = server.AddHook(new(SelectHook), server)

	tcp := listeners.NewTCP("t1", *tcpAddr, nil)
	err := server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	ws := listeners.NewWebsocket("ws1", *wsAddr, nil)
	err = server.AddListener(ws)
	if err != nil {
		log.Fatal(err)
	}

	stats := listeners.NewHTTPStats("stats", *infoAddr, nil, server.Info)
	err = server.AddListener(stats)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	<-done
	server.Log.Warn().Msg("caught signal, stopping...")
	server.Close()
	server.Log.Info().Msg("main.go finished")
}

type SelectHook struct {
	server *mqtt.Server
	rnd    *rand.Rand
	mqtt.HookBase
}

func (h *SelectHook) ID() string {
	return "select-shared-subscriber"
}

func (h *SelectHook) Init(config any) error {
	h.server = config.(*mqtt.Server)
	h.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	return nil
}

func (h *SelectHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSelectSubscribers,
	}, []byte{b})
}

func (h *SelectHook) OnSelectSubscribers(s *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	s.SharedSelected = map[string]packets.Subscription{}
	for _, shared := range s.Shared {
		// extract subscribers which can be sent
		clients := make([]string, 0)
		subs := make([]packets.Subscription, 0)
		for _, sub := range shared {
			//clt, ok := h.server.Clients.Get(clientId)
			//if ok && clt.State.Selector.CanSend {
			//	clients = append(clients, clientId)
			//	subs = append(subs, sub)
			//}
			subs = append(subs, sub)
		}
		// Select one subscriber in subscribers which can be sent. If no subscriber can be sent,
		// one is randomly selected from the group.
		if len(clients) != 0 {
			i := h.rnd.Intn(len(clients))
			clientId := clients[i]
			sub := subs[i]
			cls, ok := s.SharedSelected[clientId]
			if !ok {
				cls = sub
			}

			s.SharedSelected[clientId] = cls.Merge(sub)
		} else {
			for clientId, sub := range shared {
				cls, ok := s.SharedSelected[clientId]
				if !ok {
					cls = sub
				}

				s.SharedSelected[clientId] = cls.Merge(sub)
				break
			}
		}
	}
	return s
}
