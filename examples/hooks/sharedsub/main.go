package main

import (
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/hooks/sharedsub"
	"github.com/mochi-co/mqtt/v2/listeners"
	"github.com/rs/zerolog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	tcpAddr := ":1883"
	infoAddr := ":8080"

	// When signal is notified shut down server
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	log := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.InfoLevel).Output(zerolog.ConsoleWriter{Out: os.Stderr})
	opts := new(mqtt.Options)
	opts.Logger = &log
	server := mqtt.New(opts)
	_ = server.AddHook(new(auth.AllowHook), nil)

	manager := sharedsub.NewManager(&log)
	hook := sharedsub.NewHook(manager)
	_ = server.AddHook(hook, nil)

	tcp := listeners.NewTCP("t1", tcpAddr, nil)
	err := server.AddListener(tcp)
	if err != nil {
		log.Error().Err(err).Msg("failed to create TCP listener")
		return
	}

	stats := listeners.NewHTTPStats("stats", infoAddr, nil, server.Info)
	err = server.AddListener(stats)
	if err != nil {
		log.Error().Err(err).Msg("failed to create HTTP status listener")
		return
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Error().Err(err).Msg("an error occurred on the server")
		}
	}()

	<-done
	server.Log.Warn().Msg("caught signal, stopping...")
	server.Close()
	server.Log.Info().Msg("main.go finished")
}
