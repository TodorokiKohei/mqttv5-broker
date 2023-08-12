package main

import (
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/hooks/sharedsub"
	"github.com/mochi-co/mqtt/v2/listeners"
	"github.com/rs/zerolog"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

	//
	dirName := "results"
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dirName, "logfile.txt"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	logger := zerolog.New(file).With().Timestamp().Logger().Level(zerolog.InfoLevel)

	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)

	manager := sharedsub.NewManager(&logger)
	hook := sharedsub.NewHook(manager)
	_ = server.AddHook(hook, nil)

	tcp := listeners.NewTCP("t1", tcpAddr, nil)
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	stats := listeners.NewHTTPStats("stats", infoAddr, nil, server.Info)
	err = server.AddListener(stats)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			logger.Error().Err(err).Msg("an error occurred on the server")
		}
	}()

	<-done
	log.Println("caught signal, stopping...")
	server.Close()
	log.Println("main.go finished")
}
