package main

import (
	"context"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/hooks/sharedsub"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/rs/zerolog"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
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

	// Create a directory for the results
	timeString := time.Now().Format("20060102_150405")
	dirName := filepath.Join("results", timeString)
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// Create a logger
	file, err := os.OpenFile(filepath.Join(dirName, "logfile.txt"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	logger := zerolog.New(file).With().Timestamp().Logger().Level(zerolog.InfoLevel)

	// Create a server
	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)

	// Create a shared subscription manager
	//manager := sharedsub.NewStatusManager(&logger, dirName)
	manager := sharedsub.NewRandomManager(&logger, dirName)
	hook := sharedsub.NewHook(manager)
	_ = server.AddHook(hook, nil)

	// Add listeners
	tcp := listeners.NewTCP("t1", tcpAddr, nil)
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	// Add stats listener
	stats := listeners.NewHTTPStats("stats", infoAddr, nil, server.Info)
	err = server.AddListener(stats)
	if err != nil {
		log.Fatal(err)
	}

	// Start recording status
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := manager.StartRecording(ctx, wg)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Start the server
	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Wait for signal
	<-done
	log.Println("caught signal, stopping...")
	server.Close()
	cancel()
	wg.Wait()
	log.Println("main.go finished")
}
