package main

import (
	"context"
	"flag"
	"github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/hooks/sharedsub"
	"github.com/mochi-mqtt/server/v2/hooks/sharedsub/algorithms"
	"github.com/mochi-mqtt/server/v2/listeners"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

func main() {
	tcpAddr := ":1883"
	infoAddr := ":8080"
	algoFlg := flag.String("algo", "", "shared subscription algorithm")
	flag.Parse()

	// When signal is notified shut down server
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	// Create a directory for the results
	//timeString := time.Now().Format("20060102_150405")
	//dirName := filepath.Join("results", timeString)
	dirName := "results"
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
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{Level: level}))

	// Create a server
	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)

	var algo sharedsub.Algorithm
	if *algoFlg == "random" {
		algo = algorithms.NewRandomAlgorithm()
		log.Println("selected random algorithm")
	} else if *algoFlg == "round" {
		algo = algorithms.NewRoundRobinAlgorithm()
		log.Println("selected round robin algorithm")
	} else if *algoFlg == "score" {
		algo = algorithms.NewScoreAlgorithm(logger)
		log.Println("selected score algorithm")
	} else {
		log.Fatal("invalid algorithm")
	}

	// Create a shared subscription lb
	opts := sharedsub.Options{
		Algorithm: algo,
		Log:       logger,
		DirName:   dirName,
	}
	lb := sharedsub.NewLoadBalancer(opts)
	hook := sharedsub.NewHook(lb, logger)
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
		err := lb.StartRecording(ctx, wg)
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
