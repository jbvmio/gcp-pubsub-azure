package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gcppubsubazure "github.com/jbvmio/gcp-pubsub-azure"
	"github.com/jbvmio/gcp-pubsub-azure/internal"
	"github.com/jbvmio/gcp-pubsub-azure/loganalytics"
	"github.com/jbvmio/gcp-pubsub-azure/pubsub"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	configPath string
	buildTime  string
	commitHash string
	logLevel   string
	threads    int
	showQueue  int64
	testOnly   bool
)

func main() {
	pf := pflag.NewFlagSet(`gcp-pubsub-azure`, pflag.ExitOnError)
	pf.StringVarP(&configPath, "config", "c", "", "Path to Config")
	pf.IntVar(&threads, "threads", 1, "Number of Sender Threads to Azure")
	pf.Int64Var(&showQueue, "show-queue", 30, "Interval to Print Current Queue - 0 to disable")
	pf.StringVarP(&logLevel, "log-level", "l", "info", "Set Log Level")
	pf.BoolVarP(&testOnly, "test", "t", false, "Print to Terminal only, do not send to Azure")
	showVer := pf.Bool("version", false, "Show version and exit.")
	pf.Parse(os.Args[1:])
	if *showVer {
		fmt.Printf("Processor : %s\n", "gcp-pubsub-azure")
		fmt.Printf("Version   : %s\n", buildTime)
		fmt.Printf("Commit    : %s\n", commitHash)
		return
	}

	logger := internal.ConfigureLogger(logLevel, os.Stdout)
	defer logger.Sync()
	L := logger.With(zap.String("process", "gcp-pubsub-azure"))
	L.Info("starting", zap.String(`Version`, buildTime), zap.String(`Commit`, commitHash))
	gcpCreds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	switch {
	case gcpCreds == "":
		L.Fatal("GOOGLE_APPLICATION_CREDENTIALS not provided")
	case !fileExists(gcpCreds):
		L.Fatal("GOOGLE_APPLICATION_CREDENTIALS path is invalid", zap.String("path", gcpCreds))
	}
	config, err := gcppubsubazure.GetConfig(configPath)
	if err != nil {
		L.Fatal("error retrieving config", zap.Error(err))
	}
	switch "" {
	case config.GCP.Project:
		L.Fatal("GCP Project ID not Provided")
	case config.GCP.Subscription:
		L.Fatal("GCP Subscription not Provided")
	case config.Azure.WorkspaceID:
		L.Fatal("Azure WorkspaceID not Provided")
	case config.Azure.WorkspaceKey:
		L.Fatal("Azure WorkspaceKey not Provided")
	case config.Azure.ResourceGroup:
		L.Fatal("Azure ResourceGroup not Provided")
	case config.Azure.LogType:
		L.Fatal("Azure LogType not Provided")
	case config.Azure.TimestampField:
		L.Fatal("Azure TimestampField not Provided")
	}
	gcpClient, err := pubsub.NewClient(config.GCP, logger)
	if err != nil {
		L.Fatal("error initializing GCP client")
	}

	ctx := context.Background()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	launchProcessors(threads, config, gcpClient.Data(), logger, nil, testOnly)
	if showQueue > 0 {
		go printQueue(ctx, gcpClient.Data(), showQueue, logger)
	}
	gcpClient.Start()

	select {
	case <-sigChan:
		gcpClient.Stop()
	case <-gcpClient.Stopped():
	}

	L.Info("Draining Queue")
	wg := sync.WaitGroup{}
	launchProcessors(threads, config, gcpClient.Data(), logger, &wg, testOnly)
	wg.Wait()
	L.Info("Drained Queue")
	L.Warn("Stopped.")
}

func launchProcessors(threads int, config *gcppubsubazure.Config, eventChan <-chan []byte, logger *zap.Logger, wg *sync.WaitGroup, testOnly bool) {
	for i := 0; i < threads; i++ {
		if wg != nil {
			wg.Add(1)
		}
		go processEvents(config, eventChan, logger, wg, testOnly)
	}
}

func processEvents(config *gcppubsubazure.Config, eventChan <-chan []byte, logger *zap.Logger, wg *sync.WaitGroup, testOnly bool) {
	if wg != nil {
		defer wg.Done()
	}
	L := logger.With(zap.String("process", "eventProcessor"))
	azClient := loganalytics.NewClient(config.Azure, L)
	parser := gcppubsubazure.NewParser(config.ExcludeFilter, L)
	for d := range eventChan {
		data, err := parser.Parse(d)
		switch {
		case err != nil:
			L.Error("Encountered Error Parsing Data")
		case len(data) <= 1:
			L.Debug("dropping event")
		case testOnly:
			fmt.Printf("%s\n", data)
		default:
			err := azClient.SendEvent(data)
			if err != nil {
				L.Error("could not send event", zap.Error(err), zap.Int("queueSize", len(eventChan)))
			} else {
				L.Debug("success", zap.Int("queueSize", len(eventChan)))
			}
		}
	}
}

func printQueue(ctx context.Context, dataChan <-chan []byte, secs int64, logger *zap.Logger) {
	ticker := time.NewTicker(time.Second * time.Duration(secs))
runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case <-ticker.C:
			logger.Info("current queue size", zap.Int("queue", len(dataChan)))
		}
	}
	logger.Debug("print queue stopped.")
}

// fileExists returns true if the file exists, false otherwise.
func fileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}
