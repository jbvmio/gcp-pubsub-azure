package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
)

func main() {
	pf := pflag.NewFlagSet(`gcp-pubsub-azure`, pflag.ExitOnError)
	pf.StringVarP(&configPath, "config", "c", "", "Path to Config")
	showVer := pf.Bool("version", false, "Show version and exit.")
	pf.Parse(os.Args[1:])
	if *showVer {
		fmt.Printf("Processor : %s\n", "gcp-pubsub-azure")
		fmt.Printf("Version   : %s\n", buildTime)
		fmt.Printf("Commit    : %s\n", commitHash)
		return
	}

	logger := internal.ConfigureLogger("info", os.Stdout)
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

	azureClient := loganalytics.NewClient(config.Azure, logger)
	parser := gcppubsubazure.NewParser(config.ExcludeFilter, logger)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	gcpClient.Start()

runLoop:
	for {
		select {
		case <-sigChan:
			gcpClient.Stop()
			break runLoop
		case <-gcpClient.Stopped():
			break runLoop
		case d := <-gcpClient.Data():
			data, err := parser.Parse(d)
			switch {
			case err != nil:
				L.Error("Encountered Error Parsing Data")
			case len(data) <= 1:
			default:
				azureClient.SendEvent(data)
				if err != nil {
					L.Error("could not send event", zap.Error(err))
				} else {
					L.Info("success")
				}
			}
		}
	}
	L.Info("Draining Queue")
	var drainQueue int
	for drain := range gcpClient.Data() {
		drainQueue++
		data, err := parser.Parse(drain)
		switch {
		case err != nil:
			L.Error("Encountered Error Parsing Data")
		case len(data) <= 1:
		default:
			azureClient.SendEvent(data)
			if err != nil {
				L.Error("could not send event", zap.Error(err))
			} else {
				L.Info("success")
			}
		}
		fmt.Printf(".")
	}
	L.Info("Drained Queue", zap.Int("queue", drainQueue))
	L.Warn("Stopped.")
}

// fileExists returns true if the file exists, false otherwise.
func fileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}
