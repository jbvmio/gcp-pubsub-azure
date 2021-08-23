package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbvmio/gcp-pubsub-azure/internal"
	"github.com/jbvmio/gcp-pubsub-azure/pubsub"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	projectID    string
	subscription string
	buildTime    string
	commitHash   string
)

func main() {
	pf := pflag.NewFlagSet(`gcp-pubsub-azure`, pflag.ExitOnError)
	pf.StringVarP(&projectID, "project", "p", "", "GCP Project ID")
	pf.StringVarP(&subscription, "subscription", "s", "", "PubSub Subscription")
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
	if projectID == "" {
		projectID = os.Getenv("GCP_PROJECT_ID")
	}
	if subscription == "" {
		subscription = os.Getenv("GCP_SUBSCRIPTION")
	}
	switch "" {
	case projectID:
		L.Fatal("GCP Project ID not Provided")
	case subscription:
		L.Fatal("GCP Subscription not Provided")
	}

	gcpClient, err := pubsub.NewClient(projectID, subscription, logger)
	if err != nil {
		L.Fatal("error initializing GCP client")
	}

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
		}
	}

	L.Warn("Stopped.")

}

// fileExists returns true if the file exists, false otherwise.
func fileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}
