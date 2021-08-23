package pubsub

import (
	"context"
	"fmt"
	"sync"

	ps "cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

// Client communicates with the GCP PubSub API.
type Client struct {
	ctx          context.Context
	cancel       context.CancelFunc
	psClient     *ps.Client
	projectID    string
	subscription string
	logger       *zap.Logger
	stopped      chan struct{}
	wg           sync.WaitGroup
}

func NewClient(projectID, subscription string, L *zap.Logger) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := ps.NewClient(ctx, projectID)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Client{
		ctx:          ctx,
		cancel:       cancel,
		psClient:     client,
		projectID:    projectID,
		subscription: subscription,
		logger:       L.With(zap.String("process", "GCP PubSub Client")),
		stopped:      make(chan struct{}),
		wg:           sync.WaitGroup{},
	}, nil
}

// Start starts consuming from the PubSub subscription.
func (c *Client) Start() {
	c.logger.Info("starting ...", zap.String("subscription", c.subscription))
	c.consumeSubscription()
}

func (c *Client) Stop() {
	c.logger.Warn("Stopping...")
	c.cancel()
	c.wg.Wait()
}

func (c *Client) Stopped() <-chan struct{} {
	return c.stopped
}

func (c *Client) consumeSubscription() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.stopped)
		c.logger.Info("consuming from pub/sub", zap.String("subscription", c.subscription))
		sub := c.psClient.Subscription(c.subscription)
		err := sub.Receive(c.ctx, func(ctx context.Context, m *ps.Message) {
			fmt.Printf("ID:      %+v\n", m.ID)
			fmt.Printf("PubTime: %+v\n", m.PublishTime)
			fmt.Printf("ATTR:    %+v\n", m.Attributes)
			fmt.Printf("Data:\n%s\n\n", m.Data)
			m.Ack() // Acknowledge that we've consumed the message.
		})
		if err != nil {
			c.logger.Error("error consuming subscription", zap.Error(err))
		}
	}()
}
