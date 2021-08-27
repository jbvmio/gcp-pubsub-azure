package pubsub

import (
	"context"
	"sync"
	"time"

	ps "cloud.google.com/go/pubsub"
	gcppubsubazure "github.com/jbvmio/gcp-pubsub-azure"
	"go.uber.org/zap"
)

// Client communicates with the GCP PubSub API.
type Client struct {
	ctx          context.Context
	cancel       context.CancelFunc
	psClient     *ps.Client
	projectID    string
	subscription string
	dataChan     chan []byte
	stopped      chan struct{}
	logger       *zap.Logger
	wg           sync.WaitGroup
}

func NewClient(config gcppubsubazure.GCPConfig, L *zap.Logger) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := ps.NewClient(ctx, config.Project)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Client{
		ctx:          ctx,
		cancel:       cancel,
		psClient:     client,
		projectID:    config.Project,
		subscription: config.Subscription,
		dataChan:     make(chan []byte, 1000),
		stopped:      make(chan struct{}),
		logger:       L.With(zap.String("process", "GCP PubSub Client")),
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

func (c *Client) Data() <-chan []byte {
	return c.dataChan
}

func (c *Client) consumeSubscription() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.dataChan)
		defer close(c.stopped)
		c.logger.Info("consuming from pub/sub", zap.String("subscription", c.subscription))
		sub := c.psClient.Subscription(c.subscription)
		err := sub.Receive(c.ctx, func(ctx context.Context, m *ps.Message) {
			to, nm := context.WithTimeout(ctx, time.Second*30)
			defer nm()
			select {
			case <-to.Done():
				switch {
				case c.ctx.Err() == context.Canceled:
					c.logger.Debug("context has been cancelled", zap.String("ID", m.ID))
				default:
					c.logger.Error("timed out sending event", zap.String("ID", m.ID))
				}
				m.Nack()
			case c.dataChan <- m.Data:
				c.logger.Debug("sending event", zap.String("ID", m.ID))
				m.Ack() // Acknowledge that we've consumed the message.
			}
		})
		if err != nil {
			c.logger.Error("error consuming subscription", zap.Error(err))
		}
		c.logger.Warn("stopped consuming from pub/sub", zap.String("subscription", c.subscription))
	}()
}
