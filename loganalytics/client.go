package loganalytics

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	gcppubsubazure "github.com/jbvmio/gcp-pubsub-azure"
	"go.uber.org/zap"
)

const (
	urlFmt = `https://%s.ods.opinsights.azure.com/api/logs?api-version=2016-04-01`
)

//Client .
type Client struct {
	sender  http.Client
	baseURL string
	config  gcppubsubazure.AzureConfig
	logger  *zap.Logger
}

// NewClient .
func NewClient(config gcppubsubazure.AzureConfig, L *zap.Logger) *Client {
	return &Client{
		sender:  http.Client{},
		baseURL: fmt.Sprintf(urlFmt, config.WorkspaceID),
		config:  config,
		logger:  L.With(zap.String("process", "Azure Log Analytics Client")),
	}
}

// SendEvent .
func (c *Client) SendEvent(logType string, data []byte) error {
	req, err := c.makeRequest(logType, data)
	if err != nil {
		return err
	}
	resp, err := c.sender.Do(req)
	if err != nil {
		return fmt.Errorf("error sending event: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			b = []byte(`unknown`)
		}
		return fmt.Errorf("error status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (c *Client) makeRequest(logType string, data []byte) (*http.Request, error) {
	if logType == "" {
		logType = c.config.LogType
	}
	dateString := strings.Replace(time.Now().UTC().Format(time.RFC1123), `UTC`, `GMT`, -1)
	stringToHash := "POST\n" + strconv.Itoa(len(data)) + "\napplication/json\n" + "x-ms-date:" + dateString + "\n/api/logs"
	hashedString, err := buildSignature(stringToHash, c.config.WorkspaceKey)
	if err != nil {
		return &http.Request{}, fmt.Errorf("error building signature")
	}
	payload := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", c.baseURL, payload)
	if err != nil {
		return req, fmt.Errorf("error building signature")
	}
	signature := "SharedKey " + c.config.WorkspaceID + ":" + hashedString
	req.Header.Add("Log-Type", logType)
	req.Header.Add("Authorization", signature)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-ms-date", dateString)
	req.Header.Add("time-generated-field", c.config.TimestampField)
	return req, nil
}

func buildSignature(message, secret string) (string, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, keyBytes)
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}
