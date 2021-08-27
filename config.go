package gcppubsubazure

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// Config .
type Config struct {
	GCP           GCPConfig              `yaml:"gcp"`
	Azure         AzureConfig            `yaml:"azure"`
	ExcludeFilter map[string]interface{} `yaml:"excludeFilter"`
}

// GCPConfig .
type GCPConfig struct {
	Project      string `yaml:"project"`
	Subscription string `yaml:"subscription"`
}

type AzureConfig struct {
	WorkspaceID    string `yaml:"workspaceID"`
	WorkspaceKey   string `yaml:"workspaceKey"`
	ResourceGroup  string `yaml:"resourceGroup"`
	TimestampField string `yaml:"timestampField"`
	LogType        string `yaml:"logType"`
}

// GetConfig .
func GetConfig(path string) (*Config, error) {
	var C Config
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return &C, err
	}
	err = yaml.Unmarshal(b, &C)
	return &C, err
}
