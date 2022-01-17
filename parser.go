package gcppubsubazure

import (
	"encoding/json"
	"fmt"

	"github.com/jeremywohl/flatten"
	"go.uber.org/zap"
)

// Parser .
type Parser struct {
	logTypeField  string
	excludeFilter map[string]interface{}
	logger        *zap.Logger
}

// NewParser .
func NewParser(logTypeField string, excludeFilter map[string]interface{}, L *zap.Logger) *Parser {
	return &Parser{
		logTypeField:  logTypeField,
		excludeFilter: excludeFilter,
		logger:        L.With(zap.String("process", "Parser")),
	}
}

// Parse parses the given data and processes any exclusions.
// Returns the logTypeField value if found.
func (p *Parser) Parse(data []byte) ([]byte, string, error) {
	var logTypeValue string
	var flatMap map[string]interface{}
	err := json.Unmarshal(data, &flatMap)
	if err != nil {
		p.logger.Error("error unmarshaling data", zap.Error(err))
		return []byte{}, logTypeValue, err
	}
	flat, err := flatten.Flatten(flatMap, "", flatten.DotStyle)
	if err != nil {
		p.logger.Error("error flattening data", zap.Error(err))
		return []byte{}, logTypeValue, err
	}

	if p.logTypeField != "" {
		if match, there := flat[p.logTypeField]; there {
			if val, isString := match.(string); isString {
				logTypeValue = val
			}
		}
	}

	for k, v := range p.excludeFilter {
		if match, there := flat[k]; there {
			if excludeVal, isString := v.(string); isString {
				if excludeVal == `*` {
					return []byte{}, logTypeValue, nil
				}
			}
			excludeVal := fmt.Sprintf("%T", v)
			val := fmt.Sprintf("%T", match)
			switch {
			case val != excludeVal:
			case val == excludeVal:
				if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", match) {
					return []byte{}, logTypeValue, nil
				}
			}
		}
	}
	j, err := json.Marshal(flat)
	return j, logTypeValue, err
}
