package gcppubsubazure

import (
	"encoding/json"
	"fmt"

	"github.com/jeremywohl/flatten"
	"go.uber.org/zap"
)

// Parser .
type Parser struct {
	excludeFilter map[string]interface{}
	logger        *zap.Logger
}

// NewParser .
func NewParser(excludeFilter map[string]interface{}, L *zap.Logger) *Parser {
	return &Parser{
		excludeFilter: excludeFilter,
		logger:        L.With(zap.String("process", "Parser")),
	}
}

// Parse parses the given data and processes any exclusions.
func (p *Parser) Parse(data []byte) ([]byte, error) {
	var flatMap map[string]interface{}
	err := json.Unmarshal(data, &flatMap)
	if err != nil {
		p.logger.Error("error unmarshaling data", zap.Error(err))
		return []byte{}, err
	}
	flat, err := flatten.Flatten(flatMap, "", flatten.DotStyle)
	if err != nil {
		p.logger.Error("error flattening data", zap.Error(err))
		return []byte{}, err
	}

	for k, v := range p.excludeFilter {
		if match, there := flat[k]; there {
			if excludeVal, isString := v.(string); isString {
				if excludeVal == `*` {
					return []byte{}, nil
				}
			}
			excludeVal := fmt.Sprintf("%T", v)
			val := fmt.Sprintf("%T", match)
			switch {
			case val != excludeVal:
			case val == excludeVal:
				if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", match) {
					return []byte{}, nil
				}
			}
		}
	}
	return json.Marshal(flat)
}
