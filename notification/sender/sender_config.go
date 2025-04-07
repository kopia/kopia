package sender

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// Method represents the configuration of a Sender.
type Method string

// MethodConfig represents JSON-serializable configuration of a notification method and parameters.
//
//nolint:recvcheck
type MethodConfig struct {
	Type   Method
	Config any
}

// UnmarshalJSON parses the JSON-encoded notification method configuration into MethodInfo.
func (c *MethodConfig) UnmarshalJSON(b []byte) error {
	raw := struct {
		Type Method          `json:"type"`
		Data json.RawMessage `json:"config"`
	}{}

	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.Wrap(err, "error unmarshaling connection info JSON")
	}

	c.Type = raw.Type

	if f := allSenders[raw.Type]; f == nil {
		return errors.Errorf("sender type '%v' not registered", raw.Type)
	}

	c.Config = defaultOptions[raw.Type]
	if err := json.Unmarshal(raw.Data, &c.Config); err != nil {
		return errors.Wrap(err, "unable to unmarshal config")
	}

	return nil
}

// Options unmarshals the configuration into the provided structure.
func (c MethodConfig) Options(result any) error {
	b, err := json.Marshal(c.Config)
	if err != nil {
		return errors.Wrap(err, "unable to marshal config")
	}

	if err := json.Unmarshal(b, result); err != nil {
		return errors.Wrap(err, "unable to unmarshal config")
	}

	return nil
}

// MarshalJSON returns JSON-encoded notification method configuration.
func (c MethodConfig) MarshalJSON() ([]byte, error) {
	//nolint:wrapcheck
	return json.Marshal(struct {
		Type Method      `json:"type"`
		Data interface{} `json:"config"`
	}{
		Type: c.Type,
		Data: c.Config,
	})
}
