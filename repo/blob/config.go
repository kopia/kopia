package blob

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// ConnectionInfo represents JSON-serializable configuration of a blob storage.
//
//nolint:recvcheck
type ConnectionInfo struct {
	Type   string
	Config interface{}
}

// UnmarshalJSON parses the JSON-encoded data into ConnectionInfo.
func (c *ConnectionInfo) UnmarshalJSON(b []byte) error {
	raw := struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"config"`
	}{}

	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.Wrap(err, "error unmarshaling connection info JSON")
	}

	c.Type = raw.Type

	f := factories[raw.Type]
	if f == nil {
		return errors.Errorf("storage type '%v' not registered", raw.Type)
	}

	c.Config = f.defaultConfigFunc()
	if err := json.Unmarshal(raw.Data, c.Config); err != nil {
		return errors.Wrap(err, "unable to unmarshal config")
	}

	return nil
}

// MarshalJSON returns JSON-encoded storage configuration.
func (c ConnectionInfo) MarshalJSON() ([]byte, error) {
	//nolint:wrapcheck
	return json.Marshal(struct {
		Type string      `json:"type"`
		Data interface{} `json:"config"`
	}{
		Type: c.Type,
		Data: c.Config,
	})
}
