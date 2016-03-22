package storage

import (
	"encoding/json"
)

// RepositoryConfiguration is a JSON-serializable description of Repository and its configuration.
type RepositoryConfiguration struct {
	Type   string
	Config interface{}
}

// UnmarshalJSON parses the JSON-encoded data into RepositoryConfiguration.
func (c *RepositoryConfiguration) UnmarshalJSON(b []byte) error {
	raw := struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"config"`
	}{}

	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	c.Type = raw.Type
	c.Config = factories[raw.Type].defaultConfigFunc()
	if err := json.Unmarshal(raw.Data, c.Config); err != nil {
		return err
	}

	return nil
}

// MarshalJSON returns JSON-encoded repository configuration.
func (c *RepositoryConfiguration) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string      `json:"type"`
		Data interface{} `json:"config"`
	}{
		Type: c.Type,
		Data: c.Config,
	})
}
