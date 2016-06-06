package blob

import (
	"encoding/json"
)

// StorageConfiguration represents JSON-serializable configuration of a blob storage.
type StorageConfiguration struct {
	Type   string
	Config interface{}
}

// UnmarshalJSON parses the JSON-encoded data into StorageConfiguration.
func (c *StorageConfiguration) UnmarshalJSON(b []byte) error {
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

// MarshalJSON returns JSON-encoded storage configuration.
func (c StorageConfiguration) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string      `json:"type"`
		Data interface{} `json:"config"`
	}{
		Type: c.Type,
		Data: c.Config,
	})
}