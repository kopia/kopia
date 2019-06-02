package manifest

import (
	"encoding/json"
	"time"
)

type manifest struct {
	Entries []*manifestEntry `json:"entries"`
}

type manifestEntry struct {
	ID      ID                `json:"id"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"modified"`
	Deleted bool              `json:"deleted,omitempty"`
	Content json.RawMessage   `json:"data"`
}
