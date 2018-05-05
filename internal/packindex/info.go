package packindex

import (
	"encoding/json"
	"time"
)

// PhysicalBlockID is the name of a physical block in storage.
type PhysicalBlockID string

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID          string          `json:"blockID"`
	Length           uint32          `json:"length"`
	TimestampSeconds int64           `json:"time"`
	PackBlockID      PhysicalBlockID `json:"packBlockID,omitempty"`
	PackOffset       uint32          `json:"packOffset,omitempty"`
	Deleted          bool            `json:"deleted"`
	Payload          []byte          `json:"payload"` // set for payloads stored inline
	FormatVersion    byte            `json:"formatVersion"`
}

// Timestamp returns the time when a block was created or deleted.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

func (i Info) String() string {
	b, _ := json.Marshal(i)
	return string(b)
}
