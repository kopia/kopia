// Package hashcache implements streaming cache of file hashes.
package hashcache

import "github.com/kopia/kopia/object"

var hashCacheStreamType = "kopia:hashcache"

// Entry represents an entry in hash cache database about single file or directory.
type Entry struct {
	Name     string    `json:"name,omitempty"`
	Hash     uint64    `json:"hash,omitempty"`
	ObjectID object.ID `json:"oid,omitempty"`
}
