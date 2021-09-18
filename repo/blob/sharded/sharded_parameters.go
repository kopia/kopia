// Package sharded implements common support for sharded blob providers, such as filesystem or webdav.
package sharded

import (
	"encoding/json"
	"io"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// ParametersFile is the name of the hidden parameters file in a sharded storage.
const ParametersFile = ".shards"

const defaultMinShardedBlobIDLength = 20

// PrefixAndShards defines shards to use for a particular blob ID prefix.
type PrefixAndShards struct {
	Prefix blob.ID `json:"prefix"`
	Shards []int   `json:"shards"`
}

// Parameters contains sharded storage configuration optionally persisted in the storage itself.
type Parameters struct {
	DefaultShards   []int             `json:"default"`
	UnshardedLength int               `json:"maxNonShardedLength"`
	Overrides       []PrefixAndShards `json:"overrides,omitempty"`
}

// DefaultParameters constructs Parameters based on the provided shards specification.
func DefaultParameters(shards []int) *Parameters {
	return &Parameters{
		DefaultShards:   shards,
		UnshardedLength: defaultMinShardedBlobIDLength,
	}
}

// Load loads the Parameters from the provided reader.
func (p *Parameters) Load(r io.Reader) error {
	return errors.Wrap(json.NewDecoder(r).Decode(p), "error parsing JSON")
}

// Save saves the parameters to the provided writer.
func (p *Parameters) Save(w io.Writer) error {
	return errors.Wrap(json.NewEncoder(w).Encode(p), "error writing JSON")
}

func cloneShards(v []int) []int {
	if v != nil {
		return append(([]int(nil)), v...)
	}

	return nil
}

// Clone returns a clone of sharding parameters.
func (p *Parameters) Clone() *Parameters {
	var clonedOverrides []PrefixAndShards

	for _, o := range p.Overrides {
		clonedOverrides = append(clonedOverrides, PrefixAndShards{o.Prefix, cloneShards(o.Shards)})
	}

	return &Parameters{
		DefaultShards:   cloneShards(p.DefaultShards),
		UnshardedLength: p.UnshardedLength,
		Overrides:       clonedOverrides,
	}
}

func (p *Parameters) getShardsForBlobID(id blob.ID) []int {
	for _, o := range p.Overrides {
		if strings.HasPrefix(string(id), string(o.Prefix)) {
			return o.Shards
		}
	}

	return p.DefaultShards
}

// GetShardDirectoryAndBlob gets that sharded directory and blob ID for a provided blob.
func (p *Parameters) GetShardDirectoryAndBlob(rootPath string, blobID blob.ID) (string, blob.ID) {
	shardPath := rootPath

	if len(blobID) <= p.UnshardedLength {
		return shardPath, blobID
	}

	for _, size := range p.getShardsForBlobID(blobID) {
		if len(blobID) <= size {
			break
		}

		shardPath = path.Join(shardPath, string(blobID[0:size]))
		blobID = blobID[size:]
	}

	return shardPath, blobID
}
