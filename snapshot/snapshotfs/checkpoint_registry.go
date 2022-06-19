package snapshotfs

import (
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot"
)

// checkpointFunc is invoked when checkpoint occurs. The callback must checkpoint current state of
// file or directory and return directory entry.
type checkpointFunc func() (*snapshot.DirEntry, error)

type checkpointRegistry struct {
	mu sync.Mutex

	// +checklocks:mu
	checkpoints map[string]checkpointFunc
}

func (r *checkpointRegistry) addCheckpointCallback(entryName string, f checkpointFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.checkpoints == nil {
		r.checkpoints = map[string]checkpointFunc{}
	}

	r.checkpoints[entryName] = f
}

func (r *checkpointRegistry) removeCheckpointCallback(entryName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.checkpoints, entryName)
}

// runCheckpoints invokes all registered checkpointers and adds results to the provided builder, while
// randomizing file names for non-directory entries. this is to prevent the use of checkpointed objects
// as authoritative on subsequent runs.
func (r *checkpointRegistry) runCheckpoints(checkpointBuilder *DirManifestBuilder) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for n, cp := range r.checkpoints {
		de, err := cp()
		if err != nil {
			return errors.Wrapf(err, "error checkpointing %v", n)
		}

		if de == nil {
			// no checkpoint.
			continue
		}

		if de.Type != snapshot.EntryTypeDirectory {
			de.Name = ".checkpointed." + de.Name + "." + uuid.New().String()
		}

		checkpointBuilder.AddEntry(de)
	}

	return nil
}
