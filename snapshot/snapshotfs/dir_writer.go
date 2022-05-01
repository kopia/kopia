package snapshotfs

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

var dirRewriterLog = logging.Module("dirRewriter")

// DirRewriterCallback copies all directory entries that should be preserved from the inputs to the output.
// Must return true to indicate any modifications were made.
type DirRewriterCallback func(ctx context.Context, parentPath string, input *snapshot.DirEntry) (*snapshot.DirEntry, error)

// DirRewriter rewrites contents of directories by walking the snapshot tree recursively.
type DirRewriter struct {
	cache sync.Map // string -> noChange{} if unchanged or []*snapshot.DirEntry

	rep      repo.RepositoryWriter
	rewriter DirRewriterCallback
}

func (rw *DirRewriter) getCachedReplacement(ctx context.Context, parentPath string, input *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal")
	}

	key := string(b)

	if v, ok := rw.cache.Load(key); ok {
		// nolint:forcetypeassert
		return v.(*snapshot.DirEntry), nil
	}

	result, err := rw.rewriter(ctx, parentPath, input)
	if err != nil {
		return nil, err
	}

	rw.cache.Store(key, result)

	return result, nil
}

func (rw *DirRewriter) processDirectory(ctx context.Context, parentPath string, entry *snapshot.DirEntry) (*snapshot.DirEntry, error) {
	r, err := rw.rep.OpenObject(ctx, entry.ObjectID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open object: %v", entry.ObjectID)
	}
	defer r.Close() //nolint:errcheck

	entries, _, err := readDirEntries(r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read directory entries")
	}

	var (
		builder      DirManifestBuilder
		changed      bool
		addedEntries = map[string]struct{}{}
	)

	for _, child := range entries {
		replacement, repErr := rw.getCachedReplacement(ctx, parentPath, child)
		if repErr != nil {
			return nil, errors.Wrap(repErr, child.Name)
		}

		if replacement != nil && replacement.Type == snapshot.EntryTypeDirectory {
			rep2, subdirErr := rw.processDirectory(ctx, parentPath+"/"+child.Name, replacement)
			if subdirErr != nil {
				return nil, errors.Wrap(subdirErr, child.Name)
			}

			if rep2 != child {
				changed = true
			}

			replacement = rep2
		}

		if replacement != child {
			changed = true
		}

		if replacement != nil {
			if _, ok := addedEntries[replacement.Name]; !ok {
				addedEntries[replacement.Name] = struct{}{}

				builder.AddEntry(replacement)
			}
		}
	}

	if !changed {
		return entry, nil
	}

	dm := builder.Build(entry.ModTime, entry.DirSummary.IncompleteReason)

	oid, err := writeDirManifest(ctx, rw.rep, string(entry.ObjectID), dm)
	if err != nil {
		return nil, errors.Wrap(err, "unable to write directory manifest")
	}

	result := *entry
	result.DirSummary = dm.Summary
	result.ObjectID = oid

	if entry.ObjectID == oid {
		return entry, nil
	}

	dirRewriterLog(ctx).Debugf("rewrote directory %v/%v %v=>%v", parentPath, entry.Name, entry.ObjectID, oid)

	return &result, nil
}

// RewriteSnapshotManifest rewrites the directory tree starting at a given manifest.
func (rw *DirRewriter) RewriteSnapshotManifest(ctx context.Context, man *snapshot.Manifest) (bool, error) {
	newEntry, err := rw.processDirectory(ctx, ".", man.RootEntry)
	if err != nil {
		return false, errors.Wrapf(err, "error processing snapshot %v", man.ID)
	}

	if newEntry != man.RootEntry {
		man.RootEntry = newEntry
		return true, nil
	}

	return false, nil
}

// NewDirRewriter creates a new directory rewriter.
func NewDirRewriter(rep repo.RepositoryWriter, rewriter DirRewriterCallback) *DirRewriter {
	return &DirRewriter{
		rep:      rep,
		rewriter: rewriter,
	}
}

func writeDirManifest(ctx context.Context, rep repo.RepositoryWriter, dirRelativePath string, dirManifest *snapshot.DirManifest) (object.ID, error) {
	writer := rep.NewObjectWriter(ctx, object.WriterOptions{
		Description: "DIR:" + dirRelativePath,
		Prefix:      objectIDPrefixDirectory,
	})

	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(dirManifest); err != nil {
		return "", errors.Wrap(err, "unable to encode directory JSON")
	}

	oid, err := writer.Result()
	if err != nil {
		return "", errors.Wrap(err, "unable to write directory")
	}

	return oid, nil
}
