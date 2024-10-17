package snapshotfs

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"path"
	"runtime"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/internal/workshare"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var dirRewriterLog = logging.Module("dirRewriter")

type dirRewriterCacheKey [sha1.Size]byte

// RewriteDirEntryCallback returns a replacement for the provided directory entry in the provided path.
// nil indicates that the entry should be removed.
type RewriteDirEntryCallback func(ctx context.Context, parentPath string, input *snapshot.DirEntry) (*snapshot.DirEntry, error)

// RewriteFailedEntryCallback is invoked rewriting a file or directory.
type RewriteFailedEntryCallback func(ctx context.Context, parentPath string, input *snapshot.DirEntry, err error) (*snapshot.DirEntry, error)

// UnreadableDirEntryReplacement is serialized as a stub object replacing unreadable file or directory.
type UnreadableDirEntryReplacement struct {
	Info  string             `json:"info"`
	Error string             `json:"error"`
	Entry *snapshot.DirEntry `json:"entry"`
}

// DirRewriterOptions provides options for directory rewriter.
type DirRewriterOptions struct {
	Parallel int

	RewriteEntry RewriteDirEntryCallback

	// when != nil will be invoked to replace directory that can't be read,
	// by default RewriteAsStub()
	OnDirectoryReadFailure RewriteFailedEntryCallback
}

// DirRewriter rewrites contents of directories by walking the snapshot tree recursively.
type DirRewriter struct {
	ws   *workshare.Pool[*dirRewriterRequest]
	opts DirRewriterOptions

	cache *bigmap.Map

	rep repo.RepositoryWriter
}

type dirRewriterRequest struct {
	ctx                 context.Context //nolint:containedctx
	parentPath          string
	input               *snapshot.DirEntry
	result              *snapshot.DirEntry
	metadataCompression compression.Name
	err                 error
}

func (rw *DirRewriter) processRequest(pool *workshare.Pool[*dirRewriterRequest], req *dirRewriterRequest) {
	_ = pool

	req.result, req.err = rw.getCachedReplacement(req.ctx, req.parentPath, req.input, req.metadataCompression)
}

func (rw *DirRewriter) getCacheKey(input *snapshot.DirEntry) dirRewriterCacheKey {
	// cache key = SHA1 hash of the input as JSON (20 bytes)
	h := sha1.New()

	if err := json.NewEncoder(h).Encode(input); err != nil {
		impossible.PanicOnError(err)
	}

	var out dirRewriterCacheKey

	h.Sum(out[:0])

	return out
}

func (rw *DirRewriter) getCachedReplacement(ctx context.Context, parentPath string, input *snapshot.DirEntry, metadataComp compression.Name) (*snapshot.DirEntry, error) {
	key := rw.getCacheKey(input)

	// see if we already processed this exact directory entry
	cached, ok, err := rw.cache.Get(ctx, nil, key[:])
	if err != nil {
		return nil, errors.Wrap(err, "cache get")
	}

	if ok {
		de := &snapshot.DirEntry{}
		jerr := json.Unmarshal(cached, de)

		return de, errors.Wrap(jerr, "json unmarshal")
	}

	// entry not cached yet, run rewriter
	result, err := rw.opts.RewriteEntry(ctx, parentPath, input)

	// if the rewriter did not return the entry, return any error.
	if result == nil {
		return nil, err
	}

	// the rewriter returned a directory, we must recursively process it.
	if result.Type == snapshot.EntryTypeDirectory {
		rep2, subdirErr := rw.processDirectory(ctx, parentPath, result, metadataComp)
		if rep2 == nil {
			return nil, errors.Wrap(subdirErr, input.Name)
		}

		result = rep2
	}

	v, err := json.Marshal(result)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal JSON")
	}

	rw.cache.PutIfAbsent(ctx, key[:], v)

	return result, nil
}

func (rw *DirRewriter) processDirectory(ctx context.Context, pathFromRoot string, entry *snapshot.DirEntry, metadataComp compression.Name) (*snapshot.DirEntry, error) {
	dirRewriterLog(ctx).Debugw("processDirectory", "path", pathFromRoot)

	r, err := rw.rep.OpenObject(ctx, entry.ObjectID)
	if err != nil {
		return rw.opts.OnDirectoryReadFailure(ctx, pathFromRoot, entry, errors.Wrapf(err, "unable to open directory object %v", entry.ObjectID))
	}
	defer r.Close() //nolint:errcheck

	entries, _, err := readDirEntries(r)
	if err != nil {
		return rw.opts.OnDirectoryReadFailure(ctx, pathFromRoot, entry, errors.Wrap(err, "unable to read directory entries"))
	}

	return rw.processDirectoryEntries(ctx, pathFromRoot, entry, entries, metadataComp)
}

func (rw *DirRewriter) processDirectoryEntries(ctx context.Context, parentPath string, entry *snapshot.DirEntry, entries []*snapshot.DirEntry, metadataComp compression.Name) (*snapshot.DirEntry, error) {
	var (
		builder DirManifestBuilder
		wg      workshare.AsyncGroup[*dirRewriterRequest]
	)

	// ensure we wait for all work items before returning
	defer wg.Close()

	for _, child := range entries {
		if wg.CanShareWork(rw.ws) {
			// see if we can run this child in a goroutine
			wg.RunAsync(rw.ws, rw.processRequest, &dirRewriterRequest{
				ctx,
				path.Join(parentPath, child.Name),
				child,
				nil,
				metadataComp,
				nil,
			})

			continue
		}

		// run in current goroutine
		replacement, repErr := rw.getCachedReplacement(ctx, path.Join(parentPath, child.Name), child, metadataComp)
		if repErr != nil {
			return nil, errors.Wrap(repErr, child.Name)
		}

		if replacement == nil {
			continue
		}

		builder.AddEntry(replacement)
	}

	// now wait for all asynchronous work to complete and add resulting entries to
	// the builder
	for _, req := range wg.Wait() {
		if req.result != nil {
			builder.AddEntry(req.result)
		}
	}

	dm := builder.Build(entry.ModTime, entry.DirSummary.IncompleteReason)

	oid, err := writeDirManifest(ctx, rw.rep, entry.ObjectID.String(), dm, metadataComp)
	if err != nil {
		return nil, errors.Wrap(err, "unable to write directory manifest")
	}

	result := *entry
	result.DirSummary = dm.Summary
	result.ObjectID = oid

	return &result, nil
}

func (rw *DirRewriter) equalEntries(e1, e2 *snapshot.DirEntry) bool {
	if e1 == nil {
		return e2 == nil
	}

	if e2 == nil {
		return false
	}

	return rw.getCacheKey(e1) == rw.getCacheKey(e2)
}

// RewriteSnapshotManifest rewrites the directory tree starting at a given manifest.
func (rw *DirRewriter) RewriteSnapshotManifest(ctx context.Context, man *snapshot.Manifest, metadataComp compression.Name) (bool, error) {
	newEntry, err := rw.getCachedReplacement(ctx, ".", man.RootEntry, metadataComp)
	if err != nil {
		return false, errors.Wrapf(err, "error processing snapshot %v", man.ID)
	}

	if newEntry == nil {
		newEntry, err = rw.opts.OnDirectoryReadFailure(ctx, ".", man.RootEntry, errors.Errorf("invalid root directory %v", man.ID))
		if err != nil {
			return false, err
		}
	}

	if !rw.equalEntries(newEntry, man.RootEntry) {
		man.RootEntry = newEntry
		return true, nil
	}

	return false, nil
}

// Close closes the rewriter.
func (rw *DirRewriter) Close(ctx context.Context) {
	rw.ws.Close()

	rw.cache.Close(ctx)
}

// RewriteKeep is a callback that keeps the unreadable entry.
//
//nolint:revive
func RewriteKeep(ctx context.Context, parentPath string, input *snapshot.DirEntry, err error) (*snapshot.DirEntry, error) {
	return input, nil
}

// RewriteAsStub returns a callback that replaces the invalid entry with a stub that describes
// the error.
func RewriteAsStub(rep repo.RepositoryWriter) RewriteFailedEntryCallback {
	return func(ctx context.Context, parentPath string, input *snapshot.DirEntry, originalErr error) (*snapshot.DirEntry, error) {
		_ = parentPath

		var buf bytes.Buffer

		e := json.NewEncoder(&buf)
		e.SetIndent("  ", "    ")

		if err := e.Encode(UnreadableDirEntryReplacement{
			"Kopia replaced the original entry with this stub because of an error.",
			originalErr.Error(),
			input,
		}); err != nil {
			return nil, errors.Wrap(err, "error writing stub contents")
		}

		pol, _, _, err := policy.GetEffectivePolicy(ctx, rep, policy.GlobalPolicySourceInfo)
		if err != nil {
			return nil, errors.Wrap(err, "error getting policy")
		}

		metadataCompressor := pol.MetadataCompressionPolicy.MetadataCompressor()
		w := rep.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: metadataCompressor})

		n, err := buf.WriteTo(w)
		if err != nil {
			return nil, errors.Wrap(err, "error writing stub")
		}

		oid, err := w.Result()
		if err != nil {
			return nil, errors.Wrap(err, "error writing stub")
		}

		return &snapshot.DirEntry{
			Name:        ".INVALID." + input.Name,
			Type:        snapshot.EntryTypeFile,
			ModTime:     input.ModTime,
			FileSize:    n,
			UserID:      input.UserID,
			GroupID:     input.GroupID,
			ObjectID:    oid,
			Permissions: input.Permissions,
		}, nil
	}
}

// RewriteFail is a callback that fails the entire rewrite process when a directory is unreadable.
//
//nolint:revive
func RewriteFail(ctx context.Context, parentPath string, entry *snapshot.DirEntry, err error) (*snapshot.DirEntry, error) {
	return nil, err
}

// RewriteRemove is a callback that removes the entire failed entry.
//
//nolint:revive
func RewriteRemove(ctx context.Context, parentPath string, entry *snapshot.DirEntry, err error) (*snapshot.DirEntry, error) {
	return nil, nil
}

// NewDirRewriter creates a new directory rewriter.
func NewDirRewriter(ctx context.Context, rep repo.RepositoryWriter, opts DirRewriterOptions) (*DirRewriter, error) {
	if opts.Parallel == 0 {
		opts.Parallel = runtime.NumCPU()
	}

	if opts.OnDirectoryReadFailure == nil {
		opts.OnDirectoryReadFailure = RewriteFail
	}

	cache, err := bigmap.NewMap(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "new map")
	}

	return &DirRewriter{
		ws:    workshare.NewPool[*dirRewriterRequest](opts.Parallel - 1),
		opts:  opts,
		rep:   rep,
		cache: cache,
	}, nil
}
