package repo

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"hash"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/pkg/errors"
)

var _ RepositoryWriter = &OverlayRepositoryClientWrapper{}

type manifestEntry struct {
	ID      manifest.ID       `json:"id"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"modified"`
	Deleted bool              `json:"deleted,omitempty"`
	Content json.RawMessage   `json:"data"`
}

// OverlayRepositoryClientWrapper is an implementation of RepositoryWriter
// with nullified write operations.
type OverlayRepositoryClientWrapper struct {
	RepositoryWriter

	m sync.Mutex

	manifestsOverlay map[manifest.ID]*manifestEntry
	objects          map[object.ID][]byte
}

// LegacyWriter returns nil. This can be implemented as needed.
func (r *OverlayRepositoryClientWrapper) LegacyWriter() RepositoryWriter {
	return nil
}

func copyLabels(m map[string]string) map[string]string {
	r := map[string]string{}
	for k, v := range m {
		r[k] = v
	}

	return r
}

// PutManifest generates a random fake manifest-id without actual writes.
func (r *OverlayRepositoryClientWrapper) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	if labels[manifest.TypeLabelKey] == "" {
		return "", errors.Errorf("'type' label is required")
	}

	var (
		e = &manifestEntry{
			ModTime: r.RepositoryWriter.Time().UTC(),
			Labels:  copyLabels(labels),
		}
		err error
	)

	e.Content, err = json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "marshal error")
	}

	e.ID, err = manifest.GenManifestID()
	if err != nil {
		return "", errors.Wrap(err, "can't initialize randomness")
	}

	r.m.Lock()
	r.manifestsOverlay[e.ID] = e
	r.m.Unlock()

	return e.ID, nil
}

// DeleteManifest skips the delete operation on the internal repo.
func (r *OverlayRepositoryClientWrapper) DeleteManifest(ctx context.Context, id manifest.ID) error {
	r.m.Lock()
	if e, overlayExists := r.manifestsOverlay[id]; overlayExists {
		e.ModTime = r.RepositoryWriter.Time().UTC()
		e.Deleted = true

		r.m.Unlock()
		return nil
	}
	r.m.Unlock()

	var b json.RawMessage
	if _, err := r.RepositoryWriter.GetManifest(ctx, id, &b); err != nil {
		if errors.Is(err, manifest.ErrNotFound) {
			return nil
		}

		return errors.Wrapf(err, "error getting metadata for %q", id)
	}

	r.m.Lock()
	r.manifestsOverlay[id] = &manifestEntry{
		ID:      id,
		ModTime: r.RepositoryWriter.Time().UTC(),
		Deleted: true,
	}
	r.m.Unlock()

	return nil
}

// Flush skips the internal repo flush operation.
func (r *OverlayRepositoryClientWrapper) Flush(ctx context.Context) error {
	return nil
}

// UpdateDescription ensures that the internal repo description cannot be updated.
func (r *OverlayRepositoryClientWrapper) UpdateDescription(d string) {
}

func (r *OverlayRepositoryClientWrapper) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	r.m.Lock()
	if e, overlayExists := r.manifestsOverlay[id]; overlayExists {
		if e.Deleted {
			r.m.Unlock()
			return nil, manifest.ErrNotFound
		}

		if err := json.Unmarshal(e.Content, data); err != nil {
			r.m.Unlock()
			return nil, errors.Wrap(err, "unmarshal error")
		}
		r.m.Unlock()

		return &manifest.EntryMetadata{
			ID:      e.ID,
			Length:  0,
			Labels:  copyLabels(e.Labels),
			ModTime: e.ModTime,
		}, nil
	}
	r.m.Unlock()

	return r.RepositoryWriter.GetManifest(ctx, id, data)
}

func (r *OverlayRepositoryClientWrapper) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	return ctx, r, nil
}

func (r *OverlayRepositoryClientWrapper) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	r.m.Lock()
	if data, found := r.objects[id]; found {
		r.m.Unlock()
		return object.NewObjectReaderWithData(data), nil
	}
	r.m.Unlock()

	return r.RepositoryWriter.OpenObject(ctx, id)
}

func (r *OverlayRepositoryClientWrapper) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	r.m.Lock()
	if _, found := r.objects[id]; found {
		r.m.Unlock()
		cid, _, _ := id.ContentID()
		return []content.ID{cid}, nil
	}
	r.m.Unlock()

	return r.RepositoryWriter.VerifyObject(ctx, id)
}

func (r *OverlayRepositoryClientWrapper) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	r.m.Lock()
	// all objects in the overlay are direct objects / have single content block
	oid := object.DirectObjectID(contentID)
	if data, found := r.objects[oid]; found {
		info := &content.InfoStruct{ContentID: contentID, OriginalLength: uint32(len(data))}
		r.m.Unlock()
		return info, nil
	}
	r.m.Unlock()

	return r.RepositoryWriter.ContentInfo(ctx, contentID)
}

func (r *OverlayRepositoryClientWrapper) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	var (
		overlayCids      []content.ID
		oidsNotInOverlay []object.ID
	)

	r.m.Lock()
	for _, oid := range objectIDs {
		if _, found := r.objects[oid]; found {
			// all objects in the overlay are direct objects / have single content block
			cid, _, _ := oid.ContentID()
			overlayCids = append(overlayCids, cid)
		} else {
			oidsNotInOverlay = append(oidsNotInOverlay, oid)
		}
	}
	r.m.Unlock()

	if len(oidsNotInOverlay) == 0 {
		return overlayCids, nil
	}

	cids, err := r.RepositoryWriter.PrefetchObjects(ctx, oidsNotInOverlay, hint)
	if err != nil {
		return nil, err
	}

	return append(overlayCids, cids...), nil
}

func (r *OverlayRepositoryClientWrapper) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	var (
		overlayCids      []content.ID
		cidsNotInOverlay []content.ID
	)

	r.m.Lock()
	for _, cid := range contentIDs {
		// all objects in the overlay are direct objects / have single content block
		oid := object.DirectObjectID(cid)
		if _, found := r.objects[oid]; found {
			overlayCids = append(overlayCids, cid)
		} else {
			cidsNotInOverlay = append(cidsNotInOverlay, cid)
		}
	}
	r.m.Unlock()

	if len(cidsNotInOverlay) == 0 {
		return overlayCids
	}

	return append(overlayCids, r.RepositoryWriter.PrefetchContents(ctx, cidsNotInOverlay, hint)...)
}

// NewObjectWriter returns a null-object-writer implemention.
func (r *OverlayRepositoryClientWrapper) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return &overlayObjectWriterWrapper{r: r, h: sha256.New()}
}

type overlayObjectWriterWrapper struct {
	r    *OverlayRepositoryClientWrapper
	h    hash.Hash
	data []byte
}

func (w *overlayObjectWriterWrapper) Close() error {
	return nil
}

func (w *overlayObjectWriterWrapper) Write(data []byte) (n int, err error) {
	n, err = w.h.Write(data)
	w.data = append(w.data, data[0:n]...)
	return n, err
}

func (w *overlayObjectWriterWrapper) Result() (object.ID, error) {
	cid, err := content.IDFromHash("", w.h.Sum(nil))
	if err != nil {
		// nolint:wrapcheck
		return object.EmptyID, err
	}

	// all objects have a single content
	oid := object.DirectObjectID(cid)

	w.r.m.Lock()
	w.r.objects[oid] = w.data
	w.r.m.Unlock()

	return oid, nil
}

func (w *overlayObjectWriterWrapper) Checkpoint() (object.ID, error) {
	return w.Result()
}

// NewOverlayRepositoryClientWrapper is an implementation of Repository that
// redirects all write operations to the repository.
func NewOverlayRepositoryClientWrapper(rep RepositoryWriter) *OverlayRepositoryClientWrapper {
	return &OverlayRepositoryClientWrapper{
		RepositoryWriter: rep,
		manifestsOverlay: make(map[manifest.ID]*manifestEntry),
		objects:          make(map[object.ID][]byte),
	}
}
