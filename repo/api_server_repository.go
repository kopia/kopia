package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// APIServerInfo is remote repository configuration stored in local configuration.
type APIServerInfo struct {
	BaseURL                             string `json:"url"`
	TrustedServerCertificateFingerprint string `json:"serverCertFingerprint"`
	DisableGRPC                         bool   `json:"disableGRPC,omitempty"`
}

// remoteRepository is an implementation of Repository that connects to an instance of
// API server hosted by `kopia server`, instead of directly manipulating files in the BLOB storage.
type apiServerRepository struct {
	cli                              *apiclient.KopiaAPIClient
	h                                hashing.HashFunc
	objectFormat                     object.Format
	serverSupportsContentCompression bool
	cliOpts                          ClientOptions
	omgr                             *object.Manager
	wso                              WriteSessionOptions

	isSharedReadOnlySession bool
	contentCache            *cache.PersistentCache
}

func (r *apiServerRepository) APIServerURL() string {
	return r.cli.BaseURL
}

func (r *apiServerRepository) Description() string {
	if r.cliOpts.Description != "" {
		return r.cliOpts.Description
	}

	return fmt.Sprintf("Repository Server: %v", r.cli.BaseURL)
}

func (r *apiServerRepository) ClientOptions() ClientOptions {
	return r.cliOpts
}

func (r *apiServerRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	// nolint:wrapcheck
	return object.Open(ctx, r, id)
}

func (r *apiServerRepository) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.omgr.NewWriter(ctx, opt)
}

func (r *apiServerRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	// nolint:wrapcheck
	return object.VerifyObject(ctx, r, id)
}

func (r *apiServerRepository) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	var mm remoterepoapi.ManifestWithMetadata

	if err := r.cli.Get(ctx, "manifests/"+string(id), manifest.ErrNotFound, &mm); err != nil {
		return nil, errors.Wrap(err, "GetManifest")
	}

	// nolint:wrapcheck
	return mm.Metadata, json.Unmarshal(mm.Payload, data)
}

func (r *apiServerRepository) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	v, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal JSON")
	}

	req := &remoterepoapi.ManifestWithMetadata{
		Payload: json.RawMessage(v),
		Metadata: &manifest.EntryMetadata{
			Labels: labels,
		},
	}

	resp := &manifest.EntryMetadata{}

	if err := r.cli.Post(ctx, "manifests", req, resp); err != nil {
		return "", errors.Wrap(err, "PutManifest")
	}

	return resp.ID, nil
}

func (r *apiServerRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	uv := make(url.Values)

	for k, v := range labels {
		uv.Add(k, v)
	}

	var mm []*manifest.EntryMetadata

	if err := r.cli.Get(ctx, "manifests?"+uv.Encode(), nil, &mm); err != nil {
		return nil, errors.Wrap(err, "FindManifests")
	}

	return mm, nil
}

func (r *apiServerRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	return errors.Wrap(r.cli.Delete(ctx, "manifests/"+string(id), manifest.ErrNotFound, nil, nil), "DeleteManifest")
}

func (r *apiServerRepository) Time() time.Time {
	return clock.Now()
}

func (r *apiServerRepository) Refresh(ctx context.Context) error {
	return nil
}

func (r *apiServerRepository) Flush(ctx context.Context) error {
	return errors.Wrap(r.cli.Post(ctx, "flush", nil, nil), "Flush")
}

func (r *apiServerRepository) SupportsContentCompression() bool {
	return r.serverSupportsContentCompression
}

func (r *apiServerRepository) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	// apiServerRepository is stateless except object manager.
	r2 := *r
	w := &r2

	// create object manager using a remote repo as contentManager implementation.
	omgr, err := object.NewObjectManager(ctx, w, r.objectFormat)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error initializing object manager")
	}

	w.omgr = omgr
	w.wso = opt
	w.isSharedReadOnlySession = false

	return ctx, w, nil
}

func (r *apiServerRepository) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	var bi content.InfoStruct

	if err := r.cli.Get(ctx, "contents/"+contentID.String()+"?info=1", content.ErrContentNotFound, &bi); err != nil {
		return nil, errors.Wrap(err, "ContentInfo")
	}

	return &bi, nil
}

func (r *apiServerRepository) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := r.contentCache.GetOrLoad(ctx, contentID.String(), func(output *gather.WriteBuffer) error {
		var result []byte

		if err := r.cli.Get(ctx, "contents/"+contentID.String(), content.ErrContentNotFound, &result); err != nil {
			return errors.Wrap(err, "GetContent")
		}

		tmp.Write(result) // nolint:errcheck

		return nil
	}, &tmp)
	if err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	return tmp.ToByteSlice(), nil
}

func (r *apiServerRepository) WriteContent(ctx context.Context, data gather.Bytes, prefix content.IDPrefix, comp compression.HeaderID) (content.ID, error) {
	if err := prefix.ValidateSingle(); err != nil {
		return content.EmptyID, errors.Wrap(err, "invalid prefix")
	}

	var hashOutput [128]byte

	contentID, err := content.IDFromHash(prefix, r.h(hashOutput[:0], data))
	if err != nil {
		return content.EmptyID, errors.Wrap(err, "invalid content ID")
	}

	// avoid uploading the content body if it already exists.
	if _, err := r.ContentInfo(ctx, contentID); err == nil {
		// content already exists
		return contentID, nil
	}

	r.wso.OnUpload(int64(data.Length()))

	maybeCompression := ""
	if comp != content.NoCompression {
		maybeCompression = fmt.Sprintf("?compression=%x", comp)
	}

	if err := r.cli.Put(ctx, "contents/"+contentID.String()+maybeCompression, data.ToByteSlice(), nil); err != nil {
		return content.EmptyID, errors.Wrapf(err, "error writing content %v", contentID)
	}

	if prefix != "" {
		// add all prefixed contents to the cache.
		r.contentCache.Put(ctx, contentID.String(), data)
	}

	return contentID, nil
}

// UpdateDescription updates the description of a connected repository.
func (r *apiServerRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

func (r *apiServerRepository) Close(ctx context.Context) error {
	if r.isSharedReadOnlySession && r.contentCache != nil {
		r.contentCache.Close(ctx)
		r.contentCache = nil
	}

	return nil
}

func (r *apiServerRepository) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	// nolint:wrapcheck
	return object.PrefetchBackingContents(ctx, r, objectIDs, hint)
}

func (r *apiServerRepository) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	resp := &remoterepoapi.PrefetchContentsResponse{}

	if err := r.cli.Post(ctx, "contents/prefetch", remoterepoapi.PrefetchContentsRequest{
		ContentIDs: contentIDs,
		Hint:       hint,
	}, resp); err != nil {
		log(ctx).Warnf("unable to prefetch contents: %v", err)
		return nil
	}

	return resp.ContentIDs
}

var _ Repository = (*apiServerRepository)(nil)

// openRestAPIRepository connects remote repository over Kopia API.
func openRestAPIRepository(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, contentCache *cache.PersistentCache, password string) (Repository, error) {
	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            cliOpts.UsernameAtHost(),
		Password:                            password,
		LogRequests:                         true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create API client")
	}

	rr := &apiServerRepository{
		cli:          cli,
		cliOpts:      cliOpts,
		contentCache: contentCache,
		wso: WriteSessionOptions{
			OnUpload: func(i int64) {},
		},
		isSharedReadOnlySession: true,
	}

	var p remoterepoapi.Parameters

	if err = cli.Get(ctx, "repo/parameters", nil, &p); err != nil {
		return nil, errors.Wrap(err, "unable to get repository parameters")
	}

	hf, err := hashing.CreateHashFunc(&p)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create hash function")
	}

	rr.h = hf
	rr.objectFormat = p.Format
	rr.serverSupportsContentCompression = p.SupportsContentCompression

	// create object manager using rr as contentManager implementation.
	omgr, err := object.NewObjectManager(ctx, rr, rr.objectFormat)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing object manager")
	}

	rr.omgr = omgr

	return rr, nil
}

// ConnectAPIServer sets up repository connection to a particular API server.
func ConnectAPIServer(ctx context.Context, configFile string, si *APIServerInfo, password string, opt *ConnectOptions) error {
	lc := LocalConfig{
		APIServer:     si,
		ClientOptions: opt.ClientOptions.ApplyDefaults(ctx, "API Server: "+si.BaseURL),
	}

	if err := setupCachingOptionsWithDefaults(ctx, configFile, &lc, &opt.CachingOptions, []byte(si.BaseURL)); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	if err := lc.writeToFile(configFile); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return verifyConnect(ctx, configFile, password)
}
