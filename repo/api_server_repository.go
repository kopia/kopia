package repo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// APIServerInfo is remote repository configuration stored in local configuration.
type APIServerInfo struct {
	BaseURL                             string `json:"url"`
	TrustedServerCertificateFingerprint string `json:"serverCertFingerprint"`
}

// remoteRepository is an implementation of Repository that connects to an instance of
// API server hosted by `kopia server`, instead of directly manipulating files in the BLOB storage.
type apiServerRepository struct {
	cli *apiclient.KopiaAPIClient
	h   hashing.HashFunc

	omgr    *object.Manager
	cliOpts ClientOptions
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
	return r.omgr.Open(ctx, id)
}

func (r *apiServerRepository) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.omgr.NewWriter(ctx, opt)
}

func (r *apiServerRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	return r.omgr.VerifyObject(ctx, id)
}

func (r *apiServerRepository) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	var mm remoterepoapi.ManifestWithMetadata

	if err := r.cli.Get(ctx, "manifests/"+string(id), manifest.ErrNotFound, &mm); err != nil {
		return nil, errors.Wrap(err, "GetManifest")
	}

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
	return errors.Wrap(r.cli.Delete(ctx, "manifests/"+string(id), nil, nil), "DeleteManifest")
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

func (r *apiServerRepository) Close(ctx context.Context) error {
	if err := r.omgr.Close(); err != nil {
		return errors.Wrap(err, "error closing object manager")
	}

	return errors.Wrap(r.Flush(ctx), "Close")
}

func (r *apiServerRepository) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	var bi content.Info

	if err := r.cli.Get(ctx, "contents/"+string(contentID)+"?info=1", content.ErrContentNotFound, &bi); err != nil {
		return content.Info{}, errors.Wrap(err, "ContentInfo")
	}

	return bi, nil
}

func (r *apiServerRepository) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	var result []byte

	if err := r.cli.Get(ctx, "contents/"+string(contentID), content.ErrContentNotFound, &result); err != nil {
		return nil, errors.Wrap(err, "GetContent")
	}

	return result, nil
}

func (r *apiServerRepository) WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error) {
	if err := content.ValidatePrefix(prefix); err != nil {
		return "", errors.Wrap(err, "invalid prefix")
	}

	var hashOutput [128]byte

	contentID := prefix + content.ID(hex.EncodeToString(r.h(hashOutput[:0], data)))

	if err := r.cli.Put(ctx, "contents/"+string(contentID), data, nil); err != nil {
		return "", errors.Wrapf(err, "error writing content %v", contentID)
	}

	return contentID, nil
}

// UpdateDescription updates the description of a connected repository.
func (r *apiServerRepository) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

var _ Repository = (*apiServerRepository)(nil)

// openAPIServer connects remote repository over Kopia API.
func openAPIServer(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, password string) (Repository, error) {
	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             si.BaseURL,
		TrustedServerCertificateFingerprint: si.TrustedServerCertificateFingerprint,
		Username:                            cliOpts.Username + "@" + cliOpts.Hostname,
		Password:                            password,
		LogRequests:                         true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create API client")
	}

	rr := &apiServerRepository{
		cli:     cli,
		cliOpts: cliOpts,
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

	// create object manager using rr as contentManager implementation.
	omgr, err := object.NewObjectManager(ctx, rr, p.Format, object.ManagerOptions{})
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

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return errors.Wrap(err, "unable to marshal config JSON")
	}

	if err = os.MkdirAll(filepath.Dir(configFile), 0o700); err != nil {
		return errors.Wrap(err, "unable to create config directory")
	}

	if err = ioutil.WriteFile(configFile, d, 0o600); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return verifyConnect(ctx, configFile, password, opt.PersistCredentials)
}
