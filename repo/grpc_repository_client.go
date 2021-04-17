package repo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ctxutil"
	apipb "github.com/kopia/kopia/internal/grpcapi"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// MaxGRPCMessageSize is the maximum size of a message sent or received over GRPC API when talking to
// Kopia repository server. This is bigger than the size of any possible content, which is
// defined by supported splitters.
const MaxGRPCMessageSize = 20 << 20

var errShouldRetry = errors.New("should retry")

func errNoSessionResponse() error {
	return errors.New("did not receive response from the server")
}

// grpcRepositoryClient is an implementation of Repository that connects to an instance of
// GPRC API server hosted by `kopia server`.
type grpcRepositoryClient struct {
	connRefCount *int32
	conn         *grpc.ClientConn

	innerSessionMutex sync.Mutex
	innerSession      *grpcInnerSession

	opt                WriteSessionOptions
	isReadOnly         bool
	transparentRetries bool

	// how many times we tried to establish inner session
	innerSessionAttemptCount int

	h            hashing.HashFunc
	objectFormat object.Format
	cliOpts      ClientOptions
	omgr         *object.Manager

	contentCache *cache.PersistentCache
}

type grpcInnerSession struct {
	sendMutex sync.Mutex

	activeRequestsMutex sync.Mutex
	nextRequestID       int64
	activeRequests      map[int64]chan *apipb.SessionResponse
	cli                 apipb.KopiaRepository_SessionClient
	repoParams          *apipb.RepositoryParameters
}

// readLoop runs in a goroutine and consumes all messages in session and forwards them to appropriate channels.
func (r *grpcInnerSession) readLoop(ctx context.Context) {
	msg, err := r.cli.Recv()

	for ; err == nil; msg, err = r.cli.Recv() {
		r.activeRequestsMutex.Lock()
		ch := r.activeRequests[msg.RequestId]
		delete(r.activeRequests, msg.RequestId)

		r.activeRequestsMutex.Unlock()

		ch <- msg
		close(ch)
	}

	log(ctx).Debugf("GRPC stream read loop terminated with %v", err)

	// when a read loop error occurs, close all pending client channels with an artificial error.
	r.activeRequestsMutex.Lock()
	defer r.activeRequestsMutex.Unlock()

	for id := range r.activeRequests {
		r.sendStreamBrokenAndClose(r.getAndDeleteResponseChannelLocked(id), err)
	}
}

// sendRequest sends the provided request to the server and returns a channel on which the
// caller can receive session response(s).
func (r *grpcInnerSession) sendRequest(ctx context.Context, req *apipb.SessionRequest) chan *apipb.SessionResponse {
	_ = ctx

	// allocate request ID and create channel to which we're forwarding the responses.
	r.activeRequestsMutex.Lock()
	rid := r.nextRequestID
	r.nextRequestID++

	ch := make(chan *apipb.SessionResponse, 1)

	r.activeRequests[rid] = ch
	r.activeRequestsMutex.Unlock()

	req.RequestId = rid

	// sends to GRPC stream must be single-threaded.
	r.sendMutex.Lock()
	defer r.sendMutex.Unlock()

	// try sending the request and if unable to do so, stuff an error response to the channel
	// to simplify client code.
	if err := r.cli.Send(req); err != nil {
		r.activeRequestsMutex.Lock()
		ch2 := r.getAndDeleteResponseChannelLocked(rid)
		r.activeRequestsMutex.Unlock()

		r.sendStreamBrokenAndClose(ch2, err)
	}

	return ch
}

func (r *grpcInnerSession) getAndDeleteResponseChannelLocked(rid int64) chan *apipb.SessionResponse {
	ch := r.activeRequests[rid]
	delete(r.activeRequests, rid)

	return ch
}

func (r *grpcInnerSession) sendStreamBrokenAndClose(ch chan *apipb.SessionResponse, err error) {
	if ch != nil {
		ch <- &apipb.SessionResponse{
			Response: &apipb.SessionResponse_Error{
				Error: &apipb.ErrorResponse{
					Code:    apipb.ErrorResponse_STREAM_BROKEN,
					Message: err.Error(),
				},
			},
		}

		close(ch)
	}
}

// Description returns description associated with a repository client.
func (r *grpcRepositoryClient) Description() string {
	if r.cliOpts.Description != "" {
		return r.cliOpts.Description
	}

	return "Repository Server"
}

func (r *grpcRepositoryClient) LegacyWriter() RepositoryWriter {
	return nil
}

func (r *grpcRepositoryClient) ClientOptions() ClientOptions {
	return r.cliOpts
}

func (r *grpcRepositoryClient) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	return object.Open(ctx, r, id)
}

func (r *grpcRepositoryClient) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.omgr.NewWriter(ctx, opt)
}

func (r *grpcRepositoryClient) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	return object.VerifyObject(ctx, r, id)
}

func (r *grpcInnerSession) initializeSession(ctx context.Context, purpose string, readOnly bool) (*apipb.RepositoryParameters, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_InitializeSession{
			InitializeSession: &apipb.InitializeSessionRequest{
				Purpose:  purpose,
				ReadOnly: readOnly,
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_InitializeSession:
			return rr.InitializeSession.GetParameters(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	v, err := r.maybeRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return sess.GetManifest(ctx, id, data)
	})
	if err != nil {
		return nil, err
	}

	return v.(*manifest.EntryMetadata), nil
}

func (r *grpcInnerSession) GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetManifest{
			GetManifest: &apipb.GetManifestRequest{
				ManifestId: string(id),
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_GetManifest:
			return decodeManifestEntryMetadata(rr.GetManifest.GetMetadata()), json.Unmarshal(rr.GetManifest.GetJsonData(), data)

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func decodeManifestEntryMetadataList(md []*apipb.ManifestEntryMetadata) []*manifest.EntryMetadata {
	var result []*manifest.EntryMetadata

	for _, v := range md {
		result = append(result, decodeManifestEntryMetadata(v))
	}

	return result
}

func decodeManifestEntryMetadata(md *apipb.ManifestEntryMetadata) *manifest.EntryMetadata {
	return &manifest.EntryMetadata{
		ID:      manifest.ID(md.Id),
		Length:  int(md.Length),
		Labels:  md.GetLabels(),
		ModTime: time.Unix(0, md.GetModTimeNanos()),
	}
}

func (r *grpcRepositoryClient) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	v, err := r.inSessionWithoutRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return sess.PutManifest(ctx, labels, payload)
	})
	if err != nil {
		return "", err
	}

	return v.(manifest.ID), nil
}

func (r *grpcInnerSession) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	v, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal JSON")
	}

	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_PutManifest{
			PutManifest: &apipb.PutManifestRequest{
				JsonData: v,
				Labels:   labels,
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_PutManifest:
			return manifest.ID(rr.PutManifest.GetManifestId()), nil

		default:
			return "", unhandledSessionResponse(resp)
		}
	}

	return "", errNoSessionResponse()
}

func (r *grpcRepositoryClient) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	v, err := r.maybeRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return sess.FindManifests(ctx, labels)
	})
	if err != nil {
		return nil, err
	}

	return v.([]*manifest.EntryMetadata), nil
}

func (r *grpcInnerSession) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_FindManifests{
			FindManifests: &apipb.FindManifestsRequest{
				Labels: labels,
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_FindManifests:
			return decodeManifestEntryMetadataList(rr.FindManifests.GetMetadata()), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) DeleteManifest(ctx context.Context, id manifest.ID) error {
	_, err := r.inSessionWithoutRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return false, sess.DeleteManifest(ctx, id)
	})

	return err
}

func (r *grpcInnerSession) DeleteManifest(ctx context.Context, id manifest.ID) error {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_DeleteManifest{
			DeleteManifest: &apipb.DeleteManifestRequest{
				ManifestId: string(id),
			},
		},
	}) {
		switch resp.Response.(type) {
		case *apipb.SessionResponse_DeleteManifest:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}

	return errNoSessionResponse()
}

func (r *grpcRepositoryClient) Time() time.Time {
	return clock.Now()
}

func (r *grpcRepositoryClient) Refresh(ctx context.Context) error {
	return nil
}

func (r *grpcRepositoryClient) Flush(ctx context.Context) error {
	_, err := r.inSessionWithoutRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return false, sess.Flush(ctx)
	})

	return err
}

func (r *grpcInnerSession) Flush(ctx context.Context) error {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_Flush{
			Flush: &apipb.FlushRequest{},
		},
	}) {
		switch resp.Response.(type) {
		case *apipb.SessionResponse_Flush:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}

	return errNoSessionResponse()
}

func (r *grpcRepositoryClient) NewWriter(ctx context.Context, opt WriteSessionOptions) (RepositoryWriter, error) {
	w, err := newGRPCAPIRepositoryForConnection(ctx, r.conn, r.connRefCount, r.cliOpts, opt, r.contentCache, false)
	if err != nil {
		return nil, err
	}

	return w, nil
}

type sessionAttemptFunc func(ctx context.Context, sess *grpcInnerSession) (interface{}, error)

// maybeRetry executes the provided callback with or without automatic retries depending on how
// the grpcRepositoryClient is configured.
func (r *grpcRepositoryClient) maybeRetry(ctx context.Context, attempt sessionAttemptFunc) (interface{}, error) {
	if !r.transparentRetries {
		return r.inSessionWithoutRetry(ctx, attempt)
	}

	return r.retry(ctx, attempt)
}

// retry executes the provided callback and provides it with *grpcInnerSession.
// If the grpcRepositoryClient set to automatically retry and the provided callback returns io.EOF,
// the inner session will be killed and re-established as necessary.
func (r *grpcRepositoryClient) retry(ctx context.Context, attempt sessionAttemptFunc) (interface{}, error) {
	return retry.WithExponentialBackoff(ctx, "invoking GRPC API", func() (interface{}, error) {
		v, err := r.inSessionWithoutRetry(ctx, attempt)
		if errors.Is(err, io.EOF) {
			r.killInnerSession()

			return nil, errShouldRetry //nolint:wrapcheck
		}

		return v, err
	}, func(err error) bool {
		return errors.Is(err, errShouldRetry)
	})
}

func (r *grpcRepositoryClient) inSessionWithoutRetry(ctx context.Context, attempt sessionAttemptFunc) (interface{}, error) {
	sess, err := r.getOrEstablishInnerSession(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to establish session for purpose=%v", r.opt.Purpose)
	}

	return attempt(ctx, sess)
}

func (r *grpcRepositoryClient) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	v, err := r.maybeRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return sess.contentInfo(ctx, contentID)
	})
	if err != nil {
		return content.Info{}, err
	}

	return v.(content.Info), nil
}

func (r *grpcInnerSession) contentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContentInfo{
			GetContentInfo: &apipb.GetContentInfoRequest{
				ContentId: string(contentID),
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_GetContentInfo:
			return content.Info{
				ID:               content.ID(rr.GetContentInfo.GetInfo().GetId()),
				PackedLength:     rr.GetContentInfo.GetInfo().GetPackedLength(),
				TimestampSeconds: rr.GetContentInfo.GetInfo().GetTimestampSeconds(),
				PackBlobID:       blob.ID(rr.GetContentInfo.GetInfo().GetPackBlobId()),
				PackOffset:       rr.GetContentInfo.GetInfo().GetPackOffset(),
				Deleted:          rr.GetContentInfo.GetInfo().GetDeleted(),
				FormatVersion:    byte(rr.GetContentInfo.GetInfo().GetFormatVersion()),
				OriginalLength:   rr.GetContentInfo.GetInfo().GetOriginalLength(),
			}, nil

		default:
			return content.Info{}, unhandledSessionResponse(resp)
		}
	}

	return content.Info{}, errNoSessionResponse()
}

func errorFromSessionResponse(rr *apipb.ErrorResponse) error {
	switch rr.GetCode() {
	case apipb.ErrorResponse_MANIFEST_NOT_FOUND:
		return manifest.ErrNotFound
	case apipb.ErrorResponse_OBJECT_NOT_FOUND:
		return object.ErrObjectNotFound
	case apipb.ErrorResponse_CONTENT_NOT_FOUND:
		return content.ErrContentNotFound
	case apipb.ErrorResponse_STREAM_BROKEN:
		return errors.Wrap(io.EOF, rr.Message)
	default:
		return errors.New(rr.Message)
	}
}

func unhandledSessionResponse(resp *apipb.SessionResponse) error {
	if e := resp.GetError(); e != nil {
		return errorFromSessionResponse(e)
	}

	return errors.Errorf("unsupported session response: %v", resp)
}

func (r *grpcRepositoryClient) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	return r.contentCache.GetOrLoad(ctx, string(contentID), func() ([]byte, error) {
		v, err := r.maybeRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
			return sess.GetContent(ctx, contentID)
		})
		if err != nil {
			return nil, err
		}

		return v.([]byte), nil
	})
}

func (r *grpcInnerSession) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContent{
			GetContent: &apipb.GetContentRequest{
				ContentId: string(contentID),
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_GetContent:
			return rr.GetContent.GetData(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error) {
	if err := content.ValidatePrefix(prefix); err != nil {
		return "", errors.Wrap(err, "invalid prefix")
	}

	var hashOutput [128]byte

	contentID := prefix + content.ID(hex.EncodeToString(r.h(hashOutput[:0], data)))

	// avoid uploading the content body if it already exists.
	if _, err := r.ContentInfo(ctx, contentID); err == nil {
		// content already exists
		return contentID, nil
	}

	r.opt.OnUpload(int64(len(data)))

	v, err := r.inSessionWithoutRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		return sess.WriteContent(ctx, data, prefix)
	})
	if err != nil {
		return "", err
	}

	if prefix != "" {
		// add all prefixed contents to the cache.
		r.contentCache.Put(ctx, string(contentID), data)
	}

	return v.(content.ID), nil
}

func (r *grpcInnerSession) WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error) {
	if err := content.ValidatePrefix(prefix); err != nil {
		return "", errors.Wrap(err, "invalid prefix")
	}

	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_WriteContent{
			WriteContent: &apipb.WriteContentRequest{
				Data:   data,
				Prefix: string(prefix),
			},
		},
	}) {
		switch rr := resp.Response.(type) {
		case *apipb.SessionResponse_WriteContent:
			return content.ID(rr.WriteContent.GetContentId()), nil

		default:
			return "", unhandledSessionResponse(resp)
		}
	}

	return "", errNoSessionResponse()
}

// UpdateDescription updates the description of a connected repository.
func (r *grpcRepositoryClient) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

func (r *grpcRepositoryClient) Close(ctx context.Context) error {
	if r.omgr == nil {
		// already closed
		return nil
	}

	if err := r.omgr.Close(); err != nil {
		return errors.Wrap(err, "error closing object manager")
	}

	r.omgr = nil

	if atomic.AddInt32(r.connRefCount, -1) == 0 {
		log(ctx).Debugf("closing GPRC connection to %v", r.conn.Target())

		defer r.contentCache.Close(ctx)

		return errors.Wrap(r.conn.Close(), "error closing GRPC connection")
	}

	return nil
}

var _ Repository = (*grpcRepositoryClient)(nil)

type grpcCreds struct {
	hostname string
	username string
	password string
}

func (c grpcCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"kopia-hostname":   c.hostname,
		"kopia-username":   c.username,
		"kopia-password":   c.password,
		"kopia-version":    BuildVersion,
		"kopia-build-info": BuildInfo,
		"kopia-repo":       BuildGitHubRepo,
		"kopia-os":         runtime.GOOS,
		"kopia-arch":       runtime.GOARCH,
	}, nil
}

func (c grpcCreds) RequireTransportSecurity() bool {
	return true
}

// OpenGRPCAPIRepository opens the Repository based on remote GRPC server.
// The APIServerInfo must have the address of the repository as 'https://host:port'
func OpenGRPCAPIRepository(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, contentCache *cache.PersistentCache, password string) (Repository, error) {
	var transportCreds credentials.TransportCredentials

	if si.TrustedServerCertificateFingerprint != "" {
		transportCreds = credentials.NewTLS(tlsutil.TLSConfigTrustingSingleCertificate(si.TrustedServerCertificateFingerprint))
	} else {
		transportCreds = credentials.NewClientTLSFromCert(nil, "")
	}

	u, err := url.Parse(si.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse server URL")
	}

	if u.Scheme != "kopia" && u.Scheme != "https" {
		return nil, errors.Errorf("invalid server address, must be 'https://host:port'")
	}

	conn, err := grpc.Dial(
		u.Hostname()+":"+u.Port(),
		grpc.WithPerRPCCredentials(grpcCreds{cliOpts.Hostname, cliOpts.Username, password}),
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, errors.Wrap(err, "dial error")
	}

	rep, err := newGRPCAPIRepositoryForConnection(ctx, conn, new(int32), cliOpts, WriteSessionOptions{}, contentCache, true)
	if err != nil {
		return nil, err
	}

	return rep, nil
}

func (r *grpcRepositoryClient) getOrEstablishInnerSession(ctx context.Context) (*grpcInnerSession, error) {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.innerSession == nil {
		cli := apipb.NewKopiaRepositoryClient(r.conn)

		log(ctx).Debugf("establishing new GRPC streaming session (purpose=%v)", r.opt.Purpose)

		retryPolicy := retry.Always
		if r.transparentRetries && r.innerSessionAttemptCount == 0 {
			// the first time the read-only session is established, don't do retries
			// to avoid spinning in place while the server is not connectable.
			retryPolicy = retry.Never
		}

		r.innerSessionAttemptCount++

		v, err := retry.WithExponentialBackoff(ctx, "establishing session", func() (interface{}, error) {
			sess, err := cli.Session(ctxutil.Detach(ctx))
			if err != nil {
				return nil, errors.Wrap(err, "Session()")
			}

			newSess := &grpcInnerSession{
				cli:            sess,
				activeRequests: make(map[int64]chan *apipb.SessionResponse),
				nextRequestID:  1,
			}

			go newSess.readLoop(ctx)

			newSess.repoParams, err = newSess.initializeSession(ctx, r.opt.Purpose, r.isReadOnly)
			if err != nil {
				return nil, errors.Wrap(err, "unable to initialize session")
			}

			return newSess, nil
		}, retryPolicy)
		if err != nil {
			return nil, errors.Wrap(err, "error establishing session")
		}

		r.innerSession = v.(*grpcInnerSession)
	}

	return r.innerSession, nil
}

func (r *grpcRepositoryClient) killInnerSession() {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.innerSession != nil {
		r.innerSession.cli.CloseSend() //nolint:errcheck
		r.innerSession = nil
	}
}

// newGRPCAPIRepositoryForConnection opens GRPC-based repository connection.
func newGRPCAPIRepositoryForConnection(ctx context.Context, conn *grpc.ClientConn, connRefCount *int32, cliOpts ClientOptions, opt WriteSessionOptions, contentCache *cache.PersistentCache, transparentRetries bool) (*grpcRepositoryClient, error) {
	if opt.OnUpload == nil {
		opt.OnUpload = func(i int64) {}
	}

	rr := &grpcRepositoryClient{
		connRefCount:       connRefCount,
		conn:               conn,
		cliOpts:            cliOpts,
		transparentRetries: transparentRetries,
		opt:                opt,
		isReadOnly:         cliOpts.ReadOnly,
		contentCache:       contentCache,
	}

	v, err := rr.inSessionWithoutRetry(ctx, func(ctx context.Context, sess *grpcInnerSession) (interface{}, error) {
		p := sess.repoParams
		hf, err := hashing.CreateHashFunc(p)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create hash function")
		}

		rr.h = hf

		rr.objectFormat = object.Format{
			Splitter: p.Splitter,
		}

		rr.omgr, err = object.NewObjectManager(ctx, rr, rr.objectFormat)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize object manager")
		}

		atomic.AddInt32(connRefCount, 1)

		return rr, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*grpcRepositoryClient), nil
}
