package repo

import (
	"context"
	"encoding/json"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/kopia/kopia/internal/clock"
	apipb "github.com/kopia/kopia/internal/grpcapi"
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

func errNoSessionResponse() error {
	return errors.New("did not receive response from the server")
}

// grpcRepositoryClient is an implementation of Repository that connects to an instance of
// GPRC API server hosted by `kopia server`.
type grpcRepositoryClient struct {
	connRefCount *int32
	conn         *grpc.ClientConn
	sess         apipb.KopiaRepository_SessionClient

	h            hashing.HashFunc
	objectFormat object.Format
	cliOpts      ClientOptions
	omgr         *object.Manager

	sendMutex sync.Mutex

	activeRequestsMutex sync.Mutex
	nextRequestID       int64
	activeRequests      map[int64]chan *apipb.SessionResponse
}

// readLoop runs in a goroutine and consumes all messages in session and forwards them to appropriate channels.
func (r *grpcRepositoryClient) readLoop() {
	msg, err := r.sess.Recv()

	for ; err == nil; msg, err = r.sess.Recv() {
		r.activeRequestsMutex.Lock()
		ch := r.activeRequests[msg.RequestId]
		delete(r.activeRequests, msg.RequestId)

		r.activeRequestsMutex.Unlock()

		ch <- msg
		close(ch)
	}

	// when a read loop error occurs, close all pending client channels with an artificial error.
	r.activeRequestsMutex.Lock()
	defer r.activeRequestsMutex.Unlock()

	errResponse := &apipb.SessionResponse{
		Response: &apipb.SessionResponse_Error{
			Error: &apipb.ErrorResponse{
				Code:    apipb.ErrorResponse_UNKNOWN_ERROR,
				Message: err.Error(),
			},
		},
	}

	for id, ch := range r.activeRequests {
		delete(r.activeRequests, id)

		ch <- errResponse
		close(ch)
	}
}

// sendRequest sends the provided request to the server and returns a channel on which the
// caller can receive session response(s).
func (r *grpcRepositoryClient) sendRequest(req *apipb.SessionRequest) chan *apipb.SessionResponse {
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
	if err := r.sess.Send(req); err != nil {
		ch <- &apipb.SessionResponse{
			Response: &apipb.SessionResponse_Error{
				Error: &apipb.ErrorResponse{
					Code:    apipb.ErrorResponse_CLIENT_ERROR,
					Message: err.Error(),
				},
			},
		}
	}

	return ch
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

func (r *grpcRepositoryClient) initializeSession(purpose string, readOnly bool) (*apipb.RepositoryParameters, error) {
	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	v, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal JSON")
	}

	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	for resp := range r.sendRequest(&apipb.SessionRequest{
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

func (r *grpcRepositoryClient) NewWriter(ctx context.Context, purpose string) (RepositoryWriter, error) {
	return NewGRPCAPIRepositoryForConnection(ctx, r.conn, r.connRefCount, r.cliOpts, purpose)
}

func (r *grpcRepositoryClient) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	for resp := range r.sendRequest(&apipb.SessionRequest{
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
				Length:           rr.GetContentInfo.GetInfo().GetLength(),
				TimestampSeconds: rr.GetContentInfo.GetInfo().GetTimestampSeconds(),
				PackBlobID:       blob.ID(rr.GetContentInfo.GetInfo().GetPackBlobId()),
				PackOffset:       rr.GetContentInfo.GetInfo().GetPackOffset(),
				Deleted:          rr.GetContentInfo.GetInfo().GetDeleted(),
				FormatVersion:    byte(rr.GetContentInfo.GetInfo().GetFormatVersion()),
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
	for resp := range r.sendRequest(&apipb.SessionRequest{
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

	for resp := range r.sendRequest(&apipb.SessionRequest{
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
	if err := r.omgr.Close(); err != nil {
		return errors.Wrap(err, "error closing object manager")
	}

	if atomic.AddInt32(r.connRefCount, -1) == 0 {
		log(ctx).Debugf("closing GPRC connection to %v", r.conn.Target())
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
// The APIServerInfo must have the address of the repository as 'kopia://host:port'
func OpenGRPCAPIRepository(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, password string) (Repository, error) {
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
		return nil, errors.Errorf("invalid server address, must be 'kopia://host:port'")
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

	rep, err := NewGRPCAPIRepositoryForConnection(ctx, conn, new(int32), cliOpts, "")
	if err != nil {
		return nil, err
	}

	return rep, nil
}

// NewGRPCAPIRepositoryForConnection opens GRPC-based repository connection.
func NewGRPCAPIRepositoryForConnection(ctx context.Context, conn *grpc.ClientConn, connRefCount *int32, cliOpts ClientOptions, purpose string) (RepositoryWriter, error) {
	cli := apipb.NewKopiaRepositoryClient(conn)

	sess, err := cli.Session(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error starting session")
	}

	rr := &grpcRepositoryClient{
		connRefCount:   connRefCount,
		conn:           conn,
		cliOpts:        cliOpts,
		sess:           sess,
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
		nextRequestID:  1,
	}

	go rr.readLoop()

	p, err := rr.initializeSession(purpose, cliOpts.ReadOnly)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize session")
	}

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
}
