package repo

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	apipb "github.com/kopia/kopia/internal/grpcapi"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// MaxGRPCMessageSize is the maximum size of a message sent or received over GRPC API when talking to
// Kopia repository server. This is bigger than the size of any possible content, which is
// defined by supported splitters.
const MaxGRPCMessageSize = 20 << 20

const (
	// when writing contents of this size or above, make a round-trip to the server to
	// check if the content exists.
	writeContentCheckExistenceAboveSize = 50_000

	// size of per-session cache of content IDs that were previously read
	// helps avoid round trip to the server to write the same content since we know it already exists
	// this greatly helps with performance of incremental snapshots.
	numRecentReadsToCache = 1024

	// number of manifests to fetch in a single batch.
	defaultFindManifestsPageSize = 1000

	// default upper bound for establishing a fresh streaming session when caller did not
	// provide an explicit deadline.
	defaultSessionEstablishmentTimeout = 30 * time.Second
)

var errShouldRetry = errors.New("should retry")

func errNoSessionResponse() error {
	return errors.New("did not receive response from the server")
}

type grpcConnection interface {
	grpc.ClientConnInterface
	Connect()
	ResetConnectBackoff()
	GetState() connectivity.State
	WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool
	Close() error
}

type grpcConnectionDialer func(ctx context.Context) (grpcConnection, error)

var errGRPCConnectionClosed = errors.New("gRPC connection is closed")

const grpcDialSingleflightKey = "grpc-dial"

type grpcConnectionManager struct {
	mu sync.Mutex

	conn             grpcConnection
	dialer           grpcConnectionDialer
	refreshOnNextUse bool
	closed           bool
	dialGroup        singleflight.Group
}

func newGRPCConnectionManager(conn grpcConnection, dialer grpcConnectionDialer) *grpcConnectionManager {
	return &grpcConnectionManager{
		conn:   conn,
		dialer: dialer,
	}
}

func (m *grpcConnectionManager) current() grpcConnection {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.conn
}

func (m *grpcConnectionManager) replace(ctx context.Context) (grpcConnection, error) {
	m.markForRefresh()

	return m.currentOrDial(ctx)
}

// dialPrerequisitesLocked reports whether dialing is needed and validates dial configuration.
//
// +checklocks:m.mu
func (m *grpcConnectionManager) dialPrerequisitesLocked() (grpcConnection, grpcConnectionDialer, error) {
	if m.closed {
		return nil, nil, errGRPCConnectionClosed
	}

	if m.conn != nil && !m.refreshOnNextUse {
		return m.conn, nil, nil
	}

	if m.dialer == nil {
		return nil, nil, errors.New("gRPC redial is not configured")
	}

	return nil, m.dialer, nil
}

func (m *grpcConnectionManager) currentOrDial(ctx context.Context) (grpcConnection, error) {
	for {
		m.mu.Lock()
		conn, _, err := m.dialPrerequisitesLocked()
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}

		if conn != nil {
			return conn, nil
		}

		resultCh := m.dialGroup.DoChan(grpcDialSingleflightKey, func() (interface{}, error) {
			return m.dialAndInstall(ctx)
		})

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-resultCh:
			if res.Err != nil {
				if shouldRetryDialAfterSharedDialError(ctx, res.Err) {
					continue
				}

				return nil, res.Err
			}

			conn, ok := res.Val.(grpcConnection)
			if !ok || conn == nil {
				return nil, errors.New("invalid gRPC dial result")
			}

			return conn, nil
		}
	}
}

func (m *grpcConnectionManager) dialAndInstall(ctx context.Context) (grpcConnection, error) {
	m.mu.Lock()
	conn, dialer, err := m.dialPrerequisitesLocked()
	m.mu.Unlock()

	if err != nil {
		return nil, err
	}

	if conn != nil {
		return conn, nil
	}

	newConn, err := dialer(ctx)
	if err != nil {
		return nil, err
	}

	var oldConn grpcConnection
	var usableConn grpcConnection

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = newConn.Close()
		return nil, errGRPCConnectionClosed
	}

	if m.conn != nil && !m.refreshOnNextUse {
		usableConn = m.conn
	} else {
		oldConn = m.conn
		m.conn = newConn
		m.refreshOnNextUse = false
		usableConn = newConn
	}
	m.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}

	if usableConn != newConn {
		_ = newConn.Close()
	}

	return usableConn, nil
}

func (m *grpcConnectionManager) markForRefresh() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.refreshOnNextUse = true
}

func (m *grpcConnectionManager) close() error {
	m.mu.Lock()
	conn := m.conn
	m.conn = nil
	m.refreshOnNextUse = false
	m.closed = true
	m.mu.Unlock()

	if conn == nil {
		return nil
	}

	return conn.Close()
}

func shouldRetryDialAfterSharedDialError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// grpcRepositoryClient is an implementation of Repository that connects to an instance of
// GPRC API server hosted by `kopia server`.
type grpcRepositoryClient struct {
	connManager *grpcConnectionManager

	innerSessionMutex sync.Mutex

	// +checklocks:innerSessionMutex
	innerSession *grpcInnerSession

	// +checklocks:innerSessionMutex
	sessionState grpcSessionState

	opt                WriteSessionOptions
	isReadOnly         bool
	transparentRetries bool

	afterFlush []RepositoryWriterCallback

	// per-client time budget for creating a streaming session.
	sessionEstablishmentTimeout time.Duration

	asyncWritesWG *errgroup.Group

	*immutableServerRepositoryParameters

	serverSupportsContentCompression bool
	omgr                             *object.Manager

	findManifestsPageSize int32

	recent recentlyRead
}

type grpcInnerSession struct {
	sendMutex sync.Mutex

	nextRequestID int64

	cli        apipb.KopiaRepository_SessionClient
	repoParams *apipb.RepositoryParameters
	cancelFunc context.CancelFunc
	router     *sessionResponseRouter

	wg sync.WaitGroup
}

type grpcSessionState string

const (
	grpcSessionStateIdle       grpcSessionState = "idle"
	grpcSessionStateConnecting grpcSessionState = "connecting"
	grpcSessionStateReady      grpcSessionState = "ready"
	grpcSessionStateClosing    grpcSessionState = "closing"
)

type sessionRequestRoute struct {
	responseCh  chan *apipb.SessionResponse
	requestDone <-chan struct{}
}

type sessionRouterCommandType int

const (
	sessionRouterCommandRegister sessionRouterCommandType = iota
	sessionRouterCommandCancel
	sessionRouterCommandRouteResponse
	sessionRouterCommandRequestBroken
	sessionRouterCommandFailAll
	sessionRouterCommandHasActiveRequest
	sessionRouterCommandActiveRequestCount
)

type sessionRouterCommand struct {
	commandType sessionRouterCommandType

	// Mutation commands.
	requestID int64
	route     sessionRequestRoute
	response  *apipb.SessionResponse
	err       error

	// Query command responses.
	hasActiveResponseCh   chan bool
	activeCountResponseCh chan int
}

type sessionResponseRouter struct {
	commandCh chan sessionRouterCommand
	done      chan struct{}
}

func newSessionResponseRouter(ctx context.Context) *sessionResponseRouter {
	router := &sessionResponseRouter{
		commandCh: make(chan sessionRouterCommand),
		done:      make(chan struct{}),
	}

	go router.run(ctx)

	return router
}

func (router *sessionResponseRouter) run(ctx context.Context) {
	defer close(router.done)

	activeRequests := map[int64]sessionRequestRoute{}

	for {
		select {
		case <-ctx.Done():
			router.failAllActiveRequests(activeRequests, ctx.Err())
			return

		case cmd := <-router.commandCh:
			switch cmd.commandType {
			case sessionRouterCommandRegister:
				activeRequests[cmd.requestID] = cmd.route

			case sessionRouterCommandCancel:
				if route, ok := activeRequests[cmd.requestID]; ok {
					delete(activeRequests, cmd.requestID)
					close(route.responseCh)
				}

			case sessionRouterCommandRouteResponse:
				router.routeResponse(activeRequests, cmd.response)

			case sessionRouterCommandRequestBroken:
				if route, ok := activeRequests[cmd.requestID]; ok {
					delete(activeRequests, cmd.requestID)
					router.sendStreamBrokenAndClose(route.responseCh, cmd.err)
				}

			case sessionRouterCommandFailAll:
				router.failAllActiveRequests(activeRequests, cmd.err)

			case sessionRouterCommandHasActiveRequest:
				_, ok := activeRequests[cmd.requestID]
				cmd.hasActiveResponseCh <- ok

			case sessionRouterCommandActiveRequestCount:
				cmd.activeCountResponseCh <- len(activeRequests)
			}
		}
	}
}

func (router *sessionResponseRouter) routeResponse(activeRequests map[int64]sessionRequestRoute, response *apipb.SessionResponse) {
	if response == nil {
		return
	}

	requestID := response.GetRequestId()
	route, ok := activeRequests[requestID]
	if !ok {
		// The caller may have canceled this request and removed its route.
		return
	}

	if !router.deliverResponse(route, response) {
		delete(activeRequests, requestID)

		close(route.responseCh)

		return
	}

	if !response.GetHasMore() {
		delete(activeRequests, requestID)
		close(route.responseCh)
	}
}

func (router *sessionResponseRouter) deliverResponse(route sessionRequestRoute, response *apipb.SessionResponse) bool {
	if route.requestDone != nil {
		select {
		case <-route.requestDone:
			return false
		default:
		}

		select {
		case route.responseCh <- response:
			return true
		case <-route.requestDone:
			return false
		}
	}

	route.responseCh <- response

	return true
}

// tryPostCommand submits a command unless the router is already shut down.
func (router *sessionResponseRouter) tryPostCommand(cmd sessionRouterCommand) bool {
	select {
	case <-router.done:
		return false
	default:
	}

	select {
	case <-router.done:
		return false
	case router.commandCh <- cmd:
		return true
	}
}

func (router *sessionResponseRouter) registerRequest(requestID int64, responseCh chan *apipb.SessionResponse, requestDone <-chan struct{}) {
	router.tryPostCommand(sessionRouterCommand{
		commandType: sessionRouterCommandRegister,
		requestID:   requestID,
		route: sessionRequestRoute{
			responseCh:  responseCh,
			requestDone: requestDone,
		},
	})
}

func (router *sessionResponseRouter) cancelRequest(requestID int64) {
	router.tryPostCommand(sessionRouterCommand{
		commandType: sessionRouterCommandCancel,
		requestID:   requestID,
	})
}

func (router *sessionResponseRouter) routeIncomingResponse(response *apipb.SessionResponse) {
	router.tryPostCommand(sessionRouterCommand{
		commandType: sessionRouterCommandRouteResponse,
		response:    response,
	})
}

func (router *sessionResponseRouter) reportRequestBroken(requestID int64, err error) {
	router.tryPostCommand(sessionRouterCommand{
		commandType: sessionRouterCommandRequestBroken,
		requestID:   requestID,
		err:         err,
	})
}

func (router *sessionResponseRouter) failAllRequests(err error) {
	router.tryPostCommand(sessionRouterCommand{
		commandType: sessionRouterCommandFailAll,
		err:         err,
	})
}

func (router *sessionResponseRouter) hasActiveRequest(requestID int64) bool {
	resultCh := make(chan bool, 1)
	if !router.tryPostCommand(sessionRouterCommand{
		commandType:         sessionRouterCommandHasActiveRequest,
		requestID:           requestID,
		hasActiveResponseCh: resultCh,
	}) {
		return false
	}

	select {
	case <-router.done:
		return false
	case ok := <-resultCh:
		return ok
	}
}

func (router *sessionResponseRouter) activeRequestCount() int {
	resultCh := make(chan int, 1)
	if !router.tryPostCommand(sessionRouterCommand{
		commandType:           sessionRouterCommandActiveRequestCount,
		activeCountResponseCh: resultCh,
	}) {
		return 0
	}

	select {
	case <-router.done:
		return 0
	case n := <-resultCh:
		return n
	}
}

func (router *sessionResponseRouter) sendStreamBrokenAndClose(responseCh chan *apipb.SessionResponse, err error) {
	if responseCh == nil {
		return
	}

	select {
	case responseCh <- streamBrokenResponse(err):
	default:
	}

	close(responseCh)
}

func (router *sessionResponseRouter) failAllActiveRequests(activeRequests map[int64]sessionRequestRoute, err error) {
	for requestID, route := range activeRequests {
		delete(activeRequests, requestID)
		router.sendStreamBrokenAndClose(route.responseCh, err)
	}
}

func streamBrokenResponse(err error) *apipb.SessionResponse {
	errMessage := io.EOF.Error()
	if err != nil {
		errMessage = err.Error()
	}

	return &apipb.SessionResponse{
		Response: &apipb.SessionResponse_Error{
			Error: &apipb.ErrorResponse{
				Code:    apipb.ErrorResponse_STREAM_BROKEN,
				Message: errMessage,
			},
		},
	}
}

// readLoop runs in a goroutine and consumes all messages in session and forwards them to the response router.
func (r *grpcInnerSession) readLoop(ctx context.Context) {
	defer r.wg.Done()

	msg, err := r.cli.Recv()

	for ; err == nil; msg, err = r.cli.Recv() {
		r.router.routeIncomingResponse(msg)
	}

	log(ctx).Debugf("GRPC stream read loop terminated with %v", err)

	// The stream is broken. Notify all pending requests with a synthetic STREAM_BROKEN error.
	r.router.failAllRequests(err)
	log(ctx).Debug("finished closing active requests")
}

// sendRequest sends the provided request to the server and returns a channel on which the
// caller can receive session response(s).
func (r *grpcInnerSession) sendRequest(ctx context.Context, req *apipb.SessionRequest) (int64, chan *apipb.SessionResponse) {
	// Sends to GRPC stream must be single-threaded.
	r.sendMutex.Lock()
	defer r.sendMutex.Unlock()

	// Allocate request ID and create channel to which we're forwarding the responses.
	rid := r.nextRequestID
	r.nextRequestID++

	ch := make(chan *apipb.SessionResponse, 1)
	r.router.registerRequest(rid, ch, ctx.Done())

	req.RequestId = rid

	// pass trace context to the server
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		var tc propagation.TraceContext

		req.TraceContext = map[string]string{}

		tc.Inject(ctx, propagation.MapCarrier(req.GetTraceContext()))
	}

	// try sending the request and if unable to do so, stuff an error response to the channel
	// to simplify client code.
	if err := r.cli.Send(req); err != nil {
		r.router.reportRequestBroken(rid, err)
	}

	return rid, ch
}

func (r *grpcInnerSession) waitForResponse(ctx context.Context, rid int64, ch chan *apipb.SessionResponse) (*apipb.SessionResponse, bool, error) {
	select {
	case <-ctx.Done():
		// Remove the request mapping so stale responses for canceled operations
		// won't be routed to a channel with no receiver.
		r.router.cancelRequest(rid)

		return nil, false, errors.Wrap(ctx.Err(), "context cancelled waiting for session response")

	case resp, ok := <-ch:
		return resp, ok, nil
	}
}

func (r *grpcInnerSession) shutdown() {
	if r == nil {
		return
	}

	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	if r.cli != nil {
		if err := r.cli.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
			_ = err // best-effort close during teardown; stream may already be closed.
		}
	}

	r.wg.Wait()

	if r.router != nil {
		<-r.router.done
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

func (r *grpcRepositoryClient) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	//nolint:wrapcheck
	return object.Open(ctx, r, id)
}

func (r *grpcRepositoryClient) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	return r.omgr.NewWriter(ctx, opt)
}

func (r *grpcRepositoryClient) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	//nolint:wrapcheck
	return object.VerifyObject(ctx, r, id)
}

func (r *grpcInnerSession) initializeSession(ctx context.Context, purpose string, readOnly bool) (*apipb.RepositoryParameters, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_InitializeSession{
			InitializeSession: &apipb.InitializeSessionRequest{
				Purpose:  purpose,
				ReadOnly: readOnly,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return nil, errors.Wrap(err, "context cancelled waiting for session initialization")
		}

		if !ok {
			return nil, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_InitializeSession:
			return rr.InitializeSession.GetParameters(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error) {
	return maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (*manifest.EntryMetadata, error) {
		return sess.GetManifest(ctx, id, data)
	})
}

func (r *grpcInnerSession) GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetManifest{
			GetManifest: &apipb.GetManifestRequest{
				ManifestId: string(id),
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetManifest:
			return decodeManifestEntryMetadata(rr.GetManifest.GetMetadata()), json.Unmarshal(rr.GetManifest.GetJsonData(), data)

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}
}

func appendManifestEntryMetadataList(result []*manifest.EntryMetadata, md []*apipb.ManifestEntryMetadata) []*manifest.EntryMetadata {
	for _, v := range md {
		result = append(result, decodeManifestEntryMetadata(v))
	}

	return result
}

func decodeManifestEntryMetadata(md *apipb.ManifestEntryMetadata) *manifest.EntryMetadata {
	return &manifest.EntryMetadata{
		ID:      manifest.ID(md.GetId()),
		Length:  int(md.GetLength()),
		Labels:  md.GetLabels(),
		ModTime: time.Unix(0, md.GetModTimeNanos()),
	}
}

func (r *grpcRepositoryClient) PutManifest(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error) {
	return inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (manifest.ID, error) {
		return sess.PutManifest(ctx, labels, payload)
	})
}

// ReplaceManifests saves the given manifest payload with a set of labels and replaces any previous manifests with the same labels.
func (r *grpcRepositoryClient) ReplaceManifests(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error) {
	return replaceManifestsHelper(ctx, r, labels, payload)
}

func (r *grpcInnerSession) PutManifest(ctx context.Context, labels map[string]string, payload any) (manifest.ID, error) {
	v, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal JSON")
	}

	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_PutManifest{
			PutManifest: &apipb.PutManifestRequest{
				JsonData: v,
				Labels:   labels,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return "", err
		}

		if !ok {
			return "", errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_PutManifest:
			return manifest.ID(rr.PutManifest.GetManifestId()), nil

		default:
			return "", unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) SetFindManifestPageSizeForTesting(v int32) {
	r.findManifestsPageSize = v
}

func (r *grpcRepositoryClient) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	return maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) ([]*manifest.EntryMetadata, error) {
		return sess.FindManifests(ctx, labels, r.findManifestsPageSize)
	})
}

func (r *grpcInnerSession) FindManifests(ctx context.Context, labels map[string]string, pageSize int32) ([]*manifest.EntryMetadata, error) {
	var entries []*manifest.EntryMetadata

	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_FindManifests{
			FindManifests: &apipb.FindManifestsRequest{
				Labels:   labels,
				PageSize: pageSize,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_FindManifests:
			entries = appendManifestEntryMetadataList(entries, rr.FindManifests.GetMetadata())

			if !resp.GetHasMore() {
				return entries, nil
			}

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) DeleteManifest(ctx context.Context, id manifest.ID) error {
	_, err := inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (bool, error) {
		return false, sess.DeleteManifest(ctx, id)
	})

	return err
}

func (r *grpcInnerSession) DeleteManifest(ctx context.Context, id manifest.ID) error {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_DeleteManifest{
			DeleteManifest: &apipb.DeleteManifestRequest{
				ManifestId: string(id),
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return err
		}

		if !ok {
			return errNoSessionResponse()
		}

		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_DeleteManifest:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	//nolint:wrapcheck
	return object.PrefetchBackingContents(ctx, r, objectIDs, hint)
}

func (r *grpcRepositoryClient) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	result, _ := inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) ([]content.ID, error) {
		return sess.PrefetchContents(ctx, contentIDs, hint), nil
	})

	return result
}

func (r *grpcInnerSession) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_PrefetchContents{
			PrefetchContents: &apipb.PrefetchContentsRequest{
				ContentIds: content.IDsToStrings(contentIDs),
				Hint:       hint,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			log(ctx).Warnf("context canceled waiting for PrefetchContents response: %v", err)
			return nil
		}

		if !ok {
			log(ctx).Warn("missing response to PrefetchContents")
			return nil
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_PrefetchContents:
			ids, err := content.IDsFromStrings(rr.PrefetchContents.GetContentIds())
			if err != nil {
				log(ctx).Warnf("invalid response to PrefetchContents: %v", err)
			}

			return ids

		default:
			log(ctx).Warnf("unexpected response to PrefetchContents: %v", resp)
			return nil
		}
	}
}

func (r *grpcRepositoryClient) ApplyRetentionPolicy(ctx context.Context, sourcePath string, reallyDelete bool) ([]manifest.ID, error) {
	return inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) ([]manifest.ID, error) {
		return sess.ApplyRetentionPolicy(ctx, sourcePath, reallyDelete)
	})
}

func (r *grpcInnerSession) ApplyRetentionPolicy(ctx context.Context, sourcePath string, reallyDelete bool) ([]manifest.ID, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_ApplyRetentionPolicy{
			ApplyRetentionPolicy: &apipb.ApplyRetentionPolicyRequest{
				SourcePath:   sourcePath,
				ReallyDelete: reallyDelete,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_ApplyRetentionPolicy:
			return manifest.IDsFromStrings(rr.ApplyRetentionPolicy.GetManifestIds()), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType apipb.NotificationEventArgType, importance int32) error {
	_, err := maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (struct{}, error) {
		return sess.SendNotification(ctx, templateName, templateDataJSON, templateDataType, importance)
	})

	return err
}

var _ RemoteNotifications = (*grpcRepositoryClient)(nil)

func (r *grpcInnerSession) SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType apipb.NotificationEventArgType, severity int32) (struct{}, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_SendNotification{
			SendNotification: &apipb.SendNotificationRequest{
				TemplateName:  templateName,
				EventArgs:     templateDataJSON,
				EventArgsType: templateDataType,
				Severity:      severity,
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return struct{}{}, err
		}

		if !ok {
			return struct{}{}, errNoSessionResponse()
		}

		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_SendNotification:
			return struct{}{}, nil

		default:
			return struct{}{}, unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) Time() time.Time {
	return clock.Now()
}

func (r *grpcRepositoryClient) Refresh(_ context.Context) error {
	return nil
}

func (r *grpcRepositoryClient) Flush(ctx context.Context) error {
	if err := r.asyncWritesWG.Wait(); err != nil {
		return errors.Wrap(err, "error waiting for async writes")
	}

	if err := invokeCallbacks(ctx, r, r.beforeFlush); err != nil {
		return errors.Wrap(err, "before flush")
	}

	if _, err := inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (bool, error) {
		return false, sess.Flush(ctx)
	}); err != nil {
		return err
	}

	if err := invokeCallbacks(ctx, r, r.afterFlush); err != nil {
		return errors.Wrap(err, "after flush")
	}

	return nil
}

func (r *grpcInnerSession) Flush(ctx context.Context) error {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_Flush{
			Flush: &apipb.FlushRequest{},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return err
		}

		if !ok {
			return errNoSessionResponse()
		}

		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_Flush:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	w, err := newGRPCAPIRepositoryForConnection(ctx, r.connManager, opt, false, r.immutableServerRepositoryParameters)
	if err != nil {
		return nil, nil, err
	}

	w.addRef()

	return ctx, w, nil
}

// ConcatenateObjects creates a concatenated objects from the provided object IDs.
func (r *grpcRepositoryClient) ConcatenateObjects(ctx context.Context, objectIDs []object.ID, opt ConcatenateOptions) (object.ID, error) {
	//nolint:wrapcheck
	return r.omgr.Concatenate(ctx, objectIDs, opt.Compressor)
}

// maybeRetry executes the provided callback with or without automatic retries depending on how
// the grpcRepositoryClient is configured.
func maybeRetry[T any](ctx context.Context, r *grpcRepositoryClient, attempt func(ctx context.Context, sess *grpcInnerSession) (T, error)) (T, error) {
	if !r.transparentRetries {
		return inSessionWithoutRetry(ctx, r, attempt)
	}

	return doRetry(ctx, r, attempt)
}

// retry executes the provided callback and provides it with *grpcInnerSession.
// If the grpcRepositoryClient set to automatically retry and the provided callback returns io.EOF,
// the inner session will be killed and re-established as necessary.
func doRetry[T any](ctx context.Context, r *grpcRepositoryClient, attempt func(ctx context.Context, sess *grpcInnerSession) (T, error)) (T, error) {
	var defaultT T

	return retry.WithExponentialBackoff(ctx, "invoking GRPC API", func() (T, error) {
		v, err := inSessionWithoutRetry(ctx, r, attempt)
		if errors.Is(err, io.EOF) {
			r.killInnerSession()

			return defaultT, errShouldRetry
		}

		return v, err
	}, func(err error) bool {
		return errors.Is(err, errShouldRetry)
	})
}

func inSessionWithoutRetry[T any](ctx context.Context, r *grpcRepositoryClient, attempt func(ctx context.Context, sess *grpcInnerSession) (T, error)) (T, error) {
	var defaultT T

	sess, err := r.getOrEstablishInnerSession(ctx)
	if err != nil {
		return defaultT, errors.Wrapf(err, "unable to establish session for purpose=%v", r.opt.Purpose)
	}

	return attempt(ctx, sess)
}

func (r *grpcRepositoryClient) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	return maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (content.Info, error) {
		return sess.contentInfo(ctx, contentID)
	})
}

func (r *grpcInnerSession) contentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContentInfo{
			GetContentInfo: &apipb.GetContentInfoRequest{
				ContentId: contentID.String(),
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return content.Info{}, err
		}

		if !ok {
			return content.Info{}, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetContentInfo:
			contentID, err := content.ParseID(rr.GetContentInfo.GetInfo().GetId())
			if err != nil {
				return content.Info{}, errors.Wrap(err, "invalid content ID")
			}

			return content.Info{
				ContentID:        contentID,
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
		return errors.Wrap(io.EOF, rr.GetMessage())
	default:
		return errors.New(rr.GetMessage())
	}
}

func unhandledSessionResponse(resp *apipb.SessionResponse) error {
	if e := resp.GetError(); e != nil {
		return errorFromSessionResponse(e)
	}

	return errors.Errorf("unsupported session response: %v", resp)
}

func (r *grpcRepositoryClient) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	var b gather.WriteBuffer
	defer b.Close()

	err := r.contentCache.GetOrLoad(ctx, contentID.String(), func(output *gather.WriteBuffer) error {
		v, err := maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) ([]byte, error) {
			return sess.GetContent(ctx, contentID)
		})
		if err != nil {
			return err
		}

		_, err = output.Write(v)

		//nolint:wrapcheck
		return err
	}, &b)

	if err == nil && contentID.HasPrefix() {
		r.recent.add(contentID)
	}

	return b.ToByteSlice(), err
}

func (r *grpcInnerSession) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContent{
			GetContent: &apipb.GetContentRequest{
				ContentId: contentID.String(),
			},
		},
	})

	for {
		resp, ok, err := r.waitForResponse(ctx, rid, ch)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errNoSessionResponse()
		}

		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetContent:
			return rr.GetContent.GetData(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}
}

func (r *grpcRepositoryClient) SupportsContentCompression() bool {
	return r.serverSupportsContentCompression
}

func (r *grpcRepositoryClient) doWriteAsync(ctx context.Context, contentID content.ID, data []byte, prefix content.IDPrefix, comp compression.HeaderID) error {
	// if content is large enough, perform existence check on the server,
	// for small contents we skip the check, since the server-side existence
	// check is fast and we avoid double round trip.
	if len(data) >= writeContentCheckExistenceAboveSize {
		if _, err := r.ContentInfo(ctx, contentID); err == nil {
			// content already exists
			return nil
		}
	}

	r.opt.OnUpload(int64(len(data)))

	if _, err := inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (content.ID, error) {
		sess.writeContentAsyncAndVerify(ctx, contentID, data, prefix, comp, r.asyncWritesWG)
		return contentID, nil
	}); err != nil {
		return err
	}

	if prefix != "" {
		// add all prefixed contents to the cache.
		r.contentCache.Put(ctx, contentID.String(), gather.FromSlice(data))
	}

	return nil
}

func (r *grpcRepositoryClient) WriteContent(ctx context.Context, data gather.Bytes, prefix content.IDPrefix, comp compression.HeaderID) (content.ID, error) {
	if err := prefix.ValidateSingle(); err != nil {
		return content.EmptyID, errors.Wrap(err, "invalid prefix")
	}

	// we will be writing asynchronously and server will reject this write, fail early.
	if prefix == manifest.ContentPrefix {
		return content.EmptyID, errors.New("writing manifest contents not allowed")
	}

	var hashOutput [128]byte

	contentID, err := content.IDFromHash(prefix, r.h(hashOutput[:0], data))
	if err != nil {
		return content.EmptyID, errors.Errorf("invalid content ID: %v", err)
	}

	if r.recent.exists(contentID) {
		return contentID, nil
	}

	// clone so that caller can reuse the buffer
	clone := data.ToByteSlice()

	if err := r.doWriteAsync(context.WithoutCancel(ctx), contentID, clone, prefix, comp); err != nil {
		return content.EmptyID, err
	}

	return contentID, nil
}

func (r *grpcInnerSession) writeContentAsyncAndVerify(ctx context.Context, contentID content.ID, data []byte, prefix content.IDPrefix, comp compression.HeaderID, eg *errgroup.Group) {
	rid, ch := r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_WriteContent{
			WriteContent: &apipb.WriteContentRequest{
				Data:        data,
				Prefix:      string(prefix),
				Compression: uint32(comp),
			},
		},
	})

	eg.Go(func() error {
		for {
			resp, ok, err := r.waitForResponse(ctx, rid, ch)
			if err != nil {
				return err
			}

			if !ok {
				return errNoSessionResponse()
			}

			switch rr := resp.GetResponse().(type) {
			case *apipb.SessionResponse_WriteContent:
				got, err := content.ParseID(rr.WriteContent.GetContentId())
				if err != nil {
					return errors.Wrap(err, "unable to parse server content ID")
				}

				if got != contentID {
					return errors.Errorf("unexpected content ID: %v, wanted %v", got, contentID)
				}

				return nil

			default:
				return unhandledSessionResponse(resp)
			}
		}
	})
}

// UpdateDescription updates the description of a connected repository.
func (r *grpcRepositoryClient) UpdateDescription(d string) {
	r.cliOpts.Description = d
}

// OnSuccessfulFlush registers the provided callback to be invoked after flush succeeds.
func (r *grpcRepositoryClient) OnSuccessfulFlush(callback RepositoryWriterCallback) {
	r.afterFlush = append(r.afterFlush, callback)
}

var _ Repository = (*grpcRepositoryClient)(nil)

type grpcCreds struct {
	hostname string
	username string
	password string
}

func (c grpcCreds) GetRequestMetadata(_ context.Context, uri ...string) (map[string]string, error) {
	_ = uri

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

// openGRPCAPIRepository opens the Repository based on remote GRPC server.
// The APIServerInfo must have the address of the repository as 'https://host:port'
func openGRPCAPIRepository(ctx context.Context, si *APIServerInfo, password string, par *immutableServerRepositoryParameters) (Repository, error) {
	var transportCreds credentials.TransportCredentials

	if si.TrustedServerCertificateFingerprint != "" {
		transportCreds = credentials.NewTLS(tlsutil.TLSConfigTrustingSingleCertificate(si.TrustedServerCertificateFingerprint))
	} else {
		transportCreds = credentials.NewClientTLSFromCert(nil, "")
	}

	uri, err := baseURLToURI(si.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "parsing base URL")
	}

	creds := grpcCreds{par.cliOpts.Hostname, par.cliOpts.Username, password}
	dialGRPCConnection := func(_ context.Context) (grpcConnection, error) {
		conn, err := grpc.NewClient(
			uri,
			grpc.WithPerRPCCredentials(creds),
			grpc.WithTransportCredentials(transportCreds),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
				grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
			),
			// Keepalive detects dead TCP connections when the server becomes unreachable.
			// Time is the interval between pings; Timeout is how long to wait for a response.
			// PermitWithoutStream enables pings even when no active RPCs exist, which is
			// needed to detect failures during idle periods between backup operations.
			// See https://github.com/kopia/kopia/issues/3073
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err != nil {
			return nil, errors.Wrap(err, "gRPC client creation error")
		}

		return conn, nil
	}

	conn, err := dialGRPCConnection(ctx)
	if err != nil {
		return nil, err
	}

	connManager := newGRPCConnectionManager(conn, dialGRPCConnection)

	par.registerEarlyCloseFunc(
		func(_ context.Context) error {
			return errors.Wrap(connManager.close(), "error closing GRPC connection")
		})

	rep, err := newGRPCAPIRepositoryForConnection(ctx, connManager, WriteSessionOptions{}, true, par)
	if err != nil {
		return nil, err
	}

	return rep, nil
}

func baseURLToURI(baseURL string) (uri string, err error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", errors.Wrap(err, "unable to parse server URL")
	}

	if u.Scheme != "kopia" && u.Scheme != "https" && u.Scheme != "unix+https" {
		return "", errors.New("invalid server address, must be 'https://host:port' or 'unix+https://<path>")
	}

	uri = net.JoinHostPort(u.Hostname(), u.Port())
	if u.Scheme == "unix+https" {
		uri = "unix:" + u.Path
	}

	return uri, nil
}

func (r *grpcRepositoryClient) getOrEstablishInnerSession(ctx context.Context) (*grpcInnerSession, error) {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.sessionState == grpcSessionStateClosing {
		return nil, errors.New("gRPC session is closing")
	}

	if r.innerSession != nil {
		r.markSessionReadyLocked()
		return r.innerSession, nil
	}

	restoreState := r.markSessionConnectingLocked()
	defer restoreState()

	log(ctx).Debugf("establishing new GRPC streaming session (purpose=%v)", r.opt.Purpose)

	establishCtx, cancelEstablish := r.sessionEstablishmentContext(ctx)
	defer cancelEstablish()

	conn, err := r.prepareConnectionForSession(establishCtx)
	if err != nil {
		return nil, errors.Wrap(err, "error establishing session")
	}

	if err := waitForGRPCConnectionReady(establishCtx, conn); err != nil {
		r.connManager.markForRefresh()
		return nil, errors.Wrap(err, "error establishing session")
	}

	newSession, err := r.establishAndInitializeSession(ctx, establishCtx, conn)
	if err != nil {
		return nil, err
	}

	r.innerSession = newSession
	r.markSessionReadyLocked()

	return r.innerSession, nil
}

// +checklocks:r.innerSessionMutex
func (r *grpcRepositoryClient) markSessionConnectingLocked() func() {
	r.sessionState = grpcSessionStateConnecting

	return func() {
		if r.innerSession == nil {
			r.sessionState = grpcSessionStateIdle
		}
	}
}

// +checklocks:r.innerSessionMutex
func (r *grpcRepositoryClient) markSessionReadyLocked() {
	if r.innerSession != nil {
		r.sessionState = grpcSessionStateReady
	}
}

func (r *grpcRepositoryClient) sessionEstablishmentContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, r.effectiveSessionEstablishmentTimeout())
}

func (r *grpcRepositoryClient) establishAndInitializeSession(ctx, establishCtx context.Context, conn grpcConnection) (*grpcInnerSession, error) {
	sessionClient, sessCtx, sessCancel, err := r.openSessionStream(ctx, establishCtx, conn)
	if err != nil {
		return nil, err
	}

	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred {
			sessCancel()
		}
	}()

	newSession, err := r.newInitializedInnerSession(establishCtx, sessCtx, sessCancel, sessionClient)
	if err != nil {
		return nil, err
	}

	ownershipTransferred = true

	return newSession, nil
}

func (r *grpcRepositoryClient) openSessionStream(ctx, establishCtx context.Context, conn grpcConnection) (apipb.KopiaRepository_SessionClient, context.Context, context.CancelFunc, error) {
	client := apipb.NewKopiaRepositoryClient(conn)

	// sessCtx is detached from ctx (via WithoutCancel) so the GRPC stream
	// outlives the caller's context, but wrapped with WithCancel so
	// killInnerSession can terminate it.
	sessCtx, sessCancel := context.WithCancel(context.WithoutCancel(ctx))

	type sessionResult struct {
		sess apipb.KopiaRepository_SessionClient
		err  error
	}

	resultCh := make(chan sessionResult, 1)
	go func() {
		sess, sessionErr := client.Session(sessCtx)
		resultCh <- sessionResult{sess, sessionErr}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			sessCancel()
			r.connManager.markForRefresh()
			return nil, nil, nil, errors.Wrap(result.err, "error establishing session")
		}

		return result.sess, sessCtx, sessCancel, nil

	case <-establishCtx.Done():
		sessCancel()
		r.connManager.markForRefresh()
		if errors.Is(establishCtx.Err(), context.DeadlineExceeded) {
			return nil, nil, nil, errors.Errorf("error establishing session: session establishment timed out (connection state=%v)", conn.GetState())
		}

		return nil, nil, nil, errors.Wrap(establishCtx.Err(), "error establishing session: context cancelled during session establishment")
	}
}

func (r *grpcRepositoryClient) newInitializedInnerSession(establishCtx, sessCtx context.Context, sessCancel context.CancelFunc, sessionClient apipb.KopiaRepository_SessionClient) (*grpcInnerSession, error) {
	newSession := &grpcInnerSession{
		cli:           sessionClient,
		nextRequestID: 1,
		cancelFunc:    sessCancel,
		router:        newSessionResponseRouter(sessCtx),
	}

	newSession.wg.Add(1)
	go newSession.readLoop(sessCtx)

	repoParams, initErr := newSession.initializeSession(establishCtx, r.opt.Purpose, r.isReadOnly)
	if initErr != nil {
		newSession.shutdown()
		r.connManager.markForRefresh()

		if errors.Is(establishCtx.Err(), context.DeadlineExceeded) {
			return nil, errors.Wrap(initErr, "error establishing session: session establishment timed out")
		}

		return nil, errors.Wrap(initErr, "error establishing session: unable to initialize session")
	}

	newSession.repoParams = repoParams

	return newSession, nil
}

func (r *grpcRepositoryClient) effectiveSessionEstablishmentTimeout() time.Duration {
	if r.sessionEstablishmentTimeout <= 0 {
		return defaultSessionEstablishmentTimeout
	}

	return r.sessionEstablishmentTimeout
}

func (r *grpcRepositoryClient) prepareConnectionForSession(ctx context.Context) (grpcConnection, error) {
	conn, err := r.connManager.currentOrDial(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to (re)create gRPC connection")
	}

	conn.ResetConnectBackoff()
	conn.Connect()

	return conn, nil
}

type grpcConnectionReadiness interface {
	GetState() connectivity.State
	WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool
	Connect()
}

func waitForGRPCConnectionReady(ctx context.Context, conn grpcConnectionReadiness) error {
	for {
		state := conn.GetState()

		switch state {
		case connectivity.Idle:
			// gRPC Connect() only kicks the channel out of Idle. If the channel re-enters
			// Idle after a wake/reconnect transition we must trigger Connect() again.
			conn.Connect()
		case connectivity.Connecting:
			// Keep waiting for a transition to READY or terminal failure.
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure:
			// Keep waiting for a transition to READY or terminal failure.
		case connectivity.Shutdown:
			return errors.New("gRPC connection is shut down")
		default:
			return errors.Errorf("unexpected gRPC connection state: %v", state)
		}

		if conn.WaitForStateChange(ctx, state) {
			continue
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errors.Errorf("timed out waiting for gRPC connection readiness (last state=%v)", state)
		}

		//nolint:wrapcheck
		return ctx.Err()
	}
}

func (r *grpcRepositoryClient) killInnerSession() {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.innerSession != nil {
		r.sessionState = grpcSessionStateClosing
		r.innerSession.shutdown()
		r.innerSession = nil
	}

	r.sessionState = grpcSessionStateIdle
}

// newGRPCAPIRepositoryForConnection opens GRPC-based repository connection.
func newGRPCAPIRepositoryForConnection(
	ctx context.Context,
	connManager *grpcConnectionManager,
	opt WriteSessionOptions,
	transparentRetries bool,
	par *immutableServerRepositoryParameters,
) (*grpcRepositoryClient, error) {
	if opt.OnUpload == nil {
		opt.OnUpload = func(_ int64) {}
	}

	rr := &grpcRepositoryClient{
		immutableServerRepositoryParameters: par,
		connManager:                         connManager,
		transparentRetries:                  transparentRetries,
		opt:                                 opt,
		isReadOnly:                          par.cliOpts.ReadOnly,
		asyncWritesWG:                       new(errgroup.Group),
		findManifestsPageSize:               defaultFindManifestsPageSize,
		sessionEstablishmentTimeout:         defaultSessionEstablishmentTimeout,
		sessionState:                        grpcSessionStateIdle,
	}

	return inSessionWithoutRetry(ctx, rr, func(ctx context.Context, sess *grpcInnerSession) (*grpcRepositoryClient, error) {
		p := sess.repoParams

		hf, err := hashing.CreateHashFunc(p)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create hash function")
		}

		rr.h = hf

		rr.objectFormat = format.ObjectFormat{
			Splitter: p.GetSplitter(),
		}

		rr.serverSupportsContentCompression = p.GetSupportsContentCompression()

		rr.omgr, err = object.NewObjectManager(ctx, rr, rr.objectFormat, rr.metricsRegistry)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize object manager")
		}

		return rr, nil
	})
}
