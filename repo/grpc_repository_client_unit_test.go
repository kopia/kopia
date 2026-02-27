package repo

import (
	"context"
	stderrors "errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	apipb "github.com/kopia/kopia/internal/grpcapi"
)

// fakeSessionClient implements apipb.KopiaRepository_SessionClient for testing.
// When recvCh is nil, Recv blocks until ctx is done (matching the old mockSessionClient).
// When recvCh is non-nil, Recv selects between ctx cancellation and channel receives.
type fakeSessionClient struct {
	grpc.ClientStream
	ctx     context.Context
	recvCh  chan recvResult // nil → blocks until ctx cancel
	sendErr error
}

type recvResult struct {
	resp *apipb.SessionResponse
	err  error
}

func (c *fakeSessionClient) Send(*apipb.SessionRequest) error { return c.sendErr }
func (c *fakeSessionClient) Recv() (*apipb.SessionResponse, error) {
	select {
	case <-c.ctx.Done():
		return nil, io.EOF
	case r := <-c.recvCh:
		return r.resp, r.err
	}
}
func (c *fakeSessionClient) CloseSend() error             { return nil }
func (c *fakeSessionClient) Header() (metadata.MD, error) { return nil, nil }
func (c *fakeSessionClient) Trailer() metadata.MD         { return nil }
func (c *fakeSessionClient) Context() context.Context     { return c.ctx }
func (c *fakeSessionClient) SendMsg(interface{}) error    { return c.sendErr }
func (c *fakeSessionClient) RecvMsg(interface{}) error    { return nil }

func newTestInnerSession(t *testing.T, cli apipb.KopiaRepository_SessionClient) *grpcInnerSession {
	t.Helper()

	sessCtx, sessCancel := context.WithCancel(context.Background())

	sess := &grpcInnerSession{
		cli:           cli,
		cancelFunc:    sessCancel,
		nextRequestID: 1,
		router:        newSessionResponseRouter(sessCtx),
	}

	t.Cleanup(func() {
		sess.shutdown()
	})

	return sess
}

func TestBaseURLToURI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		baseURL   string
		expURI    string
		expErrMsg string
	}{
		{
			name:      "ipv4",
			baseURL:   "https://1.2.3.4:5678",
			expURI:    "1.2.3.4:5678",
			expErrMsg: "",
		},
		{
			name:      "ipv6",
			baseURL:   "https://[2600:1f14:253f:ef00:87b9::10]:51515",
			expURI:    "[2600:1f14:253f:ef00:87b9::10]:51515",
			expErrMsg: "",
		},
		{
			name:      "unix https scheme",
			baseURL:   "unix+https:///tmp/kopia-test606141450/sock",
			expURI:    "unix:/tmp/kopia-test606141450/sock",
			expErrMsg: "",
		},
		{
			name:      "kopia scheme",
			baseURL:   "kopia://a:0",
			expURI:    "a:0",
			expErrMsg: "",
		},
		{
			name:      "unix http scheme is invalid",
			baseURL:   "unix+http:///tmp/kopia-test606141450/sock",
			expURI:    "",
			expErrMsg: "invalid server address",
		},
		{
			name:      "invalid address",
			baseURL:   "a",
			expURI:    "",
			expErrMsg: "invalid server address",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotURI, err := baseURLToURI(tc.baseURL)
			if tc.expErrMsg != "" {
				require.ErrorContains(t, err, tc.expErrMsg)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expURI, gotURI)
		})
	}
}

func TestKillInnerSessionCancelsContext(t *testing.T) {
	sessCtx, sessCancel := context.WithCancel(context.Background())

	cli := &fakeSessionClient{ctx: sessCtx}

	sess := &grpcInnerSession{
		cancelFunc: sessCancel,
		cli:        cli,
	}

	// Verify the session context is not cancelled yet.
	require.NoError(t, sessCtx.Err())

	client := &grpcRepositoryClient{
		innerSession: sess,
	}

	// Set up a goroutine that simulates readLoop blocking on the context.
	readLoopDone := make(chan struct{})

	sess.wg.Add(1)

	go func() {
		defer sess.wg.Done()
		<-sessCtx.Done()
		close(readLoopDone)
	}()

	// Kill the inner session - this should cancel the context.
	client.killInnerSession()

	// The readLoop goroutine should have terminated promptly.
	select {
	case <-readLoopDone:
		// Success - readLoop terminated.
	case <-time.After(5 * time.Second):
		t.Fatal("readLoop goroutine did not terminate after killInnerSession")
	}

	// The session context should be cancelled.
	require.ErrorIs(t, sessCtx.Err(), context.Canceled)

	// innerSession should be nil after kill.
	require.Nil(t, client.innerSession)
}

func TestKillInnerSessionNilCancelFunc(t *testing.T) {
	ctx := context.Background()

	cli := &fakeSessionClient{ctx: ctx}

	sess := &grpcInnerSession{
		cancelFunc: nil,
		cli:        cli,
	}

	sess.wg.Add(1)

	go func() {
		defer sess.wg.Done()
	}()

	client := &grpcRepositoryClient{
		innerSession: sess,
	}

	// Should not panic when cancelFunc is nil.
	require.NotPanics(t, func() {
		client.killInnerSession()
	})
}

func TestKillInnerSessionConcurrency(t *testing.T) {
	// Verify that concurrent calls to killInnerSession don't race.
	sessCtx, sessCancel := context.WithCancel(context.Background())

	cli := &fakeSessionClient{ctx: sessCtx}

	sess := &grpcInnerSession{
		cancelFunc: sessCancel,
		cli:        cli,
	}

	sess.wg.Add(1)

	go func() {
		defer sess.wg.Done()
		<-sessCtx.Done()
	}()

	client := &grpcRepositoryClient{
		innerSession: sess,
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			client.killInnerSession()
		}()
	}

	wg.Wait()

	require.Nil(t, client.innerSession)
}

func TestWaitForResponseContextCancelRemovesActiveRequest(t *testing.T) {
	t.Parallel()

	sess := newTestInnerSession(t, nil)

	rid := int64(123)
	ch := make(chan *apipb.SessionResponse, 1)

	ctx, cancel := context.WithCancel(context.Background())
	sess.router.registerRequest(rid, ch, ctx.Done())

	cancel()

	_, ok, err := sess.waitForResponse(ctx, rid, ch)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, sess.router.hasActiveRequest(rid))
}

func TestSendAndReceiveWhenRouterClosedReturnsPromptly(t *testing.T) {
	t.Parallel()

	sess := newTestInnerSession(t, &fakeSessionClient{
		ctx:    context.Background(),
		recvCh: make(chan recvResult),
	})

	sess.cancelFunc()
	<-sess.router.done

	done := make(chan error, 1)

	go func() {
		_, err := sendAndReceive(context.Background(), sess, &apipb.SessionRequest{
			Request: &apipb.SessionRequest_Flush{Flush: &apipb.FlushRequest{}},
		}, func(resp *apipb.SessionResponse) (struct{}, error) {
			return struct{}{}, unhandledSessionResponse(resp)
		})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)

	case <-time.After(2 * time.Second):
		t.Fatal("sendAndReceive blocked after router shutdown")
	}
}

func TestReadLoopIgnoresResponsesForCanceledRequests(t *testing.T) {
	t.Parallel()

	recvCh := make(chan recvResult, 2)
	recvCh <- recvResult{resp: &apipb.SessionResponse{
		RequestId: 999,
		Response: &apipb.SessionResponse_Flush{
			Flush: &apipb.FlushResponse{},
		},
	}}
	recvCh <- recvResult{err: io.EOF}

	sess := newTestInnerSession(t, &fakeSessionClient{ctx: context.Background(), recvCh: recvCh})

	sess.wg.Add(1)

	done := make(chan struct{})
	go func() {
		sess.readLoop(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop blocked on response for unknown request id")
	}
}

type scriptedReadinessConn struct {
	mu sync.Mutex

	states       []connectivity.State
	stateIndex   int
	connectCalls int
	blockOnLast  bool
}

func (c *scriptedReadinessConn) GetState() connectivity.State {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.states) == 0 {
		return connectivity.Shutdown
	}

	if c.stateIndex >= len(c.states) {
		return c.states[len(c.states)-1]
	}

	return c.states[c.stateIndex]
}

func (c *scriptedReadinessConn) WaitForStateChange(ctx context.Context, _ connectivity.State) bool {
	c.mu.Lock()
	if c.stateIndex < len(c.states)-1 {
		c.stateIndex++
		c.mu.Unlock()

		return true
	}

	block := c.blockOnLast
	c.mu.Unlock()

	if !block {
		return false
	}

	<-ctx.Done()

	return false
}

func (c *scriptedReadinessConn) Connect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connectCalls++
}

type fakeSessionStream struct {
	ctx                context.Context
	onSend             func(*apipb.SessionRequest)
	recvCh             chan *apipb.SessionResponse
	blockRecvUntilDone bool
}

func (s *fakeSessionStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeSessionStream) Trailer() metadata.MD         { return nil }
func (s *fakeSessionStream) CloseSend() error             { return nil }
func (s *fakeSessionStream) Context() context.Context     { return s.ctx }

func (s *fakeSessionStream) SendMsg(m interface{}) error {
	req, ok := m.(*apipb.SessionRequest)
	if !ok {
		return stderrors.New("unexpected message type")
	}

	if s.onSend != nil {
		s.onSend(req)
	}

	return nil
}

func (s *fakeSessionStream) RecvMsg(m interface{}) error {
	resp, ok := m.(*apipb.SessionResponse)
	if !ok {
		return stderrors.New("unexpected response type")
	}

	if s.blockRecvUntilDone {
		<-s.ctx.Done()
		return s.ctx.Err()
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case r := <-s.recvCh:
		proto.Reset(resp)
		proto.Merge(resp, r)
		return nil
	}
}

type fakeGRPCConn struct {
	mu sync.Mutex

	state            connectivity.State
	connectCalls     int
	resetCalls       int
	closeCalls       int
	newStreamHandler func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error)
}

func (c *fakeGRPCConn) Invoke(context.Context, string, interface{}, interface{}, ...grpc.CallOption) error {
	return nil
}

func (c *fakeGRPCConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if c.newStreamHandler == nil {
		return nil, stderrors.New("missing NewStream handler")
	}

	return c.newStreamHandler(ctx, desc, method, opts...)
}

func (c *fakeGRPCConn) Connect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connectCalls++
}

func (c *fakeGRPCConn) ResetConnectBackoff() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.resetCalls++
}

func (c *fakeGRPCConn) GetState() connectivity.State {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state
}

func (c *fakeGRPCConn) WaitForStateChange(ctx context.Context, source connectivity.State) bool {
	for {
		c.mu.Lock()
		current := c.state
		c.mu.Unlock()

		if current != source {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(1 * time.Millisecond):
		}
	}
}

func (c *fakeGRPCConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeCalls++

	return nil
}

func TestWaitForGRPCConnectionReadyReconnectsFromIdle(t *testing.T) {
	t.Parallel()

	conn := &scriptedReadinessConn{
		states: []connectivity.State{
			connectivity.Idle,
			connectivity.Connecting,
			connectivity.Idle,
			connectivity.Connecting,
			connectivity.Ready,
		},
	}

	require.NoError(t, waitForGRPCConnectionReady(context.Background(), conn))
	require.Equal(t, 2, conn.connectCalls)
}

func TestWaitForGRPCConnectionReadyTimesOut(t *testing.T) {
	t.Parallel()

	conn := &scriptedReadinessConn{
		states:      []connectivity.State{connectivity.Connecting},
		blockOnLast: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitForGRPCConnectionReady(ctx, conn)
	require.Error(t, err)
	require.ErrorContains(t, err, "timed out waiting for gRPC connection readiness")
	require.ErrorContains(t, err, "last state=CONNECTING")
}

func TestGRPCConnectionManagerReplaceClosesOldConnection(t *testing.T) {
	t.Parallel()

	oldConn := &fakeGRPCConn{state: connectivity.Ready}
	newConn := &fakeGRPCConn{state: connectivity.Ready}

	dialCount := 0

	mgr := newGRPCConnectionManager(oldConn, func(context.Context) (grpcConnection, error) {
		dialCount++
		return newConn, nil
	})

	got, err := mgr.replace(context.Background())
	require.NoError(t, err)
	require.Same(t, newConn, got)
	require.Equal(t, 1, dialCount)
	require.Equal(t, 1, oldConn.closeCalls)
	require.Same(t, newConn, mgr.current())

	require.NoError(t, mgr.close())
	require.Equal(t, 1, newConn.closeCalls)

	require.NoError(t, mgr.close())
	require.Equal(t, 1, newConn.closeCalls)
	require.Nil(t, mgr.current())
}

func TestReadLoopStreamBreakCleansUpActiveRequests(t *testing.T) {
	t.Parallel()

	recvCh := make(chan recvResult, 10)

	cli := &fakeSessionClient{
		ctx:    context.Background(),
		recvCh: recvCh,
	}

	sess := newTestInnerSession(t, cli)

	// Register two active requests.
	ch1 := make(chan *apipb.SessionResponse, 1)
	ch2 := make(chan *apipb.SessionResponse, 1)

	sess.router.registerRequest(1, ch1, nil)
	sess.router.registerRequest(2, ch2, nil)

	sess.wg.Add(1)

	done := make(chan struct{})
	go func() {
		sess.readLoop(context.Background())
		close(done)
	}()

	// Inject a stream error.
	streamErr := stderrors.New("transport closed")
	recvCh <- recvResult{nil, streamErr}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not exit after stream error")
	}

	// Both channels should have received a STREAM_BROKEN response and been closed.
	for _, ch := range []chan *apipb.SessionResponse{ch1, ch2} {
		resp, ok := <-ch
		require.True(t, ok, "channel should have a STREAM_BROKEN response")
		require.NotNil(t, resp.GetError())
		require.Equal(t, apipb.ErrorResponse_STREAM_BROKEN, resp.GetError().GetCode())
		require.Contains(t, resp.GetError().GetMessage(), "transport closed")

		// Channel should be closed after the error response.
		_, ok = <-ch
		require.False(t, ok, "channel should be closed")
	}

	// All active requests should be cleared after stream break.
	require.Equal(t, 0, sess.router.activeRequestCount())
}

func TestReadLoopMultiPageResponseCallerCancelDoesNotBlock(t *testing.T) {
	t.Parallel()

	recvCh := make(chan recvResult, 10)

	cli := &fakeSessionClient{
		ctx:    context.Background(),
		recvCh: recvCh,
	}

	sess := newTestInnerSession(t, cli)

	// Register a request with a buffer-of-1 channel (same as production code).
	ch := make(chan *apipb.SessionResponse, 1)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	sess.router.registerRequest(42, ch, reqCtx.Done())

	sess.wg.Add(1)

	readLoopDone := make(chan struct{})
	go func() {
		sess.readLoop(context.Background())
		close(readLoopDone)
	}()

	// First page: HasMore=true. This fills the buffer.
	recvCh <- recvResult{
		resp: &apipb.SessionResponse{
			RequestId: 42,
			HasMore:   true,
			Response: &apipb.SessionResponse_FindManifests{
				FindManifests: &apipb.FindManifestsResponse{},
			},
		},
	}

	// Drain the first page so readLoop can proceed.
	<-ch

	// Now simulate the caller cancelling.
	cancelReq()

	// Second page: HasMore=true again. readLoop should see the request as canceled
	// and skip delivery. If the fix is missing, readLoop would block forever trying
	// to send on a full channel.
	recvCh <- recvResult{
		resp: &apipb.SessionResponse{
			RequestId: 42,
			HasMore:   true,
			Response: &apipb.SessionResponse_FindManifests{
				FindManifests: &apipb.FindManifestsResponse{},
			},
		},
	}

	// Third page: final page. readLoop should skip this too.
	recvCh <- recvResult{
		resp: &apipb.SessionResponse{
			RequestId: 42,
			HasMore:   false,
			Response: &apipb.SessionResponse_FindManifests{
				FindManifests: &apipb.FindManifestsResponse{},
			},
		},
	}

	// Now terminate the stream so readLoop exits.
	recvCh <- recvResult{nil, io.EOF}

	select {
	case <-readLoopDone:
		// Success — readLoop did not block.
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop blocked on cancelled multi-page response (non-blocking send fix missing?)")
	}

	require.False(t, sess.router.hasActiveRequest(42))
}

func TestDeliverResponseReturnsFalseAfterRequestCancellation(t *testing.T) {
	t.Parallel()

	ch := make(chan *apipb.SessionResponse, 1)
	ch <- &apipb.SessionResponse{RequestId: 99}

	reqCtx, cancelReq := context.WithCancel(context.Background())
	cancelReq()

	ok := deliverResponse(sessionRequestRoute{
		responseCh:  ch,
		requestDone: reqCtx.Done(),
	}, &apipb.SessionResponse{RequestId: 99, HasMore: true})
	require.False(t, ok)
}

func TestDeliverResponseSucceedsWhenBackpressureClears(t *testing.T) {
	t.Parallel()

	ch := make(chan *apipb.SessionResponse, 1)
	ch <- &apipb.SessionResponse{RequestId: 7}

	done := make(chan bool, 1)
	msg := &apipb.SessionResponse{RequestId: 7, HasMore: true}

	go func() {
		done <- deliverResponse(sessionRequestRoute{
			responseCh:  ch,
			requestDone: context.Background().Done(),
		}, msg)
	}()

	// Relieve backpressure by consuming the queued message.
	<-ch

	select {
	case ok := <-done:
		require.True(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("deliverResponse did not complete after backpressure cleared")
	}

	select {
	case got := <-ch:
		require.Same(t, msg, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected delivered message after backpressure cleared")
	}
}

func TestConcurrentCurrentOrDialUnderRefresh(t *testing.T) {
	t.Parallel()

	var (
		mu        sync.Mutex
		dialCount int
	)

	mgr := newGRPCConnectionManager(
		&fakeGRPCConn{state: connectivity.Ready},
		func(context.Context) (grpcConnection, error) {
			mu.Lock()
			dialCount++
			mu.Unlock()

			return &fakeGRPCConn{state: connectivity.Ready}, nil
		},
	)

	mgr.markForRefresh()

	const goroutines = 20

	var wg sync.WaitGroup

	conns := make([]grpcConnection, goroutines)
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			c, err := mgr.currentOrDial(context.Background())
			if err != nil {
				errCh <- err
				return
			}

			conns[idx] = c
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	// All goroutines must see the same final connection.
	final := mgr.current()
	require.NotNil(t, final)

	for i, c := range conns {
		require.Samef(t, final, c, "goroutine %d got a different connection", i)
	}

	// Exactly one dial should happen; concurrent callers must share the same in-flight dial.
	mu.Lock()
	require.Equal(t, 1, dialCount, "expected exactly one shared dial")
	mu.Unlock()
}

func TestConcurrentCurrentOrDialUnderRefreshSharesDialFailure(t *testing.T) {
	t.Parallel()

	var (
		mu        sync.Mutex
		dialCount int
	)

	dialErr := stderrors.New("dial failed")

	mgr := newGRPCConnectionManager(
		&fakeGRPCConn{state: connectivity.Ready},
		func(context.Context) (grpcConnection, error) {
			mu.Lock()
			dialCount++
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			return nil, dialErr
		},
	)

	mgr.markForRefresh()

	const goroutines = 20

	var wg sync.WaitGroup

	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := mgr.currentOrDial(context.Background())
			errCh <- err
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.ErrorIs(t, err, dialErr)
	}

	mu.Lock()
	require.Equal(t, 1, dialCount, "expected exactly one shared dial attempt")
	mu.Unlock()
}

func TestConcurrentCurrentOrDialLeaderTimeoutDoesNotPoisonWaiters(t *testing.T) {
	t.Parallel()

	var dialCount atomic.Int32

	firstDialStarted := make(chan struct{})

	mgr := newGRPCConnectionManager(
		nil,
		func(ctx context.Context) (grpcConnection, error) {
			n := dialCount.Add(1)
			if n == 1 {
				close(firstDialStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}

			return &fakeGRPCConn{state: connectivity.Ready}, nil
		},
	)

	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelLeader()

	leaderErrCh := make(chan error, 1)
	go func() {
		_, err := mgr.currentOrDial(leaderCtx)
		leaderErrCh <- err
	}()

	<-firstDialStarted

	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), time.Second)
	defer cancelWaiter()

	conn, err := mgr.currentOrDial(waiterCtx)
	require.NoError(t, err)
	require.NotNil(t, conn)

	leaderErr := <-leaderErrCh
	require.ErrorIs(t, leaderErr, context.DeadlineExceeded)
	require.Equal(t, int32(2), dialCount.Load(), "expected waiter to perform a new dial after leader timeout")
	require.Same(t, conn, mgr.current())
}

func TestCurrentOrDialCloseDuringInFlightDialDoesNotInstallConnection(t *testing.T) {
	t.Parallel()

	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})

	mgr := newGRPCConnectionManager(
		nil,
		func(context.Context) (grpcConnection, error) {
			close(dialStarted)
			<-releaseDial

			return &fakeGRPCConn{state: connectivity.Ready}, nil
		},
	)

	errCh := make(chan error, 1)

	go func() {
		_, err := mgr.currentOrDial(context.Background())
		errCh <- err
	}()

	<-dialStarted
	require.NoError(t, mgr.close())

	close(releaseDial)

	err := <-errCh
	require.ErrorIs(t, err, errGRPCConnectionClosed)
	require.Nil(t, mgr.current())
}

func TestSlowInitializeSessionTimesOut(t *testing.T) {
	t.Parallel()

	conn := &fakeGRPCConn{
		state: connectivity.Ready,
		newStreamHandler: func(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			// cli.Session() succeeds immediately, but initializeSession
			// will block because Recv never returns a response.
			return &fakeSessionStream{
				ctx:    ctx,
				recvCh: make(chan *apipb.SessionResponse), // unbuffered, never fed
			}, nil
		},
	}

	client := &grpcRepositoryClient{
		connManager: newGRPCConnectionManager(conn, nil),
		opt: WriteSessionOptions{
			Purpose: "test-slow-init",
		},
		sessionEstablishmentTimeout: 80 * time.Millisecond,
	}

	start := time.Now()

	_, err := client.getOrEstablishInnerSession(context.Background())

	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorContains(t, err, "session establishment timed out")
	require.Less(t, elapsed, 2*time.Second, "should have timed out promptly")
}

func TestGetOrEstablishInnerSessionRedialsAfterFailedAttempt(t *testing.T) {
	badConn := &fakeGRPCConn{
		state: connectivity.Ready,
		newStreamHandler: func(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			return &fakeSessionStream{
				ctx:                ctx,
				blockRecvUntilDone: true,
			}, nil
		},
	}

	goodConn := &fakeGRPCConn{
		state: connectivity.Ready,
		newStreamHandler: func(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			stream := &fakeSessionStream{
				ctx:    ctx,
				recvCh: make(chan *apipb.SessionResponse, 1),
			}

			stream.onSend = func(req *apipb.SessionRequest) {
				if req.GetInitializeSession() == nil {
					return
				}

				stream.recvCh <- &apipb.SessionResponse{
					RequestId: req.GetRequestId(),
					Response: &apipb.SessionResponse_InitializeSession{
						InitializeSession: &apipb.InitializeSessionResponse{
							Parameters: &apipb.RepositoryParameters{},
						},
					},
				}
			}

			return stream, nil
		},
	}

	dialCount := 0

	client := &grpcRepositoryClient{
		connManager: newGRPCConnectionManager(badConn, func(context.Context) (grpcConnection, error) {
			dialCount++
			return goodConn, nil
		}),
		opt: WriteSessionOptions{
			Purpose: "test",
		},
		sessionEstablishmentTimeout: 80 * time.Millisecond,
	}

	_, err := client.getOrEstablishInnerSession(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "session establishment timed out")
	require.Equal(t, 0, dialCount)
	require.Equal(t, 0, badConn.closeCalls)
	require.GreaterOrEqual(t, badConn.connectCalls, 1)

	client.sessionEstablishmentTimeout = 600 * time.Millisecond

	sess, err := client.getOrEstablishInnerSession(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.Equal(t, 1, dialCount)
	require.Equal(t, 1, badConn.closeCalls)
	require.GreaterOrEqual(t, goodConn.connectCalls, 1)

	client.killInnerSession()
}

func TestDoRetryEOFKillsAndReEstablishesSession(t *testing.T) {
	t.Parallel()

	// conn responds to initializeSession, and provides a session that returns
	// a controllable response to Flush requests.
	var mu sync.Mutex

	attempt := 0

	conn := &fakeGRPCConn{
		state: connectivity.Ready,
		newStreamHandler: func(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			stream := &fakeSessionStream{
				ctx:    ctx,
				recvCh: make(chan *apipb.SessionResponse, 4),
			}

			stream.onSend = func(req *apipb.SessionRequest) {
				if req.GetInitializeSession() != nil {
					stream.recvCh <- &apipb.SessionResponse{
						RequestId: req.GetRequestId(),
						Response: &apipb.SessionResponse_InitializeSession{
							InitializeSession: &apipb.InitializeSessionResponse{
								Parameters: &apipb.RepositoryParameters{},
							},
						},
					}

					return
				}

				if req.GetFlush() != nil {
					mu.Lock()
					attempt++
					cur := attempt
					mu.Unlock()

					if cur == 1 {
						// First attempt: return STREAM_BROKEN (maps to io.EOF).
						stream.recvCh <- &apipb.SessionResponse{
							RequestId: req.GetRequestId(),
							Response: &apipb.SessionResponse_Error{
								Error: &apipb.ErrorResponse{
									Code:    apipb.ErrorResponse_STREAM_BROKEN,
									Message: "connection lost",
								},
							},
						}
					} else {
						// Second attempt: succeed.
						stream.recvCh <- &apipb.SessionResponse{
							RequestId: req.GetRequestId(),
							Response: &apipb.SessionResponse_Flush{
								Flush: &apipb.FlushResponse{},
							},
						}
					}
				}
			}

			return stream, nil
		},
	}

	client := &grpcRepositoryClient{
		connManager:                 newGRPCConnectionManager(conn, nil),
		transparentRetries:          true,
		opt:                         WriteSessionOptions{Purpose: "retry-test"},
		sessionEstablishmentTimeout: 2 * time.Second,
	}

	// doRetry should:
	// 1. Establish a session, call Flush → get STREAM_BROKEN (io.EOF)
	// 2. Kill the inner session
	// 3. Re-establish a new session, call Flush → succeed
	_, err := doRetry(context.Background(), client, func(ctx context.Context, sess *grpcInnerSession) (bool, error) {
		return false, sess.Flush(ctx)
	})
	require.NoError(t, err)

	mu.Lock()
	require.Equal(t, 2, attempt, "expected 2 attempts: first fails with EOF, second succeeds")
	mu.Unlock()

	client.killInnerSession()
}
