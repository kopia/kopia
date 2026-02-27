package repo

import (
	"context"
	stderrors "errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	apipb "github.com/kopia/kopia/internal/grpcapi"
)

// mockSessionClient implements apipb.KopiaRepository_SessionClient for testing.
type mockSessionClient struct {
	grpc.ClientStream
	ctx context.Context
}

func (m *mockSessionClient) Send(*apipb.SessionRequest) error { return nil }
func (m *mockSessionClient) Recv() (*apipb.SessionResponse, error) {
	<-m.ctx.Done()
	return nil, io.EOF
}
func (m *mockSessionClient) CloseSend() error             { return nil }
func (m *mockSessionClient) Header() (metadata.MD, error) { return nil, nil }
func (m *mockSessionClient) Trailer() metadata.MD         { return nil }
func (m *mockSessionClient) Context() context.Context     { return m.ctx }
func (m *mockSessionClient) SendMsg(interface{}) error    { return nil }
func (m *mockSessionClient) RecvMsg(interface{}) error    { return nil }

type scriptedSessionClient struct {
	grpc.ClientStream

	mu        sync.Mutex
	responses []*apipb.SessionResponse
}

func (m *scriptedSessionClient) Send(*apipb.SessionRequest) error { return nil }
func (m *scriptedSessionClient) Recv() (*apipb.SessionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.responses) == 0 {
		return nil, io.EOF
	}

	resp := m.responses[0]
	m.responses = m.responses[1:]

	return resp, nil
}
func (m *scriptedSessionClient) CloseSend() error             { return nil }
func (m *scriptedSessionClient) Header() (metadata.MD, error) { return nil, nil }
func (m *scriptedSessionClient) Trailer() metadata.MD         { return nil }
func (m *scriptedSessionClient) Context() context.Context     { return context.Background() }
func (m *scriptedSessionClient) SendMsg(interface{}) error    { return nil }
func (m *scriptedSessionClient) RecvMsg(interface{}) error    { return nil }

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

	mock := &mockSessionClient{ctx: sessCtx}

	sess := &grpcInnerSession{
		cancelFunc:     sessCancel,
		cli:            mock,
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
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

	mock := &mockSessionClient{ctx: ctx}

	sess := &grpcInnerSession{
		cancelFunc:     nil,
		cli:            mock,
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
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

	mock := &mockSessionClient{ctx: sessCtx}

	sess := &grpcInnerSession{
		cancelFunc:     sessCancel,
		cli:            mock,
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
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

	sess := &grpcInnerSession{
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
	}

	rid := int64(123)
	ch := make(chan *apipb.SessionResponse, 1)

	sess.activeRequestsMutex.Lock()
	sess.activeRequests[rid] = ch
	sess.activeRequestsMutex.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, err := sess.waitForResponse(ctx, rid, ch)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)

	sess.activeRequestsMutex.Lock()
	defer sess.activeRequestsMutex.Unlock()

	_, found := sess.activeRequests[rid]
	require.False(t, found)
}

func TestReadLoopIgnoresResponsesForCanceledRequests(t *testing.T) {
	t.Parallel()

	sess := &grpcInnerSession{
		cli: &scriptedSessionClient{
			responses: []*apipb.SessionResponse{
				{
					RequestId: 999,
					Response: &apipb.SessionResponse_Flush{
						Flush: &apipb.FlushResponse{},
					},
				},
			},
		},
		activeRequests: make(map[int64]chan *apipb.SessionResponse),
	}

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
