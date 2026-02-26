package repo

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

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
