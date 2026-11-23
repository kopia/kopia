package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

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

	// maximum number of content IDs written since the last flush that a write session tracks
	// so that it can resume on a new inner session after the previous one broke.
	// Beyond this number the write session cannot resume and fails as before.
	maxUnflushedWritesToTrack = 100_000

	// number of attempts to find contents written on a broken inner session in the repository,
	// with exponential backoff (about one minute in total). The server flushes a broken session
	// only once it notices that the connection is gone.
	resumeVerificationAttempts = 14
)

var (
	errShouldRetry = errors.New("should retry")

	// errRequestNotSent is returned when a request could not be sent because the stream
	// had already ended, so it is safe to send it again on a new session.
	errRequestNotSent = errors.New("request not sent")

	// errContentsNotDurable is returned when contents written on a broken inner session
	// are not (yet) visible in the repository.
	errContentsNotDurable = errors.New("contents written before the session broke are not in the repository")
)

func errNoSessionResponse() error {
	return errors.New("did not receive response from the server")
}

// grpcRepositoryClient is an implementation of Repository that connects to an instance of
// GPRC API server hosted by `kopia server`.
type grpcRepositoryClient struct {
	conn *grpc.ClientConn

	innerSessionMutex sync.Mutex

	// +checklocks:innerSessionMutex
	innerSession *grpcInnerSession

	opt                WriteSessionOptions
	isReadOnly         bool
	transparentRetries bool

	afterFlush []RepositoryWriterCallback

	// how many times we tried to establish inner session
	// +checklocks:innerSessionMutex
	innerSessionAttemptCount int

	// set when a write session lost its inner session and could not resume safely,
	// all further requests fail with this error.
	// +checklocks:innerSessionMutex
	writeSessionLostErr error

	// set when a write session discarded a broken inner session and the next one must be
	// verified before use.
	// +checklocks:innerSessionMutex
	needsResumeVerification bool

	// what the write session wrote since its last successful flush.
	writes writeTracker

	asyncWritesWG *errgroup.Group

	*immutableServerRepositoryParameters

	serverSupportsContentCompression bool
	omgr                             *object.Manager

	findManifestsPageSize int32

	recent recentlyRead
}

type grpcInnerSession struct {
	sendMutex sync.Mutex

	activeRequestsMutex sync.Mutex

	// +checklocks:activeRequestsMutex
	nextRequestID int64

	// +checklocks:activeRequestsMutex
	activeRequests map[int64]chan *apipb.SessionResponse

	cli        apipb.KopiaRepository_SessionClient
	repoParams *apipb.RepositoryParameters

	// set when the stream is known to be unusable, because the read loop terminated
	// or a request could not be sent.
	broken atomic.Bool

	wg sync.WaitGroup
}

// readLoop runs in a goroutine and consumes all messages in session and forwards them to appropriate channels.
func (r *grpcInnerSession) readLoop(ctx context.Context) {
	defer r.wg.Done()

	msg, err := r.cli.Recv()

	for ; err == nil; msg, err = r.cli.Recv() {
		r.activeRequestsMutex.Lock()
		ch := r.activeRequests[msg.GetRequestId()]

		if !msg.GetHasMore() {
			delete(r.activeRequests, msg.GetRequestId())
		}

		r.activeRequestsMutex.Unlock()

		ch <- msg

		if !msg.GetHasMore() {
			close(ch)
		}
	}

	log(ctx).Debugf("GRPC stream read loop terminated with %v", err)

	r.broken.Store(true)

	// when a read loop error occurs, close all pending client channels with an artificial error.
	r.activeRequestsMutex.Lock()
	defer r.activeRequestsMutex.Unlock()

	for id := range r.activeRequests {
		r.sendStreamBrokenAndClose(r.getAndDeleteResponseChannelLocked(id), err)
	}

	log(ctx).Debug("finished closing active requests")
}

// sendRequest sends the provided request to the server and returns a channel on which the
// caller can receive session response(s). If the request cannot be sent, the channel
// receives a STREAM_BROKEN error response.
func (r *grpcInnerSession) sendRequest(ctx context.Context, req *apipb.SessionRequest) chan *apipb.SessionResponse {
	ch, err := r.trySendRequest(ctx, req)
	if err != nil {
		ch = make(chan *apipb.SessionResponse, 1)
		r.sendStreamBrokenAndClose(ch, err)
	}

	return ch
}

// trySendRequest sends the provided request to the server and returns a channel on which the
// caller can receive session response(s). If the request cannot be sent, it returns an error
// wrapping errRequestNotSent and the session is marked as broken. gRPC reports a send error only
// for a stream that has already ended, so the server did not process the request on this session.
func (r *grpcInnerSession) trySendRequest(ctx context.Context, req *apipb.SessionRequest) (chan *apipb.SessionResponse, error) {
	_ = ctx

	// allocate request ID and create channel to which we're forwarding the responses.
	r.activeRequestsMutex.Lock()
	rid := r.nextRequestID
	r.nextRequestID++

	ch := make(chan *apipb.SessionResponse, 1)

	r.activeRequests[rid] = ch
	r.activeRequestsMutex.Unlock()

	req.RequestId = rid

	// pass trace context to the server
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		var tc propagation.TraceContext

		req.TraceContext = map[string]string{}

		tc.Inject(ctx, propagation.MapCarrier(req.GetTraceContext()))
	}

	// sends to GRPC stream must be single-threaded.
	r.sendMutex.Lock()
	defer r.sendMutex.Unlock()

	if err := r.cli.Send(req); err != nil {
		r.broken.Store(true)

		r.activeRequestsMutex.Lock()
		r.getAndDeleteResponseChannelLocked(rid)
		r.activeRequestsMutex.Unlock()

		return nil, fmt.Errorf("%w: %w", errRequestNotSent, err)
	}

	return ch, nil
}

// +checklocks:r.activeRequestsMutex
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
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_InitializeSession{
			InitializeSession: &apipb.InitializeSessionRequest{
				Purpose:  purpose,
				ReadOnly: readOnly,
			},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_InitializeSession:
			return rr.InitializeSession.GetParameters(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error) {
	return maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (*manifest.EntryMetadata, error) {
		return sess.GetManifest(ctx, id, data)
	})
}

func (r *grpcInnerSession) GetManifest(ctx context.Context, id manifest.ID, data any) (*manifest.EntryMetadata, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetManifest{
			GetManifest: &apipb.GetManifestRequest{
				ManifestId: string(id),
			},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetManifest:
			return decodeManifestEntryMetadata(rr.GetManifest.GetMetadata()), json.Unmarshal(rr.GetManifest.GetJsonData(), data)

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
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
	// not idempotent: only resend when the server did not receive the request.
	return inSessionWithResume(ctx, r, false, func(ctx context.Context, sess *grpcInnerSession) (manifest.ID, error) {
		r.writes.beginManifestMutation()

		id, err := sess.PutManifest(ctx, labels, payload)
		r.writes.manifestMutationDone(err)

		return id, err
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

	ch, err := r.trySendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_PutManifest{
			PutManifest: &apipb.PutManifestRequest{
				JsonData: v,
				Labels:   labels,
			},
		},
	})
	if err != nil {
		return "", err
	}

	for resp := range ch {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_PutManifest:
			return manifest.ID(rr.PutManifest.GetManifestId()), nil

		default:
			return "", unhandledSessionResponse(resp)
		}
	}

	return "", errNoSessionResponse()
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

	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_FindManifests{
			FindManifests: &apipb.FindManifestsRequest{
				Labels:   labels,
				PageSize: pageSize,
			},
		},
	}) {
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

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) DeleteManifest(ctx context.Context, id manifest.ID) error {
	_, err := inSessionWithResume(ctx, r, false, func(ctx context.Context, sess *grpcInnerSession) (bool, error) {
		r.writes.beginManifestMutation()

		err := sess.DeleteManifest(ctx, id)
		r.writes.manifestMutationDone(err)

		return false, err
	})

	return err
}

func (r *grpcInnerSession) DeleteManifest(ctx context.Context, id manifest.ID) error {
	ch, err := r.trySendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_DeleteManifest{
			DeleteManifest: &apipb.DeleteManifestRequest{
				ManifestId: string(id),
			},
		},
	})
	if err != nil {
		return err
	}

	for resp := range ch {
		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_DeleteManifest:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}

	return errNoSessionResponse()
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
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_PrefetchContents{
			PrefetchContents: &apipb.PrefetchContentsRequest{
				ContentIds: content.IDsToStrings(contentIDs),
				Hint:       hint,
			},
		},
	}) {
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

	log(ctx).Warn("missing response to PrefetchContents")

	return nil
}

func (r *grpcRepositoryClient) ApplyRetentionPolicy(ctx context.Context, sourcePath string, reallyDelete bool) ([]manifest.ID, error) {
	return inSessionWithoutRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) ([]manifest.ID, error) {
		if !reallyDelete {
			return sess.ApplyRetentionPolicy(ctx, sourcePath, reallyDelete)
		}

		r.writes.beginManifestMutation()

		ids, err := sess.ApplyRetentionPolicy(ctx, sourcePath, reallyDelete)
		r.writes.manifestMutationDone(err)

		return ids, err
	})
}

func (r *grpcInnerSession) ApplyRetentionPolicy(ctx context.Context, sourcePath string, reallyDelete bool) ([]manifest.ID, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_ApplyRetentionPolicy{
			ApplyRetentionPolicy: &apipb.ApplyRetentionPolicyRequest{
				SourcePath:   sourcePath,
				ReallyDelete: reallyDelete,
			},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_ApplyRetentionPolicy:
			return manifest.IDsFromStrings(rr.ApplyRetentionPolicy.GetManifestIds()), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
}

func (r *grpcRepositoryClient) SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType apipb.NotificationEventArgType, importance int32) error {
	_, err := maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (struct{}, error) {
		return sess.SendNotification(ctx, templateName, templateDataJSON, templateDataType, importance)
	})

	return err
}

var _ RemoteNotifications = (*grpcRepositoryClient)(nil)

func (r *grpcInnerSession) SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType apipb.NotificationEventArgType, severity int32) (struct{}, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_SendNotification{
			SendNotification: &apipb.SendNotificationRequest{
				TemplateName:  templateName,
				EventArgs:     templateDataJSON,
				EventArgsType: templateDataType,
				Severity:      severity,
			},
		},
	}) {
		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_SendNotification:
			return struct{}{}, nil

		default:
			return struct{}{}, unhandledSessionResponse(resp)
		}
	}

	return struct{}{}, errNoSessionResponse()
}

func (r *grpcRepositoryClient) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return maybeRetry(ctx, r, func(ctx context.Context, sess *grpcInnerSession) (blob.Capacity, error) {
		return sess.getCapacity(ctx)
	})
}

func (r *grpcInnerSession) getCapacity(ctx context.Context) (blob.Capacity, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetCapacity{
			GetCapacity: &apipb.GetCapacityRequest{},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetCapacity:
			return blob.Capacity{
				SizeB: rr.GetCapacity.GetCapacity().GetSizeB(),
				FreeB: rr.GetCapacity.GetCapacity().GetFreeB(),
			}, nil

		default:
			return blob.Capacity{}, unhandledSessionResponse(resp)
		}
	}

	return blob.Capacity{}, errNoSessionResponse()
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

	epoch := r.writes.startFlush()

	// flush is idempotent, so it can be resent even if the server may have received it.
	if _, err := inSessionWithResume(ctx, r, true, func(ctx context.Context, sess *grpcInnerSession) (bool, error) {
		return false, sess.Flush(ctx)
	}); err != nil {
		return err
	}

	r.writes.flushSucceeded(epoch)

	if err := invokeCallbacks(ctx, r, r.afterFlush); err != nil {
		return errors.Wrap(err, "after flush")
	}

	return nil
}

func (r *grpcInnerSession) Flush(ctx context.Context) error {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_Flush{
			Flush: &apipb.FlushRequest{},
		},
	}) {
		switch resp.GetResponse().(type) {
		case *apipb.SessionResponse_Flush:
			return nil

		default:
			return unhandledSessionResponse(resp)
		}
	}

	return errNoSessionResponse()
}

func (r *grpcRepositoryClient) NewWriter(ctx context.Context, opt WriteSessionOptions) (context.Context, RepositoryWriter, error) {
	w, err := newGRPCAPIRepositoryForConnection(ctx, r.conn, opt, false, r.immutableServerRepositoryParameters)
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
		return inSessionWithResume(ctx, r, true, attempt)
	}

	return doRetry(ctx, r, attempt)
}

// inSessionWithResume executes the provided callback in the inner session.
//
// In a write session (no transparent retries), if the inner session broke, the callback is run once more
// on a new inner session when it is safe to do so: always when the request was never sent to the server,
// and also when the response was lost if the callback is idempotent. The new inner session is only
// established after all contents written since the last flush are verified to be in the repository
// (see getOrEstablishInnerSession), so a resumed session never references contents that were lost
// with the broken one.
func inSessionWithResume[T any](ctx context.Context, r *grpcRepositoryClient, idempotent bool, attempt func(ctx context.Context, sess *grpcInnerSession) (T, error)) (T, error) {
	v, err := inSessionWithoutRetry(ctx, r, attempt)
	if err == nil || r.transparentRetries {
		return v, err
	}

	if !errors.Is(err, errRequestNotSent) && (!idempotent || !errors.Is(err, io.EOF)) {
		return v, err
	}

	log(ctx).Debugf("GRPC streaming session (purpose=%v) broke, resuming on a new session: %v", r.opt.Purpose, err)

	return inSessionWithoutRetry(ctx, r, attempt)
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

const maxUInt8 = 255

func (r *grpcInnerSession) contentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContentInfo{
			GetContentInfo: &apipb.GetContentInfoRequest{
				ContentId: contentID.String(),
			},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetContentInfo:
			contentID, err := content.ParseID(rr.GetContentInfo.GetInfo().GetId())
			if err != nil {
				return content.Info{}, errors.Wrap(err, "invalid content ID")
			}

			fv := rr.GetContentInfo.GetInfo().GetFormatVersion()
			if fv > maxUInt8 {
				return content.Info{}, errors.Errorf("invalid format version: %v", fv)
			}

			return content.Info{
				ContentID:        contentID,
				PackedLength:     rr.GetContentInfo.GetInfo().GetPackedLength(),
				TimestampSeconds: rr.GetContentInfo.GetInfo().GetTimestampSeconds(),
				PackBlobID:       blob.ID(rr.GetContentInfo.GetInfo().GetPackBlobId()),
				PackOffset:       rr.GetContentInfo.GetInfo().GetPackOffset(),
				Deleted:          rr.GetContentInfo.GetInfo().GetDeleted(),
				FormatVersion:    byte(fv),
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
		return errors.Wrap(io.EOF, rr.GetMessage())
	case apipb.ErrorResponse_NOT_A_VOLUME:
		return blob.ErrNotAVolume
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
	for resp := range r.sendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_GetContent{
			GetContent: &apipb.GetContentRequest{
				ContentId: contentID.String(),
			},
		},
	}) {
		switch rr := resp.GetResponse().(type) {
		case *apipb.SessionResponse_GetContent:
			return rr.GetContent.GetData(), nil

		default:
			return nil, unhandledSessionResponse(resp)
		}
	}

	return nil, errNoSessionResponse()
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

	// writes are content-addressed, but the response is processed asynchronously,
	// so only resend when the server did not receive the request.
	if _, err := inSessionWithResume(ctx, r, false, func(ctx context.Context, sess *grpcInnerSession) (content.ID, error) {
		// track before sending, so that a concurrent resume never misses a write in flight.
		kind := r.writes.beginContentWrite(contentID)

		if err := sess.writeContentAsyncAndVerify(ctx, contentID, data, prefix, comp, r.asyncWritesWG, func() {
			r.writes.contentWriteAcked(contentID, kind)
		}); err != nil {
			if errors.Is(err, errRequestNotSent) {
				r.writes.contentWriteNotSent(contentID, kind)
			}

			return content.EmptyID, err
		}

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

func (r *grpcInnerSession) writeContentAsyncAndVerify(ctx context.Context, contentID content.ID, data []byte, prefix content.IDPrefix, comp compression.HeaderID, eg *errgroup.Group, onAck func()) error {
	ch, err := r.trySendRequest(ctx, &apipb.SessionRequest{
		Request: &apipb.SessionRequest_WriteContent{
			WriteContent: &apipb.WriteContentRequest{
				Data:        data,
				Prefix:      string(prefix),
				Compression: uint32(comp),
			},
		},
	})
	if err != nil {
		return err
	}

	eg.Go(func() error {
		for resp := range ch {
			switch rr := resp.GetResponse().(type) {
			case *apipb.SessionResponse_WriteContent:
				got, err := content.ParseID(rr.WriteContent.GetContentId())
				if err != nil {
					return errors.Wrap(err, "unable to parse server content ID")
				}

				if got != contentID {
					return errors.Errorf("unexpected content ID: %v, wanted %v", got, contentID)
				}

				onAck()

				return nil

			default:
				return unhandledSessionResponse(resp)
			}
		}

		return errNoSessionResponse()
	})

	return nil
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
		c, err := tlsutil.TLSConfigTrustingSingleCertificate(si.TrustedServerCertificateFingerprint)
		if err != nil {
			return nil, errors.Wrap(err, "creating TLS config")
		}

		transportCreds = credentials.NewTLS(c)
	} else {
		transportCreds = credentials.NewClientTLSFromCert(nil, "")
	}

	uri, err := baseURLToURI(si.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "parsing base URL")
	}

	conn, err := grpc.NewClient(
		uri,
		grpc.WithPerRPCCredentials(grpcCreds{par.cliOpts.Hostname, par.cliOpts.Username, password}),
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, errors.Wrap(err, "gRPC client creation error")
	}

	par.registerEarlyCloseFunc(
		func(_ context.Context) error {
			return errors.Wrap(conn.Close(), "error closing GRPC connection")
		},
	)

	rep, err := newGRPCAPIRepositoryForConnection(ctx, conn, WriteSessionOptions{}, true, par)
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

// verifyUnflushedWrites checks on the provided (new) inner session that every content written since
// the last flush is in the repository. When an inner session breaks, the server flushes what it received
// once it notices the end of the session, which may happen after the client reconnects,
// so missing contents are checked again with backoff before giving up.
func (r *grpcRepositoryClient) verifyUnflushedWrites(ctx context.Context, sess *grpcInnerSession) error {
	verified := map[content.ID]bool{}

	_, err := retry.WithExponentialBackoffMaxRetries(ctx, resumeVerificationAttempts, "verifying contents written before the session broke", func() (bool, error) {
		// re-read on every attempt, writes that were never sent are no longer tracked.
		ids, err := r.writes.contentsToVerify()
		if err != nil {
			return false, err
		}

		missing := 0

		for _, cid := range ids {
			if verified[cid] {
				continue
			}

			if _, err := sess.contentInfo(ctx, cid); err != nil {
				if errors.Is(err, content.ErrContentNotFound) {
					missing++
					continue
				}

				return false, errors.Wrapf(err, "unable to verify content %v", cid)
			}

			verified[cid] = true
		}

		if missing > 0 {
			return false, errors.Wrapf(errContentsNotDurable, "%v of %v contents missing", missing, len(ids))
		}

		return true, nil
	}, func(err error) bool {
		return errors.Is(err, errContentsNotDurable)
	})

	return err
}

// verifyResumedSessionLocked verifies that a write session can safely continue on the new inner session.
// If it cannot, the new inner session is closed. When contents are known to be lost, or the session cannot
// prove they are not, the write session fails permanently; other errors (for example a canceled context
// or another broken stream) leave the verification pending for the next attempt.
//
// The inner session mutex is held on purpose: no request may use the write session until it is verified.
//
// +checklocks:r.innerSessionMutex
func (r *grpcRepositoryClient) verifyResumedSessionLocked(ctx context.Context, sess *grpcInnerSession) error {
	if err := r.verifyUnflushedWrites(ctx, sess); err != nil {
		sess.cli.CloseSend() //nolint:errcheck
		sess.wg.Wait()

		err = errors.Wrap(err, "unable to resume write session after its connection broke")

		if errors.Is(err, errContentsNotDurable) || errors.Is(err, errCannotResume) {
			// the write session cannot continue without risking references to lost contents or manifests.
			r.writeSessionLostErr = err
		}

		return err
	}

	r.needsResumeVerification = false

	log(ctx).Debugf("resumed GRPC streaming session (purpose=%v)", r.opt.Purpose)

	return nil
}

func (r *grpcRepositoryClient) getOrEstablishInnerSession(ctx context.Context) (*grpcInnerSession, error) {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.writeSessionLostErr != nil {
		return nil, r.writeSessionLostErr
	}

	if r.innerSession != nil && !r.transparentRetries && r.innerSession.broken.Load() {
		// a write session whose inner session broke; read-only sessions are handled by doRetry().
		log(ctx).Debugf("GRPC streaming session (purpose=%v) is broken, establishing a new one", r.opt.Purpose)

		r.innerSession.cli.CloseSend() //nolint:errcheck
		r.innerSession.wg.Wait()
		r.innerSession = nil
		r.needsResumeVerification = true
	}

	if r.innerSession != nil {
		return r.innerSession, nil
	}

	cli := apipb.NewKopiaRepositoryClient(r.conn)

	log(ctx).Debugf("establishing new GRPC streaming session (purpose=%v)", r.opt.Purpose)

	retryPolicy := retry.Always
	if r.transparentRetries && r.innerSessionAttemptCount == 0 {
		// the first time the read-only session is established, don't do retries
		// to avoid spinning in place while the server is not connectable.
		retryPolicy = retry.Never
	}

	r.innerSessionAttemptCount++

	v, err := retry.WithExponentialBackoff(ctx, "establishing session", func() (*grpcInnerSession, error) {
		sess, err := cli.Session(context.WithoutCancel(ctx))
		if err != nil {
			return nil, errors.Wrap(err, "Session()")
		}

		newSess := &grpcInnerSession{
			cli:            sess,
			activeRequests: make(map[int64]chan *apipb.SessionResponse),
			nextRequestID:  1,
		}

		newSess.wg.Add(1)

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

	if r.needsResumeVerification {
		if err := r.verifyResumedSessionLocked(ctx, v); err != nil {
			return nil, err
		}
	}

	r.innerSession = v

	return v, nil
}

func (r *grpcRepositoryClient) killInnerSession() {
	r.innerSessionMutex.Lock()
	defer r.innerSessionMutex.Unlock()

	if r.innerSession != nil {
		r.innerSession.cli.CloseSend() //nolint:errcheck
		r.innerSession.wg.Wait()
		r.innerSession = nil
	}
}

// newGRPCAPIRepositoryForConnection opens GRPC-based repository connection.
func newGRPCAPIRepositoryForConnection(
	ctx context.Context,
	conn *grpc.ClientConn,
	opt WriteSessionOptions,
	transparentRetries bool,
	par *immutableServerRepositoryParameters,
) (*grpcRepositoryClient, error) {
	if opt.OnUpload == nil {
		opt.OnUpload = func(_ int64) {}
	}

	rr := &grpcRepositoryClient{
		immutableServerRepositoryParameters: par,
		conn:                                conn,
		transparentRetries:                  transparentRetries,
		opt:                                 opt,
		isReadOnly:                          par.cliOpts.ReadOnly,
		asyncWritesWG:                       new(errgroup.Group),
		findManifestsPageSize:               defaultFindManifestsPageSize,
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
