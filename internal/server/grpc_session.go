package server

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/grpcapi"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type grpcServerState struct {
	sendMutex sync.RWMutex

	grpcapi.UnimplementedKopiaRepositoryServer

	sem *semaphore.Weighted
}

// send sends the provided session response with the provided request ID.
func (s *Server) send(srv grpcapi.KopiaRepository_SessionServer, requestID int64, resp *grpcapi.SessionResponse) error {
	s.grpcServerState.sendMutex.Lock()
	defer s.grpcServerState.sendMutex.Unlock()

	resp.RequestId = requestID

	if err := srv.Send(resp); err != nil {
		return errors.Wrap(err, "unable to send response")
	}

	return nil
}

func (s *Server) authenticateGRPCSession(ctx context.Context, rep repo.Repository) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.PermissionDenied, "metadata not found in context")
	}

	if u, h, p := md.Get("kopia-username"), md.Get("kopia-hostname"), md.Get("kopia-password"); len(u) == 1 && len(p) == 1 && len(h) == 1 {
		username := u[0] + "@" + h[0]
		password := p[0]

		if s.authenticator.IsValid(ctx, rep, username, password) {
			return username, nil
		}

		return "", status.Errorf(codes.PermissionDenied, "access denied for %v", username)
	}

	return "", status.Errorf(codes.PermissionDenied, "missing credentials")
}

// Session handles GRPC session from a repository client.
func (s *Server) Session(srv grpcapi.KopiaRepository_SessionServer) error {
	ctx := srv.Context()

	s.serverMutex.RLock()
	dr, ok := s.rep.(repo.DirectRepository)
	s.serverMutex.RUnlock()

	if !ok {
		return status.Errorf(codes.Unavailable, "not connected to a direct repository")
	}

	usernameAtHostname, err := s.authenticateGRPCSession(ctx, dr)
	if err != nil {
		return err
	}

	authz := s.authorizer.Authorize(ctx, dr, usernameAtHostname)
	if authz == nil {
		authz = auth.NoAccess()
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.PermissionDenied, "peer not found in context")
	}

	log(ctx).Infof("starting session for user %q from %v", usernameAtHostname, p.Addr)
	defer log(ctx).Infof("session ended for user %q from %v", usernameAtHostname, p.Addr)

	opt, err := s.handleInitialSessionHandshake(srv, dr)
	if err != nil {
		log(ctx).Errorf("session handshake error: %v", err)
		return err
	}

	//nolint:wrapcheck
	return repo.DirectWriteSession(ctx, dr, opt, func(ctx context.Context, dw repo.DirectRepositoryWriter) error {
		// channel to which workers will be sending errors, only holds 1 slot and sends are non-blocking.
		lastErr := make(chan error, 1)

		for req, err := srv.Recv(); err == nil; req, err = srv.Recv() {
			// propagate any error from the goroutines
			select {
			case err := <-lastErr:
				log(ctx).Errorf("error handling session request: %v", err)
				return err

			default:
			}

			// enforce limit on concurrent handling
			if err := s.grpcServerState.sem.Acquire(ctx, 1); err != nil {
				return errors.Wrap(err, "unable to acquire semaphore")
			}

			go func() {
				defer s.grpcServerState.sem.Release(1)

				s.handleSessionRequest(ctx, dw, authz, usernameAtHostname, req, func(resp *grpcapi.SessionResponse) {
					if err := s.send(srv, req.GetRequestId(), resp); err != nil {
						select {
						case lastErr <- err:
						default:
						}
					}
				})
			}()
		}

		return nil
	})
}

var tracer = otel.Tracer("kopia/grpc")

func (s *Server) handleSessionRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, usernameAtHostname string, req *grpcapi.SessionRequest, respond func(*grpcapi.SessionResponse)) {
	if req.GetTraceContext() != nil {
		var tc propagation.TraceContext
		ctx = tc.Extract(ctx, propagation.MapCarrier(req.GetTraceContext()))
	}

	switch inner := req.GetRequest().(type) {
	case *grpcapi.SessionRequest_GetContentInfo:
		respond(handleGetContentInfoRequest(ctx, dw, authz, inner.GetContentInfo))

	case *grpcapi.SessionRequest_GetContent:
		respond(handleGetContentRequest(ctx, dw, authz, inner.GetContent))

	case *grpcapi.SessionRequest_WriteContent:
		respond(handleWriteContentRequest(ctx, dw, authz, inner.WriteContent))

	case *grpcapi.SessionRequest_Flush:
		respond(handleFlushRequest(ctx, dw, authz, inner.Flush))

	case *grpcapi.SessionRequest_GetManifest:
		respond(handleGetManifestRequest(ctx, dw, authz, inner.GetManifest))

	case *grpcapi.SessionRequest_PutManifest:
		respond(handlePutManifestRequest(ctx, dw, authz, inner.PutManifest))

	case *grpcapi.SessionRequest_FindManifests:
		handleFindManifestsRequest(ctx, dw, authz, inner.FindManifests, respond)

	case *grpcapi.SessionRequest_DeleteManifest:
		respond(handleDeleteManifestRequest(ctx, dw, authz, inner.DeleteManifest))

	case *grpcapi.SessionRequest_PrefetchContents:
		respond(handlePrefetchContentsRequest(ctx, dw, authz, inner.PrefetchContents))

	case *grpcapi.SessionRequest_ApplyRetentionPolicy:
		respond(handleApplyRetentionPolicyRequest(ctx, dw, authz, usernameAtHostname, inner.ApplyRetentionPolicy))

	case *grpcapi.SessionRequest_SendNotification:
		respond(s.handleSendNotificationRequest(ctx, dw, authz, inner.SendNotification))

	case *grpcapi.SessionRequest_InitializeSession:
		respond(errorResponse(errors.New("InitializeSession must be the first request in a session")))

	default:
		respond(errorResponse(errors.New("unhandled session request")))
	}
}

func handleGetContentInfoRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.GetContentInfoRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.GetContentInfo")
	defer span.End()

	if authz.ContentAccessLevel() < auth.AccessLevelRead {
		return accessDeniedResponse()
	}

	contentID, err := content.ParseID(req.GetContentId())
	if err != nil {
		return errorResponse(err)
	}

	ci, err := dw.ContentManager().ContentInfo(ctx, contentID)
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_GetContentInfo{
			GetContentInfo: &grpcapi.GetContentInfoResponse{
				Info: &grpcapi.ContentInfo{
					Id:               ci.ContentID.String(),
					PackedLength:     ci.PackedLength,
					TimestampSeconds: ci.TimestampSeconds,
					PackBlobId:       string(ci.PackBlobID),
					PackOffset:       ci.PackOffset,
					Deleted:          ci.Deleted,
					FormatVersion:    uint32(ci.FormatVersion),
					OriginalLength:   ci.OriginalLength,
				},
			},
		},
	}
}

func handleGetContentRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.GetContentRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.GetContent")
	defer span.End()

	if authz.ContentAccessLevel() < auth.AccessLevelRead {
		return accessDeniedResponse()
	}

	contentID, err := content.ParseID(req.GetContentId())
	if err != nil {
		return errorResponse(err)
	}

	data, err := dw.ContentManager().GetContent(ctx, contentID)
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_GetContent{
			GetContent: &grpcapi.GetContentResponse{
				Data: data,
			},
		},
	}
}

func handleWriteContentRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.WriteContentRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.WriteContent")
	defer span.End()

	if authz.ContentAccessLevel() < auth.AccessLevelAppend {
		return accessDeniedResponse()
	}

	if strings.HasPrefix(req.GetPrefix(), manifest.ContentPrefix) {
		// it's not allowed to create contents prefixed with 'm' since those could be mistaken for manifest contents.
		return accessDeniedResponse()
	}

	contentID, err := dw.ContentManager().WriteContent(ctx, gather.FromSlice(req.GetData()), content.IDPrefix(req.GetPrefix()), compression.HeaderID(req.GetCompression()))
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_WriteContent{
			WriteContent: &grpcapi.WriteContentResponse{
				ContentId: contentID.String(),
			},
		},
	}
}

func handleFlushRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, _ *grpcapi.FlushRequest) *grpcapi.SessionResponse {
	if authz.ContentAccessLevel() < auth.AccessLevelAppend {
		return accessDeniedResponse()
	}

	err := dw.Flush(ctx)
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_Flush{
			Flush: &grpcapi.FlushResponse{},
		},
	}
}

func handleGetManifestRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.GetManifestRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.GetManifest")
	defer span.End()

	var data json.RawMessage

	em, err := dw.GetManifest(ctx, manifest.ID(req.GetManifestId()), &data)
	if err != nil {
		return errorResponse(err)
	}

	if authz.ManifestAccessLevel(em.Labels) < auth.AccessLevelRead {
		return accessDeniedResponse()
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_GetManifest{
			GetManifest: &grpcapi.GetManifestResponse{
				JsonData: data,
				Metadata: makeEntryMetadata(em),
			},
		},
	}
}

func handlePutManifestRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.PutManifestRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.PutManifest")
	defer span.End()

	if authz.ManifestAccessLevel(req.GetLabels()) < auth.AccessLevelAppend {
		return accessDeniedResponse()
	}

	manifestID, err := dw.PutManifest(ctx, req.GetLabels(), json.RawMessage(req.GetJsonData()))
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_PutManifest{
			PutManifest: &grpcapi.PutManifestResponse{
				ManifestId: string(manifestID),
			},
		},
	}
}

func handleFindManifestsRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.FindManifestsRequest, respond func(*grpcapi.SessionResponse)) {
	ctx, span := tracer.Start(ctx, "GRPCSession.FindManifests")
	defer span.End()

	em, err := dw.FindManifests(ctx, req.GetLabels())
	if err != nil {
		respond(errorResponse(err))
		return
	}

	// only return manifests which the caller can read
	var filtered []*manifest.EntryMetadata

	for _, m := range em {
		if authz.ManifestAccessLevel(m.Labels) < auth.AccessLevelRead {
			continue
		}

		// if pagination was requested and we've already reached the page size,
		// send a response with the current batch of manifests and reset the batch.
		if ps := int(req.GetPageSize()); ps > 0 && len(filtered) >= ps {
			respond(&grpcapi.SessionResponse{
				HasMore: true,
				Response: &grpcapi.SessionResponse_FindManifests{
					FindManifests: &grpcapi.FindManifestsResponse{
						Metadata: makeEntryMetadataList(filtered),
					},
				},
			})

			filtered = nil
		}

		filtered = append(filtered, m)
	}

	// respond with the final page of manifests
	respond(&grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_FindManifests{
			FindManifests: &grpcapi.FindManifestsResponse{
				Metadata: makeEntryMetadataList(filtered),
			},
		},
	})
}

func handleDeleteManifestRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.DeleteManifestRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.DeleteManifest")
	defer span.End()

	var data json.RawMessage

	em, err := dw.GetManifest(ctx, manifest.ID(req.GetManifestId()), &data)
	if err != nil {
		return errorResponse(err)
	}

	if authz.ManifestAccessLevel(em.Labels) < auth.AccessLevelFull {
		return accessDeniedResponse()
	}

	if err := dw.DeleteManifest(ctx, manifest.ID(req.GetManifestId())); err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_DeleteManifest{
			DeleteManifest: &grpcapi.DeleteManifestResponse{},
		},
	}
}

func handlePrefetchContentsRequest(ctx context.Context, rep repo.Repository, authz auth.AuthorizationInfo, req *grpcapi.PrefetchContentsRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.PrefetchContents")
	defer span.End()

	if authz.ContentAccessLevel() < auth.AccessLevelRead {
		return accessDeniedResponse()
	}

	contentIDs, err := content.IDsFromStrings(req.GetContentIds())
	if err != nil {
		return errorResponse(err)
	}

	cids := rep.PrefetchContents(ctx, contentIDs, req.GetHint())

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_PrefetchContents{
			PrefetchContents: &grpcapi.PrefetchContentsResponse{
				ContentIds: content.IDsToStrings(cids),
			},
		},
	}
}

func handleApplyRetentionPolicyRequest(ctx context.Context, rep repo.RepositoryWriter, authz auth.AuthorizationInfo, usernameAtHostname string, req *grpcapi.ApplyRetentionPolicyRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.ApplyRetentionPolicy")
	defer span.End()

	parts := strings.Split(usernameAtHostname, "@")
	if len(parts) != 2 { //nolint:mnd
		return errorResponse(errors.Errorf("invalid username@hostname: %q", usernameAtHostname))
	}

	username := parts[0]
	hostname := parts[1]

	// only allow users to apply retention policy if they have permission to add snapshots
	// for a particular path.
	if authz.ManifestAccessLevel(map[string]string{
		manifest.TypeLabelKey:  snapshot.ManifestType,
		snapshot.UsernameLabel: username,
		snapshot.HostnameLabel: hostname,
		snapshot.PathLabel:     req.GetSourcePath(),
	}) < auth.AccessLevelAppend {
		return accessDeniedResponse()
	}

	manifestIDs, err := policy.ApplyRetentionPolicy(ctx, rep, snapshot.SourceInfo{
		Host:     hostname,
		UserName: username,
		Path:     req.GetSourcePath(),
	}, req.GetReallyDelete())
	if err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_ApplyRetentionPolicy{
			ApplyRetentionPolicy: &grpcapi.ApplyRetentionPolicyResponse{
				ManifestIds: manifest.IDsToStrings(manifestIDs),
			},
		},
	}
}

func (s *Server) handleSendNotificationRequest(ctx context.Context, rep repo.RepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.SendNotificationRequest) *grpcapi.SessionResponse {
	ctx, span := tracer.Start(ctx, "GRPCSession.SendNotification")
	defer span.End()

	if authz.ContentAccessLevel() < auth.AccessLevelAppend {
		return accessDeniedResponse()
	}

	if err := notification.SendInternal(ctx, rep,
		req.GetTemplateName(),
		json.RawMessage(req.GetEventArgs()),
		notification.Severity(req.GetSeverity()),
		s.options.NotifyTemplateOptions); err != nil {
		return errorResponse(err)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_SendNotification{
			SendNotification: &grpcapi.SendNotificationResponse{},
		},
	}
}

func accessDeniedResponse() *grpcapi.SessionResponse {
	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_Error{
			Error: &grpcapi.ErrorResponse{
				Code:    grpcapi.ErrorResponse_ACCESS_DENIED,
				Message: "access denied",
			},
		},
	}
}

func errorResponse(err error) *grpcapi.SessionResponse {
	var errorCode grpcapi.ErrorResponse_Code

	switch {
	case errors.Is(err, content.ErrContentNotFound):
		errorCode = grpcapi.ErrorResponse_CONTENT_NOT_FOUND
	case errors.Is(err, manifest.ErrNotFound):
		errorCode = grpcapi.ErrorResponse_MANIFEST_NOT_FOUND
	case errors.Is(err, object.ErrObjectNotFound):
		errorCode = grpcapi.ErrorResponse_OBJECT_NOT_FOUND
	default:
		errorCode = grpcapi.ErrorResponse_UNKNOWN_ERROR
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_Error{
			Error: &grpcapi.ErrorResponse{
				Code:    errorCode,
				Message: err.Error(),
			},
		},
	}
}

func makeEntryMetadataList(em []*manifest.EntryMetadata) []*grpcapi.ManifestEntryMetadata {
	var result []*grpcapi.ManifestEntryMetadata

	for _, v := range em {
		result = append(result, makeEntryMetadata(v))
	}

	return result
}

func makeEntryMetadata(em *manifest.EntryMetadata) *grpcapi.ManifestEntryMetadata {
	return &grpcapi.ManifestEntryMetadata{
		Id:           string(em.ID),
		Length:       int32(em.Length), //nolint:gosec
		ModTimeNanos: em.ModTime.UnixNano(),
		Labels:       em.Labels,
	}
}

func (s *Server) handleInitialSessionHandshake(srv grpcapi.KopiaRepository_SessionServer, dr repo.DirectRepository) (repo.WriteSessionOptions, error) {
	initializeReq, err := srv.Recv()
	if err != nil {
		return repo.WriteSessionOptions{}, errors.Wrap(err, "unable to read initialization request")
	}

	ir := initializeReq.GetInitializeSession()
	if ir == nil {
		return repo.WriteSessionOptions{}, errors.New("missing initialization request")
	}

	scc := dr.ContentReader().SupportsContentCompression()

	if err := s.send(srv, initializeReq.GetRequestId(), &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_InitializeSession{
			InitializeSession: &grpcapi.InitializeSessionResponse{
				Parameters: &grpcapi.RepositoryParameters{
					HashFunction:               dr.ContentReader().ContentFormat().GetHashFunction(),
					HmacSecret:                 dr.ContentReader().ContentFormat().GetHmacSecret(),
					Splitter:                   dr.ObjectFormat().Splitter,
					SupportsContentCompression: scc,
				},
			},
		},
	}); err != nil {
		return repo.WriteSessionOptions{}, errors.Wrap(err, "unable to send response")
	}

	return repo.WriteSessionOptions{
		Purpose: ir.GetPurpose(),
	}, nil
}

// RegisterGRPCHandlers registers server gRPC handler.
func (s *Server) RegisterGRPCHandlers(r grpc.ServiceRegistrar) {
	grpcapi.RegisterKopiaRepositoryServer(r, s)
}

func makeGRPCServerState(maxConcurrency int) grpcServerState {
	if maxConcurrency == 0 {
		maxConcurrency = 2 * runtime.NumCPU() //nolint:mnd
	}

	return grpcServerState{
		sem: semaphore.NewWeighted(int64(maxConcurrency)),
	}
}

// GRPCRouterHandler returns HTTP handler that supports GRPC services and
// routes non-GRPC calls to the provided handler.
func (s *Server) GRPCRouterHandler(handler http.Handler) http.Handler {
	grpcServer := grpc.NewServer(
		grpc.MaxSendMsgSize(repo.MaxGRPCMessageSize),
		grpc.MaxRecvMsgSize(repo.MaxGRPCMessageSize),
	)

	s.RegisterGRPCHandlers(grpcServer)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			handler.ServeHTTP(w, r)
		}
	})
}
