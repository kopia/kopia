package server

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/grpcapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
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

	username, err := s.authenticateGRPCSession(ctx, dr)
	if err != nil {
		return err
	}

	authz := s.authorizer.Authorize(ctx, dr, username)
	if authz == nil {
		authz = auth.NoAccess()
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.PermissionDenied, "peer not found in context")
	}

	log(ctx).Infof("starting session for user %q from %v", username, p.Addr)
	defer log(ctx).Infof("session ended for user %q from %v", username, p.Addr)

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
			req := req

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

				resp := handleSessionRequest(ctx, dw, authz, req)

				if err := s.send(srv, req.RequestId, resp); err != nil {
					select {
					case lastErr <- err:
					default:
					}
				}
			}()
		}

		return nil
	})
}

func handleSessionRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.SessionRequest) *grpcapi.SessionResponse {
	switch inner := req.GetRequest().(type) {
	case *grpcapi.SessionRequest_GetContentInfo:
		return handleGetContentInfoRequest(ctx, dw, authz, inner.GetContentInfo)

	case *grpcapi.SessionRequest_GetContent:
		return handleGetContentRequest(ctx, dw, authz, inner.GetContent)

	case *grpcapi.SessionRequest_WriteContent:
		return handleWriteContentRequest(ctx, dw, authz, inner.WriteContent)

	case *grpcapi.SessionRequest_Flush:
		return handleFlushRequest(ctx, dw, authz, inner.Flush)

	case *grpcapi.SessionRequest_GetManifest:
		return handleGetManifestRequest(ctx, dw, authz, inner.GetManifest)

	case *grpcapi.SessionRequest_PutManifest:
		return handlePutManifestRequest(ctx, dw, authz, inner.PutManifest)

	case *grpcapi.SessionRequest_FindManifests:
		return handleFindManifestsRequest(ctx, dw, authz, inner.FindManifests)

	case *grpcapi.SessionRequest_DeleteManifest:
		return handleDeleteManifestRequest(ctx, dw, authz, inner.DeleteManifest)

	case *grpcapi.SessionRequest_PrefetchContents:
		return handlePrefetchContentsRequest(ctx, dw, authz, inner.PrefetchContents)

	case *grpcapi.SessionRequest_InitializeSession:
		return errorResponse(errors.Errorf("InitializeSession must be the first request in a session"))

	default:
		return errorResponse(errors.Errorf("unhandled session request"))
	}
}

func handleGetContentInfoRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.GetContentInfoRequest) *grpcapi.SessionResponse {
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
					Id:               ci.GetContentID().String(),
					PackedLength:     ci.GetPackedLength(),
					TimestampSeconds: ci.GetTimestampSeconds(),
					PackBlobId:       string(ci.GetPackBlobID()),
					PackOffset:       ci.GetPackOffset(),
					Deleted:          ci.GetDeleted(),
					FormatVersion:    uint32(ci.GetFormatVersion()),
					OriginalLength:   ci.GetOriginalLength(),
				},
			},
		},
	}
}

func handleGetContentRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.GetContentRequest) *grpcapi.SessionResponse {
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

func handleFindManifestsRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.FindManifestsRequest) *grpcapi.SessionResponse {
	em, err := dw.FindManifests(ctx, req.GetLabels())
	if err != nil {
		return errorResponse(err)
	}

	// only return manifests which the caller can read
	var filtered []*manifest.EntryMetadata

	for _, m := range em {
		if authz.ManifestAccessLevel(m.Labels) < auth.AccessLevelRead {
			continue
		}

		filtered = append(filtered, m)
	}

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_FindManifests{
			FindManifests: &grpcapi.FindManifestsResponse{
				Metadata: makeEntryMetadataList(filtered),
			},
		},
	}
}

func handleDeleteManifestRequest(ctx context.Context, dw repo.DirectRepositoryWriter, authz auth.AuthorizationInfo, req *grpcapi.DeleteManifestRequest) *grpcapi.SessionResponse {
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
	if authz.ContentAccessLevel() < auth.AccessLevelRead {
		return accessDeniedResponse()
	}

	contentIDs, err := content.IDsFromStrings(req.ContentIds)
	if err != nil {
		return errorResponse(err)
	}

	cids := rep.PrefetchContents(ctx, contentIDs, req.Hint)

	return &grpcapi.SessionResponse{
		Response: &grpcapi.SessionResponse_PrefetchContents{
			PrefetchContents: &grpcapi.PrefetchContentsResponse{
				ContentIds: content.IDsToStrings(cids),
			},
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
		Length:       int32(em.Length),
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
		return repo.WriteSessionOptions{}, errors.Errorf("missing initialization request")
	}

	scc, err := dr.ContentReader().SupportsContentCompression()
	if err != nil {
		return repo.WriteSessionOptions{}, errors.Wrap(err, "supports content compression")
	}

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
		maxConcurrency = 2 * runtime.NumCPU() //nolint:gomnd
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
