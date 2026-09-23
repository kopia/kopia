// Package apiserver implements the gRPC repository server that exposes a Kopia
// repository to remote clients.
package apiserver

import (
	"context"
	"sync"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

var userLog = logging.Module("kopia/server")

// Server holds the connection to a repository along with the gRPC server state
// that serves it to clients.
type Server struct {
	ServerMutex sync.RWMutex

	// +checklocks:ServerMutex
	Rep repo.Repository

	Authenticator auth.Authenticator
	Authorizer    auth.Authorizer

	notifyTemplateOptions notifytemplate.Options

	grpcServerState
}

// New creates a new gRPC repository server.
func New(authenticator auth.Authenticator, authorizer auth.Authorizer, notifyTemplateOptions notifytemplate.Options, maxConcurrency int) *Server {
	return &Server{
		Authenticator:         authenticator,
		Authorizer:            authorizer,
		notifyTemplateOptions: notifyTemplateOptions,
		grpcServerState:       makeGRPCServerState(maxConcurrency),
	}
}

// SetRepository atomically sets the repository (nil indicates not connected).
func (s *Server) SetRepository(_ context.Context, rep repo.Repository) {
	s.ServerMutex.Lock()
	defer s.ServerMutex.Unlock()

	s.Rep = rep
}
