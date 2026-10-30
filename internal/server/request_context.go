package server

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

//nolint:interfacebloat
type serverInterface interface {
	deleteSourceManager(ctx context.Context, src snapshot.SourceInfo) bool
	generateShortTermAuthCookie(username string, now time.Time) (string, error)
	isAuthCookieValid(username, cookieValue string) bool
	getAuthorizer() auth.Authorizer
	getAuthenticator() auth.Authenticator
	getOptions() *Options
	snapshotAllSourceManagers() map[snapshot.SourceInfo]*sourceManager
	taskManager() *uitask.Manager
	Refresh()
	getMountController(ctx context.Context, rep repo.Repository, oid object.ID, createIfNotFound bool) (mount.Controller, error)
	deleteMount(oid object.ID)
	listMounts() map[object.ID]mount.Controller
	disconnect(ctx context.Context) error
	requestShutdown(ctx context.Context)
	getOrCreateSourceManager(ctx context.Context, src snapshot.SourceInfo) *sourceManager
	getInitRepositoryTaskID() string
	getConnectOptions(cliOpts repo.ClientOptions) *repo.ConnectOptions
	SetRepository(ctx context.Context, rep repo.Repository) error
	InitRepositoryAsync(ctx context.Context, mode string, initializer InitRepositoryFunc, wait bool) (string, error)
	rootContext() context.Context
}

type requestContext struct {
	w    http.ResponseWriter
	req  *http.Request
	body []byte
	rep  repo.Repository
	srv  serverInterface
}

func (r *requestContext) muxVar(s string) string {
	return mux.Vars(r.req)[s]
}

func (r *requestContext) queryParam(s string) string {
	return r.req.URL.Query().Get(s)
}
