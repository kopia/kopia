package mount

import (
	"context"
	"net"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/webdavmount"
)

func webdavServerLogger(r *http.Request, err error) {
	var maybeRange string
	if r := r.Header.Get("Range"); r != "" {
		maybeRange = " " + r
	}

	if err != nil {
		log(r.Context()).Errorf("%v %v%v err: %v\n", r.Method, r.URL.RequestURI(), maybeRange, err)
	} else {
		log(r.Context()).Debugw("OK", "method", r.Method, "url", r.URL.RequestURI(), "range", maybeRange)
	}
}

// DirectoryWebDAV exposes the provided filesystem directory via WebDAV on a random port on localhost
// and returns a controller.
func DirectoryWebDAV(ctx context.Context, entry fs.Directory) (Controller, error) {
	log(ctx).Debugw("creating webdav server...")

	mux := http.NewServeMux()

	var logger func(r *http.Request, err error)

	if os.Getenv("WEBDAV_LOG_REQUESTS") != "" {
		logger = webdavServerLogger
	}

	mux.Handle("/", &webdav.Handler{
		FileSystem: webdavmount.WebDAVFS(entry),
		LockSystem: webdav.NewMemLS(),
		Logger:     logger,
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.Wrap(err, "listen error")
	}

	srv := &http.Server{
		Handler: mux,
	}

	done := make(chan struct{})

	srv.RegisterOnShutdown(func() {
		close(done)
	})

	go func() {
		log(ctx).Debugw("web server finished", "output", srv.Serve(l))
	}()

	return webdavController{"http://" + l.Addr().String(), srv, done}, nil
}

type webdavController struct {
	webdavURL string
	s         *http.Server
	done      chan struct{}
}

func (c webdavController) Unmount(ctx context.Context) error {
	return errors.Wrap(c.s.Shutdown(ctx), "error shutting down webdav server")
}

func (c webdavController) MountPath() string {
	return c.webdavURL
}

func (c webdavController) Done() <-chan struct{} {
	return c.done
}
