package mount

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

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
		log(r.Context()).Debugf("%v %v%v OK\n", r.Method, r.URL.RequestURI(), maybeRange)
	}
}

// DirectoryWebDAV exposes the provided filesystem directory via WebDAV on localhost at the specified port
// and returns a controller. If port is 0, a random port will be used.
func DirectoryWebDAV(ctx context.Context, entry fs.Directory, port int) (Controller, error) {
	log(ctx).Debug("creating webdav server...")

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

	addr := "127.0.0.1:0"
	if port > 0 {
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "listen error")
	}

	srv := &http.Server{
		ReadHeaderTimeout: 15 * time.Second, //nolint:mnd
		Handler:           mux,
	}

	done := make(chan struct{})

	srv.RegisterOnShutdown(func() {
		close(done)
	})

	go func() {
		log(ctx).Debugf("web server finished with %v", srv.Serve(l))
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
