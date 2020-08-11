package cli

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/webdavmount"
)

var traceWebDAVServer = mountCommand.Flag("trace-webdav", "Enable tracing on WebDAV server").Bool()

func webdavServerLogger(r *http.Request, err error) {
	var maybeRange string
	if r := r.Header.Get("Range"); r != "" {
		maybeRange = " " + r
	}

	if err != nil {
		printStderr("%v %v%v err: %v\n", r.Method, r.URL.RequestURI(), maybeRange, err)
	} else {
		printStderr("%v %v%v OK\n", r.Method, r.URL.RequestURI(), maybeRange)
	}
}

func mountDirectoryWebDAV(ctx context.Context, entry fs.Directory, mountPoint string) error {
	mux := http.NewServeMux()

	var logger func(r *http.Request, err error)

	if *traceWebDAVServer {
		logger = webdavServerLogger
	}

	mux.Handle("/", &webdav.Handler{
		FileSystem: webdavmount.WebDAVFS(entry),
		LockSystem: webdav.NewMemLS(),
		Logger:     logger,
	})

	s := http.Server{
		Addr:    "127.0.0.1:9998",
		Handler: mux,
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		printStderr("Server listening at http://%v/ Press Ctrl-C to shut down.\n", s.Addr)

		if err := s.ListenAndServe(); err != nil {
			log(ctx).Warningf("server shut down with error: %v", err)
		}
	}()

	if err := browseMount(ctx, mountPoint, fmt.Sprintf("http://%v", s.Addr)); err != nil {
		log(ctx).Warningf("unable to browse %v: %v", s.Addr, err)
	}

	// Shut down the server and wait for it.
	if err := s.Shutdown(ctx); err != nil {
		log(ctx).Warningf("shutdown failed: %v", err)
	}

	wg.Wait()

	return nil
}
