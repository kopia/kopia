package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/webdavmount"
)

var (
	traceWebDAVServer = mountCommand.Flag("trace-webdav", "Enable tracing on WebDAV server").Bool()
)

func webdavServerLogger(r *http.Request, err error) {
	var maybeRange string
	if r := r.Header.Get("Range"); r != "" {
		maybeRange = " " + r
	}
	if err != nil {
		log.Debugf("%v %v%v err: %v", r.Method, r.URL.RequestURI(), maybeRange, err)
	} else {
		log.Debugf("%v %v%v OK", r.Method, r.URL.RequestURI(), maybeRange)
	}
}

func mountDirectoryWebDAV(entry fs.Directory, mountPoint string) error {
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
		fmt.Fprintf(os.Stderr, "Server listening at http://%v/ Press Ctrl-C to shut down.\n", s.Addr)
		if err := s.ListenAndServe(); err != nil {
			log.Warningf("server shut down with error: %v", err)
		}
	}()

	if err := browseMount(mountPoint, fmt.Sprintf("http://%v", s.Addr)); err != nil {
		log.Warningf("unable to browse %v: %v", s.Addr, err)
	}

	// Shut down the server and wait for it.
	if err := s.Shutdown(context.Background()); err != nil {
		log.Warningf("shutdown failed: %v", err)
	}
	wg.Wait()
	return nil
}
