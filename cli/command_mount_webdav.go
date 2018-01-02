package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/rs/zerolog/log"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/webdavmount"
)

var (
	traceWebDAVServer = mountCommand.Flag("trace-webdav", "Enable tracing on WebDAV server").Hidden().Bool()
)

func webdavServerLogger(r *http.Request, err error) {
	var maybeRange string
	if r := r.Header.Get("Range"); r != "" {
		maybeRange = " " + r
	}
	if err != nil {
		log.Printf("%v %v%v err: %v", r.Method, r.URL.RequestURI(), maybeRange, err)
	} else {
		log.Printf("%v %v%v OK", r.Method, r.URL.RequestURI(), maybeRange)
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
		s.ListenAndServe()
		fmt.Println("Server shut down.")
	}()

	browseMount(mountPoint, fmt.Sprintf("http://%v", s.Addr))

	// Shut down the server and wait for it.
	s.Shutdown(context.Background())
	wg.Wait()
	return nil
}
