package cli

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"golang.org/x/net/webdav"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fscache"
	"github.com/kopia/kopia/internal/webdavmount"
)

func mountDirectoryWebDAV(entry fs.Directory, mountPoint string, cache *fscache.Cache) error {
	mux := http.NewServeMux()
	mux.Handle("/", &webdav.Handler{
		FileSystem: webdavmount.WebDAVFS(entry, cache),
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("%v %v err: %v", r.Method, r.URL.RequestURI(), err)
			} else {
				log.Printf("%v %v OK", r.Method, r.URL.RequestURI())
			}
		},
	})

	s := http.Server{
		Addr:    "127.0.0.1:9998",
		Handler: mux,
	}

	onCtrlC(func() {
		s.Shutdown(context.Background())
	})

	fmt.Printf("Server listening at http://%v/ Press Ctrl-C to shut down.\n", s.Addr)

	err := s.ListenAndServe()
	if err == http.ErrServerClosed {
		fmt.Println("Server shut down.")

	}
	if err != http.ErrServerClosed {
		return err
	}

	return nil
}
