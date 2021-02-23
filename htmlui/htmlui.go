// +build embedhtml

package htmlui

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/kopia/kopia/repo/logging"
	"golang.org/x/net/context"
)

//go:embed build
var data embed.FS

// AssetFile return a http.FileSystem instance that data backend by asset.
func AssetFile() http.FileSystem {
	f, err := fs.Sub(data, "build")
	if err != nil {
		logging.GetContextLoggerFunc("htmluil")(context.Background()).Fatalf("Build time error: could not embed htmlui")
	}

	return http.FS(f)
}
