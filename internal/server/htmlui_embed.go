//go:build !nohtmlui

package server

import (
	"net/http"

	"github.com/kopia/htmluibuild"
)

// AssetFile exposes HTML UI files.
func AssetFile() http.FileSystem {
	return htmluibuild.AssetFile()
}
