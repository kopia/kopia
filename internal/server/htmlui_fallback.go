//go:build nohtmlui

package server

import (
	"embed"
	"net/http"
)

//go:embed index.html
var data embed.FS

// AssetFile return a http.FileSystem instance that data backend by asset.
func AssetFile() http.FileSystem {
	return http.FS(data)
}
