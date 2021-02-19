// +build embedhtml

package server

import (
	"net/http"

	"github.com/kopia/kopia/htmlui"
)

func AssetFile() http.FileSystem {
	return htmlui.AssetFile()
}
