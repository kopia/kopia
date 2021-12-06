package server

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/kopia/kopia/internal/serverapi"
)

func (s *Server) handleCLIInfo(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	executable, err := os.Executable()
	if err != nil {
		executable = "kopia"
	}

	return &serverapi.CLIInfo{
		Executable: maybeQuote(executable) + " --config-file=" + maybeQuote(s.options.ConfigFile) + "",
	}, nil
}

func maybeQuote(s string) string {
	if !strings.Contains(s, " ") {
		return s
	}

	return "\"" + s + "\""
}
