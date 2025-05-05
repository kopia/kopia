package server

import (
	"context"
	"os"
	"strings"

	"github.com/kopia/kopia/internal/serverapi"
)

func handleCLIInfo(_ context.Context, rc requestContext) (interface{}, *apiError) {
	executable, err := os.Executable()
	if err != nil {
		executable = "kopia"
	}

	return &serverapi.CLIInfo{
		Executable: maybeQuote(executable) + " --config-file=" + maybeQuote(rc.srv.getOptions().ConfigFile) + "",
	}, nil
}

func maybeQuote(s string) string {
	if !strings.Contains(s, " ") {
		return s
	}

	return "\"" + s + "\""
}
