package server

import (
	"net/http"

	"github.com/kopia/kopia/internal/serverapi"
)

func (s *Server) handleStatus(r *http.Request) (interface{}, *apiError) {
	bf := s.rep.Blocks.Format
	bf.HMACSecret = nil
	bf.MasterKey = nil

	return &serverapi.StatusResponse{
		ConfigFile:      s.rep.ConfigFile,
		CacheDir:        s.rep.CacheDirectory,
		BlockFormatting: bf,
		Storage:         s.rep.Storage.ConnectionInfo().Type,
	}, nil
}
