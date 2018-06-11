package server

import (
	"net/http"

	"github.com/kopia/kopia/block"
)

type statusResponse struct {
	ConfigFile      string                  `json:"configFile"`
	CacheDir        string                  `json:"cacheDir"`
	BlockFormatting block.FormattingOptions `json:"blockFormatting"`
	Storage         string                  `json:"storage"`
}

func (s *Server) handleStatus(r *http.Request) (interface{}, *apiError) {
	bf := s.rep.Blocks.Format
	bf.HMACSecret = nil
	bf.MasterKey = nil

	return &statusResponse{
		ConfigFile:      s.rep.ConfigFile,
		CacheDir:        s.rep.CacheDirectory,
		BlockFormatting: bf,
		Storage:         s.rep.Storage.ConnectionInfo().Type,
	}, nil
}
