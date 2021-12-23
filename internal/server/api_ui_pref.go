package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
)

func (s *Server) getUIPreferencesOrEmpty() (serverapi.UIPreferences, error) {
	p := serverapi.UIPreferences{}

	if s.options.UIPreferencesFile == "" {
		return p, nil
	}

	f, err := os.Open(s.options.UIPreferencesFile)
	if os.IsNotExist(err) {
		return p, nil
	}

	if err != nil {
		return p, errors.Wrap(err, "unable to open UI preferences file")
	}

	defer f.Close() //nolint:errcheck,gosec

	if err := json.NewDecoder(f).Decode(&p); err != nil {
		return p, errors.Wrap(err, "invalid UI preferences file")
	}

	return p, nil
}

func (s *Server) handleGetUIPreferences(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	p, err := s.getUIPreferencesOrEmpty()
	if err != nil {
		return nil, internalServerError(err)
	}

	return &p, nil
}

func (s *Server) handleSetUIPreferences(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var p serverapi.UIPreferences

	// verify the JSON is valid by unmarshaling it
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	if err := atomic.WriteFile(s.options.UIPreferencesFile, bytes.NewReader(body)); err != nil {
		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}
