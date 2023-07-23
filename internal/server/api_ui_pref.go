package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
)

func getUIPreferencesOrEmpty(s serverInterface) (serverapi.UIPreferences, error) {
	p := serverapi.UIPreferences{}

	if s.getOptions().UIPreferencesFile == "" {
		return p, nil
	}

	f, err := os.Open(s.getOptions().UIPreferencesFile)
	if os.IsNotExist(err) {
		return p, nil
	}

	if err != nil {
		return p, errors.Wrap(err, "unable to open UI preferences file")
	}

	defer f.Close() //nolint:errcheck

	if err := json.NewDecoder(f).Decode(&p); err != nil {
		return p, errors.Wrap(err, "invalid UI preferences file")
	}

	return p, nil
}

func handleGetUIPreferences(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	p, err := getUIPreferencesOrEmpty(rc.srv)
	if err != nil {
		return nil, internalServerError(err)
	}

	return &p, nil
}

func handleSetUIPreferences(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var p serverapi.UIPreferences

	// verify the JSON is valid by unmarshaling it
	if err := json.Unmarshal(rc.body, &p); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "malformed request")
	}

	if err := atomic.WriteFile(rc.srv.getOptions().UIPreferencesFile, bytes.NewReader(rc.body)); err != nil {
		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}
