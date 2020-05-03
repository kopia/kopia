package server

import (
	"fmt"

	"github.com/kopia/kopia/internal/serverapi"
)

type apiError struct {
	httpErrorCode int
	apiErrorCode  serverapi.APIErrorCode
	message       string
}

func requestError(apiErrorCode serverapi.APIErrorCode, message string) *apiError {
	return &apiError{400, apiErrorCode, message}
}

func notFoundError(message string) *apiError {
	return &apiError{404, serverapi.ErrorNotFound, message}
}

func internalServerError(err error) *apiError {
	return &apiError{500, serverapi.ErrorInternal, fmt.Sprintf("internal server error: %v", err)}
}
