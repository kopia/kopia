package server

import (
	"fmt"
	"net/http"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
)

type apiError struct {
	httpErrorCode int
	apiErrorCode  serverapi.APIErrorCode
	message       string
}

func requestError(apiErrorCode serverapi.APIErrorCode, message string) *apiError {
	return &apiError{http.StatusBadRequest, apiErrorCode, message}
}

func notFoundError(message string) *apiError {
	return &apiError{http.StatusNotFound, serverapi.ErrorNotFound, message}
}

func accessDeniedError() *apiError {
	return &apiError{http.StatusForbidden, serverapi.ErrorAccessDenied, "access is denied"}
}

func repositoryNotWritableError() *apiError {
	return internalServerError(errors.Errorf("repository is not writable"))
}

func internalServerError(err error) *apiError {
	return &apiError{http.StatusInternalServerError, serverapi.ErrorInternal, fmt.Sprintf("internal server error: %v", err)}
}
