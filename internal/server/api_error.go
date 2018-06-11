package server

import (
	"fmt"
)

type apiError struct {
	code    int
	message string
}

func internalServerError(err error) *apiError {
	return &apiError{500, fmt.Sprintf("internal server error: %v", err)}
}
