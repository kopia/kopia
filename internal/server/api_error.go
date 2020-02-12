package server

import (
	"fmt"
)

type apiError struct {
	code    int
	message string
}

func requestError(message string) *apiError {
	return &apiError{400, message}
}

func internalServerError(err error) *apiError {
	return &apiError{500, fmt.Sprintf("internal server error: %v", err)}
}
