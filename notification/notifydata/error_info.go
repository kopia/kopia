package notifydata

import (
	"fmt"
	"time"
)

// ErrorInfo represents information about errors.
type ErrorInfo struct {
	Operation        string    `json:"operation"`
	OperationDetails string    `json:"operationDetails"`
	StartTime        time.Time `json:"start"`
	EndTime          time.Time `json:"end"`
	ErrorMessage     string    `json:"error"`
	ErrorDetails     string    `json:"errorDetails"`
}

// StartTimestamp returns the start time of the operation that caused the error.
func (e *ErrorInfo) StartTimestamp() time.Time {
	return e.StartTime.Truncate(time.Second)
}

// EndTimestamp returns the end time of the operation that caused the error.
func (e *ErrorInfo) EndTimestamp() time.Time {
	return e.EndTime.Truncate(time.Second)
}

// Duration returns the duration of the operation.
func (e *ErrorInfo) Duration() time.Duration {
	return e.EndTimestamp().Sub(e.StartTimestamp())
}

// NewErrorInfo creates a new ErrorInfo.
func NewErrorInfo(operation, operationDetails string, startTime, endTime time.Time, err error) *ErrorInfo {
	return &ErrorInfo{
		Operation:        operation,
		OperationDetails: operationDetails,
		StartTime:        startTime,
		EndTime:          endTime,
		ErrorMessage:     fmt.Sprintf("%v", err),
		ErrorDetails:     fmt.Sprintf("%+v", err),
	}
}
