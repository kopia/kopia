//go:build !no_extra_providers

package rclone

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsRetriableRemoteControlError(t *testing.T) {
	cases := []struct {
		desc string
		err  error
		want bool
	}{
		{desc: "connection-level EOF", err: errors.Wrap(io.EOF, "RC error"), want: true},
		{desc: "server error", err: remoteControlStatusError{statusCode: http.StatusInternalServerError, status: "500 Internal Server Error"}, want: true},
		{desc: "service unavailable", err: remoteControlStatusError{statusCode: http.StatusServiceUnavailable, status: "503 Service Unavailable"}, want: true},
		{desc: "auth failure", err: remoteControlStatusError{statusCode: http.StatusForbidden, status: "403 Forbidden"}, want: false},
		{desc: "process exited", err: errRCloneExited, want: false},
		{desc: "process exited wrapped", err: errors.Wrapf(errRCloneExited, "%v", "exit status 1"), want: false},
		{desc: "context canceled", err: errors.Wrap(context.Canceled, "RC error"), want: false},
		{desc: "client timeout", err: errors.Wrap(&url.Error{Op: "Post", URL: "https://127.0.0.1:1/vfs/forget", Err: context.DeadlineExceeded}, "RC error"), want: false},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, isRetriableRemoteControlError(tc.err), tc.desc)
	}
}
