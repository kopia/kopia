package iocopy_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/iocopy"
)

const (
	testBuf    = "Hello, World!"
	lenTestBuf = len(testBuf)
)

type errorWriter struct{}

func (errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error") //nolint:goerr113
}

func TestGetBuffer(t *testing.T) {
	buf := iocopy.GetBuffer()
	require.Len(t, buf, iocopy.BufSize)
}

func TestReleaseBuffer(t *testing.T) {
	buf := iocopy.GetBuffer()
	iocopy.ReleaseBuffer(buf)
	buf2 := iocopy.GetBuffer()
	require.Equal(t, &buf[0], &buf2[0], "Buffer was not recycled after ReleaseBuffer")
}

func TestCopy(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := &bytes.Buffer{}

	n, err := iocopy.Copy(dst, src)
	require.NoError(t, err)
	require.Equal(t, int64(lenTestBuf), n)
	require.Equal(t, testBuf, dst.String())
}

func TestJustCopy(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := &bytes.Buffer{}

	err := iocopy.JustCopy(dst, src)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, testBuf, dst.String())
}

func TestCopyError(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := errorWriter{}

	_, err := iocopy.Copy(dst, src)
	require.Error(t, err)
}

func TestJustCopyError(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := errorWriter{}

	err := iocopy.JustCopy(dst, src)
	require.Error(t, err)
}

type customReader struct {
	io.Reader
}

func TestCustomReader(t *testing.T) {
	src := customReader{strings.NewReader(testBuf)}
	dst := &bytes.Buffer{}

	n, err := iocopy.Copy(dst, src)
	require.NoError(t, err)
	require.Equal(t, n, int64(lenTestBuf))
	require.Equal(t, testBuf, dst.String())
}

type customWriter struct {
	io.Writer
}

func TestCopyWithCustomReaderAndWriter(t *testing.T) {
	src := customReader{strings.NewReader(testBuf)}
	dst := &bytes.Buffer{}
	customDst := customWriter{dst}

	n, err := iocopy.Copy(customDst, src)
	require.NoError(t, err)
	require.Equal(t, n, int64(lenTestBuf))
	require.Equal(t, testBuf, dst.String())
}
