package throttle

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/pkg/errors"
)

type baseRoundTripper struct {
	responses map[*http.Request]*http.Response
}

func (rt *baseRoundTripper) add(req *http.Request, resp *http.Response) (*http.Request, *http.Response) {
	rt.responses[req] = resp
	return req, resp
}

func (rt *baseRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := rt.responses[req]
	if resp != nil {
		return resp, nil
	}

	return nil, errors.Errorf("error occurred")
}

type fakePool struct {
	readers []io.ReadCloser
}

func (fp *fakePool) reset() {
	fp.readers = nil
}

func (fp *fakePool) AddReader(r io.ReadCloser) (io.ReadCloser, error) {
	fp.readers = append(fp.readers, r)
	return r, nil
}

//nolint:gocyclo
func TestRoundTripper(t *testing.T) {
	downloadBody := ioutil.NopCloser(bytes.NewReader([]byte("data1")))
	uploadBody := ioutil.NopCloser(bytes.NewReader([]byte("data1")))

	base := &baseRoundTripper{
		responses: make(map[*http.Request]*http.Response),
	}
	downloadPool := &fakePool{}
	uploadPool := &fakePool{}
	rt := NewRoundTripper(base, downloadPool, uploadPool)

	// Empty request (no request, no response)
	uploadPool.reset()
	downloadPool.reset()

	req1, resp1 := base.add(&http.Request{}, &http.Response{}) //nolint:bodyclose
	resp, err := rt.RoundTrip(req1)                            //nolint:bodyclose

	if resp != resp1 || err != nil {
		t.Errorf("invalid response or error: %v", err)
	}

	if len(downloadPool.readers) != 0 || len(uploadPool.readers) != 0 {
		t.Errorf("invalid pool contents: %v %v", downloadPool.readers, uploadPool.readers)
	}

	// Upload request
	uploadPool.reset()
	downloadPool.reset()

	req2, resp2 := base.add(&http.Request{ //nolint:bodyclose
		Body: uploadBody,
	}, &http.Response{})
	resp, err = rt.RoundTrip(req2) //nolint:bodyclose

	if resp != resp2 || err != nil {
		t.Errorf("invalid response or error: %v", err)
	}

	if len(downloadPool.readers) != 0 || len(uploadPool.readers) != 1 {
		t.Errorf("invalid pool contents: %v %v", downloadPool.readers, uploadPool.readers)
	}

	// Download request
	uploadPool.reset()
	downloadPool.reset()

	req3, resp3 := base.add(&http.Request{}, &http.Response{Body: downloadBody}) //nolint:bodyclose
	resp, err = rt.RoundTrip(req3)                                               //nolint:bodyclose

	if resp != resp3 || err != nil {
		t.Errorf("invalid response or error: %v", err)
	}

	if len(downloadPool.readers) != 1 || len(uploadPool.readers) != 0 {
		t.Errorf("invalid pool contents: %v %v", downloadPool.readers, uploadPool.readers)
	}

	// Upload/Download request
	uploadPool.reset()
	downloadPool.reset()

	req4, resp4 := base.add(&http.Request{Body: uploadBody}, &http.Response{Body: downloadBody}) //nolint:bodyclose

	resp, err = rt.RoundTrip(req4) //nolint:bodyclose
	if resp != resp4 || err != nil {
		t.Errorf("invalid response or error: %v", err)
	}

	if len(downloadPool.readers) != 1 || len(uploadPool.readers) != 1 {
		t.Errorf("invalid pool contents: %v %v", downloadPool.readers, uploadPool.readers)
	}
}
