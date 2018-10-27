package throttle

import (
	"io"
	"net/http"
)

type throttlerPool interface {
	AddReader(io.ReadCloser) (io.ReadCloser, error)
}

type throttlingRoundTripper struct {
	base         http.RoundTripper
	downloadPool throttlerPool
	uploadPool   throttlerPool
}

func (rt *throttlingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && rt.uploadPool != nil {
		var err error
		req.Body, err = rt.uploadPool.AddReader(req.Body)
		if err != nil {
			return nil, err
		}
	}
	resp, err := rt.base.RoundTrip(req)
	if resp != nil && resp.Body != nil && rt.downloadPool != nil {
		resp.Body, err = rt.downloadPool.AddReader(resp.Body)
	}
	return resp, err
}

// NewRoundTripper returns http.RoundTripper that throttles upload and downloads.
func NewRoundTripper(base http.RoundTripper, downloadPool throttlerPool, uploadPool throttlerPool) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &throttlingRoundTripper{
		base:         base,
		downloadPool: downloadPool,
		uploadPool:   uploadPool,
	}
}
