package webdav

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/kopia/kopia/internal/retry"
)

type retriableError struct {
	inner error
}

func (e *retriableError) Error() string {
	return fmt.Sprintf("retriable: %v", e.inner)
}

func (d *davStorage) executeRequest(req *http.Request, body []byte) (*http.Response, error) {
	v, err := retry.WithExponentialBackoff(fmt.Sprintf("%v %v", req.Method, req.URL.RequestURI()), func() (interface{}, error) {
		resp, err := d.executeRequestInternal(req, body)
		if err != nil {
			// Failed to receive response.
			return nil, &retriableError{err}
		}

		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			// Retry on server errors.
			resp.Body.Close() //nolint:errcheck
			return nil, &retriableError{fmt.Errorf("server returned status %v", resp.StatusCode)}
		}

		return resp, nil
	}, func(e error) bool {
		_, ok := e.(*retriableError)
		return ok
	})
	if err != nil {
		return nil, err
	}

	return v.(*http.Response), nil
}

func (d *davStorage) executeRequestInternal(req *http.Request, body []byte) (*http.Response, error) {
	if body != nil {
		req.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	defer resp.Body.Close() // nolint:errcheck

	method, params := parseAuthParams(resp.Header.Get("WWW-Authenticate"))
	switch method {
	case "Basic":
		req.SetBasicAuth(d.Username, d.Password)

	case "Digest":
		var ha1, ha2 string

		nonce := params["nonce"]
		realm := params["realm"]
		algo := params["algorithm"]
		opaque := params["opaque"]
		if algo == "" {
			algo = "MD5"
		}
		qop := params["qop"]

		switch algo {
		case "MD5":
			ha1 = h(fmt.Sprintf("%s:%s:%s", d.Username, realm, d.Password))

		default:
			// TODO - implement me
			return nil, fmt.Errorf("unsupported digest algorithm: %q", algo)
		}

		switch qop {
		case "auth", "":
			ha2 = h(fmt.Sprintf("%s:%s", req.Method, req.URL.RequestURI()))

		default:
			// TODO - implement me
			return nil, fmt.Errorf("unsupported digest qop: %q", qop)
		}

		switch qop {
		case "auth":
			cnonce := makeClientNonce()
			nonceCount := atomic.AddInt32(&d.clientNonceCount, 1)
			response := h(fmt.Sprintf("%s:%s:%08x:%s:%s:%s", ha1, nonce, nonceCount, cnonce, qop, ha2))
			authHeader := fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", cnonce="%s", nc=%08x, qop=%s, response="%s", algorithm=%s`,
				d.Username, realm, nonce, req.URL.RequestURI(), cnonce, nonceCount, qop, response, algo)
			if opaque != "" {
				authHeader += fmt.Sprintf(`, opaque="%s"`, opaque)
			}
			req.Header.Add("Authorization", authHeader)

		case "":
			response := h(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
			authHeader := fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", qop=%s, response="%s", algorithm=%s`,
				d.Username, realm, nonce, req.URL.RequestURI(), qop, response, algo)
			if opaque != "" {
				authHeader += fmt.Sprintf(`, opaque="%s"`, opaque)
			}
			req.Header.Add("Authorization", authHeader)
		}

	default:
		return nil, fmt.Errorf("unsupported authentication scheme: %q", method)
	}

	// Reset the body and re-run the request after auth headers have been added
	if body != nil {
		req.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	return d.Client.Do(req)
}

func makeClientNonce() string {
	tmp := make([]byte, 8)
	io.ReadFull(rand.Reader, tmp) //nolint:errcheck
	return hex.EncodeToString(tmp)
}

func h(s string) string {
	h := md5.New()
	io.WriteString(h, s) //nolint:errcheck
	return fmt.Sprintf("%x", h.Sum(nil))
}

func parseAuthParams(s string) (string, map[string]string) {
	p := strings.Index(s, " ")
	if p < 0 {
		return s, nil
	}

	method := s[0:p]
	parts := strings.Split(s[p+1:], ",")
	params := map[string]string{}
	for _, p := range parts {
		eq := strings.Index(p, "=")
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(p[0:eq])
		value := strings.Trim(p[eq+1:], "\"")
		params[key] = value
	}

	return method, params
}
