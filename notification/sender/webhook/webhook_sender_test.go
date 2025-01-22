package webhook_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/webhook"
)

func TestWebhook(t *testing.T) {
	ctx := testlogging.Context(t)

	mux := http.NewServeMux()

	var requests []*http.Request
	var requestBodies []bytes.Buffer

	mux.HandleFunc("/some-path", func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		io.Copy(&b, r.Body)

		requestBodies = append(requestBodies, b)
		requests = append(requests, r)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	p, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: server.URL + "/some-path",
		Method:   "POST",
		Headers:  "X-Some: thing\nX-Another-Header: z",
	})
	require.NoError(t, err)

	require.NoError(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "This is a test.\n\n* one\n* two\n* three\n\n# Header\n## Subheader\n\n- a\n- b\n- c",
		Headers: map[string]string{
			"X-Some-Header": "x",
			"Content-Type":  "some/content-type",
		},
	}))

	p2, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: server.URL + "/some-path",
		Method:   "PUT",
		Headers:  "X-Another-Header: y",
		Format:   "html",
	})
	require.NoError(t, err)

	require.NoError(t, p2.Send(ctx, &sender.Message{
		Subject: "Test 2",
		Body:    "This is a test.\n\n* one\n* two\n* three",
		Headers: map[string]string{
			"Content-Type": "text/html",
		},
	}))

	require.Len(t, requests, 2)

	// first request - POST in md format
	require.Equal(t, "some/content-type", requests[0].Header.Get("Content-Type"))
	require.Equal(t, "x", requests[0].Header.Get("X-Some-Header"))
	require.Equal(t, "thing", requests[0].Header.Get("X-Some"))
	require.Equal(t, "z", requests[0].Header.Get("X-Another-Header"))
	require.Equal(t, "Test", requests[0].Header.Get("Subject"))
	require.Equal(t, "POST", requests[0].Method)
	require.Equal(t,
		"This is a test.\n\n* one\n* two\n* three\n\n# Header\n## Subheader\n\n- a\n- b\n- c",
		requestBodies[0].String())

	// second request - PUT in HTML format
	require.Equal(t, "text/html", requests[1].Header.Get("Content-Type"))
	require.Equal(t, "y", requests[1].Header.Get("X-Another-Header"))
	require.Equal(t, "Test 2", requests[1].Header.Get("Subject"))
	require.Equal(t, "PUT", requests[1].Method)
	require.Equal(t, "This is a test.\n\n* one\n* two\n* three", requestBodies[1].String())

	p3, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: server.URL + "/nonexixtent-path",
	})
	require.NoError(t, err)

	require.Contains(t, p3.Summary(), "Webhook POST http://")

	require.ErrorContains(t, p3.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    `This is a test.`,
	}), "404")
}

func TestWebhook_Failure(t *testing.T) {
	ctx := testlogging.Context(t)
	p, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: "http://localhost:41123/no-such-path",
	})
	require.NoError(t, err)

	require.ErrorContains(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "test",
	}), "error sending webhook notification")
}

func TestWebhook_InvalidURL(t *testing.T) {
	ctx := testlogging.Context(t)
	_, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: "!",
	})
	require.ErrorContains(t, err, "invalid endpoint")
}

func TestWebhook_InvalidURLScheme(t *testing.T) {
	ctx := testlogging.Context(t)
	_, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: "hfasd-ttp:",
	})
	require.ErrorContains(t, err, "invalid endpoint scheme, must be http:// or https://")
}

func TestWebhook_InvalidMethod(t *testing.T) {
	ctx := testlogging.Context(t)
	p, err := sender.GetSender(ctx, "my-profile", "webhook", &webhook.Options{
		Endpoint: "http://localhost:41123/no-such-path",
		Method:   "?",
	})

	require.NoError(t, err)

	require.ErrorContains(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "test",
	}), "net/http: invalid method \"?\"")
}

func TestMergeOptions(t *testing.T) {
	var dst webhook.Options

	require.NoError(t, webhook.MergeOptions(context.Background(), webhook.Options{
		Endpoint: "http://localhost:1234",
		Method:   "POST",
		Format:   "txt",
	}, &dst, false))

	require.Equal(t, "http://localhost:1234", dst.Endpoint)
	require.Equal(t, "POST", dst.Method)
	require.Equal(t, "txt", dst.Format)

	require.NoError(t, webhook.MergeOptions(context.Background(), webhook.Options{
		Method: "PUT",
	}, &dst, true))

	require.Equal(t, "http://localhost:1234", dst.Endpoint)
	require.Equal(t, "PUT", dst.Method)
	require.Equal(t, "txt", dst.Format)

	require.NoError(t, webhook.MergeOptions(context.Background(), webhook.Options{
		Endpoint: "http://localhost:5678",
		Method:   "PUT",
		Format:   "html",
	}, &dst, true))

	require.Equal(t, "http://localhost:5678", dst.Endpoint)
	require.Equal(t, "PUT", dst.Method)
	require.Equal(t, "html", dst.Format)
}
