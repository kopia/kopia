package pushover_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/pushover"
)

func TestPushover(t *testing.T) {
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

	p, err := sender.GetSender(ctx, "my-profile", "pushover", &pushover.Options{
		AppToken: "app-token1",
		UserKey:  "user-key1",
		Endpoint: server.URL + "/some-path",
	})
	require.NoError(t, err)

	ph, err := sender.GetSender(ctx, "my-html-profile", "pushover", &pushover.Options{
		AppToken: "app-token1",
		UserKey:  "user-key1",
		Format:   "html",
		Endpoint: server.URL + "/some-path",
	})
	require.NoError(t, err)
	require.Equal(t, "Pushover user \"user-key1\" app \"app-token1\" format \"txt\"", p.Summary())

	require.NoError(t, p.Send(ctx, &sender.Message{Subject: "Test", Body: "This is a test.\n\n* one\n* two\n* three\n\n# Header\n## Subheader\n\n- a\n- b\n- c"}))
	require.NoError(t, ph.Send(ctx, &sender.Message{Subject: "Test", Body: "<p>This is a HTML test</p>"}))

	require.Len(t, requests, 2)
	require.Equal(t, "application/json", requests[0].Header.Get("Content-Type"))

	var body map[string]interface{}

	// Plain-text request
	require.NoError(t, json.NewDecoder(&requestBodies[0]).Decode(&body))

	require.Equal(t, "app-token1", body["token"])
	require.Equal(t, "user-key1", body["user"])
	require.Nil(t, body["html"])
	require.Equal(t, "Test\n\nThis is a test.\n\n* one\n* two\n* three\n\n# Header\n## Subheader\n\n- a\n- b\n- c", body["message"])

	require.NoError(t, json.NewDecoder(&requestBodies[1]).Decode(&body))

	// HTML request
	require.Equal(t, "app-token1", body["token"])
	require.Equal(t, "user-key1", body["user"])
	require.Equal(t, "1", body["html"])
	require.Equal(t, "Test\n\n<p>This is a HTML test</p>", body["message"])

	p2, err := sender.GetSender(ctx, "my-profile", "pushover", &pushover.Options{
		AppToken: "app-token1",
		UserKey:  "user-key1",
		Endpoint: server.URL + "/not-found-path",
	})
	require.NoError(t, err)
	require.ErrorContains(t, p2.Send(ctx, &sender.Message{Subject: "Test", Body: "test"}), "error sending pushover notification")

	p3, err := sender.GetSender(ctx, "my-profile", "pushover", &pushover.Options{
		AppToken: "app-token1",
		UserKey:  "user-key1",
		Endpoint: "http://localhost:59123/not-found-path",
	})
	require.NoError(t, err)
	require.ErrorContains(t, p3.Send(ctx, &sender.Message{Subject: "Test", Body: "test"}), "error sending pushover notification")
}

func TestPushover_Invalid(t *testing.T) {
	ctx := testlogging.Context(t)

	_, err := sender.GetSender(ctx, "my-profile", "pushover", &pushover.Options{})
	require.ErrorContains(t, err, "App Token must be provided")

	_, err = sender.GetSender(ctx, "my-profile", "pushover", &pushover.Options{
		AppToken: "some-token",
	})
	require.ErrorContains(t, err, "User Key must be provided")
}

func TestMergeOptions(t *testing.T) {
	var dst pushover.Options

	require.NoError(t, pushover.MergeOptions(context.Background(), pushover.Options{
		AppToken: "app1",
		UserKey:  "user1",
	}, &dst, false))

	require.Equal(t, "app1", dst.AppToken)
	require.Equal(t, "user1", dst.UserKey)

	require.NoError(t, pushover.MergeOptions(context.Background(), pushover.Options{
		UserKey: "user2",
	}, &dst, true))

	require.Equal(t, "app1", dst.AppToken)
	require.Equal(t, "user2", dst.UserKey)

	require.NoError(t, pushover.MergeOptions(context.Background(), pushover.Options{
		AppToken: "app2",
		UserKey:  "user2",
	}, &dst, true))

	require.Equal(t, "app2", dst.AppToken)
	require.Equal(t, "user2", dst.UserKey)
}
