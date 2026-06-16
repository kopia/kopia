package nats_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natslib "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/nats"
)

const testServerReadyTimeout = 5 * time.Second

func startTestServer(t *testing.T, opts *natsserver.Options) *natsserver.Server {
	t.Helper()

	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}

	opts.Port = -1 // pick a random free port

	srv, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	srv.Start()
	t.Cleanup(srv.Shutdown)

	require.True(t, srv.ReadyForConnections(testServerReadyTimeout), "NATS test server did not become ready")

	return srv
}

func TestNats(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := startTestServer(t, &natsserver.Options{})

	sub, err := natslib.Connect(srv.ClientURL())
	require.NoError(t, err)

	defer sub.Close()

	msgCh := make(chan *natslib.Msg, 1)

	subscription, err := sub.ChanSubscribe("kopia.notifications", msgCh)
	require.NoError(t, err)

	defer subscription.Unsubscribe() //nolint:errcheck

	p, err := sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		ServerURL: srv.ClientURL(),
		Subject:   "kopia.notifications",
	})
	require.NoError(t, err)

	require.Contains(t, p.Summary(), "NATS")
	require.Contains(t, p.Summary(), "kopia.notifications")

	require.NoError(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "This is a test.",
		Headers: map[string]string{
			"X-Some-Header": "x",
		},
	}))

	select {
	case m := <-msgCh:
		require.Equal(t, "This is a test.", string(m.Data))
		require.Equal(t, "Test", m.Header.Get("Subject"))
		require.Equal(t, "x", m.Header.Get("X-Some-Header"))
	case <-time.After(testServerReadyTimeout):
		t.Fatal("timed out waiting for NATS message")
	}
}

func TestNats_Auth(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := startTestServer(t, &natsserver.Options{
		Username: "alice",
		Password: "wonderland",
	})

	p, err := sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		ServerURL: srv.ClientURL(),
		Subject:   "kopia.notifications",
		Username:  "alice",
		Password:  "wonderland",
	})
	require.NoError(t, err)

	require.NoError(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "This is a test.",
	}))

	p2, err := sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		ServerURL: srv.ClientURL(),
		Subject:   "kopia.notifications",
		Username:  "alice",
		Password:  "wrong-password",
	})
	require.NoError(t, err)

	require.ErrorContains(t, p2.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "This is a test.",
	}), "error connecting to NATS server")
}

func TestNats_ConnectFailure(t *testing.T) {
	ctx := testlogging.Context(t)

	p, err := sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		ServerURL: "nats://127.0.0.1:1",
		Subject:   "kopia.notifications",
	})
	require.NoError(t, err)

	require.ErrorContains(t, p.Send(ctx, &sender.Message{
		Subject: "Test",
		Body:    "test",
	}), "error connecting to NATS server")
}

func TestNats_InvalidOptions(t *testing.T) {
	ctx := testlogging.Context(t)

	_, err := sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		Subject: "kopia.notifications",
	})
	require.ErrorContains(t, err, "server URL must be provided")

	_, err = sender.GetSender(ctx, "my-profile", nats.ProviderType, &nats.Options{
		ServerURL: "nats://127.0.0.1:4222",
	})
	require.ErrorContains(t, err, "subject must be provided")
}

func TestMergeOptions(t *testing.T) {
	var dst nats.Options

	require.NoError(t, nats.MergeOptions(context.Background(), nats.Options{
		ServerURL: "nats://localhost:4222",
		Subject:   "kopia.notifications",
		Format:    "txt",
	}, &dst, false))

	require.Equal(t, "nats://localhost:4222", dst.ServerURL)
	require.Equal(t, "kopia.notifications", dst.Subject)
	require.Equal(t, "txt", dst.Format)

	require.NoError(t, nats.MergeOptions(context.Background(), nats.Options{
		Subject: "kopia.other",
	}, &dst, true))

	require.Equal(t, "nats://localhost:4222", dst.ServerURL)
	require.Equal(t, "kopia.other", dst.Subject)
	require.Equal(t, "txt", dst.Format)

	require.NoError(t, nats.MergeOptions(context.Background(), nats.Options{
		ServerURL: "nats://localhost:5678",
		Subject:   "kopia.third",
		Format:    "html",
	}, &dst, true))

	require.Equal(t, "nats://localhost:5678", dst.ServerURL)
	require.Equal(t, "kopia.third", dst.Subject)
	require.Equal(t, "html", dst.Format)
}
