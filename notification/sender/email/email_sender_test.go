package email_test

import (
	"context"
	"net"
	"testing"
	"time"

	smtpmock "github.com/mocktools/go-smtp-mock/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/email"
)

func TestEmailProvider(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := smtpmock.New(smtpmock.ConfigurationAttr{
		LogServerActivity: true,
		LogToStdout:       true,
	})

	require.NoError(t, srv.Start())
	defer srv.Stop()

	p, err := sender.GetSender(ctx, "my-profile", "email", &email.Options{
		SMTPServer: "localhost",
		SMTPPort:   srv.PortNumber(),
		From:       "some-user@example.com",
		To:         "another-user@example.com",
		Format:     sender.FormatHTML,
	})
	require.NoError(t, err)

	require.Equal(t, "SMTP server: \"localhost\", Mail from: \"some-user@example.com\" Mail to: \"another-user@example.com\" Format: \"html\"", p.Summary())

	require.NoError(t, p.Send(ctx, &sender.Message{Subject: "Test", Body: `This is a test.

* one
* two
* three

# Header
## Subheader

- a
- b
- c`, Headers: map[string]string{
		"X-ExtraHeader": "value",
	}}))

	require.Eventually(t, func() bool {
		return len(srv.Messages()) == 1
	}, 10*time.Second, time.Second)
	require.Len(t, srv.Messages(), 1)
	msg := srv.Messages()[0]

	require.Equal(t, "Subject: Test\r\n"+
		"From: some-user@example.com\r\n"+
		"To: another-user@example.com\r\n"+
		"MIME-version: 1.0;\r\n"+
		"Content-Type: text/html; charset=\"UTF-8\";\r\n"+
		"X-ExtraHeader: value\r\n"+
		"\r\n"+
		"This is a test.\r\n"+
		"\r\n"+
		"* one\r\n"+
		"* two\r\n"+
		"* three\r\n"+
		"\r\n"+
		"# Header\r\n"+
		"## Subheader\r\n"+
		"\r\n"+
		"- a\r\n"+
		"- b\r\n"+
		"- c\r\n", msg.MsgRequest())
}

func TestEmailProvider_Text(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := smtpmock.New(smtpmock.ConfigurationAttr{
		LogServerActivity: true,
		LogToStdout:       true,
	})

	require.NoError(t, srv.Start())
	defer srv.Stop()

	p, err := sender.GetSender(ctx, "my-profile", "email", &email.Options{
		SMTPServer: "localhost",
		SMTPPort:   srv.PortNumber(),
		From:       "some-user@example.com",
		To:         "another-user@example.com",
		Format:     sender.FormatPlainText,
	})
	require.NoError(t, err)

	require.Equal(t, "SMTP server: \"localhost\", Mail from: \"some-user@example.com\" Mail to: \"another-user@example.com\" Format: \"txt\"", p.Summary())

	require.NoError(t, p.Send(ctx, &sender.Message{Subject: "Test", Body: `This is a test.

* one
* two
* three

# Header
## Subheader

- a
- b
- c`, Headers: map[string]string{
		"X-ExtraHeader": "value",
	}}))

	require.Eventually(t, func() bool {
		return len(srv.Messages()) == 1
	}, 10*time.Second, time.Second)
	require.Len(t, srv.Messages(), 1)
	msg := srv.Messages()[0]

	require.Equal(t, "Subject: Test\r\n"+
		"From: some-user@example.com\r\n"+
		"To: another-user@example.com\r\n"+
		"X-ExtraHeader: value\r\n"+
		"\r\n"+
		"This is a test.\r\n"+
		"\r\n"+
		"* one\r\n"+
		"* two\r\n"+
		"* three\r\n"+
		"\r\n"+
		"# Header\r\n"+
		"## Subheader\r\n"+
		"\r\n"+
		"- a\r\n"+
		"- b\r\n"+
		"- c\r\n", msg.MsgRequest())
}

func TestEmailProvider_AUTH(t *testing.T) {
	ctx := testlogging.Context(t)

	srv := smtpmock.New(smtpmock.ConfigurationAttr{
		LogServerActivity: true,
		LogToStdout:       true,
	})

	require.NoError(t, srv.Start())
	defer srv.Stop()

	p2, err := sender.GetSender(ctx, "my-profile", "email", &email.Options{
		SMTPServer:   "localhost",
		SMTPPort:     srv.PortNumber(),
		From:         "some-user@example.com",
		To:           "another-user@example.com",
		SMTPIdentity: "some-identity",
		SMTPUsername: "some-username",
		SMTPPassword: "some-password",
		CC:           "cc1@example.com",
	})
	require.NoError(t, err)
	require.ErrorContains(t,
		p2.Send(ctx, &sender.Message{Subject: "Test", Body: "test"}),
		"smtp: server doesn't support AUTH")
}

// stalledSMTPServer accepts TCP connections and then never speaks, which is how a
// wedged SMTP server behaves: the connection succeeds and the greeting never arrives.
func stalledSMTPServer(t *testing.T) (host string, port int) {
	t.Helper()

	var lc net.ListenConfig

	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			go func() {
				<-done

				conn.Close() //nolint:errcheck
			}()
		}
	}()

	t.Cleanup(func() {
		close(done)
		listener.Close() //nolint:errcheck
	})

	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return addr.IP.String(), addr.Port
}

// sendToStalledServer sends a notification to a server that never answers and returns
// the resulting error, failing the test if the send does not give up on its own.
func sendToStalledServer(ctx context.Context, t *testing.T, giveUpAfter time.Duration) error {
	t.Helper()

	host, port := stalledSMTPServer(t)

	p, err := sender.GetSender(testlogging.Context(t), "my-profile", "email", &email.Options{
		SMTPServer: host,
		SMTPPort:   port,
		From:       "some-user@example.com",
		To:         "another-user@example.com",
	})
	require.NoError(t, err)

	errCh := make(chan error, 1)

	go func() {
		errCh <- p.Send(ctx, &sender.Message{Subject: "Test", Body: "test"})
	}()

	select {
	case sendErr := <-errCh:
		return sendErr
	case <-time.After(giveUpAfter):
		t.Fatal("email send did not give up on its own")

		return nil
	}
}

// TestEmailProvider_SendTimeout covers the failure in #5563: the caller's context has no
// deadline of its own, so the provider's own timeout is the only thing that ends the send.
func TestEmailProvider_SendTimeout(t *testing.T) {
	defer email.TestingSetSendTimeout(200 * time.Millisecond)()

	require.ErrorIs(t, sendToStalledServer(context.Background(), t, 30*time.Second), context.DeadlineExceeded)
}

// TestEmailProvider_ContextDeadline covers a caller deadline shorter than the provider's
// own timeout, which must win.
func TestEmailProvider_ContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, sendToStalledServer(ctx, t, 30*time.Second), context.DeadlineExceeded)
}

// TestEmailProvider_Cancellation covers cancellation while the connection is already
// established, which a connection deadline alone does not observe.
func TestEmailProvider_Cancellation(t *testing.T) {
	// A send timeout far longer than the guard below, so that only the cancellation can
	// end the send in time. A connection deadline alone would run to the full timeout.
	defer email.TestingSetSendTimeout(60 * time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	require.ErrorIs(t, sendToStalledServer(ctx, t, 10*time.Second), context.Canceled)
}

func TestEmailProvider_Invalid(t *testing.T) {
	ctx := testlogging.Context(t)

	cases := []struct {
		opt       email.Options
		wantError string
	}{
		{opt: email.Options{}, wantError: "SMTP server must be provided"},
		{opt: email.Options{SMTPServer: "some.server.com"}, wantError: "From address must be provided"},
		{opt: email.Options{SMTPServer: "some.server.com", From: "some@example.com"}, wantError: "To address must be provided"},
	}

	for _, tc := range cases {
		_, err := sender.GetSender(ctx, "my-profile", "email", &tc.opt)
		require.ErrorContains(t, err, tc.wantError)
	}
}

func TestMergeOptions(t *testing.T) {
	var dst email.Options

	require.NoError(t, email.MergeOptions(context.Background(), email.Options{
		SMTPServer: "server1",
		From:       "from1",
		To:         "to1",
	}, &dst, false))

	require.Equal(t, "server1", dst.SMTPServer)
	require.Equal(t, "from1", dst.From)
	require.Equal(t, "to1", dst.To)
	require.Equal(t, "html", dst.Format)

	require.NoError(t, email.MergeOptions(context.Background(), email.Options{
		From: "user2",
	}, &dst, true))

	require.Equal(t, "server1", dst.SMTPServer)
	require.Equal(t, "user2", dst.From)

	require.NoError(t, email.MergeOptions(context.Background(), email.Options{
		SMTPServer: "app2",
		From:       "user2",
	}, &dst, true))

	require.Equal(t, "app2", dst.SMTPServer)
	require.Equal(t, "user2", dst.From)
}
