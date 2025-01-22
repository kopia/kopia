package email_test

import (
	"context"
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

	require.NoError(t, p.Send(ctx, &sender.Message{Subject: "Test", Body: `
This is a test.

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

	require.NoError(t, p.Send(ctx, &sender.Message{Subject: "Test", Body: `
This is a test.

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
