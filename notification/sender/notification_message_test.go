package sender_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/notification/sender"
)

func TestParseMessage(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected *sender.Message
	}{
		{
			name: "ValidMessage",
			input: `Subject: Test Subject
Header1: Value1
InvalidHeaderLine will be dropped
Header2: Value2

This is the body of the message.`,
			expected: &sender.Message{
				Subject: "Test Subject",
				Headers: map[string]string{
					"Header1": "Value1",
					"Header2": "Value2",
				},
				Body: "This is the body of the message.",
			},
		},
		{
			name: "ValidMessage",
			input: `Subject: Test Subject
Header1: Value1
InvalidHeaderLine will be dropped
Header2: Value2

This is the body of the message.`,
			expected: &sender.Message{
				Subject: "Test Subject",
				Headers: map[string]string{
					"Header1": "Value1",
					"Header2": "Value2",
				},
				Body: "This is the body of the message.",
			},
		}, // Add more test cases here...
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.input)
			ctx := testlogging.Context(t)
			actual, err := sender.ParseMessage(ctx, reader)

			require.NoError(t, err)
			require.Equal(t, tc.expected.Subject, actual.Subject, "ParseMessage() Subject mismatch")
			require.Equal(t, tc.expected.Body, actual.Body, "ParseMessage() Body mismatch")
			require.Equal(t, tc.expected.Headers, actual.Headers, "ParseMessage() Headers mismatch")

			actualString := actual.ToString()
			roundTrip, err := sender.ParseMessage(ctx, strings.NewReader(actualString))
			require.NoError(t, err)

			require.Equal(t, tc.expected, roundTrip, "ToString() did not roundtrip")
		})
	}
}

func TestParseMessageNoBody(t *testing.T) {
	reader := strings.NewReader(`Subject: Test Subject`)
	ctx := testlogging.Context(t)
	_, err := sender.ParseMessage(ctx, reader)
	require.ErrorContains(t, err, "no body found in message")
}

func TestToString(t *testing.T) {
	msg := &sender.Message{
		Subject: "Test Subject",
		Headers: map[string]string{
			"Header1": "Value1",
			"Header2": "Value2",
		},
		Body: "This is the body of the message.",
	}

	expected := "Subject: Test Subject\nHeader1: Value1\nHeader2: Value2\n\nThis is the body of the message."
	actual := msg.ToString()

	if actual != expected {
		t.Errorf("ToString() = %v, want %v", actual, expected)
	}
}

func TestValidateMessageFormatAndSetDefault(t *testing.T) {
	var f string

	require.NoError(t, sender.ValidateMessageFormatAndSetDefault(&f, "html"))
	require.Equal(t, "html", f)

	f = "txt"
	require.NoError(t, sender.ValidateMessageFormatAndSetDefault(&f, "html"))
	require.Equal(t, "txt", f)

	f = "html"
	require.NoError(t, sender.ValidateMessageFormatAndSetDefault(&f, "html"))
	require.Equal(t, "html", f)

	f = "bad"
	require.ErrorContains(t, sender.ValidateMessageFormatAndSetDefault(&f, "html"), "invalid format: bad")
}
