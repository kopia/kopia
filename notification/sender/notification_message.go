package sender

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

// Severity represents the severity of a notification message.
type Severity int32

// Message represents a notification message.
type Message struct {
	Subject  string            `json:"subject"`
	Headers  map[string]string `json:"headers,omitempty"`
	Severity Severity          `json:"severity"`
	Body     string            `json:"body"`
}

// ParseMessage parses a notification message string into a Message structure.
func ParseMessage(ctx context.Context, in io.Reader) (*Message, error) {
	var bodyLines []string

	// parse headers until we encounter "MarkdownBody:" or an empty line.
	sr := bufio.NewScanner(in)

	msg := &Message{
		Headers: map[string]string{},
	}

	for sr.Scan() {
		line := sr.Text()

		if line == "" {
			// no more headers after that
			break
		}

		if strings.HasPrefix(line, "Subject:") {
			msg.Subject = strings.TrimSpace(line[len("Subject:"):])
			continue
		}

		// parse headers
		const numParts = 2

		parts := strings.SplitN(line, ":", numParts)
		if len(parts) != numParts {
			log(ctx).Warnw("invalid header line in notification template", "line", line)
			continue
		}

		msg.Headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	for sr.Scan() {
		line := sr.Text()
		bodyLines = append(bodyLines, line)
	}

	msg.Body = strings.Join(bodyLines, "\n")

	if len(bodyLines) == 0 {
		return nil, errors.New("no body found in message")
	}

	return msg, errors.Wrap(sr.Err(), "error reading message")
}

// ToString returns a string representation of the message.
func (m Message) ToString() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "Subject: %v\n", m.Subject)

	headers := maps.Keys(m.Headers)

	sort.Strings(headers)

	for _, k := range headers {
		fmt.Fprintf(&buf, "%v: %v\n", k, m.Headers[k])
	}

	fmt.Fprintf(&buf, "\n%v", m.Body)

	return buf.String()
}

// Supported message formats.
const (
	FormatPlainText = "txt"
	FormatHTML      = "html"
)

// ValidateMessageFormatAndSetDefault validates message the format and sets the default value if empty.
func ValidateMessageFormatAndSetDefault(f *string, defaultValue string) error {
	switch *f {
	case FormatHTML, FormatPlainText:
		// ok
		return nil

	case "":
		*f = defaultValue
		return nil

	default:
		return errors.Errorf("invalid format: %v", *f)
	}
}
