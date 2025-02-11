// Package jsonsender provides a notification sender that writes messages in JSON format to the provided writer.
package jsonsender

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification/sender"
)

type jsonSender struct {
	prefix      string
	out         io.Writer
	minSeverity sender.Severity
}

func (p *jsonSender) Send(ctx context.Context, msg *sender.Message) error {
	if msg.Severity < p.minSeverity {
		return nil
	}

	var buf bytes.Buffer

	buf.WriteString(p.prefix)

	if err := json.NewEncoder(&buf).Encode(msg); err != nil {
		return errors.Wrap(err, "unable to encode JSON")
	}

	_, err := p.out.Write(buf.Bytes())

	return err //nolint:wrapcheck
}

func (p *jsonSender) Summary() string {
	return "JSON sender"
}

func (p *jsonSender) Format() string {
	return sender.FormatPlainText
}

func (p *jsonSender) ProfileName() string {
	return "jsonsender"
}

// NewJSONSender creates a new JSON sender that writes messages to the provided writer.
func NewJSONSender(prefix string, out io.Writer, minSeverity sender.Severity) sender.Sender {
	return &jsonSender{
		prefix:      prefix,
		out:         out,
		minSeverity: minSeverity,
	}
}
