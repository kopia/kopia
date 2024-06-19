package logging_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/logging"
)

func TestBroadcast(t *testing.T) {
	var lines []string

	l0 := testlogging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	}, "[first] ")

	l1 := testlogging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	}, "[second] ")

	l := logging.Broadcast(l0, l1)
	l.Debug("A")
	l.Debugw("S", "b", 123)
	l.Info("B")
	l.Error("C")
	l.Warn("W")

	require.Equal(t, []string{
		"[first] A",
		"[second] A",
		"[first] S\t{\"b\":123}",
		"[second] S\t{\"b\":123}",
		"[first] B",
		"[second] B",
		"[first] C",
		"[second] C",
		"[first] W",
		"[second] W",
	}, lines)
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer

	l := logging.ToWriter(&buf)("module1")
	l.Debug("A")
	l.Debugw("S", "b", 123)
	l.Info("B")
	l.Error("C")
	l.Warn("W")

	require.Equal(t, "A\nS\t{\"b\":123}\nB\nC\nW\n", buf.String())
}

func TestNullWriterModule(t *testing.T) {
	l := logging.Module("mod1")(context.Background())

	l.Debug("A")
	l.Debugw("S", "b", 123)
	l.Info("B")
	l.Error("C")
	l.Warn("W")
}

func TestNonNullWriterModule(t *testing.T) {
	var buf bytes.Buffer

	ctx := logging.WithLogger(context.Background(), logging.ToWriter(&buf))
	l := logging.Module("mod1")(ctx)

	l.Debug("A")
	l.Debugw("S", "b", 123)
	l.Info("B")
	l.Error("C")
	l.Warn("W")

	require.Equal(t, "A\nS\t{\"b\":123}\nB\nC\nW\n", buf.String())
}

func TestWithAdditionalLogger(t *testing.T) {
	var buf, buf2 bytes.Buffer

	ctx := logging.WithLogger(context.Background(), logging.ToWriter(&buf))
	ctx = logging.WithAdditionalLogger(ctx, logging.ToWriter(&buf2))
	l := logging.Module("mod1")(ctx)

	l.Debug("A")
	l.Debugw("S", "b", 123)
	l.Info("B")
	l.Error("C")
	l.Warn("W")

	require.Equal(t, "A\nS\t{\"b\":123}\nB\nC\nW\n", buf.String())
	require.Equal(t, "A\nS\t{\"b\":123}\nB\nC\nW\n", buf2.String())
}

func BenchmarkLogger(b *testing.B) {
	mod1 := logging.Module("mod1")
	ctx := logging.WithLogger(context.Background(), testlogging.PrintfFactory(b.Logf))

	b.ResetTimer()

	for range b.N {
		mod1(ctx)
	}
}
