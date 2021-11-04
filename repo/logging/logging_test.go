package logging_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/logging"
)

func TestPrefix(t *testing.T) {
	var lines []string

	l0 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	}, "[module1] ")

	l := logging.WithPrefix("PREFIX:", l0)
	l.Debugf("A")
	l.Debugw("S", "a", 1, "b", true, "c", 3.14, "d", "eee")
	l.Infof("B")
	l.Errorf("C")
	l.Warnf("W")

	require.Equal(t, []string{
		"[module1] PREFIX:A",
		"[module1] PREFIX:S a:1 b:true c:3.14 d:\"eee\"",
		"[module1] PREFIX:B",
		"[module1] PREFIX:C",
		"[module1] PREFIX:W",
	}, lines)
}

func TestBroadcast(t *testing.T) {
	var lines []string

	l0 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	}, "[first] ")

	l1 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	}, "[second] ")

	l := logging.Broadcast{l0, l1}
	l.Debugf("A")
	l.Debugw("S", "b", 123)
	l.Infof("B")
	l.Errorf("C")
	l.Warnf("W")

	require.Equal(t, []string{
		"[first] A",
		"[second] A",
		"[first] S b:123",
		"[second] S b:123",
		"[first] B",
		"[second] B",
		"[first] C",
		"[second] C",
		"[first] W",
		"[second] W",
	}, lines)
}

func TestDebugMessageWithKeyValuePairs(t *testing.T) {
	cases := []struct {
		msg           string
		keysAndValues []interface{}
		want          string
	}{
		{"msg", nil, "msg"},
		{"msg", []interface{}{"foo", 1, "bar", true, "baz", 3.14, "str", "string value"}, "msg foo:1 bar:true baz:3.14 str:\"string value\""},
		{"msg", []interface{}{"foo", 1}, "msg foo:1"},
		{"msg", []interface{}{"foo"}, "msg malformed-foo"},
		{"msg", []interface{}{1, 2}, "msg malformed-1:2"},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, logging.DebugMessageWithKeyValuePairs(tc.msg, tc.keysAndValues))
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer

	l := logging.Writer(&buf)("module1")
	l.Debugf("A")
	l.Debugw("S", "b", 123)
	l.Infof("B")
	l.Errorf("C")
	l.Warnf("W")

	require.Equal(t, "A\nS b:123\nB\nC\nW\n", buf.String())
}

func TestNullWriterModule(t *testing.T) {
	l := logging.Module("mod1")(context.Background())

	l.Debugf("A")
	l.Debugw("S", "b", 123)
	l.Infof("B")
	l.Errorf("C")
	l.Warnf("W")
}

func TestNonNullWriterModule(t *testing.T) {
	var buf bytes.Buffer

	ctx := logging.WithLogger(context.Background(), logging.Writer(&buf))
	l := logging.Module("mod1")(ctx)

	l.Debugf("A")
	l.Debugw("S", "b", 123)
	l.Infof("B")
	l.Errorf("C")
	l.Warnf("W")

	require.Equal(t, "A\nS b:123\nB\nC\nW\n", buf.String())
}

func BenchmarkLogger(b *testing.B) {
	mod1 := logging.Module("mod1")
	ctx := logging.WithLogger(context.Background(), logging.PrintfFactory(b.Logf))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mod1(ctx)
	}
}
