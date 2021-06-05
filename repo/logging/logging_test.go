package logging_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/logging"
)

func TestPrefix(t *testing.T) {
	var lines []string

	l0 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	})("module1")

	l := logging.WithPrefix("PREFIX:", l0)
	l.Debugf("A")
	l.Infof("B")
	l.Errorf("C")

	require.Equal(t, []string{
		"[module1] PREFIX:A",
		"[module1] PREFIX:B",
		"[module1] PREFIX:C",
	}, lines)
}

func TestBroadcast(t *testing.T) {
	var lines []string

	l0 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	})("first")

	l1 := logging.Printf(func(msg string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(msg, args...))
	})("second")

	l := logging.Broadcast{l0, l1}
	l.Debugf("A")
	l.Infof("B")
	l.Errorf("C")

	require.Equal(t, []string{
		"[first] A",
		"[second] A",
		"[first] B",
		"[second] B",
		"[first] C",
		"[second] C",
	}, lines)
}
