package cli

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/units"
)

type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

const oneHundredPercent = 100.0

func showContentWithFlags(w io.Writer, rd io.Reader, unzip, indentJSON bool) error {
	if unzip {
		gz, err := gzip.NewReader(rd)
		if err != nil {
			return errors.Wrap(err, "unable to open gzip stream")
		}

		rd = gz
	}

	var buf1, buf2 bytes.Buffer

	if indentJSON {
		if err := iocopy.JustCopy(&buf1, rd); err != nil {
			return errors.Wrap(err, "error copying data")
		}

		if err := json.Indent(&buf2, buf1.Bytes(), "", "  "); err != nil {
			return errors.Wrap(err, "errors indenting JSON")
		}

		rd = io.NopCloser(&buf2)
	}

	if err := iocopy.JustCopy(w, rd); err != nil {
		return errors.Wrap(err, "error copying data")
	}

	return nil
}

func maybeHumanReadableBytes[I integer](enable bool, value I) string {
	if enable {
		return units.BytesString(value)
	}

	return strconv.FormatInt(int64(value), 10)
}

func maybeHumanReadableCount[I integer](enable bool, value I) string {
	if enable {
		return units.Count(value)
	}

	return strconv.FormatInt(int64(value), 10)
}

func formatTimestamp(ts time.Time, tz string) string {
	return convertTimezone(ts, tz).Format("2006-01-02 15:04:05 MST")
}

func formatTimestampPrecise(ts time.Time, tz string) string {
	return convertTimezone(ts, tz).Format("2006-01-02 15:04:05.000 MST")
}

func convertTimezone(ts time.Time, tz string) time.Time {
	switch tz {
	case "local":
		return ts.Local()
	case "utc":
		return ts.UTC()
	case "original":
		return ts
	default:
		loc, err := time.LoadLocation(tz)
		if err == nil {
			return ts.In(loc)
		}

		return ts
	}
}

func formatCompressionPercentage(original, compressed int64) string {
	if compressed >= original {
		return "0%"
	}

	if original == 0 {
		return "0%"
	}

	return fmt.Sprintf("%.1f%%", oneHundredPercent*(1-float64(compressed)/float64(original)))
}

func indentMultilineString(l, prefix string) string {
	var lines []string

	s := bufio.NewScanner(strings.NewReader(l))
	for s.Scan() {
		lines = append(lines, prefix+s.Text())
	}

	return strings.Join(lines, "\n")
}
