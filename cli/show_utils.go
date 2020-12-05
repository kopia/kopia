package cli

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/units"
)

var (
	commonIndentJSON bool
	commonUnzip      bool

	timeZone = app.Flag("timezone", "Format time according to specified time zone (local, utc, original or time zone name)").Default("local").Hidden().String()
)

func setupShowCommand(cmd *kingpin.CmdClause) {
	cmd.Flag("json", "Pretty-print JSON content").Short('j').BoolVar(&commonIndentJSON)
	cmd.Flag("unzip", "Transparently unzip the content").Short('z').BoolVar(&commonUnzip)
}

func showContent(rd io.Reader) error {
	return showContentWithFlags(rd, commonUnzip, commonIndentJSON)
}

func showContentWithFlags(rd io.Reader, unzip, indentJSON bool) error {
	if unzip {
		gz, err := gzip.NewReader(rd)
		if err != nil {
			return errors.Wrap(err, "unable to open gzip stream")
		}

		rd = gz
	}

	var buf1, buf2 bytes.Buffer

	if indentJSON {
		if _, err := iocopy.Copy(&buf1, rd); err != nil {
			return err
		}

		if err := json.Indent(&buf2, buf1.Bytes(), "", "  "); err != nil {
			return err
		}

		rd = ioutil.NopCloser(&buf2)
	}

	if _, err := iocopy.Copy(os.Stdout, rd); err != nil {
		return err
	}

	return nil
}

func maybeHumanReadableBytes(enable bool, value int64) string {
	if enable {
		return units.BytesStringBase10(value)
	}

	return fmt.Sprintf("%v", value)
}

func maybeHumanReadableCount(enable bool, value int64) string {
	if enable {
		return units.Count(value)
	}

	return fmt.Sprintf("%v", value)
}

func formatTimestamp(ts time.Time) string {
	return convertTimezone(ts).Format("2006-01-02 15:04:05 MST")
}

func formatTimestampPrecise(ts time.Time) string {
	return convertTimezone(ts).Format("2006-01-02 15:04:05.000 MST")
}

func convertTimezone(ts time.Time) time.Time {
	switch *timeZone {
	case "local":
		return ts.Local()
	case "utc":
		return ts.UTC()
	case "original":
		return ts
	default:
		loc, err := time.LoadLocation(*timeZone)
		if err == nil {
			return ts.In(loc)
		}

		return ts
	}
}
