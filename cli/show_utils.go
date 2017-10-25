package cli

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func showContent(rd io.Reader, unzip, formatJSON bool) error {
	if unzip {
		gz, err := gzip.NewReader(rd)
		if err != nil {
			return fmt.Errorf("unable to open gzip stream: %v", err)
		}

		rd = gz
	}

	var buf1, buf2 bytes.Buffer
	if formatJSON {
		if _, err := io.Copy(&buf1, rd); err != nil {
			return err
		}

		if err := json.Indent(&buf2, buf1.Bytes(), "", "  "); err != nil {
			return err
		}

		rd = ioutil.NopCloser(&buf2)
	}

	if _, err := io.Copy(os.Stdout, rd); err != nil {
		return err
	}

	return nil
}
