package pems

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	OUTPUT_FILEEXT  = "bin"
	FILE_COLLISIONS = 1000
)

var (
	elidePunctAndSpace = regexp.MustCompile("([[:space:][:cntrl:][:punct:]])")
	ErrNoPEMFound      = errors.New("no PEM found")
)

func CreateOutFile(ctx context.Context, prefix, blknm, ext string) (*os.File, error) {
	max := FILE_COLLISIONS
	i := 0
	f, err := TryFile(ctx, prefix, blknm, ext, i)
	for i < max && err != nil && os.IsExist(err) {
		f.Close()
		i++
		f, err = TryFile(ctx, prefix, blknm, ext, i)
	}
	return f, err
}

// FilenameFromBlockName turn PEM header name into string that can be used in a filename.
func FilenameFromBlockName(blknm, ext string, i int) string {
	// PEM specs spaces only - so this is safe
	nm := elidePunctAndSpace.ReplaceAllLiteralString(strings.ToLower(blknm), "_")
	var q string
	if i <= 0 {
		q = fmt.Sprintf("%s.%s", nm, ext)
	} else {
		q = fmt.Sprintf("%s.%d.%s", nm, i, ext)
	}
	return q
}

// TryFile try and create a file.
func TryFile(ctx context.Context, prefix, blknm, ext string, i int) (*os.File, error) {
	// PEM specs spaces only - so this is safe
	fnm := FilenameFromBlockName(blknm, ext, i)
	return os.OpenFile(filepath.Join(prefix, fnm), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
}

func ExportPEMAsFile(ctx context.Context, verbose bool, prefix string, bs []byte) ([]byte, error) {
	// try and decode the next PEM in bs
	blk, rest := pem.Decode(bs)
	if blk == nil {
		// no more PEMs. return.
		return rest, ErrNoPEMFound
	}
	f, err := CreateOutFile(ctx, prefix, blk.Type, OUTPUT_FILEEXT)
	if err != nil {
		return rest, err
	}
	fmt.Fprintf(os.Stdout, "%s\n", f.Name())
	if verbose {
		fmt.Fprintf(os.Stderr, "writing PEM %q as %q\n", blk.Type, f.Name())
	}
	_, err = f.Write(blk.Bytes)
	err1 := f.Close()
	err = errors.Join(err, err1)
	return rest, err
}

// ExportPEMsAsFiles look for byte blocks encoded as PEM in bs.  Export byte blocks to files.
func ExportPEMsAsFiles(ctx context.Context, verbose bool, prefix string, bs []byte) error {
	var err error
	for len(bs) > 0 && err == nil {
		bs, err = ExportPEMAsFile(ctx, verbose, prefix, bs)
	}
	return err
}
