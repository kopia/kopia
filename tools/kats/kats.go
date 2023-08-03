package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/kopia/kopia/tools/kats/pems"
)

const (
	FILE_SIZE_B_MAX = 1 << 24
)

var (
	ErrOverflow = errors.New(fmt.Sprintf("input exceeds maximum input size of %d", FILE_SIZE_B_MAX))
	flgs        *flag.FlagSet
	verbose     bool
)

func init() {
	flgs = flag.NewFlagSet(path.Base(os.Args[0]), flag.ExitOnError)
	flgs.BoolVar(&verbose, "verbose", false, "verbose output")
	err := flgs.Parse(os.Args[1:])
	if err != nil {
		flgs.Usage()
		exit("flags", err)
	}
}

func main() {
	ctx := context.Background()
	args := flgs.Args()
	// will use stdin if no args are supplied
	if len(args) == 0 {
		// not ideal ... will work for smallish files on stdin
		buf := &bytes.Buffer{}
		// copy Stdin, up to FILE_SIZE_B_MAX bytes.
		n, err := io.CopyN(buf, os.Stdin, FILE_SIZE_B_MAX)
		if n == FILE_SIZE_B_MAX {
			exit("read", ErrOverflow)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			exit("read", err)
		}
		err = pems.ExportPEMsAsFiles(ctx, verbose, "", buf.Bytes())
		if err != nil && !errors.Is(err, pems.ErrNoPEMFound) {
			exit("export", err)
		}
		return
	}
	for i := range args {
		bs, err := os.ReadFile(args[i])
		err1 := pems.ExportPEMsAsFiles(ctx, verbose, "", bs)
		err = errors.Join(err, err1)
		if err != nil {
			exit("export", err)
		}
	}
}

func exit(where string, err error) {
	if err == nil {
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "%s err: %v\n", where, err)
	os.Exit(1)
}
