package main

import (
	"bytes"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

var verbose bool
var flgs *flag.FlagSet

func init() {
	flgs = flag.NewFlagSet(path.Base(os.Args[0]), flag.ExitOnError)
	flgs.BoolVar(&verbose, "verbose", false, "verbose output")
	err := flgs.Parse(os.Args[1:])
	if err != nil {
		flgs.Usage()
		exit("flags", err)
	}
}

func isPipe(f *os.File) (bool, error) {
	instat, err := f.Stat()
	if err != nil {
		return false, err
	}
	return (instat.Mode() & os.ModeNamedPipe) != 0, nil
}

func createOutFile(blknm, ext string) (*os.File, error) {
	max := 1000
	i := 0
	f, err := tryFile(blknm, ext, i)
	for i < max && err != nil && os.IsExist(err) {
		f.Close()
		i++
		f, err = tryFile(blknm, ext, i)
	}
	return f, err
}

func tryFile(blknm, ext string, i int) (*os.File, error) {
	// PEM specs spaces only - so this is safe
	nm := strings.ReplaceAll(strings.ToLower(blknm), " ", "_")
	var fnm string
	if i <= 0 {
		fnm = fmt.Sprintf("%s.%s", nm, ext)
	} else {
		fnm = fmt.Sprintf("%s.%d.%s", nm, i, ext)
	}
	return os.OpenFile(fnm, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
}

func exportFiles(bs []byte) error {
	var err error
	for len(bs) > 0 && err == nil {
		blk, rest := pem.Decode(bs)
		if blk == nil {
			return nil
		}
		var f *os.File
		f, err = createOutFile(blk.Type, "bin")
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", f.Name())
		if verbose {
			fmt.Fprintf(os.Stderr, "writing PEM %q as %q\n", blk.Type, f.Name())
		}
		_, err = f.Write(blk.Bytes)
		err1 := f.Close()
		err = errors.Join(err, err1)
		if err != nil {
			return err
		}
		bs = rest
	}
	return nil
}

func main() {
	args := flgs.Args()
	if len(args) == 0 {
		isp, err := isPipe(os.Stdin)
		if !isp {
			flgs.Usage()
			exit("open", err)
		}
		// not ideal ... will work for smallish files on stdin
		buf := &bytes.Buffer{}
		_, err = io.CopyN(buf, os.Stdin, 1<<31)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		err1 := exportFiles(buf.Bytes())
		err = errors.Join(err, err1)
		if err != nil {
			exit("export", err)
		}
		return
	}
	for i := range args {
		bs, err := os.ReadFile(args[i])
		err1 := exportFiles(bs)
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
