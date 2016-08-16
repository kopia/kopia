package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/kopia/kopia/fs"

	"github.com/kopia/kopia/internal"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showCommand = app.Command("show", "Show contents of a repository object.")

	showObjectIDs = showCommand.Arg("id", "IDs of objects to show").Required().Strings()
	showJSON      = showCommand.Flag("json", "Pretty-print JSON content").Short('j').Bool()
	showRaw       = showCommand.Flag("raw", "Show raw content (disables format auto-detection)").Short('r').Bool()
)

func runShowCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()
	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	for _, oidString := range *showObjectIDs {
		oid, err := parseObjectID(oidString, vlt)
		if err != nil {
			return err
		}

		if err := showObject(mgr, oid); err != nil {
			return err
		}
	}

	return nil
}

func showObject(mgr repo.Repository, oid repo.ObjectID) error {
	r, err := mgr.Open(oid)
	if err != nil {
		return err
	}
	defer r.Close()

	rawdata, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	format := "raw"

	if len(rawdata) >= 8 {
		format = detectProtoFormat(rawdata[0:8])
	}

	if *showJSON {
		format = "json"
	}

	if *showRaw {
		format = "raw"
	}

	switch format {
	case "json":
		var buf bytes.Buffer

		json.Indent(&buf, rawdata, "", "  ")
		os.Stdout.Write(buf.Bytes())

	case "dir":
		return prettyPrintProtoStream(rawdata, internal.ProtoStreamTypeDir, &fs.EntryMetadata{})
	case "indirect":
		return prettyPrintProtoStream(rawdata, internal.ProtoStreamTypeIndirect, &repo.IndirectObjectEntry{})
	case "hashcache":
		return prettyPrintProtoStream(rawdata, internal.ProtoStreamTypeHashCache, &fs.HashCacheEntry{})

	default:
		os.Stdout.Write(rawdata)
	}
	return nil
}

func detectProtoFormat(b []byte) string {
	switch {
	case bytes.Equal(b, internal.ProtoStreamTypeDir):
		return "dir"
	case bytes.Equal(b, internal.ProtoStreamTypeIndirect):
		return "indirect"
	case bytes.Equal(b, internal.ProtoStreamTypeHashCache):
		return "hashcache"
	}
	return "raw"
}

func prettyPrintProtoStream(data []byte, header []byte, pb proto.Message) error {
	r := internal.NewProtoStreamReader(bufio.NewReader(bytes.NewReader(data)), header)
	i := 0
	for {
		err := r.Read(pb)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		fmt.Printf("[entry #%v]\n", i)
		fmt.Println(proto.MarshalTextString(pb))
		i++
	}
}

func init() {
	showCommand.Action(runShowCommand)
}
