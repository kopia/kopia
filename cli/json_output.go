package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/snapshot"
)

type jsonOutput struct {
	jsonOutput  bool
	jsonIndent  bool
	jsonVerbose bool // output non-essential stats as part of JSON

	out io.Writer
}

func (c *jsonOutput) setup(svc appServices, cmd *kingpin.CmdClause) {
	cmd.Flag("json", "Output result in JSON format to stdout").BoolVar(&c.jsonOutput)
	cmd.Flag("json-indent", "Output result in indented JSON format to stdout").Hidden().BoolVar(&c.jsonIndent)
	cmd.Flag("json-verbose", "Output non-essential data (e.g. statistics) in JSON format").Hidden().BoolVar(&c.jsonVerbose)

	c.out = svc.stdout()
}

func (c *jsonOutput) cleanupSnapshotManifestForJSON(v *snapshot.Manifest) interface{} {
	m := *v

	if !c.jsonVerbose {
		return struct {
			*snapshot.Manifest

			// trick to remove 'stats' completely.
			Stats string `json:"stats,omitempty"`
		}{Manifest: v}
	}

	return &m
}

func (c *jsonOutput) cleanupSnapshotManifestListForJSON(manifests []*snapshot.Manifest) interface{} {
	var res []interface{}

	for _, m := range manifests {
		res = append(res, c.cleanupSnapshotManifestForJSON(m))
	}

	return res
}

func (c *jsonOutput) cleanupForJSON(v interface{}) interface{} {
	switch v := v.(type) {
	case *snapshot.Manifest:
		return c.cleanupSnapshotManifestForJSON(v)
	case []*snapshot.Manifest:
		return c.cleanupSnapshotManifestListForJSON(v)
	default:
		return v
	}
}

func (c *jsonOutput) jsonBytes(v interface{}) []byte {
	return c.jsonIndentedBytes(v, "")
}

func (c *jsonOutput) jsonIndentedBytes(v interface{}, indent string) []byte {
	v = c.cleanupForJSON(v)

	var (
		b   []byte
		err error
	)

	if c.jsonIndent {
		b, err = json.MarshalIndent(v, indent+"", indent+"  ")
	} else {
		b, err = json.Marshal(v)
	}

	if err != nil {
		panic("error serializing JSON, that should not happen: " + err.Error())
	}

	return b
}

type jsonList struct {
	separator string
	o         *jsonOutput
}

func (l *jsonList) begin(o *jsonOutput) {
	l.o = o

	if o.jsonOutput {
		fmt.Fprint(l.o.out, "[") //nolint:errcheck

		if !o.jsonIndent {
			l.separator = "\n "
		}
	}
}

func (l *jsonList) end() {
	if l.o.jsonOutput {
		if !l.o.jsonIndent {
			fmt.Fprint(l.o.out, "\n") //nolint:errcheck
		}

		fmt.Fprint(l.o.out, "]") //nolint:errcheck
	}
}

func (l *jsonList) emit(v interface{}) {
	fmt.Fprintf(l.o.out, "%s%s", l.separator, l.o.jsonBytes(v)) //nolint:errcheck

	if l.o.jsonIndent {
		l.separator = ","
	} else {
		l.separator = ",\n "
	}
}
