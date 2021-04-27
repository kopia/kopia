package cli

import (
	"encoding/json"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
)

var (
	jsonOutput  = false
	jsonIndent  = false
	jsonVerbose = false // output addnon-essential stats as part of JSON
)

func registerJSONOutputFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("json", "Output result in JSON format to stdout").BoolVar(&jsonOutput)
	cmd.Flag("json-indent", "Output result in indented JSON format to stdout").Hidden().BoolVar(&jsonIndent)
	cmd.Flag("json-verbose", "Output non-essential data (e.g. statistics) in JSON format").Hidden().BoolVar(&jsonVerbose)
}

func cleanupSnapshotManifestForJSON(v *snapshot.Manifest) interface{} {
	m := *v

	if !jsonVerbose {
		return struct {
			*snapshot.Manifest

			// trick to remove 'stats' completely.
			Stats string `json:"stats,omitempty"`
		}{Manifest: v}
	}

	return &m
}

func cleanupSnapshotManifestListForJSON(manifests []*snapshot.Manifest) interface{} {
	var res []interface{}

	for _, m := range manifests {
		res = append(res, cleanupSnapshotManifestForJSON(m))
	}

	return res
}

func cleanupForJSON(v interface{}) interface{} {
	switch v := v.(type) {
	case content.Info:
		return content.ToInfoStruct(v)
	case *snapshot.Manifest:
		return cleanupSnapshotManifestForJSON(v)
	case []*snapshot.Manifest:
		return cleanupSnapshotManifestListForJSON(v)
	default:
		return v
	}
}

func jsonBytes(v interface{}) []byte {
	return jsonIndentedBytes(v, "")
}

func jsonIndentedBytes(v interface{}, indent string) []byte {
	v = cleanupForJSON(v)

	var (
		b   []byte
		err error
	)

	if jsonIndent {
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
}

func (l *jsonList) begin() {
	if jsonOutput {
		printStdout("[")

		if !jsonIndent {
			l.separator = "\n "
		}
	}
}

func (l *jsonList) end() {
	if jsonOutput {
		if !jsonIndent {
			printStdout("\n")
		}

		printStdout("]")
	}
}

func (l *jsonList) emit(v interface{}) {
	printStdout(l.separator)
	printStdout("%s", jsonBytes(v))

	if jsonIndent {
		l.separator = ","
	} else {
		l.separator = ",\n "
	}
}
