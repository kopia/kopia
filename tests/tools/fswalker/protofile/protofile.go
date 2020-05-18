// Package protofile contains helper functions common
// across multiple fswalker tool wrappers
package protofile

import (
	"bytes"
	"io/ioutil"

	"github.com/golang/protobuf/proto" // nolint:staticcheck
	"google.golang.org/protobuf/encoding/prototext"
)

// WriteTextProto writes a text format proto buf for the provided proto message.
func WriteTextProto(path string, pb proto.Message) error {
	blob, err := prototext.Marshal(proto.MessageV2(pb))
	if err != nil {
		return err
	}

	// replace message boundary characters as curly braces look nicer (both is fine to parse)
	blob = bytes.ReplaceAll(blob, []byte("<"), []byte("{"))
	blob = bytes.ReplaceAll(blob, []byte(">"), []byte("}"))

	return ioutil.WriteFile(path, blob, 0644)
}
