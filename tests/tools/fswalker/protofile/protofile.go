// Package protofile contains helper functions common
// across multiple fswalker tool wrappers
package protofile

import (
	"io/ioutil"
	"strings"

	"github.com/golang/protobuf/proto"
)

// WriteTextProto writes a text format proto buf for the provided proto message.
func WriteTextProto(path string, pb proto.Message) error {
	blob := proto.MarshalTextString(pb)
	// replace message boundary characters as curly braces look nicer (both is fine to parse)
	blob = strings.Replace(strings.Replace(blob, "<", "{", -1), ">", "}", -1)

	return ioutil.WriteFile(path, []byte(blob), 0644)
}
