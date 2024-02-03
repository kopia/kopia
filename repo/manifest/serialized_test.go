package manifest

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/manifest/testdata"
)

func TestManifestDecode_GoodInput(t *testing.T) {
	table := []struct {
		name  string
		input []byte
	}{
		{
			name:  "MultipleManifests",
			input: []byte(testdata.GoodManifests),
		},
		{
			name:  "IgnoredField",
			input: []byte(testdata.IgnoredField),
		},
		{
			name:  "StopsAtStructEnd",
			input: []byte(testdata.ExtraInputAtEnd),
		},
		{
			name:  "CaseInsensitive",
			input: []byte(testdata.CaseInsensitive),
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			stdlibDec := manifest{}

			stdReader := bytes.NewReader(test.input)
			require.NoError(t, json.NewDecoder(stdReader).Decode(&stdlibDec))

			arrReader := bytes.NewReader(test.input)
			arrDec, err := decodeManifestArray(arrReader)
			require.NoError(t, err)

			assert.Equal(t, stdlibDec, arrDec)

			assert.True(t, reflect.DeepEqual(stdlibDec, arrDec))
		})
	}
}

func TestManifestDecode_BadInput(t *testing.T) {
	for _, test := range testdata.BadInputs {
		t.Run(test.Name, func(t *testing.T) {
			r := bytes.NewReader([]byte(test.Input))
			_, err := decodeManifestArray(r)

			t.Logf("%v", err)

			require.Error(t, err)
		})
	}
}
