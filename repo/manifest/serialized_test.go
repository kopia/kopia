package manifest

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
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

func TestManifestDecode_StopEarly(t *testing.T) {
	table := []struct {
		name        string
		input       string
		expectEntry *manifestEntry
		expectErr   bool
	}{
		{
			name:  "EntryFoundLast_CompleteObject",
			input: `{"entries":[{"id":"efg"},{"id":"abcd"}]}`,
			expectEntry: &manifestEntry{
				ID: "abcd",
			},
		},
		{
			name:  "EntryFoundFirst_CompleteObject",
			input: `{"entries":[{"id":"abcd"},{"id":"efg"}]}`,
			expectEntry: &manifestEntry{
				ID: "abcd",
			},
		},
		{
			name:  "EntryFound_FirstInstanceWins",
			input: `{"entries":[{"id":"abcd"},{"id":"abcd","deleted":true}]}`,
			expectEntry: &manifestEntry{
				ID: "abcd",
			},
		},
		{
			name:  "EntryNotFound_CompleteObject",
			input: `{"entries":[{"id":"abcde"},{"id":"efg","deleted":true}]}`,
		},
		{
			name:  "EntryNotFound_CompleteObjectWithOtherFields",
			input: `{"foo":"bar"}`,
		},
		{
			name:  "EntryNotFound_CompleteObjectNoEntries",
			input: `{}`,
		},
		{
			name:      "EntryNotFound_IncompleteObject",
			input:     `{"foo":"bar"`,
			expectErr: true,
		},
		{
			name:      "EntryNotFound_IncompleteObject",
			input:     `{"entries":[{"id":"abcde"},{"id":"efg","deleted":true}]`,
			expectErr: true,
		},
		{
			name:      "EntryFoundLast_IncompleteObject",
			input:     `{"entries":[{"id":"efg",{"id":"abcd"}]}`,
			expectErr: true,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var found *manifestEntry

			r := strings.NewReader(test.input)
			err := forEachDeserializedEntry(
				r,
				func(e *manifestEntry) bool {
					if e.ID == "abcd" {
						found = e
						return false
					}

					return true
				},
			)

			assert.Equal(t, test.expectEntry, found)

			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
