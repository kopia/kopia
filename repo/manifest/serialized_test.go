package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/manifest/testdata"
)

func checkPopulated(
	t *testing.T,
	v reflect.Value,
	ignoreTypeSubfields []reflect.Type,
	fieldNames ...string,
) {
	t.Helper()

	if !v.IsValid() {
		return
	}

	if v.IsZero() {
		assert.Failf(
			t,
			"zero-valued field",
			"field selector: %s",
			strings.Join(fieldNames, "."),
		)
	}

	for _, typ := range ignoreTypeSubfields {
		if typ == v.Type() {
			return
		}
	}

	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		checkPopulated(t, v.Elem(), ignoreTypeSubfields, fieldNames...)

	case reflect.Array, reflect.Slice:
		if v.Len() == 0 {
			assert.Failf(
				t,
				"empty slice or array",
				"field selector: %s",
				strings.Join(fieldNames, "."),
			)
		}

		for i := range v.Len() {
			f := v.Index(i)
			fieldName := fmt.Sprintf("<index %d>", i)

			checkPopulated(t, f, ignoreTypeSubfields, append(fieldNames, fieldName)...)
		}

	case reflect.Map:
		var (
			elems int
			iter  = v.MapRange()
		)

		for iter.Next() {
			f := iter.Value()
			fieldName := fmt.Sprintf("<map key %v>", iter.Key())
			elems++

			checkPopulated(t, f, ignoreTypeSubfields, append(fieldNames, fieldName)...)
		}

		if elems == 0 {
			assert.Failf(
				t,
				"empty map",
				"field selector: %s",
				strings.Join(fieldNames, "."),
			)
		}

	case reflect.Struct:
		for i := range v.NumField() {
			f := v.Field(i)
			fieldName := v.Type().Field(i).Name

			checkPopulated(t, f, ignoreTypeSubfields, append(fieldNames, fieldName)...)
		}

	default:
		return
	}
}

// allPopulated is a helper function that fails the test if any value in input
// is the zero-value for it's type. This can be helpful to ensure tests check
// structs with all data field populated in a meaningful way.
func allPopulated(t *testing.T, input any, ignoreTypeSubfields ...any) {
	t.Helper()

	ignoreTypes := make([]reflect.Type, 0, len(ignoreTypeSubfields))

	for _, typ := range ignoreTypeSubfields {
		ignoreTypes = append(ignoreTypes, reflect.TypeOf(typ))
	}

	checkPopulated(t, reflect.ValueOf(input), ignoreTypes)
}

func TestManifestDecode_GetsAllFields(t *testing.T) {
	man := manifest{
		Entries: []*manifestEntry{
			{
				ID:      ID("foo"),
				Labels:  map[string]string{"bar": "foo"},
				ModTime: clock.Now().UTC(),
				Deleted: true,
				Content: json.RawMessage(`"foo"`),
			},
		},
	}

	allPopulated(t, man, time.Time{})

	stdlibSerialize, err := json.Marshal(man)
	require.NoError(t, err, "serializing manifest")

	stdlib := &manifest{}

	err = json.Unmarshal(stdlibSerialize, stdlib)
	require.NoError(t, err, "deserializing with stdlib")

	custom, err := decodeManifestArray(bytes.NewReader(stdlibSerialize))
	require.NoError(t, err, "deserializing with custom code")

	assert.Equal(t, stdlib, &custom, "custom deserialized content")
}

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
