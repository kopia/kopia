package contentlog

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryWriter_EmptyObject(t *testing.T) {
	var jw JSONWriter

	jw.BeginObject()
	jw.EndObject()

	require.Equal(t, "{}", string(jw.buf))
}

func TestEntryWriter_AllTypes(t *testing.T) {
	var jw JSONWriter

	jw.BeginObject()
	jw.Int64Field("k1", 123)
	jw.Int64Field("a1", 1234)
	jw.Int64Field("b1", 12345)
	jw.BoolField("b2", true)
	jw.BoolField("b3", false)

	jw.BeginObjectField("o1")
	jw.Int64Field("k2", 123456)
	jw.EndObject()

	jw.BeginObjectField("o2")
	jw.EndObject()

	jw.BeginListField("l1")
	jw.StringElement("aaa")
	jw.StringElement("bbb")
	jw.StringElement("ccc")
	jw.EndList()

	jw.BeginListField("mixedList")
	jw.StringElement("aaa")
	jw.Int64Element(123)
	jw.NullElement()
	jw.BoolElement(true)
	jw.EndList()

	jw.BeginObjectField("o3")
	jw.StringField("v", "xxx")
	jw.StringField("someUnicode", "😄")
	jw.EndObject()

	jw.UInt64Field("u1", 123456)
	jw.StringField("s", "hello\nworld\r\t\b\f")
	jw.EndObject()

	var v map[string]any

	json.NewEncoder(os.Stdout).Encode("😄")

	t.Logf("buf: %s", string(jw.buf))
	require.NoError(t, json.Unmarshal(jw.buf, &v))
	require.Equal(t, map[string]any{
		"k1":        123.0,
		"a1":        1234.0,
		"b1":        12345.0,
		"b2":        true,
		"b3":        false,
		"u1":        123456.0,
		"mixedList": []any{"aaa", 123.0, nil, true},
		"l1":        []any{"aaa", "bbb", "ccc"},
		"o2":        map[string]any{},
		"o3": map[string]any{
			"v":           "xxx",
			"someUnicode": "😄",
		},
		"o1": map[string]any{
			"k2": 123456.0,
		},
		"s": "hello\nworld\r\t\b\f",
	}, v)
}
