package object

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

func TestMain(m *testing.M) { testutil.MyTestMain(m) }

func TestParseObjectID(t *testing.T) {
	cases := []struct {
		text    string
		isValid bool
	}{
		{"Df0f0", true},
		{"IDf0f0", true},
		{"IDf0f0", true},
		{"IIDf0f0", true},
		{"Dxf0f0", true},
		{"IDxf0f0", true},
		{"IDxf0f0", true},
		{"IIDxf0f0", true},
		{"Dxf0f", false},
		{"IDxf0f", false},
		{"Da", false},
		{"Daf0f0", false},
		{"", true},
		{"B!$@#$!@#$", false},
		{"X", false},
		{"I.", false},
		{"I.x", false},
		{"I.af", false},
		{"Ix.ag", false},
		{"Iab.", false},
		{"I1", false},
		{"I1,", false},
		{"I-1,X", false},
		{"Xsomething", false},
		{"IZabcd", false},
	}

	for _, tc := range cases {
		_, err := ParseID(tc.text)
		if err != nil && tc.isValid {
			t.Errorf("error parsing %q: %v", tc.text, err)
		} else if err == nil && !tc.isValid {
			t.Errorf("unexpected success parsing %v", tc.text)
		}
	}
}

func TestFromStrings(t *testing.T) {
	ids, err := IDsFromStrings([]string{"f0f0", "f1f1"})
	require.NoError(t, err)

	require.Equal(t, []ID{mustParseID(t, "f0f0"), mustParseID(t, "f1f1")}, ids)

	_, err = IDsFromStrings([]string{"invalidf0f0", "f1f1"})
	require.Error(t, err)
}

func TestToStrings(t *testing.T) {
	strs := IDsToStrings([]ID{mustParseID(t, "f0f0"), mustParseID(t, "f1f1")})
	require.Equal(t, []string{"f0f0", "f1f1"}, strs)
}

func TestString(t *testing.T) {
	cases := map[ID]string{
		EmptyID:                  "",
		mustParseID(t, "Dabcd"):  "abcd",
		mustParseID(t, "abcd"):   "abcd",
		mustParseID(t, "IIabcd"): "IIabcd",
		mustParseID(t, "Zabcd"):  "Zabcd",
	}

	for id, str := range cases {
		require.Equal(t, str, id.String())
		require.Equal(t, str, string(id.Append(nil)))
	}
}

func mustParseID(t *testing.T, s string) ID {
	t.Helper()

	id, err := ParseID(s)
	require.NoError(t, err)

	return id
}
