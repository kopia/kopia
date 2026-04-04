package index

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDValid(t *testing.T) {
	validIDsOrdered := []string{
		"",
		"0012abcd",
		"12345678",
		"ffffffff",
		"g01234567",
		"g01234568",
		"yffffffff",
		"z00000000",
		"z00000001",
		"zffffffff",
	}

	var validContentIDsOrdered []ID

	for _, s := range validIDsOrdered {
		cid, err := ParseID(s)
		require.NoError(t, err)

		require.Equal(t, s, cid.String())

		jb := cid.AppendToJSON(nil, 10)
		require.Equal(t, "\""+s+"\"", string(jb))

		if s != "" {
			// limit to 3 bytes
			jb2 := cid.AppendToJSON(nil, 3)
			if len(s)%2 == 0 {
				// no prefix - 6 chars
				require.Equal(t, "\""+s[:6]+"\"", string(jb2))
			} else {
				// with prefix - 7 chars
				require.Equal(t, "\""+s[:7]+"\"", string(jb2))
			}
		}

		validContentIDsOrdered = append(validContentIDsOrdered, cid)
	}

	for i, v1 := range validContentIDsOrdered {
		jsonData, err := json.Marshal(v1)
		require.NoError(t, err)

		var v3 ID

		require.NoError(t, json.Unmarshal(jsonData, &v3))
		require.Equal(t, v1, v3)

		for j, v2 := range validContentIDsOrdered {
			switch {
			case i < j:
				require.True(t, v1.less(v2))
				require.Negative(t, v1.comparePrefix(IDPrefix(v2.String())))
			case i == j:
				require.Equal(t, v1, v2)
				require.Equal(t, 0, v1.comparePrefix(IDPrefix(v2.String())))
			case i > j:
				require.False(t, v1.less(v2))
				require.Positive(t, v1.comparePrefix(IDPrefix(v2.String())))
			}
		}
	}
}

func TestIDFromHash(t *testing.T) {
	cid, err := IDFromHash("", []byte{0x12, 0x34})
	require.NoError(t, err)
	require.Equal(t, "1234", cid.String())

	cid, err = IDFromHash("x", []byte{0x12, 0x34})
	require.NoError(t, err)
	require.Equal(t, "x1234", cid.String())

	_, err = IDFromHash("xx", []byte{0x12, 0x34})
	require.ErrorContains(t, err, "invalid prefix, must be empty or a single letter between 'g' and 'z'")

	cid, err = IDFromHash("x", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	require.NoError(t, err)
	require.Equal(t, "x000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f", cid.String())

	_, err = IDFromHash("x", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0})
	require.ErrorContains(t, err, "hash too long")
}

func TestParseInvalid(t *testing.T) {
	cases := map[string]string{
		"x":  "id too short",
		"x1": "invalid content hash",
		"xx": "invalid content hash",
		"x000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f11": "hash too long",
		"A00":   "invalid content prefix",
		"@abcd": "invalid content prefix",
	}

	for cid, wantErr := range cases {
		_, err := ParseID(cid)
		require.ErrorContains(t, err, wantErr)
	}
}

func TestIDPrefix(t *testing.T) {
	require.NoError(t, IDPrefix("").ValidateSingle())
	require.NoError(t, IDPrefix("x").ValidateSingle())
	require.ErrorContains(t, IDPrefix("@").ValidateSingle(), "invalid prefix, must be empty or a single letter between 'g' and 'z'")
	require.ErrorContains(t, IDPrefix("x12").ValidateSingle(), "invalid prefix, must be empty or a single letter between 'g' and 'z'")
}

func TestIDHash(t *testing.T) {
	prefixes := []IDPrefix{"", "g", "z"}
	hashes := [][]byte{
		{1, 2},
		{1, 2, 3, 4},
	}

	for _, prefix := range prefixes {
		for _, h := range hashes {
			cid, err := IDFromHash(prefix, h)
			require.NoError(t, err)
			require.Equal(t, h, cid.Hash())

			require.Equal(t, fmt.Sprintf("%v%x", prefix, h), string(cid.Append(nil)))

			require.Equal(t, prefix != "", cid.HasPrefix())
			require.Equal(t, prefix, cid.Prefix())
		}
	}

	_, err := IDFromHash("", nil)
	require.ErrorContains(t, err, "hash too short")
}

func TestComparePrefixSingleChar(t *testing.T) {
	t.Parallel()

	// Test single-char prefix fast path (case 1)
	tests := []struct {
		id     string
		prefix string
		want   int
	}{
		// Unprefixed IDs: first hex char compared with prefix
		{"0012abcd", "0", 1},  // "0012abcd" > "0" (more characters)
		{"0012abcd", "1", -1}, // "0012abcd" < "1" (first char '0' < '1')
		{"ffff0000", "f", 1},  // "ffff0000" > "f" (first char matches, more chars)
		{"ffff0000", "g", -1}, // "ffff0000" < "g" (first char 'f' < 'g')
		// Prefixed IDs: prefix byte compared with prefix string
		{"g01234567", "g", 1},  // "g01234567" > "g" (first char matches, more chars)
		{"g01234567", "h", -1}, // "g01234567" < "h" (prefix 'g' < 'h')
		{"g01234567", "f", 1},  // "g01234567" > "f" (prefix 'g' > 'f')
		{"z01234567", "z", 1},  // "z01234567" > "z" (first char matches, more chars)
		{"z01234567", "y", 1},  // "z01234567" > "y" (prefix 'z' > 'y')
		// Empty ID vs single-char prefix
		// EmptyID has case 0 returning 0, case 1 should return -1
	}

	for _, tt := range tests {
		id, err := ParseID(tt.id)
		require.NoError(t, err, "ParseID(%q)", tt.id)

		got := id.comparePrefix(IDPrefix(tt.prefix))
		require.Equalf(t, tt.want, got, "ID(%q).comparePrefix(%q)", tt.id, tt.prefix)
	}

	// Empty ID vs single char
	got := EmptyID.comparePrefix(IDPrefix("a"))
	require.Equal(t, -1, got)
}

func TestComparePrefixMultiChar(t *testing.T) {
	t.Parallel()

	// Test multi-char prefix (default case) - ensures byte-by-byte comparison works
	tests := []struct {
		id     string
		prefix string
		want   int
	}{
		{"0012abcd", "00", 1},      // "0012abcd" > "00"
		{"0012abcd", "0012abcd", 0}, // exact match
		{"0012abcd", "0012abce", -1}, // "0012abcd" < "0012abce"
		{"0012abcd", "0012abcc", 1},  // "0012abcd" > "0012abcc"
		{"g01234567", "g0", 1},       // "g01234567" > "g0"
		{"g01234567", "g01234567", 0}, // exact match
	}

	for _, tt := range tests {
		id, err := ParseID(tt.id)
		require.NoError(t, err, "ParseID(%q)", tt.id)

		got := id.comparePrefix(IDPrefix(tt.prefix))
		require.Equalf(t, tt.want, got, "ID(%q).comparePrefix(%q)", tt.id, tt.prefix)
	}
}

func BenchmarkComparePrefix_SingleChar(b *testing.B) {
	id, _ := ParseID("g01234567890abcdef")

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = id.comparePrefix("g")
	}
}

func BenchmarkComparePrefix_FullString(b *testing.B) {
	id, _ := ParseID("g01234567890abcdef")
	p := IDPrefix(id.String())

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = id.comparePrefix(p)
	}
}

func TestIDInvalidJSON(t *testing.T) {
	cases := map[string]string{
		`"x"`: "invalid ID: id too short: \"x\"",
		`123`: "cannot unmarshal number",
	}

	for jsonString, wantErr := range cases {
		var v ID

		require.ErrorContains(t, v.UnmarshalJSON([]byte(jsonString)), wantErr)
	}
}
