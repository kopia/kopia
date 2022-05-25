package index

import (
	"encoding/json"
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
				require.True(t, v1.comparePrefix(IDPrefix(v2.String())) > 0)
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

			require.Equal(t, prefix != "", cid.HasPrefix())
			require.Equal(t, prefix, cid.Prefix())
		}
	}

	_, err := IDFromHash("", nil)
	require.ErrorContains(t, err, "hash too short")
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
