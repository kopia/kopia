package cli

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/content"
)

func TestRepositoryUpgrade_checkIndexInfo_match(t *testing.T) {
	tcs := []struct {
		indexInfo0 content.Info
		indexInfo1 content.Info
	}{
		{
			// trivial case with zero-value content infos
		},
		{
			indexInfo0: content.Info{PackBlobID: "a"},
			indexInfo1: content.Info{PackBlobID: "a"},
		},
		{
			indexInfo0: content.Info{TimestampSeconds: 1},
			indexInfo1: content.Info{TimestampSeconds: 1},
		},
		{
			indexInfo0: content.Info{OriginalLength: 1},
			indexInfo1: content.Info{OriginalLength: 1},
		},
		{
			indexInfo0: content.Info{PackedLength: 1},
			indexInfo1: content.Info{PackedLength: 1},
		},
		{
			indexInfo0: content.Info{PackOffset: 1},
			indexInfo1: content.Info{PackOffset: 1},
		},
		{
			indexInfo0: content.Info{Deleted: true},
			indexInfo1: content.Info{Deleted: true},
		},
		{
			indexInfo0: content.Info{Deleted: false},
			indexInfo1: content.Info{Deleted: false},
		},
		{
			indexInfo0: content.Info{FormatVersion: 1},
			indexInfo1: content.Info{FormatVersion: 1},
		},
		{
			indexInfo0: content.Info{CompressionHeaderID: 1},
			indexInfo1: content.Info{CompressionHeaderID: 1},
		},
		{
			indexInfo0: content.Info{EncryptionKeyID: 1},
			indexInfo1: content.Info{EncryptionKeyID: 1},
		},
	}
	for i, tc := range tcs {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.NoError(t, checkIndexInfo(tc.indexInfo0, tc.indexInfo1))
		})
	}
}

func TestRepositoryUpgrade_checkIndexInfo_mismatch(t *testing.T) {
	tcs := []struct {
		indexInfo0         content.Info
		indexInfo1         content.Info
		expectErrorMessage string
	}{
		{
			indexInfo0:         content.Info{PackBlobID: "a"},
			indexInfo1:         content.Info{PackBlobID: "b"},
			expectErrorMessage: ` PackBlobID: a != b`,
		},
		{
			indexInfo0:         content.Info{TimestampSeconds: 1},
			indexInfo1:         content.Info{TimestampSeconds: 2},
			expectErrorMessage: " TimestampSeconds: 1 != 2",
		},
		{
			indexInfo0:         content.Info{OriginalLength: 1},
			indexInfo1:         content.Info{OriginalLength: 2},
			expectErrorMessage: " OriginalLength: 1 != 2",
		},
		{
			indexInfo0:         content.Info{PackedLength: 1},
			indexInfo1:         content.Info{PackedLength: 2},
			expectErrorMessage: " PackedLength: 1 != 2",
		},
		{
			indexInfo0:         content.Info{PackOffset: 1},
			indexInfo1:         content.Info{PackOffset: 2},
			expectErrorMessage: " PackOffset: 1 != 2",
		},
		{
			indexInfo0:         content.Info{Deleted: false},
			indexInfo1:         content.Info{Deleted: true},
			expectErrorMessage: " Deleted flag: false != true",
		},
		{
			indexInfo0:         content.Info{FormatVersion: 1},
			indexInfo1:         content.Info{FormatVersion: 2},
			expectErrorMessage: " FormatVersion: 1 != 2",
		},
		{
			indexInfo0:         content.Info{CompressionHeaderID: 1},
			indexInfo1:         content.Info{CompressionHeaderID: 2},
			expectErrorMessage: " CompressionHeaderID: 1 != 2",
		},

		{
			indexInfo0:         content.Info{EncryptionKeyID: 1},
			indexInfo1:         content.Info{EncryptionKeyID: 2},
			expectErrorMessage: " EncryptionKeyID: 1 != 2",
		},
	}
	for i, tc := range tcs {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			err := checkIndexInfo(tc.indexInfo0, tc.indexInfo1)

			require.Error(t, err)
			require.ErrorContains(t, err, tc.expectErrorMessage)
		})
	}
}

func TestRepositoryUpgrade_checkIndexInfo_multiMismatch(t *testing.T) {
	indexInfo0 := content.Info{
		EncryptionKeyID:     1,
		CompressionHeaderID: 1,
	}

	indexInfo1 := content.Info{
		EncryptionKeyID:     2,
		CompressionHeaderID: 2,
	}

	err := checkIndexInfo(indexInfo0, indexInfo1)

	require.Error(t, err)
	require.ErrorContains(t, err, " EncryptionKeyID: 1 != 2")
	require.ErrorContains(t, err, " CompressionHeaderID: 1 != 2")
}
