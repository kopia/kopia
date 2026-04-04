package content

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/content/index"
)

func mustParseIDForBench(t testing.TB, s string) index.ID {
	t.Helper()

	id, err := index.ParseID(s)
	require.NoError(t, err)

	return id
}

func TestContentCacheKeyForInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bi   Info
		want string
	}{
		{
			name: "all zeros - fast path",
			bi: Info{
				ContentID:           mustParseIDForBench(t, "abcdef01"),
				CompressionHeaderID: 0,
				FormatVersion:       0,
				EncryptionKeyID:     0,
			},
			want: "abcdef01",
		},
		{
			name: "with prefix - fast path",
			bi: Info{
				ContentID:           mustParseIDForBench(t, "xabcdef01"),
				CompressionHeaderID: 0,
				FormatVersion:       0,
				EncryptionKeyID:     0,
			},
			want: "xabcdef01",
		},
		{
			name: "compression only",
			bi: Info{
				ContentID:           mustParseIDForBench(t, "abcdef01"),
				CompressionHeaderID: 1,
				FormatVersion:       0,
				EncryptionKeyID:     0,
			},
			want: "abcdef01.1.0.0",
		},
		{
			name: "all set",
			bi: Info{
				ContentID:           mustParseIDForBench(t, "abcdef01"),
				CompressionHeaderID: 0x10,
				FormatVersion:       2,
				EncryptionKeyID:     3,
			},
			want: "abcdef01.10.2.3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := contentCacheKeyForInfo(tt.bi)
			require.Equal(t, tt.want, got)

			// verify the new implementation matches the old fmt.Sprintf implementation
			old := fmt.Sprintf("%v.%x.%x.%x", tt.bi.ContentID, tt.bi.CompressionHeaderID, tt.bi.FormatVersion, tt.bi.EncryptionKeyID)
			if tt.bi.CompressionHeaderID == 0 && tt.bi.FormatVersion == 0 && tt.bi.EncryptionKeyID == 0 {
				// fast path returns just the ID, old always has ".0.0.0"
				require.Equal(t, old, got+".0.0.0")
			} else {
				require.Equal(t, old, got)
			}
		})
	}
}

func BenchmarkContentCacheKeyForInfo_ZeroFields(b *testing.B) {
	bi := Info{
		ContentID:           mustParseIDForBench(b, "abcdef0123456789"),
		CompressionHeaderID: 0,
		FormatVersion:       0,
		EncryptionKeyID:     0,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = contentCacheKeyForInfo(bi)
	}
}

func BenchmarkContentCacheKeyForInfo_NonZeroFields(b *testing.B) {
	bi := Info{
		ContentID:           mustParseIDForBench(b, "abcdef0123456789"),
		CompressionHeaderID: 0x10,
		FormatVersion:       2,
		EncryptionKeyID:     3,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = contentCacheKeyForInfo(bi)
	}
}

func BenchmarkContentCacheKeyForInfo_Sprintf(b *testing.B) {
	bi := Info{
		ContentID:           mustParseIDForBench(b, "abcdef0123456789"),
		CompressionHeaderID: 0,
		FormatVersion:       0,
		EncryptionKeyID:     0,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = fmt.Sprintf("%v.%x.%x.%x", bi.ContentID, bi.CompressionHeaderID, bi.FormatVersion, bi.EncryptionKeyID)
	}
}
