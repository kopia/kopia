package policy

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/internal/mockfs"
)

func TestNewExtensionSet(t *testing.T) { // This also tests (*ExtensionSet).Add() because NewExtensionSet() calls that under the hood.
	t.Parallel()

	present := struct{}{}

	for _, tt := range []struct {
		name    string
		args    []string
		wantSet map[string]struct{}
	}{
		{
			name:    "Empty",
			args:    []string{},
			wantSet: make(map[string]struct{}),
		},
		{
			name: "Basic",
			args: []string{".txt", ".png"},
			wantSet: map[string]struct{}{
				"txt": present,
				"png": present,
			},
		},
		{
			name: "Mixed case",
			args: []string{".txt", ".PNG"},
			wantSet: map[string]struct{}{
				"txt": present,
				"png": present,
			},
		},
		{
			name: "Duplicates",
			args: []string{".txt", ".TXT", ".png", ".pNg"},
			wantSet: map[string]struct{}{
				"txt": present,
				"png": present,
			},
		},
		{
			name: "With and without prefix dot",
			args: []string{"txt", ".txt", "png", ".jpg"},
			wantSet: map[string]struct{}{
				"txt": present,
				"png": present,
				"jpg": present,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := *NewExtensionSet(tt.args...)
			assert.Equal(t, ExtensionSet(tt.wantSet), actual)
		})
	}
}

func TestExtensionSetContains(t *testing.T) {
	t.Parallel()

	commonSet := NewExtensionSet("txt")

	for _, tt := range []struct {
		name      string
		query     string
		set       *ExtensionSet
		wantFound bool
	}{
		{
			name:      "Exact found",
			query:     "txt",
			wantFound: true,
		},
		{
			name:      "Dot prefix found",
			query:     ".txt",
			wantFound: true,
		},
		{
			name:      "Differing case found",
			query:     "tXt",
			wantFound: true,
		},
		{
			name:      "Exact not found",
			query:     "png",
			wantFound: false,
		},
		{
			name:      "Dot prefix not found",
			query:     ".png",
			wantFound: false,
		},
		{
			name:      "Differing case not found",
			query:     "PNG",
			wantFound: false,
		},
		{
			name:      "Empty found",
			query:     "",
			set:       NewExtensionSet(""),
			wantFound: true,
		},
		{
			name:      "Empty not found",
			query:     "",
			wantFound: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.set == nil {
				tt.set = commonSet
			}

			assert.Equal(t, tt.wantFound, tt.set.Contains(tt.query))
		})
	}
}

func TestExtensionSetRemove(t *testing.T) {
	t.Parallel()

	initialSet := *NewExtensionSet("txt", "png", "jpg", "pdf")

	for _, tt := range []struct {
		name           string
		remove         string
		wantDifference bool
	}{
		{
			name:           "Exact found",
			remove:         "txt",
			wantDifference: true,
		},
		{
			name:           "Dot prefix found",
			remove:         ".txt",
			wantDifference: true,
		},
		{
			name:           "Differing case found",
			remove:         "tXt",
			wantDifference: true,
		},
		{
			name:           "Exact not found",
			remove:         "log",
			wantDifference: false,
		},
		{
			name:           "Dot prefix not found",
			remove:         ".log",
			wantDifference: false,
		},
		{
			name:           "Differing case not found",
			remove:         "LOG",
			wantDifference: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			set := make(ExtensionSet)
			maps.Copy(set, initialSet)
			set.Remove(tt.remove)

			assertFunc := assert.Equal
			if tt.wantDifference {
				assertFunc = assert.NotEqual
			}

			assertFunc(t, initialSet, set)
		})
	}
}

func TestExtensionSetUnmarshal(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		json    string
		wantSet *ExtensionSet
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "Empty extension set - expect empty map",
			json:    "[]",
			wantSet: &ExtensionSet{},
			wantErr: assert.NoError,
		},
		{
			name:    "Wrong input type (JSON object) - expect error",
			json:    `{".txt":true,".png":false}`,
			wantSet: nil,
			wantErr: assert.Error,
		},
		{
			name:    "Single value extension set - expect success",
			json:    `[".txt"]`,
			wantSet: NewExtensionSet(".txt"),
			wantErr: assert.NoError,
		},
		{
			name:    "Several value extension set - expect success",
			json:    `[".txt", ".png"]`,
			wantSet: NewExtensionSet(".txt", ".png"),
			wantErr: assert.NoError,
		},
		{
			name:    "Duplicate values - expect success",
			json:    `[".txt", ".png", ".txt", ".jpg"]`,
			wantSet: NewExtensionSet(".txt", ".png", ".jpg"),
			wantErr: assert.NoError,
		},
		{
			name:    "Mixed case - expect success",
			json:    `[".TxT", ".png", ".JPG"]`,
			wantSet: NewExtensionSet(".txt", ".png", ".jpg"),
			wantErr: assert.NoError,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var haveSet ExtensionSet
			err := json.Unmarshal([]byte(tt.json), &haveSet)
			tt.wantErr(t, err)

			if tt.wantSet != nil {
				assert.Equal(t, *tt.wantSet, haveSet)
			}
		})
	}
}

func TestExtensionSetMarshal(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		set      *ExtensionSet
		wantJSON string
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name:     "Empty - expect empty map",
			set:      &ExtensionSet{},
			wantJSON: "[]",
			wantErr:  assert.NoError,
		},
		{
			name:     "Wrong input type (JSON object) - expect error",
			set:      nil,
			wantJSON: `{".txt":true,".png":false}`,
			wantErr:  assert.Error,
		},
		{
			name:     "Single value - expect success",
			set:      NewExtensionSet(".txt"),
			wantJSON: `[".txt"]`,
			wantErr:  assert.NoError,
		},
		{
			name:     "Several value - expect success",
			set:      NewExtensionSet(".txt", ".png"),
			wantJSON: `[".txt", ".png"]`,
			wantErr:  assert.NoError,
		},
		{
			name:     "Mixed case and duplicates - expect success",
			set:      NewExtensionSet(".Txt", ".PNG", ".txt", ".pNG"),
			wantJSON: `[".txt", ".png"]`,
			wantErr:  assert.NoError,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var haveSet ExtensionSet
			err := json.Unmarshal([]byte(tt.wantJSON), &haveSet)
			tt.wantErr(t, err)

			if tt.set != nil {
				assert.Equal(t, haveSet, *tt.set)
			}
		})
	}
}

func TestCompressorForFile(t *testing.T) {
	const compressorName = "compress"

	tests := []struct {
		name           string
		onlyCompress   ExtensionSet
		neverCompress  ExtensionSet
		shouldCompress map[string]bool
	}{
		{
			name:          "Just OnlyCompress",
			onlyCompress:  *NewExtensionSet(".log", ".go"),
			neverCompress: *NewExtensionSet(),
			shouldCompress: map[string]bool{
				"test.txt":    false,
				"test.log":    true,
				"TEST.TxT":    false,
				"hUh.LoG":     true,
				"image.png":   false,
				"noextension": false,
			},
		},
		{
			name:          "OnlyCompress and NeverCompress",
			onlyCompress:  *NewExtensionSet(".txt", ".conf"),
			neverCompress: *NewExtensionSet(".png"),
			shouldCompress: map[string]bool{
				"test.txt":    true,
				"TEST.TXT":    true,
				"image.png":   false,
				"IMAGE.PnG":   false,
				"kopia.conf":  true,
				"kopia.jpg":   false,
				"noextension": false,
			},
		},
		{
			name:          "Some missing dot",
			onlyCompress:  *NewExtensionSet("txt", ".conf", "log"),
			neverCompress: *NewExtensionSet(),
			shouldCompress: map[string]bool{
				"test.txt":    true,
				"test.conf":   true,
				"test.log":    true,
				"test.jpg":    false,
				"TEST.CONF":   true,
				"TEST.PDF":    false,
				"noextension": false,
			},
		},
		{
			name:          "Conflicting OnlyCompress and NeverCompress",
			onlyCompress:  *NewExtensionSet("txt", "conf"),
			neverCompress: *NewExtensionSet("txt"),
			shouldCompress: map[string]bool{
				"test.txt":    false,
				"test.conf":   true,
				"test.log":    false,
				"test.jpg":    false,
				"TEST.CONF":   true,
				"TEST.PDF":    false,
				"noextension": false,
			},
		},
		{
			name:          "Just NeverCompress",
			onlyCompress:  *NewExtensionSet(),
			neverCompress: *NewExtensionSet("pdf", ".JPG"),
			shouldCompress: map[string]bool{
				"file.txt":    true,
				"noextension": true,
				"FILE.PDF":    false,
				"file.jpg":    false,
				"file.jpeg":   true,
			},
		},
	}

	for _, tt := range tests {
		cp := CompressionPolicy{
			CompressorName:        compressorName,
			OnlyCompress:          tt.onlyCompress,
			NoParentOnlyCompress:  false,
			NeverCompress:         tt.neverCompress,
			NoParentNeverCompress: false,
			MinSize:               0,
			MaxSize:               0,
		}

		t.Run(tt.name, func(t *testing.T) {
			for filename, expected := range tt.shouldCompress {
				file := mockfs.NewFile(filename, nil, 0o777)
				actual := cp.CompressorForFile(file) == compressorName
				file.Close()
				assert.Equal(t, expected, actual, filename)
			}
		})
	}
}
