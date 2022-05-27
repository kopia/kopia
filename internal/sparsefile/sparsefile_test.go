package sparsefile

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kopia/kopia/internal/stat"
)

func TestSparseCopy(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("sparse files are not supported on windows")
	}

	dir := t.TempDir()

	blk, err := stat.GetBlockSize(dir)
	if err != nil {
		t.Fatal(err)
	}

	type chunk struct {
		slice []byte
		off   uint64
		rep   uint64
	}

	cases := []struct {
		name string
		size uint64
		data []chunk
	}{
		{
			name: "null",
			size: 0,
		},
		{
			name: "empty",
			size: blk,
			data: []chunk{
				{slice: []byte{0}, off: 0, rep: blk},
			},
		},
		{
			name: "hole",
			size: 2 * blk,
			data: []chunk{
				{slice: []byte{1}, off: blk, rep: blk},
			},
		},
		{
			name: "mix",
			size: 2 * blk,
			data: []chunk{
				{slice: []byte{1}, off: 3, rep: blk - 10},
				{slice: []byte{1}, off: 2*blk - 10, rep: 10},
			},
		},
	}

	for _, c := range cases {
		src := filepath.Join(dir, "src"+c.name)
		dst := filepath.Join(dir, "dst"+c.name)

		sf, err := os.Create(src)
		if err != nil {
			t.Fatal(err)
		}

		for _, d := range c.data {
			sf.WriteAt(bytes.Repeat(d.slice, int(d.rep)), int64(d.off))
		}

		df, err := os.Create(dst)
		if err != nil {
			t.Fatal(err)
		}

		if err = df.Truncate(int64(c.size)); err != nil {
			t.Fatal(err)
		}

		blk, err := stat.GetBlockSize(dst)
		if err != nil {
			t.Fatal(err)
		}

		_, err = Copy(df, sf, blk)
		if err != nil {
			t.Fatalf("error writing %s: %v", dst, err)
		}

		s, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}

		d, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(s, d) {
			t.Fatalf("contents of %s and %s are not identical", src, dst)
		}
	}
}
