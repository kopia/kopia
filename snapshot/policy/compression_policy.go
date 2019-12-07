package policy

import (
	"path/filepath"
	"sort"

	"github.com/kopia/kopia/repo/object"
)

// CompressionPolicy specifies compression policy.
type CompressionPolicy struct {
	CompressorName object.CompressorName `json:"compressorName,omitempty"`
	OnlyCompress   []string              `json:"onlyCompress"`
	NeverCompress  []string              `json:"neverCompress"`
}

func (p *CompressionPolicy) CompressorForFile(fname string) object.CompressorName {
	ext := filepath.Ext(fname)

	if len(p.OnlyCompress) > 0 && isInSortedSlice(ext, p.OnlyCompress) {
		return p.CompressorName
	}

	if isInSortedSlice(ext, p.NeverCompress) {
		return ""
	}

	return p.CompressorName
}

// Merge applies default values from the provided policy.
func (p *CompressionPolicy) Merge(src CompressionPolicy) {
	if p.CompressorName == "" {
		p.CompressorName = src.CompressorName
	}

	p.OnlyCompress = mergeStrings(p.OnlyCompress, src.OnlyCompress)
	p.NeverCompress = mergeStrings(p.NeverCompress, src.NeverCompress)
}

var defaultCompressionPolicy = CompressionPolicy{}

func mergeStrings(s1, s2 []string) []string {
	merged := map[string]bool{}

	for _, v := range s1 {
		merged[v] = true
	}

	for _, v := range s2 {
		merged[v] = true
	}

	var result []string
	for v := range merged {
		result = append(result, v)
	}

	sort.Strings(result)

	return result
}

func isInSortedSlice(s string, slice []string) bool {
	x := sort.SearchStrings(slice, s)
	return x < len(slice) && slice[x] == s
}
