package policy

import (
	"path/filepath"
	"sort"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/compression"
)

// CompressionPolicy specifies compression policy.
type CompressionPolicy struct {
	CompressorName compression.Name `json:"compressorName,omitempty"`
	OnlyCompress   []string         `json:"onlyCompress,omitempty"`
	NeverCompress  []string         `json:"neverCompress,omitempty"`
	MinSize        int64            `json:"minSize,omitempty"`
	MaxSize        int64            `json:"maxSize,omitempty"`
}

// CompressorForFile returns compression name to be used for compressing a given file according to policy, using attributes such as name or size.
func (p *CompressionPolicy) CompressorForFile(e fs.File) compression.Name {
	ext := filepath.Ext(e.Name())
	size := e.Size()

	if p.CompressorName == "none" {
		return ""
	}

	if v := p.MinSize; v > 0 && size < v {
		return ""
	}

	if v := p.MaxSize; v > 0 && size > v {
		return ""
	}

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

	if p.MinSize == 0 {
		p.MinSize = src.MinSize
	}

	if p.MaxSize == 0 {
		p.MaxSize = src.MaxSize
	}

	p.OnlyCompress = mergeStrings(p.OnlyCompress, src.OnlyCompress)
	p.NeverCompress = mergeStrings(p.NeverCompress, src.NeverCompress)
}

var defaultCompressionPolicy = CompressionPolicy{
	CompressorName: "none",
}

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
