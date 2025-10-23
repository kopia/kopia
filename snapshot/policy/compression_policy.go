package policy

import (
	"path/filepath"
	"sort"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot"
)

// CompressionPolicy specifies compression policy.
type CompressionPolicy struct {
	CompressorName        compression.Name `json:"compressorName,omitzero"`
	OnlyCompress          []string         `json:"onlyCompress,omitzero"`
	NoParentOnlyCompress  bool             `json:"noParentOnlyCompress,omitzero"`
	NeverCompress         []string         `json:"neverCompress,omitzero"`
	NoParentNeverCompress bool             `json:"noParentNeverCompress,omitzero"`
	MinSize               int64            `json:"minSize,omitzero"`
	MaxSize               int64            `json:"maxSize,omitzero"`
}

// MetadataCompressionPolicy specifies compression policy for metadata.
type MetadataCompressionPolicy struct {
	CompressorName compression.Name `json:"compressorName,omitzero"`
}

// CompressionPolicyDefinition specifies which policy definition provided the value of a particular field.
type CompressionPolicyDefinition struct {
	CompressorName snapshot.SourceInfo `json:"compressorName,omitzero"`
	OnlyCompress   snapshot.SourceInfo `json:"onlyCompress,omitzero"`
	NeverCompress  snapshot.SourceInfo `json:"neverCompress,omitzero"`
	MinSize        snapshot.SourceInfo `json:"minSize,omitzero"`
	MaxSize        snapshot.SourceInfo `json:"maxSize,omitzero"`
}

// MetadataCompressionPolicyDefinition specifies which policy definition provided the value of a particular field.
type MetadataCompressionPolicyDefinition struct {
	CompressorName snapshot.SourceInfo `json:"compressorName,omitzero"`
}

// CompressorForFile returns compression name to be used for compressing a given file according to policy, using attributes such as name or size.
func (p *CompressionPolicy) CompressorForFile(e fs.Entry) compression.Name {
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
func (p *CompressionPolicy) Merge(src CompressionPolicy, def *CompressionPolicyDefinition, si snapshot.SourceInfo) {
	mergeCompressionName(&p.CompressorName, src.CompressorName, &def.CompressorName, si)
	mergeInt64(&p.MinSize, src.MinSize, &def.MinSize, si)
	mergeInt64(&p.MaxSize, src.MaxSize, &def.MaxSize, si)

	mergeStrings(&p.OnlyCompress, &p.NoParentOnlyCompress, src.OnlyCompress, src.NoParentOnlyCompress, &def.OnlyCompress, si)
	mergeStrings(&p.NeverCompress, &p.NoParentNeverCompress, src.NeverCompress, src.NoParentNeverCompress, &def.NeverCompress, si)
}

// Merge applies default values from the provided policy.
func (p *MetadataCompressionPolicy) Merge(src MetadataCompressionPolicy, def *MetadataCompressionPolicyDefinition, si snapshot.SourceInfo) {
	mergeCompressionName(&p.CompressorName, src.CompressorName, &def.CompressorName, si)
}

// MetadataCompressor returns compression name to be used for according to policy.
func (p *MetadataCompressionPolicy) MetadataCompressor() compression.Name {
	if p.CompressorName == "none" {
		return ""
	}

	return p.CompressorName
}

func isInSortedSlice(s string, slice []string) bool {
	x := sort.SearchStrings(slice, s)
	return x < len(slice) && slice[x] == s
}
