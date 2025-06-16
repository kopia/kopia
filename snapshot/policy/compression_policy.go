package policy

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot"
)

// CompressionPolicy specifies compression policy.
type CompressionPolicy struct {
	CompressorName        compression.Name `json:"compressorName,omitempty"`
	OnlyCompress          ExtensionSet     `json:"onlyCompress,omitempty"`
	NoParentOnlyCompress  bool             `json:"noParentOnlyCompress,omitempty"`
	NeverCompress         ExtensionSet     `json:"neverCompress,omitempty"`
	NoParentNeverCompress bool             `json:"noParentNeverCompress,omitempty"`
	MinSize               int64            `json:"minSize,omitempty"`
	MaxSize               int64            `json:"maxSize,omitempty"`
}

// MetadataCompressionPolicy specifies compression policy for metadata.
type MetadataCompressionPolicy struct {
	CompressorName compression.Name `json:"compressorName,omitempty"`
}

// CompressionPolicyDefinition specifies which policy definition provided the value of a particular field.
type CompressionPolicyDefinition struct {
	CompressorName snapshot.SourceInfo `json:"compressorName,omitempty"`
	OnlyCompress   snapshot.SourceInfo `json:"onlyCompress,omitempty"`
	NeverCompress  snapshot.SourceInfo `json:"neverCompress,omitempty"`
	MinSize        snapshot.SourceInfo `json:"minSize,omitempty"`
	MaxSize        snapshot.SourceInfo `json:"maxSize,omitempty"`
}

// MetadataCompressionPolicyDefinition specifies which policy definition provided the value of a particular field.
type MetadataCompressionPolicyDefinition struct {
	CompressorName snapshot.SourceInfo `json:"compressorName,omitempty"`
}

// CompressorForFile returns compression name to be used for compressing a given file according to policy, using attributes such as name or size.
func (p *CompressionPolicy) CompressorForFile(e fs.Entry) compression.Name {
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

	ext := filepath.Ext(e.Name())

	if len(p.OnlyCompress) > 0 && !p.OnlyCompress.Contains(ext) {
		return ""
	}

	if p.NeverCompress.Contains(ext) {
		return ""
	}

	return p.CompressorName
}

// Merge applies default values from the provided policy.
func (p *CompressionPolicy) Merge(src CompressionPolicy, def *CompressionPolicyDefinition, si snapshot.SourceInfo) {
	mergeCompressionName(&p.CompressorName, src.CompressorName, &def.CompressorName, si)
	mergeInt64(&p.MinSize, src.MinSize, &def.MinSize, si)
	mergeInt64(&p.MaxSize, src.MaxSize, &def.MaxSize, si)

	mergeExtensionSets(&p.OnlyCompress, &p.NoParentOnlyCompress, src.OnlyCompress, src.NoParentOnlyCompress, &def.OnlyCompress, si)
	mergeExtensionSets(&p.NeverCompress, &p.NoParentNeverCompress, src.NeverCompress, src.NoParentNeverCompress, &def.NeverCompress, si)
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

// ExtensionSet represents a set of file extensions, all lowercase.
type ExtensionSet map[string]struct{}

// UnmarshalJSON unmarshals an ExtensionSet from an array of strings.
func (es *ExtensionSet) UnmarshalJSON(data []byte) error {
	var stringList []string
	if err := json.Unmarshal(data, &stringList); err != nil {
		//nolint:wrapcheck // no need to wrap since this will be propagated as a JSON parse error since that's what it is
		return err
	}

	set := make(ExtensionSet, len(stringList))
	for _, ext := range stringList {
		set.Add(ext)
	}

	*es = set

	return nil
}

// MarshalJSON marshals an ExtensionSet to an array of strings.
func (es ExtensionSet) MarshalJSON() ([]byte, error) {
	stringList := make([]string, 0, len(es))
	for ext := range es {
		stringList = append(stringList, ext)
	}
	// The tests require (un)marshaling to be deterministic
	sort.Strings(stringList)
	//nolint:wrapcheck // no need to wrap since this will be propagated as a JSON parse error since that's what it is
	return json.Marshal(stringList)
}

// normalizeExtension normalizes an extension by converting it to lowercase and removing the leading dot ('.') if one exists.
func normalizeExtension(ext string) string {
	return strings.ToLower(strings.TrimPrefix(ext, "."))
}

// Contains returns true if the given extension is in the set (case insensitive).
func (es *ExtensionSet) Contains(ext string) bool {
	_, ok := (*es)[normalizeExtension(ext)]
	return ok
}

// Add adds the given extension to the set.
func (es *ExtensionSet) Add(ext string) {
	(*es)[normalizeExtension(ext)] = struct{}{}
}

// Remove removes the given extension from the set.
func (es *ExtensionSet) Remove(ext string) {
	delete(*es, normalizeExtension(ext))
}

// NewExtensionSet creates a new set containing the given extensions
func NewExtensionSet(extensions... string) *ExtensionSet {
	set := make(ExtensionSet)
	for _, ext := range extensions {
		set.Add(ext)
	}
	return &set
}
