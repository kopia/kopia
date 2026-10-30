// Package splitter manages splitting of object data into chunks.
package splitter

import (
	"sort"
)

const (
	splitterSlidingWindowSize = 64
	splitterSize128KB         = 128 << 10
	splitterSize256KB         = 256 << 10
	splitterSize512KB         = 512 << 10
	splitterSize1MB           = 1 << 20
	splitterSize2MB           = 2 << 20
	splitterSize4MB           = 4 << 20
	splitterSize8MB           = 8 << 20
)

// Splitter determines when to split a given object.
type Splitter interface {
	// NextSplitPoint() determines the location of the next split point in the given slice of bytes.
	// It returns value `n` between 1..len(b) if a split point happens AFTER byte n and the splitter
	// has consumed `n` bytes.
	// If there is no split point, the splitter returns -1 and consumes all bytes from the slice.
	NextSplitPoint(b []byte) int
	MaxSegmentSize() int
	Reset()
	Close()
}

// SupportedAlgorithms returns the list of supported splitters.
func SupportedAlgorithms() []string {
	var supportedSplitters []string

	for k := range splitterFactories {
		supportedSplitters = append(supportedSplitters, k)
	}

	sort.Strings(supportedSplitters)

	return supportedSplitters
}

// Factory creates instances of Splitter.
type Factory func() Splitter

// splitterFactories is a map of registered splitter factories.
//
//nolint:gochecknoglobals
var splitterFactories = map[string]Factory{
	"FIXED-128K": pooled(Fixed(splitterSize128KB)),
	"FIXED-256K": pooled(Fixed(splitterSize256KB)),
	"FIXED-512K": pooled(Fixed(splitterSize512KB)),
	"FIXED-1M":   pooled(Fixed(splitterSize1MB)),
	"FIXED-2M":   pooled(Fixed(splitterSize2MB)),
	"FIXED-4M":   pooled(Fixed(splitterSize4MB)),
	"FIXED-8M":   pooled(Fixed(splitterSize8MB)),

	"DYNAMIC-128K-BUZHASH": pooled(newBuzHash32SplitterFactory(splitterSize128KB)),
	"DYNAMIC-256K-BUZHASH": pooled(newBuzHash32SplitterFactory(splitterSize256KB)),
	"DYNAMIC-512K-BUZHASH": pooled(newBuzHash32SplitterFactory(splitterSize512KB)),
	"DYNAMIC-1M-BUZHASH":   pooled(newBuzHash32SplitterFactory(splitterSize1MB)),
	"DYNAMIC-2M-BUZHASH":   pooled(newBuzHash32SplitterFactory(splitterSize2MB)),
	"DYNAMIC-4M-BUZHASH":   pooled(newBuzHash32SplitterFactory(splitterSize4MB)),
	"DYNAMIC-8M-BUZHASH":   pooled(newBuzHash32SplitterFactory(splitterSize8MB)),

	"DYNAMIC-128K-RABINKARP": pooled(newRabinKarp64SplitterFactory(splitterSize128KB)),
	"DYNAMIC-256K-RABINKARP": pooled(newRabinKarp64SplitterFactory(splitterSize256KB)),
	"DYNAMIC-512K-RABINKARP": pooled(newRabinKarp64SplitterFactory(splitterSize512KB)),
	"DYNAMIC-1M-RABINKARP":   pooled(newRabinKarp64SplitterFactory(splitterSize1MB)),
	"DYNAMIC-2M-RABINKARP":   pooled(newRabinKarp64SplitterFactory(splitterSize2MB)),
	"DYNAMIC-4M-RABINKARP":   pooled(newRabinKarp64SplitterFactory(splitterSize4MB)),
	"DYNAMIC-8M-RABINKARP":   pooled(newRabinKarp64SplitterFactory(splitterSize8MB)),

	// handle deprecated legacy names to splitters of arbitrary size
	"FIXED": Fixed(splitterSize4MB),

	// we don't want to use old DYNAMIC splitter because of its license, so
	// map this one to arbitrary buzhash32 (different)
	"DYNAMIC": newBuzHash32SplitterFactory(splitterSize4MB),
}

// GetFactory gets splitter factory with a specified name or nil if not found.
func GetFactory(name string) Factory {
	return splitterFactories[name]
}

// DefaultAlgorithm is the name of the splitter used by default for new repositories.
const DefaultAlgorithm = "DYNAMIC-4M-BUZHASH"
