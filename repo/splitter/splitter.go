// Package splitter manages splitting of object data into chunks.
package splitter

import (
	"sort"
)

const (
	splitterSlidingWindowSize = 64
)

// Splitter determines when to split a given object.
// It must return true if the object should be split after byte b is processed.
type Splitter interface {
	ShouldSplit(b byte) bool
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

// Factory creates instances of Splitter
type Factory func() Splitter

// splitterFactories is a map of registered splitter factories.
var splitterFactories = map[string]Factory{
	"FIXED-1M": Fixed(megabytes(1)), //nolint:gomnd
	"FIXED-2M": Fixed(megabytes(2)), //nolint:gomnd
	"FIXED-4M": Fixed(megabytes(4)), //nolint:gomnd
	"FIXED-8M": Fixed(megabytes(8)), //nolint:gomnd

	"DYNAMIC-1M-BUZHASH": newBuzHash32SplitterFactory(megabytes(1)), //nolint:gomnd
	"DYNAMIC-2M-BUZHASH": newBuzHash32SplitterFactory(megabytes(2)), //nolint:gomnd
	"DYNAMIC-4M-BUZHASH": newBuzHash32SplitterFactory(megabytes(4)), //nolint:gomnd
	"DYNAMIC-8M-BUZHASH": newBuzHash32SplitterFactory(megabytes(8)), //nolint:gomnd

	"DYNAMIC-1M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(1)), //nolint:gomnd
	"DYNAMIC-2M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(2)), //nolint:gomnd
	"DYNAMIC-4M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(4)), //nolint:gomnd
	"DYNAMIC-8M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(8)), //nolint:gomnd

	// handle deprecated legacy names to splitters of arbitrary size
	"FIXED": Fixed(4 << 20), //nolint:gomnd

	// we don't want to use old DYNAMIC splitter because of its license, so
	// map this one to arbitrary buzhash32 (different)
	"DYNAMIC": newBuzHash32SplitterFactory(megabytes(4)), //nolint:gomnd
}

func megabytes(mb int) int {
	return mb << 20
}

// GetFactory gets splitter factory with a specified name or nil if not found.
func GetFactory(name string) Factory {
	return splitterFactories[name]
}

// DefaultAlgorithm is the name of the splitter used by default for new repositories.
const DefaultAlgorithm = "DYNAMIC-4M-BUZHASH"
