package object

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

// SupportedSplitters is a list of supported object splitters.
var SupportedSplitters []string

// SplitterFactory creates instances of Splitter
type SplitterFactory func() Splitter

// splitterFactories is a map of registered splitter factories.
var splitterFactories = map[string]SplitterFactory{
	"FIXED-1M": newFixedSplitterFactory(megabytes(1)),
	"FIXED-2M": newFixedSplitterFactory(megabytes(2)),
	"FIXED-4M": newFixedSplitterFactory(megabytes(4)),
	"FIXED-8M": newFixedSplitterFactory(megabytes(8)),

	"DYNAMIC-1M-BUZHASH": newBuzHash32SplitterFactory(megabytes(1)),
	"DYNAMIC-2M-BUZHASH": newBuzHash32SplitterFactory(megabytes(2)),
	"DYNAMIC-4M-BUZHASH": newBuzHash32SplitterFactory(megabytes(4)),
	"DYNAMIC-8M-BUZHASH": newBuzHash32SplitterFactory(megabytes(8)),

	"DYNAMIC-1M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(1)),
	"DYNAMIC-2M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(2)),
	"DYNAMIC-4M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(4)),
	"DYNAMIC-8M-RABINKARP": newRabinKarp64SplitterFactory(megabytes(8)),

	// handle deprecated legacy names to splitters of arbitrary size
	"FIXED": newFixedSplitterFactory(4 << 20),

	// we don't want to use old DYNAMIC splitter because of its license, so
	// map this one to arbitrary buzhash32 (different)
	"DYNAMIC": newBuzHash32SplitterFactory(megabytes(4)),
}

func megabytes(mb int) int {
	return mb << 20
}

// GetSplitterFactory gets splitter factory with a specified name or nil if not found.
func GetSplitterFactory(name string) SplitterFactory {
	return splitterFactories[name]
}

func init() {
	for k := range splitterFactories {
		SupportedSplitters = append(SupportedSplitters, k)
	}

	sort.Strings(SupportedSplitters)
}

// DefaultSplitter is the name of the splitter used by default for new repositories.
const DefaultSplitter = "DYNAMIC-4M-BUZHASH"
