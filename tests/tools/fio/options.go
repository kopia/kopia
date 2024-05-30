package fio

import (
	"fmt"
	"path/filepath"
	"strconv"
)

// Options are flags to be set when running fio.
type Options map[string]string

// List of FIO argument strings.
const (
	BlockSizeFioArg        = "blocksize"
	DedupePercentageFioArg = "dedupe_percentage"
	DirectoryFioArg        = "directory"
	FallocateFioArg        = "fallocate"
	FileSizeFioArg         = "filesize"
	IOLimitFioArg          = "io_limit"
	IOSizeFioArg           = "io_size"
	NumFilesFioArg         = "nrfiles"
	RandRepeatFioArg       = "randrepeat"
	SizeFioArg             = "size"
)

// List of FIO specific fields and delimiters.
const (
	NoneFio       = "none"
	RandWriteFio  = "randwrite"
	RangeDelimFio = "-"
)

// Merge will merge two Options, overwriting common option keys
// with the incoming option values. Returns the merged result.
func (o Options) Merge(other Options) Options {
	out := make(map[string]string, len(o)+len(other))

	for k, v := range o {
		out[k] = v
	}

	for k, v := range other {
		out[k] = v
	}

	return out
}

// WithSize sets the fio write size.
func (o Options) WithSize(sizeB int64) Options {
	return o.Merge(Options{
		SizeFioArg: strconv.Itoa(int(sizeB)),
	})
}

// WithSizeRange sets the fio size range.
func (o Options) WithSizeRange(sizeMinB, sizeMaxB int64) Options {
	return o.Merge(rangeOpt(SizeFioArg, int(sizeMinB), int(sizeMaxB)))
}

// WithIOLimit sets the fio io limit.
func (o Options) WithIOLimit(ioSizeB int64) Options {
	return o.Merge(Options{
		IOLimitFioArg: strconv.Itoa(int(ioSizeB)),
	})
}

// WithIOSize sets the fio io size.
func (o Options) WithIOSize(sizeB int64) Options {
	return o.Merge(Options{
		IOSizeFioArg: strconv.Itoa(int(sizeB)),
	})
}

// WithNumFiles sets the fio number of files.
func (o Options) WithNumFiles(numFiles int) Options {
	return o.Merge(Options{
		NumFilesFioArg: strconv.Itoa(numFiles),
	})
}

// WithFileSize sets the fio file size.
func (o Options) WithFileSize(fileSizeB int64) Options {
	return o.Merge(Options{
		FileSizeFioArg: strconv.Itoa(int(fileSizeB)),
	})
}

// WithFileSizeRange sets the fio file size range.
func (o Options) WithFileSizeRange(fileSizeMinB, fileSizeMaxB int64) Options {
	return o.Merge(rangeOpt(FileSizeFioArg, int(fileSizeMinB), int(fileSizeMaxB)))
}

// WithDedupePercentage sets the fio dedupe percentage.
func (o Options) WithDedupePercentage(dPcnt int) Options {
	return o.Merge(Options{
		DedupePercentageFioArg: strconv.Itoa(dPcnt),
	})
}

// WithBlockSize sets the fio block size option.
func (o Options) WithBlockSize(blockSizeB int64) Options {
	return o.Merge(Options{
		BlockSizeFioArg: strconv.Itoa(int(blockSizeB)),
	})
}

// WithNoFallocate sets the fio option fallocate to "none".
func (o Options) WithNoFallocate() Options {
	return o.Merge(Options{
		FallocateFioArg: NoneFio,
	})
}

// WithRandRepeat sets the fio option rand repeat.
func (o Options) WithRandRepeat(set bool) Options {
	return o.Merge(boolOpt(RandRepeatFioArg, set))
}

// WithDirectory sets the fio option for the directory to write in.
func (o Options) WithDirectory(dir string) Options {
	return o.Merge(Options{
		DirectoryFioArg: filepath.ToSlash(dir),
	})
}

func boolOpt(key string, val bool) Options {
	if val {
		return Options{key: strconv.Itoa(1)}
	}

	return Options{key: strconv.Itoa(0)}
}

func rangeOpt(key string, minValue, maxValue int) Options {
	if minValue > maxValue {
		minValue, maxValue = maxValue, minValue
	}

	return Options{
		key: fmt.Sprintf("%d%s%d", minValue, RangeDelimFio, maxValue),
	}
}
