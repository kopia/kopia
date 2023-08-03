package cli

type fileHistogram struct {
	totalFiles       uint
	size0Byte        uint
	size0bTo100Kb    uint
	size100KbTo100Mb uint
	size100MbTo1Gb   uint
	sizeOver1Gb      uint
}

type dirHistogram struct {
	totalDirs             uint
	numEntries0           uint
	numEntries0to100      uint
	numEntries100to1000   uint
	numEntries1000to10000 uint
	numEntries10000to1mil uint
	numEntriesOver1mil    uint
}

type sourceHistogram struct {
	totalSize uint64
	files     fileHistogram
	dirs      dirHistogram
}
