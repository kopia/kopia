package index

// MaxPartitions is the largest number of partitions supported by ID.Partition.
const MaxPartitions = 1 << 16

// Partition returns the partition (0..numPartitions-1) of the ID, based on the first two bytes of its hash.
func (i ID) Partition(numPartitions int) int {
	return (int(i.data[0])<<8 | int(i.data[1])) % numPartitions
}

// InfoGreaterThan returns true if a wins over b when both describe the same content (same rule as Builder.Add and merged indexes).
func InfoGreaterThan(a, b *Info) bool {
	return contentInfoGreaterThanStruct(a, b)
}
