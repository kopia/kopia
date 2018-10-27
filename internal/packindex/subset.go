package packindex

// IsSubset returns true if all entries in index 'a' are contained in index 'b'.
func IsSubset(a, b Index) bool {
	done := make(chan bool)
	defer close(done)

	ach := iterateChan("", a, done)
	bch := iterateChan("", b, done)

	for ait := range ach {
		bit, ok := <-bch
		if !ok {
			return false
		}
		for bit.BlockID < ait.BlockID {
			bit, ok = <-bch
			if !ok {
				return false
			}
		}

		if bit.BlockID != ait.BlockID {
			return false
		}
	}
	return true
}
