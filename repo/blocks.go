package repo

// GetStorageBlocks returns the list of storage blocks used by the specified object ID.
func (r *ObjectManager) GetStorageBlocks(oid ObjectID) ([]string, error) {
	result := map[string]bool{}
	if err := r.addStorageBlocks(result, oid); err != nil {
		return nil, err
	}

	var b []string
	for k := range result {
		b = append(b, k)
	}
	return b, nil
}

func (r *ObjectManager) addStorageBlocks(result map[string]bool, oid ObjectID) error {
	if oid.Section != nil {
		return r.addStorageBlocks(result, oid.Section.Base)
	}

	if oid.StorageBlock != "" {
		result[oid.StorageBlock] = true
	}

	if oid.PackID != "" {
		s, err := r.packIDToSection(oid)
		if err != nil {
			return err
		}

		r.addStorageBlocks(result, s.Base)
	}

	if oid.Indirect == nil {
		return nil
	}

	or, err := r.Open(*oid.Indirect)
	if err != nil {
		return err
	}
	defer or.Close()

	chunks, err := r.flattenListChunk(or)
	if err != nil {
		return err
	}
	for _, st := range chunks {
		if st.Object != nil {
			if err := r.addStorageBlocks(result, *st.Object); err != nil {
				return err
			}
		}
	}

	return r.addStorageBlocks(result, *oid.Indirect)
}
