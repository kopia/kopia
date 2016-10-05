package repo

// GetStorageBlocks returns the list of storage blocks used by the specified object ID.
func (r *Repository) GetStorageBlocks(oid ObjectID) ([]string, error) {
	var result []string
	return r.addStorageBlocks(result, oid)
}

func (r *Repository) addStorageBlocks(result []string, oid ObjectID) ([]string, error) {
	if oid.Section != nil {
		return r.addStorageBlocks(result, oid.Section.Base)
	}

	if oid.StorageBlock != "" {
		result = append(result, oid.StorageBlock)
		if oid.Indirect == 0 {
			return result, nil
		}

		or, err := r.Open(oid)
		if err != nil {
			return nil, err
		}

		for _, st := range or.(*objectReader).seekTable {
			if st.Object != nil {
				result, err = r.addStorageBlocks(result, *st.Object)
				if err != nil {
					return nil, err
				}
			}
		}
		or.Close()
	}

	return result, nil
}
