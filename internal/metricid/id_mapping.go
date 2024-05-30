// Package metricid provides mapping between metric names and persistent IDs.
package metricid

// Mapping contains translation of names to indexes and vice versa, which allows maps
// of well-known keys to be converted to slices of just values with more compact JSON representations.
type Mapping struct {
	MaxIndex    int
	NameToIndex map[string]int
	IndexToName map[int]string
}

// MapToSlice converts the given map to a slice according to the provided mapping.
// Keys not in the mapping are dropped.
func MapToSlice[T any](mapping *Mapping, input map[string]T) []T {
	result := make([]T, mapping.MaxIndex)

	for k, v := range input {
		id := mapping.NameToIndex[k]
		if id > 0 {
			result[id-1] = v
		}
	}

	return result
}

// SliceToMap converts the given slice to a map according to the provided mapping.
func SliceToMap[T any](mapping *Mapping, input []T) map[string]T {
	result := map[string]T{}

	for k, v := range input {
		if n, ok := mapping.IndexToName[k+1]; ok {
			result[n] = v
		}
	}

	return result
}

// NewMapping creates a new Mapping given the provided name-to-index map.
func NewMapping(fwd map[string]int) *Mapping {
	m := &Mapping{
		NameToIndex: fwd,
		IndexToName: inverse(fwd),
	}

	maxIndex := 0

	for _, index := range fwd {
		if index > maxIndex {
			maxIndex = index
		}
	}

	m.MaxIndex = maxIndex

	return m
}

func inverse(m map[string]int) map[int]string {
	res := map[int]string{}

	for k, v := range m {
		if v > 0 {
			res[v] = k
		}
	}

	return res
}
