package fio

// Options are flags to be set when running fio
type Options map[string]string

// Merge will merge two Options, overwriting common option keys
// with the incoming option values. Returns the merged result
func (o Options) Merge(other Options) map[string]string {
	out := make(map[string]string, len(o)+len(other))

	for k, v := range o {
		out[k] = v
	}

	for k, v := range other {
		out[k] = v
	}

	return out
}
