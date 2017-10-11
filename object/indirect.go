package object

var indirectStreamType = "kopia:indirect"

// indirectObjectEntry represents an entry in indirect object stream.
type indirectObjectEntry struct {
	Start  int64    `json:"s,omitempty"`
	Length int64    `json:"l,omitempty"`
	Object ObjectID `json:"o,omitempty"`
}
