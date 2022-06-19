package object

// IndirectObjectEntry represents an entry in indirect object stream.
type IndirectObjectEntry struct {
	Start  int64 `json:"s,omitempty"`
	Length int64 `json:"l,omitempty"`
	Object ID    `json:"o,omitempty"`
}

func (i *IndirectObjectEntry) endOffset() int64 {
	return i.Start + i.Length
}

/*

{"stream":"kopia:indirect","entries":[
{"l":1698099,"o":"D13ea27f9ad891ad4a2edfa983906863d"},
{"s":1698099,"l":1302081,"o":"De8ca8327cd3af5f4edbd5ed1009c525e"},
{"s":3000180,"l":4352499,"o":"D6b6eb48ca5361d06d72fe193813e42e1"},
{"s":7352679,"l":1170821,"o":"Dd14653f76b63802ed48be64a0e67fea9"},

{"s":91094118,"l":1645153,"o":"Daa55df764d881a1daadb5ea9de17abbb"}
]}
*/
