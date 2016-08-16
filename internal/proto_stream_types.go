package internal

var (
	// ProtoStreamTypeHashCache indicates proto stream whose elements are hash cache entries.
	ProtoStreamTypeHashCache = []byte("kopiaHCH")

	// ProtoStreamTypeDir indicates proto stream whose elements are directory entries.
	ProtoStreamTypeDir = []byte("kopiaDIR")

	// ProtoStreamTypeIndirect indicates proto stream whose elements are indirect block references.
	ProtoStreamTypeIndirect = []byte("kopiaIND")
)
