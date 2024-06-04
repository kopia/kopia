package index

func decodeBigEndianUint48(d []byte) int64 {
	_ = d[5] // early bounds check
	return int64(d[0])<<40 | int64(d[1])<<32 | int64(d[2])<<24 | int64(d[3])<<16 | int64(d[4])<<8 | int64(d[5])
}

func decodeBigEndianUint32(d []byte) uint32 {
	_ = d[3] // early bounds check
	return uint32(d[0])<<24 | uint32(d[1])<<16 | uint32(d[2])<<8 | uint32(d[3])
}

func decodeBigEndianUint24(d []byte) uint32 {
	_ = d[2] // early bounds check
	return uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
}

func decodeBigEndianUint16(d []byte) uint16 {
	_ = d[1] // early bounds check
	return uint16(d[0])<<8 | uint16(d[1])
}

func encodeBigEndianUint24(b []byte, v uint32) {
	_ = b[2]             // early bounds check
	b[0] = byte(v >> 16) //nolint:mnd
	b[1] = byte(v >> 8)  //nolint:mnd
	b[2] = byte(v)
}
