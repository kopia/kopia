package compression

// HeaderID is a unique identifier of the compressor stored in the compressed block header.
type HeaderID uint32

// defined header IDs.
const (
	headerGzipDefault         HeaderID = 0x1000
	headerGzipBestSpeed       HeaderID = 0x1001
	headerGzipBestCompression HeaderID = 0x1002

	HeaderZstdDefault           HeaderID = 0x1100
	HeaderZstdFastest           HeaderID = 0x1101
	HeaderZstdBetterCompression HeaderID = 0x1102
	HeaderZstdBestCompression   HeaderID = 0x1103

	headerS2Default   HeaderID = 0x1200
	headerS2Better    HeaderID = 0x1201
	headerS2Parallel4 HeaderID = 0x1202
	headerS2Parallel8 HeaderID = 0x1203

	headerPgzipDefault         HeaderID = 0x1300
	headerPgzipBestSpeed       HeaderID = 0x1301
	headerPgzipBestCompression HeaderID = 0x1302

	// headerLZ4Reserved was historically used for LZ4 and must not be reused,
	// since older repositories may still contain blocks with this on-disk header ID.
	headerLZ4Reserved HeaderID = 0x1400 //nolint:unused

	headerDeflateDefault         HeaderID = 0x1500
	headerDeflateBestSpeed       HeaderID = 0x1501
	headerDeflateBestCompression HeaderID = 0x1502
)
