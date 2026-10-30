package content

import (
	"github.com/kopia/kopia/internal/metrics"
)

type metricsStruct struct {
	uploadedBytes *metrics.Counter

	getContentBytes         *metrics.Throughput
	getContentErrorCount    *metrics.Counter
	getContentNotFoundCount *metrics.Counter

	// number of bytes written
	writeContentBytes *metrics.Throughput

	// number of bytes after compression was applied (if any).
	afterCompressionBytes *metrics.Counter

	nonCompressibleBytes *metrics.Counter

	compressibleBytes  *metrics.Counter
	compressionSavings *metrics.Counter

	hashedBytes               *metrics.Throughput
	encryptedBytes            *metrics.Throughput
	decryptedBytes            *metrics.Throughput
	compressionAttemptedBytes *metrics.Throughput
	decompressedBytes         *metrics.Throughput

	deduplicatedBytes    *metrics.Counter
	deduplicatedContents *metrics.Counter
}

func initMetricsStruct(mr *metrics.Registry) metricsStruct {
	return metricsStruct{
		uploadedBytes:           mr.CounterInt64("content_uploaded_bytes", "Number of bytes uploaded from content manager.", nil),
		getContentErrorCount:    mr.CounterInt64("content_get_error_count", "Number of time GetContent() was called and the result was an error", nil),
		getContentNotFoundCount: mr.CounterInt64("content_get_not_found_count", "Number of time GetContent() was called and the result was not found", nil),
		deduplicatedContents:    mr.CounterInt64("content_deduplicated", "Number of contents deduplicated.", nil),
		deduplicatedBytes:       mr.CounterInt64("content_deduplicated_bytes", "Number of bytes deduplicated.", nil),

		writeContentBytes: mr.Throughput("content_write", "WriteContent throughput (before deduplication)", nil),
		hashedBytes:       mr.Throughput("content_hashed", "Hashing throughput.", nil),

		afterCompressionBytes: mr.CounterInt64("content_after_compression_bytes", "Number of bytes after deduplication and compression", nil),
		nonCompressibleBytes:  mr.CounterInt64("content_non_compressible_bytes", "Number of bytes that were found to be non-compressible stage.", nil),
		compressibleBytes:     mr.CounterInt64("content_compressible_bytes", "Number of bytes that were found to be compressible.", nil),
		compressionSavings:    mr.CounterInt64("content_compression_savings_bytes", "Number of bytes that were saved due to compression.", nil),

		encryptedBytes:            mr.Throughput("content_encrypted", "Encryption throughput.", nil),
		compressionAttemptedBytes: mr.Throughput("content_compression_attempted", "Compression throughput.", nil),

		getContentBytes:   mr.Throughput("content_read", "Number of bytes read", nil),
		decryptedBytes:    mr.Throughput("content_decrypted", "Decryption throughput.", nil),
		decompressedBytes: mr.Throughput("content_decompressed", "Decompression throughput.", nil),
	}
}
