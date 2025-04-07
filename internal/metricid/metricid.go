// Package metricid provides mapping between metric names and persistent IDs.
package metricid

// Counters contains a mapping of counter names to ID.
//
//nolint:gochecknoglobals,mnd
var Counters = NewMapping(map[string]int{
	"blob_download_full_blob_bytes":                1,
	"blob_download_partial_blob_bytes":             2,
	"blob_errors[method:Close]":                    3,
	"blob_errors[method:DeleteBlob]":               4,
	"blob_errors[method:FlushCaches]":              5,
	"blob_errors[method:GetBlob]":                  6,
	"blob_errors[method:GetCapacity]":              7,
	"blob_errors[method:GetMetadata]":              8,
	"blob_errors[method:ListBlobs]":                9,
	"blob_errors[method:PutBlob]":                  10,
	"blob_list_items":                              11,
	"blob_upload_bytes":                            12,
	"content_after_compression_bytes":              13,
	"content_compressible_bytes":                   14,
	"content_compression_attempted_bytes":          15,
	"content_compression_attempted_duration_nanos": 16,
	"content_compression_savings_bytes":            17,
	"content_decompressed_bytes":                   18,
	"content_decompressed_duration_nanos":          19,
	"content_decrypted_bytes":                      20,
	"content_decrypted_duration_nanos":             21,
	"content_deduplicated":                         22,
	"content_deduplicated_bytes":                   23,
	"content_encrypted_bytes":                      24,
	"content_encrypted_duration_nanos":             25,
	"content_get_error_count":                      26,
	"content_get_not_found_count":                  27,
	"content_hashed_bytes":                         28,
	"content_hashed_duration_nanos":                29,
	"content_non_compressible_bytes":               30,
	"content_read_bytes":                           31,
	"content_read_duration_nanos":                  32,
	"content_uploaded_bytes":                       33,
	"content_write_bytes":                          34,
	"content_write_duration_nanos":                 35,
	// add new items here, use consecutive values
})

// DurationDistributions contains a mapping of DurationDistribution names to ID.
//
//nolint:gochecknoglobals,mnd
var DurationDistributions = NewMapping(map[string]int{
	"blob_storage_latency[method:Close]":           1,
	"blob_storage_latency[method:DeleteBlob]":      2,
	"blob_storage_latency[method:FlushCaches]":     3,
	"blob_storage_latency[method:GetBlob-full]":    4,
	"blob_storage_latency[method:GetBlob-partial]": 5,
	"blob_storage_latency[method:GetCapacity]":     6,
	"blob_storage_latency[method:GetMetadata]":     7,
	"blob_storage_latency[method:ListBlobs]":       8,
	"blob_storage_latency[method:PutBlob]":         9,
	// add new items here, use consecutive values
})

// SizeDistributions provides mapping between SizeDistribution metric names to IDs.
//
//nolint:gochecknoglobals
var SizeDistributions = NewMapping(map[string]int{
	// add new items here, use consecutive values
})
