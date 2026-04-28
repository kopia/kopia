package storj

import (
	"time"

	"storj.io/common/memory"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/testuplink"

	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options for Storj-based storage.
type Options struct {
	BucketName string `json:"bucket"`
	throttling.Limits
	AccessName string `json:"accessName"`
	KeyOrGrant string `json:"keyOrGrant" kopia:"sensitive"`

	// only relevant when apikey is used
	SatelliteAddr         string
	Passphrase            string `json:"passphrase" kopia:"sensitive"` // TODO: isn't this the encryption passphrase, so not _only_ for API key mode?
	unencryptedObjectKeys bool

	// internal field holding the uplink external interface library instance
	// ex ulext.External
	// upload options
	recursive            bool
	pending              bool
	expanded             bool
	inmemoryEC           bool
	locs                 []ulloc.Location
	byteRange            string
	progress             bool
	transfers            int
	dryrun               bool
	expires              time.Time
	parallelism          int // how many proccess can be done at the same time
	parallelismChunkSize memory.Size

	uploadConfig testuplink.ConcurrentSegmentUploadsConfig
	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"` // FIXME: check if supported by our backend?! If not: remove
}
