// Package storj implements blob storage backend for Storj decentralized cloud storage.
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
	unencryptedObjectKeys bool   //nolint:unused // TODO: figure out how/whether to use this

	// internal field holding the uplink external interface library instance
	// ex ulext.External
	// upload options
	recursive            bool             //nolint:unused // TODO: figure out how/whether to use this
	pending              bool             //nolint:unused // TODO: figure out how/whether to use this
	expanded             bool             //nolint:unused // TODO: figure out how/whether to use this
	inmemoryEC           bool             //nolint:unused // TODO: figure out how/whether to use this
	locs                 []ulloc.Location //nolint:unused // TODO: figure out how/whether to use this
	byteRange            string           //nolint:unused // TODO: figure out how/whether to use this
	progress             bool             //nolint:unused // TODO: figure out how/whether to use this
	transfers            int              //nolint:unused // TODO: figure out how/whether to use this
	dryrun               bool             //nolint:unused // TODO: figure out how/whether to use this
	expires              time.Time        //nolint:unused // TODO: figure out how/whether to use this
	parallelism          int              // how many processes can be done at the same time //nolint:unused // TODO: figure out how/whether to use this
	parallelismChunkSize memory.Size      //nolint:unused // TODO: figure out how/whether to use this

	uploadConfig testuplink.ConcurrentSegmentUploadsConfig //nolint:unused // TODO: figure out how/whether to use this
	// PointInTime specifies a view of the (versioned) store at that time.
	PointInTime *time.Time `json:"pointInTime,omitempty"` // FIXME: check if supported by our backend?! If not: remove
}
