package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandIndexEpochList struct {
	out textOutput
}

func (c *commandIndexEpochList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List the status of epochs.")
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandIndexEpochList) run(ctx context.Context, rep repo.DirectRepository) error {
	emgr, ok, err := rep.ContentReader().EpochManager(ctx)
	if err != nil {
		return errors.Wrap(err, "epoch manager")
	}

	if !ok {
		return errors.New("epoch manager is not active")
	}

	snap, err := emgr.Current(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to determine current epoch")
	}

	c.out.printStdout("Current Epoch: %v\n", snap.WriteEpoch)

	if est := snap.EpochStartTime[snap.WriteEpoch]; !est.IsZero() {
		c.out.printStdout("Epoch Started  %v\n", formatTimestamp(est))
	}

	firstNonRangeCompacted := 0
	if len(snap.LongestRangeCheckpointSets) > 0 {
		firstNonRangeCompacted = snap.LongestRangeCheckpointSets[len(snap.LongestRangeCheckpointSets)-1].MaxEpoch + 1
	}

	for e := snap.WriteEpoch; e >= firstNonRangeCompacted; e-- {
		if uces := snap.UncompactedEpochSets[e]; len(uces) > 0 {
			minTime := blob.MinTimestamp(uces)
			maxTime := blob.MaxTimestamp(uces)

			c.out.printStdout("%v %v ... %v, %v blobs, %v, span %v\n",
				e,
				formatTimestamp(minTime),
				formatTimestamp(maxTime),
				len(uces),
				units.BytesString(blob.TotalLength(uces)),
				maxTime.Sub(minTime).Round(time.Second),
			)
		}

		if secs := snap.SingleEpochCompactionSets[e]; secs != nil {
			c.out.printStdout("%v: %v single-epoch %v blobs, %v\n",
				e,
				formatTimestamp(secs[0].Timestamp),
				len(secs),
				units.BytesString(blob.TotalLength(secs)),
			)
		}
	}

	for _, cs := range snap.LongestRangeCheckpointSets {
		c.out.printStdout("%v-%v: range, %v blobs, %v\n",
			cs.MinEpoch,
			cs.MaxEpoch,
			len(cs.Blobs),
			units.BytesString(blob.TotalLength(cs.Blobs)),
		)
	}

	return nil
}
