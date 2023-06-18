package cli

import (
	"context"
	"strconv"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type commonThrottleSet struct {
	setDownloadBytesPerSecond string
	setUploadBytesPerSecond   string
	setReadsPerSecond         string
	setWritesPerSecond        string
	setListsPerSecond         string
	setConcurrentReads        string
	setConcurrentWrites       string
}

func (c *commonThrottleSet) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("download-bytes-per-second", "Set the download bytes per second").StringVar(&c.setDownloadBytesPerSecond)
	cmd.Flag("upload-bytes-per-second", "Set the upload bytes per second").StringVar(&c.setUploadBytesPerSecond)
	cmd.Flag("read-requests-per-second", "Set max reads per second").StringVar(&c.setReadsPerSecond)
	cmd.Flag("write-requests-per-second", "Set max writes per second").StringVar(&c.setWritesPerSecond)
	cmd.Flag("list-requests-per-second", "Set max lists per second").StringVar(&c.setListsPerSecond)
	cmd.Flag("concurrent-reads", "Set max concurrent reads").StringVar(&c.setConcurrentReads)
	cmd.Flag("concurrent-writes", "Set max concurrent writes").StringVar(&c.setConcurrentWrites)
}

func (c *commonThrottleSet) apply(ctx context.Context, limits *throttling.Limits, changeCount *int) error {
	if err := c.setThrottleFloat64(ctx, "max download speed", true, &limits.DownloadBytesPerSecond, c.setDownloadBytesPerSecond, changeCount); err != nil {
		return err
	}

	if err := c.setThrottleFloat64(ctx, "max upload speed", true, &limits.UploadBytesPerSecond, c.setUploadBytesPerSecond, changeCount); err != nil {
		return err
	}

	if err := c.setThrottleFloat64(ctx, "reads per second", false, &limits.ReadsPerSecond, c.setReadsPerSecond, changeCount); err != nil {
		return err
	}

	if err := c.setThrottleFloat64(ctx, "writes per second", false, &limits.WritesPerSecond, c.setWritesPerSecond, changeCount); err != nil {
		return err
	}

	if err := c.setThrottleFloat64(ctx, "lists per second", false, &limits.ListsPerSecond, c.setListsPerSecond, changeCount); err != nil {
		return err
	}

	if err := c.setThrottleInt(ctx, "concurrent reads", &limits.ConcurrentReads, c.setConcurrentReads, changeCount); err != nil {
		return err
	}

	return c.setThrottleInt(ctx, "concurrent writes", &limits.ConcurrentWrites, c.setConcurrentWrites, changeCount)
}

func (c *commonThrottleSet) setThrottleFloat64(ctx context.Context, desc string, bps bool, val *float64, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == "unlimited" || str == "-" {
		*changeCount++

		log(ctx).Infof("Setting %v to a unlimited.", desc)

		*val = 0

		return nil
	}

	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %v %q", desc, str)
	}

	*changeCount++

	if bps {
		log(ctx).Infof("Setting %v to %v.", desc, units.BytesPerSecondsString(v))
	} else {
		log(ctx).Infof("Setting %v to %v.", desc, v)
	}

	*val = v

	return nil
}

func (c *commonThrottleSet) setThrottleInt(ctx context.Context, desc string, val *int, str string, changeCount *int) error {
	if str == "" {
		// not changed
		return nil
	}

	if str == "unlimited" || str == "-" {
		*changeCount++

		log(ctx).Infof("Setting %v to a unlimited.", desc)

		*val = 0

		return nil
	}

	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "can't parse the %v %q", desc, str)
	}

	*changeCount++

	log(ctx).Infof("Setting %v to %v.", desc, v)

	*val = int(v)

	return nil
}
