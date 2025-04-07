package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandMaintenanceSet struct {
	maintenanceSetOwner          string
	maintenanceSetEnableQuick    []bool // optional boolean
	maintenanceSetEnableFull     []bool // optional boolean
	maintenanceSetQuickFrequency time.Duration
	maintenanceSetFullFrequency  time.Duration
	maintenanceSetPauseQuick     time.Duration
	maintenanceSetPauseFull      time.Duration

	maxRetainedLogCount       int
	maxRetainedLogAge         time.Duration
	maxTotalRetainedLogSizeMB int64

	extendObjectLocks []bool // optional boolean

	listParallelism int
}

func (c *commandMaintenanceSet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set maintenance parameters")

	c.maintenanceSetQuickFrequency = -1
	c.maintenanceSetFullFrequency = -1
	c.maintenanceSetPauseQuick = -1
	c.maintenanceSetPauseFull = -1

	c.maxRetainedLogCount = -1
	c.maxRetainedLogAge = -1
	c.maxTotalRetainedLogSizeMB = -1

	c.listParallelism = -1

	cmd.Flag("owner", "Set maintenance owner user@hostname").StringVar(&c.maintenanceSetOwner)

	cmd.Flag("enable-quick", "Enable or disable quick maintenance").BoolListVar(&c.maintenanceSetEnableQuick)
	cmd.Flag("enable-full", "Enable or disable full maintenance").BoolListVar(&c.maintenanceSetEnableFull)

	cmd.Flag("quick-interval", "Set quick maintenance interval").DurationVar(&c.maintenanceSetQuickFrequency)
	cmd.Flag("full-interval", "Set full maintenance interval").DurationVar(&c.maintenanceSetFullFrequency)

	cmd.Flag("pause-quick", "Pause quick maintenance for a specified duration").DurationVar(&c.maintenanceSetPauseQuick)
	cmd.Flag("pause-full", "Pause full maintenance for a specified duration").DurationVar(&c.maintenanceSetPauseFull)

	cmd.Flag("max-retained-log-count", "Set maximum number of log sessions to retain").IntVar(&c.maxRetainedLogCount)
	cmd.Flag("max-retained-log-age", "Set maximum age of log sessions to retain").DurationVar(&c.maxRetainedLogAge)
	cmd.Flag("max-retained-log-size-mb", "Set maximum total size of log sessions").Int64Var(&c.maxTotalRetainedLogSizeMB)
	cmd.Flag("extend-object-locks", "Extend retention period of locked objects as part of full maintenance.").BoolListVar(&c.extendObjectLocks)

	cmd.Flag("list-parallelism", "Override list parallelism.").IntVar(&c.listParallelism)

	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandMaintenanceSet) setLogCleanupParametersFromFlags(ctx context.Context, p *maintenance.Params, changed *bool) {
	if v := c.maxRetainedLogCount; v != -1 {
		cl := p.LogRetention.OrDefault()
		cl.MaxCount = v
		p.LogRetention = cl
		*changed = true

		log(ctx).Infof("Setting max retained log count to %v.", cl.MaxCount)
	}

	if v := c.maxRetainedLogAge; v != -1 {
		cl := p.LogRetention.OrDefault()
		cl.MaxAge = v
		p.LogRetention = cl
		*changed = true

		log(ctx).Infof("Setting max retained log age to %v.", cl.MaxAge)
	}

	if v := c.maxTotalRetainedLogSizeMB; v != -1 {
		cl := p.LogRetention.OrDefault()
		cl.MaxTotalSize = v << 20 //nolint:mnd
		p.LogRetention = cl
		*changed = true

		log(ctx).Infof("Setting total retained log size to %v.", units.BytesString(cl.MaxTotalSize))
	}
}

func (c *commandMaintenanceSet) setDeleteUnreferencedBlobsParams(ctx context.Context, p *maintenance.Params, changed *bool) {
	if v := c.listParallelism; v != -1 {
		p.ListParallelism = v
		*changed = true

		log(ctx).Infof("Setting list parallelism to %v.", v)
	}
}

func (c *commandMaintenanceSet) setMaintenanceOwnerFromFlags(ctx context.Context, p *maintenance.Params, rep repo.DirectRepositoryWriter, changed *bool) {
	if v := c.maintenanceSetOwner; v != "" {
		if v == "me" {
			p.Owner = rep.ClientOptions().UsernameAtHost()
		} else {
			p.Owner = v
		}

		*changed = true

		log(ctx).Infof("Setting maintenance owner to %v", p.Owner)
	}
}

func (c *commandMaintenanceSet) setMaintenanceEnabledAndIntervalFromFlags(ctx context.Context, cp *maintenance.CycleParams, cycleName string, enableFlag []bool, interval time.Duration, changed *bool) {
	// we use lists to distinguish between flag not set
	// Zero elements == not set, more than zero - flag set, in which case we pick the last value
	if len(enableFlag) > 0 {
		lastVal := enableFlag[len(enableFlag)-1]
		cp.Enabled = lastVal
		*changed = true

		if lastVal {
			log(ctx).Infof("Periodic %v maintenance enabled.", cycleName)
		} else {
			log(ctx).Infof("Periodic %v maintenance disabled.", cycleName)
		}
	}

	if interval != -1 {
		cp.Interval = interval
		*changed = true

		log(ctx).Infof("Interval for %v maintenance set to %v.", cycleName, interval)
	}
}

func (c *commandMaintenanceSet) setMaintenanceObjectLockExtendFromFlags(ctx context.Context, p *maintenance.Params, changed *bool) {
	// we use lists to distinguish between flag not set
	// Zero elements == not set, more than zero - flag set, in which case we pick the last value
	if len(c.extendObjectLocks) > 0 {
		lastVal := c.extendObjectLocks[len(c.extendObjectLocks)-1]
		p.ExtendObjectLocks = lastVal
		*changed = true

		if lastVal {
			log(ctx).Info("Object Lock extension maintenance enabled.")
		} else {
			log(ctx).Info("Object Lock extension maintenance disabled.")
		}
	}
}

func (c *commandMaintenanceSet) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	p, err := maintenance.GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	s, err := maintenance.GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	var changedParams, changedSchedule bool

	c.setMaintenanceOwnerFromFlags(ctx, p, rep, &changedParams)
	c.setMaintenanceEnabledAndIntervalFromFlags(ctx, &p.QuickCycle, "quick", c.maintenanceSetEnableQuick, c.maintenanceSetQuickFrequency, &changedParams)
	c.setMaintenanceEnabledAndIntervalFromFlags(ctx, &p.FullCycle, "full", c.maintenanceSetEnableFull, c.maintenanceSetFullFrequency, &changedParams)
	c.setLogCleanupParametersFromFlags(ctx, p, &changedParams)
	c.setDeleteUnreferencedBlobsParams(ctx, p, &changedParams)
	c.setMaintenanceObjectLockExtendFromFlags(ctx, p, &changedParams)

	if pauseDuration := c.maintenanceSetPauseQuick; pauseDuration != -1 {
		s.NextQuickMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		log(ctx).Infof("Quick maintenance paused until %v", formatTimestamp(s.NextQuickMaintenanceTime))
	}

	if pauseDuration := c.maintenanceSetPauseFull; pauseDuration != -1 {
		s.NextFullMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		log(ctx).Infof("Full maintenance paused until %v", formatTimestamp(s.NextFullMaintenanceTime))
	}

	if !changedParams && !changedSchedule {
		return errors.New("no changes specified")
	}

	blobCfg, err := rep.FormatManager().BlobCfgBlob(ctx)
	if err != nil {
		return errors.Wrap(err, "blob configuration")
	}

	if err = maintenance.CheckExtendRetention(ctx, blobCfg, p); err != nil {
		return errors.Wrap(err, "unable to apply maintenance changes")
	}

	if changedSchedule {
		if err := maintenance.SetSchedule(ctx, rep, s); err != nil {
			return errors.Wrap(err, "unable to set schedule")
		}
	}

	if changedParams {
		if err := maintenance.SetParams(ctx, rep, p); err != nil {
			return errors.Wrap(err, "unable to set params")
		}
	}

	return nil
}
