package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyRetentionFlags struct {
	policySetKeepLatest               string
	policySetKeepHourly               string
	policySetKeepDaily                string
	policySetKeepWeekly               string
	policySetKeepMonthly              string
	policySetKeepAnnual               string
	policySetIgnoreIdenticalSnapshots string
}

func (c *policyRetentionFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("keep-latest", "Number of most recent backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepLatest)
	cmd.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepHourly)
	cmd.Flag("keep-daily", "Number of most-recent daily backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepDaily)
	cmd.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepWeekly)
	cmd.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepMonthly)
	cmd.Flag("keep-annual", "Number of most-recent annual backups to keep per source (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetKeepAnnual)
	cmd.Flag("ignore-identical-snapshot", "Ignore identical snapshot with the last (or 'inherit')").PlaceHolder("N").StringVar(&c.policySetIgnoreIdenticalSnapshots)
}

func (c *policyRetentionFlags) setRetentionPolicyFromFlags(ctx context.Context, rp *policy.RetentionPolicy, changeCount *int) error {
	intCases := []struct {
		desc      string
		max       **policy.OptionalInt
		flagValue string
	}{
		{"number of annual backups to keep", &rp.KeepAnnual, c.policySetKeepAnnual},
		{"number of monthly backups to keep", &rp.KeepMonthly, c.policySetKeepMonthly},
		{"number of weekly backups to keep", &rp.KeepWeekly, c.policySetKeepWeekly},
		{"number of daily backups to keep", &rp.KeepDaily, c.policySetKeepDaily},
		{"number of hourly backups to keep", &rp.KeepHourly, c.policySetKeepHourly},
		{"number of latest backups to keep", &rp.KeepLatest, c.policySetKeepLatest},
	}

	for _, c := range intCases {
		if err := applyOptionalInt(ctx, c.desc, c.max, c.flagValue, changeCount); err != nil {
			return err
		}
	}

	boolCases := []struct {
		desc      string
		max       **policy.OptionalBool
		flagValue string
	}{
		{"ignore identical backups", &rp.IgnoreIdenticalSnapshots, c.policySetIgnoreIdenticalSnapshots},
	}

	for _, c := range boolCases {
		if err := applyPolicyBoolPtr(ctx, c.desc, c.max, c.flagValue, changeCount); err != nil {
			return err
		}
	}

	return nil
}
