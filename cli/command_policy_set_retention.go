package cli

import (
	"context"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	policySetKeepLatest  = policySetCommand.Flag("keep-latest", "Number of most recent backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepHourly  = policySetCommand.Flag("keep-hourly", "Number of most-recent hourly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepDaily   = policySetCommand.Flag("keep-daily", "Number of most-recent daily backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepWeekly  = policySetCommand.Flag("keep-weekly", "Number of most-recent weekly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepMonthly = policySetCommand.Flag("keep-monthly", "Number of most-recent monthly backups to keep per source (or 'inherit')").PlaceHolder("N").String()
	policySetKeepAnnual  = policySetCommand.Flag("keep-annual", "Number of most-recent annual backups to keep per source (or 'inherit')").PlaceHolder("N").String()
)

func setRetentionPolicyFromFlags(ctx context.Context, rp *policy.RetentionPolicy, changeCount *int) error {
	cases := []struct {
		desc      string
		max       **int
		flagValue *string
	}{
		{"number of annual backups to keep", &rp.KeepAnnual, policySetKeepAnnual},
		{"number of monthly backups to keep", &rp.KeepMonthly, policySetKeepMonthly},
		{"number of weekly backups to keep", &rp.KeepWeekly, policySetKeepWeekly},
		{"number of daily backups to keep", &rp.KeepDaily, policySetKeepDaily},
		{"number of hourly backups to keep", &rp.KeepHourly, policySetKeepHourly},
		{"number of latest backups to keep", &rp.KeepLatest, policySetKeepLatest},
	}

	for _, c := range cases {
		if err := applyPolicyNumber(ctx, c.desc, c.max, *c.flagValue, changeCount); err != nil {
			return err
		}
	}

	return nil
}
