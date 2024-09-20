package cli_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/tests/testenv"
)

func TestMaintenanceSetExtendObjectLocks(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var mi cli.MaintenanceInfo

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.False(t, mi.ExtendObjectLocks, "ExtendOjectLocks should not default to enabled.")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--extend-object-locks", "true")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.True(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be enabled.")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--extend-object-locks", "false")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.False(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be disabled.")
}

func TestMaintenanceSetListParallelism(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var mi cli.MaintenanceInfo

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	require.NotContains(t, e.RunAndExpectSuccess(t, "maintenance", "info"), "List parallelism: 0")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--list-parallelism", "33")
	require.Contains(t, e.RunAndExpectSuccess(t, "maintenance", "info"), "List parallelism: 33")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)
	require.Equal(t, 33, mi.ListParallelism, "List parallelism should be set to 33.")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--list-parallelism", "0")
	require.NotContains(t, e.RunAndExpectSuccess(t, "maintenance", "info"), "List parallelism: 0")
}

func (s *formatSpecificTestSuite) TestInvalidExtendRetainOptions(t *testing.T) {
	var mi cli.MaintenanceInfo

	var rs cli.RepositoryStatus

	e := s.setupInMemoryRepo(t)

	// set retention
	e.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", blob.Compliance.String(),
		"--retention-period", "48h")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--full-interval", "24h01m")

	// Cannot enable extend object locks when retention_period-full_maintenance_interval < 24h
	e.RunAndExpectFailure(t, "maintenance", "set", "--extend-object-locks", "true")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.False(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be disabled.")

	// Enable extend object locks when retention_period-full_maintenance_interval > 24h
	e.RunAndExpectSuccess(t, "maintenance", "set", "--full-interval", "23h59m")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--extend-object-locks", "true")

	// Cannot change full_maintenance_interval when retention_period-full_maintenance_interval < 24h
	e.RunAndExpectFailure(t, "maintenance", "set", "--full-interval", "24h01m")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.True(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be enabled.")
	require.Equal(t, mi.FullCycle.Interval, time.Duration(86340000000000), "maintenance-interval should be unchanged.")

	// Cannot change retention_period when retention_period-full_maintenance_interval < 24h
	e.RunAndExpectFailure(t, "repository", "set-parameters", "--retention-period", "47h")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "repo", "status", "--json"), &rs)
	require.Equal(t, rs.BlobRetention.RetentionPeriod, time.Duration(172800000000000), "retention-interval should be unchanged.")

	// Can change retention_period when retention_period-full_maintenance_interval > 24h
	e.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-period", "49h")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "repo", "status", "--json"), &rs)
	require.Equal(t, rs.BlobRetention.RetentionPeriod, time.Duration(176400000000000), "retention-interval should be unchanged.")
}
