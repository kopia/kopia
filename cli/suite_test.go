package cli_test

import (
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/format"
)

func TestMain(m *testing.M) { testutil.MyTestMain(m) }

type formatSpecificTestSuite struct {
	formatFlags   []string
	formatVersion format.Version
}

func TestFormatV1(t *testing.T) {
	testutil.RunAllTestsWithParam(t, &formatSpecificTestSuite{[]string{"--format-version=1"}, format.FormatVersion1})
}

func TestFormatV2(t *testing.T) {
	testutil.RunAllTestsWithParam(t, &formatSpecificTestSuite{[]string{"--format-version=2"}, format.FormatVersion2})
}

func TestFormatV3(t *testing.T) {
	testutil.RunAllTestsWithParam(t, &formatSpecificTestSuite{[]string{"--format-version=3"}, format.FormatVersion3})
}
