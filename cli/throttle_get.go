package cli

import (
	"strconv"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type commonThrottleGet struct {
	out textOutput
	jo  jsonOutput
}

func (c *commonThrottleGet) setup(svc appServices, cmd *kingpin.CmdClause) {
	c.out.setup(svc)
	c.jo.setup(svc, cmd)
}

func (c *commonThrottleGet) output(limits *throttling.Limits) error {
	if c.jo.jsonOutput {
		c.out.printStdout("%s\n", c.jo.jsonBytes(limits))
		return nil
	}

	c.printValueOrUnlimited("Max Download Speed:", limits.DownloadBytesPerSecond, units.BytesPerSecondsString)
	c.printValueOrUnlimited("Max Upload Speed:", limits.UploadBytesPerSecond, units.BytesPerSecondsString)
	c.printValueOrUnlimited("Max Read Requests Per Second:", limits.ReadsPerSecond, c.floatToString)
	c.printValueOrUnlimited("Max Write Requests Per Second:", limits.WritesPerSecond, c.floatToString)
	c.printValueOrUnlimited("Max List Requests Per Second:", limits.ListsPerSecond, c.floatToString)
	c.printValueOrUnlimited("Max Concurrent Reads:", float64(limits.ConcurrentReads), c.floatToString)
	c.printValueOrUnlimited("Max Concurrent Writes:", float64(limits.ConcurrentWrites), c.floatToString)

	return nil
}

func (c *commonThrottleGet) printValueOrUnlimited(label string, v float64, convert func(v float64) string) {
	if v != 0 {
		c.out.printStdout("%-30v %v\n", label, convert(v))
	} else {
		c.out.printStdout("%-30v (unlimited)\n", label)
	}
}

func (c *commonThrottleGet) floatToString(v float64) string {
	return strconv.FormatFloat(v, 'f', 0, 64) //nolint:gomnd
}
