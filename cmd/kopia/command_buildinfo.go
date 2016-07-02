package main

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	buildInfoCommand = app.Command("buildinfo", "Show build information").Hidden()

	buildVersion = "UNKNOWN"
	buildGitHash = "UNKNOWN"
	buildTime    = ""
)

func buildTimeString() string {
	t, err := strconv.ParseInt(buildTime, 10, 64)
	if err == nil {
		return time.Unix(t, 0).Format(time.RFC3339)
	}

	return "UNKNOWN"
}

func init() {
	buildInfoCommand.Action(runBuildInfoCommand)
}

func runBuildInfoCommand(context *kingpin.ParseContext) error {
	fmt.Println("Version:", buildVersion)
	fmt.Println("Built:  ", buildTimeString())
	return nil
}
