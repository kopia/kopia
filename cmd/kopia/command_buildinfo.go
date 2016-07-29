package main

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	buildInfoCommand = app.Command("buildinfo", "Show build information").Hidden()

	buildVersion = "UNKNOWN"
	buildInfo    = "UNKNOWN"
)

func init() {
	buildInfoCommand.Action(runBuildInfoCommand)
}

func runBuildInfoCommand(context *kingpin.ParseContext) error {
	fmt.Println("Version:", buildVersion)
	fmt.Println("Build Information:", buildInfo)
	return nil
}
