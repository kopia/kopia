/*
Command-line tool for creating and accessing backups.

Usage:

  $ kopia [<flags>] <subcommand> [<args> ...]

Use 'kopia help' to see more details.
*/
package main

import (
	"fmt"
	"os"

	"github.com/mattn/go-colorable"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/kopia/kopia/storage/filesystem/cli"
	_ "github.com/kopia/kopia/storage/gcs/cli"
	_ "github.com/kopia/kopia/storage/s3/cli"
	_ "github.com/kopia/kopia/storage/webdav/cli"
)

var (
	logFile  = cli.App().Flag("log-file", "log file name").String()
	logLevel = cli.App().Flag("log-level", "log level").Default("info").Enum("debug", "info", "warning", "error")
)

func initializeLogging(ctx *kingpin.ParseContext) error {
	switch *logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	}

	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000000"
	if lfn := *logFile; lfn != "" {
		lf, err := os.Create(lfn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't create log file: %v", err)
			os.Exit(1)
		}

		log.Logger = log.Output(lf)
	} else {

		log.Logger = log.Output(zerolog.ConsoleWriter{Out: colorable.NewColorableStderr()})
	}

	return nil
}

var usageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\
{{define "FormatCommandList"}}\
{{range .}}\
{{if not .Hidden}}\
{{.Depth|Indent}}{{.Name}}{{if .Default}}*{{end}}{{template "FormatCommand" .}}
{{template "FormatCommandList" .Commands}}\
{{end}}\
{{end}}\
{{end}}\
{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\
{{end}}\
{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\
{{if .Context.Flags}}\
Flags:
{{.Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.SelectedCommand}}\
{{if .Context.SelectedCommand.Commands}}\
Subcommands:
  {{.Context.SelectedCommand}}
{{template "FormatCommandList" .Context.SelectedCommand.Commands}}
{{end}}\
{{else if .App.Commands}}\
Commands (use --help-full to list all commands):

{{template "FormatCommandList" .App.Commands}}
{{end}}\
`

func main() {
	app := cli.App()
	app.Version(repo.BuildVersion + " build: " + repo.BuildInfo)
	app.PreAction(initializeLogging)
	app.UsageTemplate(usageTemplate)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	return
}
