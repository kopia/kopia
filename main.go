/*
Command-line tool for creating and accessing backups.

Usage:

  $ kopia [<flags>] <subcommand> [<args> ...]

Use 'kopia help' to see more details.
*/
package main

import (
	"os"

	gologging "github.com/op/go-logging"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/logfile"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

const usageTemplate = `{{define "FormatCommand"}}\
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

	logging.SetDefault(func(module string) logging.Logger {
		return gologging.MustGetLogger(module)
	})
	app.Version(repo.BuildVersion + " build: " + repo.BuildInfo)
	app.PreAction(logfile.Initialize)
	app.UsageTemplate(usageTemplate)
	kingpin.MustParse(app.Parse(os.Args[1:]))
}
