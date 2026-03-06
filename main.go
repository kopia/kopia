/*
Command-line tool for creating and accessing backups.

Usage:

	$ kopia [<flags>] <subcommand> [<args> ...]

Use 'kopia help' to see more details.
*/
package main

import (
	"os"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/i18n"
	"github.com/kopia/kopia/internal/logfile"
	"github.com/kopia/kopia/repo"
)

const usageTemplate = `{{define "FormatCommand" -}}
{{if .FlagSummary}} {{.FlagSummary -}}{{end -}}
{{range .Args}} {{if not .Required}}[{{end -}}<{{.Name -}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end -}}
{{end -}}
{{define "FormatCommandList" -}}
{{range . -}}
{{if not .Hidden -}}
{{.Depth|Indent -}}{{.Name -}}{{if .Default -}}*{{end -}}{{template "FormatCommand" .}}
{{template "FormatCommandList" .Commands -}}
{{end -}}
{{end -}}
{{end -}}
{{define "FormatUsage" -}}
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end -}}
{{if .Help}}
{{.Help|Wrap 0 -}}
{{end -}}
{{end}}
{{if .Context.SelectedCommand -}}
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else -}}
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end -}}
{{if .Context.Flags -}}
Flags:
{{.Context.Flags|FlagsToTwoColumns|FormatTwoColumns}}
{{end -}}
{{if .Context.Args -}}
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end -}}
{{if .Context.SelectedCommand -}}
{{if .Context.SelectedCommand.Commands -}}
Subcommands:
  {{.Context.SelectedCommand}}
{{template "FormatCommandList" .Context.SelectedCommand.Commands -}}
{{end -}}
{{else if .App.Commands -}}
Commands (use --help-full to list all commands):

{{template "FormatCommandList" .App.Commands -}}
{{end -}}
`

func main() {
	// Initialize global translator early, before creating commands
	lang := detectLanguageFromArgs(os.Args[1:])
	if lang == "" {
		lang = i18n.DetectLanguageFromEnv()
	}
	translator, err := i18n.NewTranslator(lang)
	if err != nil {
		translator, _ = i18n.NewTranslator("en")
	}
	i18n.SetGlobalTranslator(translator)

	app := cli.NewApp()
	kp := kingpin.New("kopia", i18n.T("Kopia - Fast And Secure Open-Source Backup")).Author("http://kopia.github.io/")

	kp.Version(repo.BuildVersion + " build: " + repo.BuildInfo + " from: " + repo.BuildGitHubRepo)
	logfile.Attach(app, kp)
	kp.ErrorWriter(os.Stderr)
	kp.UsageWriter(os.Stdout)
	kp.UsageTemplate(usageTemplate)

	app.Attach(kp)
	kingpin.MustParse(kp.Parse(os.Args[1:]))
}

// detectLanguageFromArgs extracts language from command line arguments.
func detectLanguageFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--language" && i+1 < len(args) {
			return args[i+1]
		}
		if len(arg) > 11 && arg[:11] == "--language=" {
			return arg[11:]
		}
	}
	return ""
}
