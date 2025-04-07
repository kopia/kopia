// Command cli2md generates documentation pages from CLI flags.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/cli"
	_ "github.com/kopia/kopia/internal/logfile"
)

//nolint:gochecknoglobals
var baseDir = flag.String("base-dir", "content/docs/Reference/Command-Line", "Base directory")

const (
	advancedSection        = "Advanced"
	advancedCommandsWeight = 6

	commonSection        = "Common"
	commonCommandsWeight = 5

	dirMode = 0o750
)

//nolint:gochecknoglobals
var overrideDefault = map[string]string{
	"config-file": "repository.config",
	"log-dir":     "kopia",
}

func emitFlags(w io.Writer, flags []*kingpin.FlagModel) {
	if len(flags) == 0 {
		return
	}

	fmt.Fprintf(w, "| Flag | Short | Default | Help |\n") //nolint:errcheck
	fmt.Fprintf(w, "| ---- | ----- | --- | --- |\n")      //nolint:errcheck

	for _, f := range sortFlags(flags) {
		maybeAdvanced := ""

		if f.Hidden {
			maybeAdvanced = "[ADV] "
		}

		shortFlag := ""
		if f.Short != 0 {
			shortFlag = "`-" + string([]byte{byte(f.Short)}) + "`"
		}

		defaultValue := ""
		if len(f.Default) > 0 {
			defaultValue = f.Default[0]
		}

		if def, ok := overrideDefault[f.Name]; ok {
			defaultValue = def
		}

		if defaultValue != "" {
			defaultValue = "`" + defaultValue + "`"
		}

		if f.IsBoolFlag() {
			if defaultValue == "" {
				defaultValue = "`false`"
			}

			fmt.Fprintf(w, "| `--[no-]%v` | %v | %v | %v%v |\n", f.Name, shortFlag, defaultValue, maybeAdvanced, f.Help) //nolint:errcheck
		} else {
			fmt.Fprintf(w, "| `--%v` | %v | %v | %v%v |\n", f.Name, shortFlag, defaultValue, maybeAdvanced, f.Help) //nolint:errcheck
		}
	}

	fmt.Fprintf(w, "\n") //nolint:errcheck
}

func combineFlags(lists ...[]*kingpin.FlagModel) []*kingpin.FlagModel {
	var result []*kingpin.FlagModel

	for _, list := range lists {
		result = append(result, list...)
	}

	return result
}

func sortFlags(f []*kingpin.FlagModel) []*kingpin.FlagModel {
	sort.Slice(f, func(i, j int) bool {
		a, b := f[i], f[j]

		if l, r := a.Hidden, b.Hidden; l != r {
			return !l
		}

		return a.Name < b.Name
	})

	return f
}

func emitArgs(w io.Writer, args []*kingpin.ArgModel) {
	if len(args) == 0 {
		return
	}

	fmt.Fprintf(w, "| Argument | Help |\n") //nolint:errcheck
	fmt.Fprintf(w, "| -------- | ---  |\n") //nolint:errcheck

	args2 := append([]*kingpin.ArgModel(nil), args...)
	sort.Slice(args2, func(i, j int) bool {
		return args2[i].Name < args2[j].Name
	})

	for _, f := range args2 {
		fmt.Fprintf(w, "| `%v` | %v |\n", f.Name, f.Help) //nolint:errcheck
	}

	fmt.Fprintf(w, "\n") //nolint:errcheck
}

func generateAppFlags(app *kingpin.ApplicationModel) error {
	f, err := os.Create(filepath.Join(*baseDir, "flags.md"))
	if err != nil {
		return errors.Wrap(err, "unable to create common flags file")
	}
	defer f.Close() //nolint:errcheck

	title := "Flags"

	//nolint:errcheck
	fmt.Fprintf(f, `---
title: %q
linkTitle: %q
weight: 3
---
`, title, title)
	emitFlags(f, app.Flags)

	return nil
}

func generateCommands(app *kingpin.ApplicationModel, section string, weight int, advanced bool) error {
	dir := filepath.Join(*baseDir, section)

	if err := os.MkdirAll(dir, dirMode); err != nil {
		return errors.Wrapf(err, "error creating section directory for %v", section)
	}

	f, err := os.Create(filepath.Join(dir, "_index.md")) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "unable to create common flags file")
	}
	defer f.Close() //nolint:errcheck

	title := section + " Commands"

	//nolint:errcheck
	fmt.Fprintf(f, `---
title: %q
linkTitle: %q
weight: %v
hide_summary: true
no_list: true
---
`, title, title, weight)

	flat := flattenCommands(app.Commands)
	for _, c := range flat {
		generateSubcommands(f, dir, c.Help, c.Commands, advanced)
	}

	return nil
}

func flattenCommands(cmds []*kingpin.CmdModel) []*kingpin.CmdModel {
	var result []*kingpin.CmdModel

	commonRoot := &kingpin.CmdModel{
		Name:          "Common Commands",
		Help:          "Common Commands",
		CmdGroupModel: &kingpin.CmdGroupModel{},
	}
	result = append(result, commonRoot)

	for _, c := range cmds {
		if len(c.Commands) == 0 {
			commonRoot.Commands = append(commonRoot.Commands, c)
			continue
		}

		root := &kingpin.CmdModel{
			Name:          c.Name,
			FullCommand:   c.FullCommand,
			Help:          c.Help,
			Hidden:        c.Hidden,
			CmdGroupModel: &kingpin.CmdGroupModel{},
		}
		result = append(result, root)
		root.Commands = flattenChildren(c, nil, c.Hidden)
	}

	return result
}

func flattenChildren(cmd *kingpin.CmdModel, parentFlags []*kingpin.FlagModel, forceHidden bool) []*kingpin.CmdModel {
	var result []*kingpin.CmdModel

	cmdFlags := combineFlags(parentFlags, cmd.Flags)

	if len(cmd.Commands) == 0 {
		cmdClone := *cmd
		if forceHidden {
			cmdClone.Hidden = true
		}

		cmdClone.Flags = cmdFlags

		result = append(result, &cmdClone)
	} else {
		for _, c := range cmd.Commands {
			result = append(result, flattenChildren(c, cmdFlags, c.Hidden || forceHidden)...)
		}
	}

	return result
}

func generateSubcommands(w io.Writer, dir, sectionTitle string, cmds []*kingpin.CmdModel, advanced bool) {
	cmds = append([]*kingpin.CmdModel(nil), cmds...)

	first := true

	for _, c := range cmds {
		if c.Hidden != advanced {
			continue
		}

		if first {
			fmt.Fprintf(w, "\n### %v\n\n", strings.TrimSuffix(sectionTitle, ".")) //nolint:errcheck

			first = false
		}

		subcommandSlug := strings.Replace(c.FullCommand, " ", "-", -1)
		helpSummary := strings.SplitN(c.Help, "\n", 2)[0] //nolint:mnd
		helpSummary = strings.TrimSuffix(helpSummary, ".")
		fmt.Fprintf(w, "* [`%v`](%v) - %v\n", c.FullCommand, subcommandSlug+"/", helpSummary) //nolint:errcheck
		generateSubcommandPage(filepath.Join(dir, subcommandSlug+".md"), c)
	}
}

func generateSubcommandPage(fname string, cmd *kingpin.CmdModel) {
	f, err := os.Create(fname) //nolint:gosec
	if err != nil {
		log.Fatalf("unable to create page: %v", err)
	}
	defer f.Close() //nolint:errcheck

	title := cmd.FullCommand

	//nolint:errcheck
	fmt.Fprintf(f, `---
title: %q
linkTitle: %q
weight: 10
toc_hide: true
hide_summary: true
---

`, title, title)

	flagSummary := ""
	argSummary := ""

	for _, a := range cmd.Args {
		if a.Required {
			argSummary += " <" + a.Name + ">"
		} else {
			argSummary += " [" + a.Name + "]"
		}
	}

	for _, fl := range cmd.Flags {
		if fl.Required {
			flagSummary += " \\\n        --" + fl.Name + "=..."
		}
	}

	fmt.Fprintf(f, "```shell\n$ kopia %v%v%v\n```\n\n", cmd.FullCommand, flagSummary, argSummary) //nolint:errcheck
	fmt.Fprintf(f, "%v\n\n", cmd.Help)                                                            //nolint:errcheck

	emitFlags(f, cmd.Flags)
	emitArgs(f, cmd.Args)
}

func main() {
	flag.Parse()

	if _, err := os.Stat(*baseDir); err != nil {
		log.Fatalf("invalid base directory: %v", err)
	}

	_ = os.RemoveAll(filepath.Join(*baseDir, commonSection))
	_ = os.RemoveAll(filepath.Join(*baseDir, advancedSection))

	kingpinApp := kingpin.New("kopia", "Kopia - Fast And Secure Open-Source Backup").Author("http://kopia.github.io/")
	cli.NewApp().Attach(kingpinApp)

	app := kingpinApp.Model()

	if err := generateAppFlags(app); err != nil {
		log.Fatalf("unable to generate common flags: %v", err)
	}

	if err := generateCommands(app, commonSection, commonCommandsWeight, false); err != nil {
		log.Fatalf("unable to generate common commands: %v", err)
	}

	if err := generateCommands(app, advancedSection, advancedCommandsWeight, true); err != nil {
		log.Fatalf("unable to generate advanced commands: %v", err)
	}
}
