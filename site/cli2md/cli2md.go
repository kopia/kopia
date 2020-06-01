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

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/cli"
	_ "github.com/kopia/kopia/internal/logfile"
)

var baseDir = "content/docs/Reference/Command-Line"

const advancedSection = "Advanced"
const commonSection = "Common"

var overrideDefault = map[string]string{
	"config-file": "repository.config",
	"log-dir":     "kopia",
}

func emitFlags(w io.Writer, flags []*kingpin.FlagModel) {
	if len(flags) == 0 {
		return
	}

	fmt.Fprintf(w, "| Flag | Short | Default | Help |\n")
	fmt.Fprintf(w, "| ---- | ----- | --- | --- |\n")

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

			fmt.Fprintf(w, "| `--[no-]%v` | %v | %v | %v%v |\n", f.Name, shortFlag, defaultValue, maybeAdvanced, f.Help)
		} else {
			fmt.Fprintf(w, "| `--%v` | %v | %v | %v%v |\n", f.Name, shortFlag, defaultValue, maybeAdvanced, f.Help)
		}
	}

	fmt.Fprintf(w, "\n")
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

	fmt.Fprintf(w, "| Argument | Help |\n")
	fmt.Fprintf(w, "| -------- | ---  |\n")

	args2 := append([]*kingpin.ArgModel(nil), args...)
	sort.Slice(args2, func(i, j int) bool {
		return args2[i].Name < args2[j].Name
	})

	for _, f := range args2 {
		fmt.Fprintf(w, "| `%v` | %v |\n", f.Name, f.Help)
	}

	fmt.Fprintf(w, "\n")
}

func generateAppFlags(app *kingpin.ApplicationModel) error {
	f, err := os.Create(filepath.Join(baseDir, "flags.md"))
	if err != nil {
		return errors.Wrap(err, "unable to create common flags file")
	}
	defer f.Close() //nolint:errcheck

	title := "Flags"
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
	dir := filepath.Join(baseDir, section)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, "_index.md"))
	if err != nil {
		return errors.Wrap(err, "unable to create common flags file")
	}
	defer f.Close() //nolint:errcheck

	title := section + " Commands"
	fmt.Fprintf(f, `---
title: %q
linkTitle: %q
weight: %v
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
			fmt.Fprintf(w, "\n### %v\n\n", sectionTitle)

			first = false
		}

		subcommandSlug := strings.Replace(c.FullCommand, " ", "-", -1)
		fmt.Fprintf(w, "* [`%v`](%v) - %v\n", c.FullCommand, subcommandSlug+"/", c.Help)
		generateSubcommandPage(filepath.Join(dir, subcommandSlug+".md"), c)
	}
}

func generateSubcommandPage(fname string, cmd *kingpin.CmdModel) {
	f, err := os.Create(fname)
	if err != nil {
		log.Fatalf("unable to create page: %v", err)
	}
	defer f.Close() //nolint:errcheck

	title := cmd.FullCommand
	fmt.Fprintf(f, `---
title: %q
linkTitle: %q
weight: 10
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

	fmt.Fprintf(f, "```shell\n$ kopia %v%v%v\n```\n\n", cmd.FullCommand, flagSummary, argSummary)
	fmt.Fprintf(f, "%v\n\n", cmd.Help)

	emitFlags(f, cmd.Flags)
	emitArgs(f, cmd.Args)
}

func main() {
	flag.Parse()

	app := cli.App().Model()

	if err := generateAppFlags(app); err != nil {
		log.Fatalf("unable to generate common flags: %v", err)
	}

	if err := generateCommands(app, commonSection, 5, false); err != nil {
		log.Fatalf("unable to generate common commands: %v", err)
	}

	if err := generateCommands(app, advancedSection, 6, true); err != nil {
		log.Fatalf("unable to generate advanced commands: %v", err)
	}
}
