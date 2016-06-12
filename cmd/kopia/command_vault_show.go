package main

import (
	"bytes"
	"encoding/json"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	vaultShowCommand   = vaultCommands.Command("show", "Show contents of a vault item")
	vaultShowID        = vaultShowCommand.Arg("id", "ID of the vault item to show").String()
	vaultShowJSON      = vaultShowCommand.Flag("json", "Pretty-print JSON").Short('j').Bool()
	vaultShowNoNewLine = vaultShowCommand.Flag("nonewline", "Do not emit newline").Short('n').Bool()
)

func init() {
	vaultShowCommand.Action(showVaultObject)
}

func showVaultObject(context *kingpin.ParseContext) error {
	v, err := openVault()
	if err != nil {
		return err
	}

	b, err := v.Get(*vaultShowID)
	if err != nil {
		return err
	}

	if *vaultShowJSON {
		var buf bytes.Buffer
		json.Indent(&buf, b, "", "  ")
		buf.WriteTo(os.Stdout)
	} else {
		os.Stdout.Write(b)
	}

	if !*vaultShowNoNewLine {
		os.Stdout.WriteString("\n")
	}

	return nil
}
