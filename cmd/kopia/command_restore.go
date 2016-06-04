package main

import kingpin "gopkg.in/alecthomas/kingpin.v2"

var (
	restoreCommand = app.Command("restore", "Restore contents of a file or directory.")

	restoreCommandLong = restoreCommand.Flag("recursive", "Copy contents of directory recursively").Short('r').Bool()
	restoreCommandPath = restoreCommand.Arg("paths", "Paths to restore").Required().Strings()
)

func runRestoreCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()
	mgr, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	_ = mgr

	// oid, err := ParseObjectID(*restoreCommandPath, vlt)
	// if err != nil {
	// 	return err
	// }
	// r, err := mgr.Open(oid)
	// if err != nil {
	// 	return err
	// }

	// var prefix string
	// if !*restoreCommandLong {
	// 	prefix = *restoreCommandPath
	// 	if !strings.HasSuffix(prefix, "/") {
	// 		prefix += "/"
	// 	}
	// }

	// dir, err := fs.ReadDirectory(r, "")
	// if err != nil {
	// 	return fmt.Errorf("unable to read directory contents")
	// }

	// if *restoreCommandLong {
	// 	listDirectory(dir)
	// } else {
	// 	for _, e := range dir {
	// 		var suffix string
	// 		if e.FileMode.IsDir() {
	// 			suffix = "/"
	// 		}

	// 		fmt.Println(prefix + e.Name + suffix)
	// 	}
	// }

	return nil
}

func init() {
	restoreCommand.Action(runRestoreCommand)
}
