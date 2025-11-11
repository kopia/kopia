// Package editor encapsulates working with external text editor.
package editor

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("editor")

// EditLoop launches OS-specific editor (VI, notepad.exe or another editor configured through environment variables)
// It creates a temporary file with 'initial' contents and repeatedly invokes the editor until the provided 'parse' function
// returns nil result indicating success. The 'parse' function is passed the contents of edited files without # line comments.
func EditLoop(ctx context.Context, fname, initial string, withComments bool, parse func(updated string) error) error {
	tmpDir, err := os.MkdirTemp("", "kopia")
	if err != nil {
		return errors.Wrap(err, "unable to create temp directory")
	}

	tmpFile := filepath.Join(tmpDir, fname)
	defer os.RemoveAll(tmpDir) //nolint:errcheck

	//nolint:mnd
	if err := os.WriteFile(tmpFile, []byte(initial), 0o600); err != nil {
		return errors.Wrap(err, "unable to write file to edit")
	}

	for {
		if err := EditFile(ctx, tmpFile); err != nil {
			return errors.Wrap(err, "error launching editor")
		}

		txt, err := readAndStripComments(tmpFile, withComments)
		if err != nil {
			return errors.Wrap(err, "error parsing edited file")
		}

		err = parse(txt)
		if err == nil {
			return nil
		}

		log(ctx).Errorf("%v", err)
		fmt.Print("Reopen editor to fix? (Y/n) ")

		var shouldReopen string

		_, _ = fmt.Scanf("%s", &shouldReopen)

		if strings.HasPrefix(strings.ToLower(shouldReopen), "n") {
			return errors.New("aborted")
		}
	}
}

func readAndStripComments(fname string, withComments bool) (string, error) {
	if !withComments {
		b, err := os.ReadFile(fname) //nolint:gosec

		return string(b), errors.Wrap(err, "error reading file")
	}

	f, err := os.Open(fname) //nolint:gosec
	if err != nil {
		return "", errors.Wrap(err, "error opening edited file")
	}
	defer f.Close() //nolint:errcheck

	var result []string

	s := bufio.NewScanner(f)
	for s.Scan() {
		l := s.Text()

		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}

		if l != "" {
			result = append(result, l)
		}
	}

	return strings.Join(result, "\n"), nil
}

// EditFile launches the OS-specific editor (VI, notepad.exe or another editor configured through environment variables)
// to edit the specified file and waits for it to complete.
//
//nolint:gochecknoglobals
var EditFile = func(ctx context.Context, file string) error {
	editor, editorArgs := getEditorCommand()

	var args []string
	args = append(args, editorArgs...)
	args = append(args, file)

	cmd := exec.Command(editor, args...) //nolint:gosec
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout

	log(ctx).Debugf("launching editor %q on file %q", editor, file)

	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, "error running editor command")
	}

	return nil
}

func getEditorCommand() (cmd string, args []string) {
	editor := os.Getenv("VISUAL")
	if editor == "" {
		editor = os.Getenv("EDITOR")
	}

	if editor != "" {
		return parseEditor(editor)
	}

	if runtime.GOOS == "windows" {
		return "notepad.exe", nil
	}

	return "vi", nil
}

func parseEditor(s string) (cmd string, args []string) {
	// quoted editor path
	if s[0] == '"' {
		p := strings.Index(s[1:], "\"")
		if p == -1 {
			// invalid
			return s, nil
		}

		return s[1 : p+1], strings.Split(strings.TrimSpace(s[p+1:]), " ")
	}

	parts := strings.Split(s, " ")

	return parts[0], parts[1:]
}
