package cli

import (
	"log"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app              = kingpin.New("kopia", "Kopia - Online Backup").Author("http://kopia.github.io/")
	appLogTimestamps *bool
)

func init() {
	appLogTimestamps = app.Flag("log-timestamps", "Log timestamps").Hidden().Action(enableLogTimestamps).Bool()
}

// App returns an instance of command-line application object.
func App() *kingpin.Application {
	return app
}

func enableLogTimestamps(context *kingpin.ParseContext) error {
	if *appLogTimestamps {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetFlags(0)
	}
	return nil
}
