package fio

import "strings"

// Config structures the fields of a FIO job run configuration.
type Config []Job

// String implements the stringer interface, formats the Config
// as it would appear in a well-formed fio config file.
func (cfg Config) String() string {
	ret := make([]string, 0, len(cfg))
	for _, job := range cfg {
		ret = append(ret, job.String())
	}

	return strings.Join(ret, "\n")
}
