// Package notifytemplate provides a way to access notification templates.
package notifytemplate

import (
	"embed"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"text/template"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/notification/notifydata"
)

//go:embed "*.html"
//go:embed "*.txt"
var embedded embed.FS

// Template names.
const (
	TestNotification = "test-notification"
)

// Options provides options for template rendering.
type Options struct {
	Timezone   *time.Location
	TimeFormat string
}

func formatCount(v int64) string {
	return strconv.FormatInt(v, 10)
}

// functions is a map of functions that can be used in templates.
func functions(opt Options) template.FuncMap {
	if opt.Timezone == nil {
		opt.Timezone = time.Local
	}

	if opt.TimeFormat == "" {
		opt.TimeFormat = time.RFC1123Z
	}

	return template.FuncMap{
		"bytes":       units.BytesString[int64],
		"formatCount": formatCount,
		"bytesDelta": func(v int64) string {
			switch {
			case v == 0:
				return ""
			case v > 0:
				return " (+" + units.BytesString(v) + ")"
			default:
				return " (-" + units.BytesString(-v) + ")"
			}
		},
		"bytesDeltaHTML": func(v int64) string {
			switch {
			case v == 0:
				return ""
			case v > 0:
				return " <span class='increase'>(&#x2191; " + units.BytesString(v) + ")</span>"
			default:
				return " <span class='decrease'>(&#x2193; " + units.BytesString(-v) + ")</span>"
			}
		},
		"countDelta": func(v int64) string {
			switch {
			case v == 0:
				return ""
			case v > 0:
				return fmt.Sprintf(" (+%v)", formatCount(v))
			default:
				return fmt.Sprintf(" (-%v)", formatCount(-v))
			}
		},
		"countDeltaHTML": func(v int64) string {
			switch {
			case v == 0:
				return ""
			case v > 0:
				return fmt.Sprintf(" <span class='increase'>(&#x2191; %v)</span>", formatCount(v))
			default:
				return fmt.Sprintf(" <span class='decrease'>(&#x2193; %v)</span>", formatCount(-v))
			}
		},
		"sortSnapshotManifestsByName": func(man []*notifydata.ManifestWithError) []*notifydata.ManifestWithError {
			res := slices.Clone(man)
			sort.Slice(res, func(i, j int) bool {
				return res[i].Manifest.Source.String() < res[j].Manifest.Source.String()
			})
			return res
		},
		"formatTime": func(t time.Time) string {
			return t.In(opt.Timezone).Format(opt.TimeFormat)
		},
	}
}

// DefaultOptions is the default set of options.
//
//nolint:gochecknoglobals
var DefaultOptions = Options{}

// GetEmbeddedTemplate returns embedded template by name.
func GetEmbeddedTemplate(templateName string) (string, error) {
	b, err := embedded.ReadFile(templateName)
	if err != nil {
		return "", errors.Wrap(err, "unable to read embedded template")
	}

	return string(b), nil
}

// SupportedTemplates returns a list of supported template names.
func SupportedTemplates() []string {
	var s []string

	entries, _ := embedded.ReadDir(".")

	for _, e := range entries {
		s = append(s, e.Name())
	}

	return s
}

// ParseTemplate parses a named template.
func ParseTemplate(tmpl string, opt Options) (*template.Template, error) {
	//nolint:wrapcheck
	return template.New("template").Funcs(functions(opt)).Parse(tmpl)
}
