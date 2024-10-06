// Package notifytemplate provides a way to access notification templates.
package notifytemplate

import (
	"embed"
	"text/template"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

//go:embed "*.html"
//go:embed "*.txt"
var embedded embed.FS

// Template names.
const (
	TestNotification = "test-notification"
)

// Functions is a map of functions that can be used in templates.
//
//nolint:gochecknoglobals
var Functions = template.FuncMap{
	"toTime": func(t any) time.Time {
		if t, ok := t.(time.Time); ok {
			return t
		}

		if t, ok := t.(fs.UTCTimestamp); ok {
			return t.ToTime()
		}

		return time.Time{}
	},
}

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
func ParseTemplate(tmpl string) (*template.Template, error) {
	//nolint:wrapcheck
	return template.New("template").Funcs(Functions).Parse(tmpl)
}
