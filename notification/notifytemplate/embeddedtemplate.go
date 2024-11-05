// Package notifytemplate provides a way to access notification templates.
package notifytemplate

import (
	"embed"
	"slices"
	"sort"
	"text/template"

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

// Functions is a map of functions that can be used in templates.
//
//nolint:gochecknoglobals
var Functions = template.FuncMap{
	"bytes": units.BytesString[int64],
	"sortSnapshotManifestsByName": func(man []*notifydata.ManifestWithError) []*notifydata.ManifestWithError {
		res := slices.Clone(man)
		sort.Slice(res, func(i, j int) bool {
			return res[i].Source.String() < res[j].Source.String()
		})
		return res
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
