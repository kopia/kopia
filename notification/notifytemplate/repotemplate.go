package notifytemplate

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

// ManifestType is the manifest type of notification templates.
const ManifestType = "notificationTemplate"

const templateNameKey = "template"

// TemplateManifest is the manifest of a notification template.
type TemplateManifest struct {
	Template string `json:"template"`
}

// Info returns information about single notification template.
type Info struct {
	Name         string     `json:"name"`
	LastModified *time.Time `json:"lastModified,omitempty"`
	IsBuiltIn    bool       `json:"isBuiltIn,omitempty"`
}

// ResolveTemplate resolves a named template from the repository by looking for most-specific defined override
// and falling back to generic embedded template.
func ResolveTemplate(ctx context.Context, rep repo.Repository, profileName, baseTemplateName, extension string) (string, error) {
	candidates := []string{
		profileName + "." + baseTemplateName + "." + extension,
		baseTemplateName + "." + extension,
	}

	for _, c := range candidates {
		t, found, err := GetTemplate(ctx, rep, c)
		if err != nil {
			return "", errors.Wrap(err, "unable to get notification template")
		}

		if found {
			return t, nil
		}
	}

	return GetEmbeddedTemplate(baseTemplateName + "." + extension)
}

// GetTemplate returns a named template from the repository.
func GetTemplate(ctx context.Context, rep repo.Repository, templateName string) (tmpl string, found bool, err error) {
	manifests, err := rep.FindManifests(ctx, labelsFor(templateName))
	if err != nil {
		return "", false, errors.Wrap(err, "unable to find notification template overrides")
	}

	if len(manifests) > 0 {
		var tm TemplateManifest

		if _, err := rep.GetManifest(ctx, manifest.PickLatestID(manifests), &tm); err != nil {
			return "", false, errors.Wrap(err, "unable to get notification template override")
		}

		return tm.Template, true, nil
	}

	return "", false, nil
}

// ListTemplates returns a list of templates.
func ListTemplates(ctx context.Context, rep repo.Repository, prefix string) ([]Info, error) {
	infos := map[string]Info{}

	for _, t := range SupportedTemplates() {
		if !strings.HasPrefix(t, prefix) {
			continue
		}

		infos[t] = Info{
			Name:      t,
			IsBuiltIn: true,
		}
	}

	manifests, err := rep.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: ManifestType,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list notification templates")
	}

	for _, m := range manifests {
		name := m.Labels[templateNameKey]
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		infos[name] = Info{
			Name:         name,
			IsBuiltIn:    false,
			LastModified: &m.ModTime,
		}
	}

	return maps.Values(infos), nil
}

// SetTemplate saves a template in the repository.
func SetTemplate(ctx context.Context, rep repo.RepositoryWriter, templateName, templateText string) error {
	_, err := rep.ReplaceManifests(ctx, labelsFor(templateName), &TemplateManifest{Template: templateText})

	return errors.Wrap(err, "unable to save notification template")
}

// ResetTemplate removes a template override from the repository.
func ResetTemplate(ctx context.Context, rep repo.RepositoryWriter, templateName string) error {
	entries, err := rep.FindManifests(ctx, labelsFor(templateName))
	if err != nil {
		return errors.Wrap(err, "unable to find notification template overrides")
	}

	for _, e := range entries {
		if err := rep.DeleteManifest(ctx, e.ID); err != nil {
			return errors.Wrap(err, "unable to delete notification template override")
		}
	}

	return nil
}

func labelsFor(templateName string) map[string]string {
	return map[string]string{
		manifest.TypeLabelKey: ManifestType,
		templateNameKey:       templateName,
	}
}
