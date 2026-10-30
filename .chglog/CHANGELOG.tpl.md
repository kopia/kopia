{{ range .Versions }}
<a name="{{ .Tag.Name }}"></a>

{{ range .CommitGroups -}}
### {{ .Title }}

{{ range .Commits -}}
* {{ if eq .Type "feat" }}**New Feature** {{ end }}{{ .Subject }} by {{ .Author.Name}}
{{ end }}
{{ end -}}

{{- if .RevertCommits -}}
### Reverts

{{ range .RevertCommits -}}
* {{ .Revert.Header }}
{{ end }}
{{ end -}}

{{- if .MergeCommits -}}
### Pull Requests

{{ range .MergeCommits -}}
* {{ .Header }}
{{ end }}
{{ end -}}

{{- if .NoteGroups -}}
{{ range .NoteGroups -}}
### {{ .Title }}

{{ range .Notes }}
{{ .Body }}
{{ end }}
{{ end -}}
{{ end -}}
{{ end -}}