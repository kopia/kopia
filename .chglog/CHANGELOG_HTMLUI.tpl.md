{{ range .Versions }}
### Graphical User Interface

{{ range .CommitGroups -}}
{{ range .Commits -}}
* {{ if eq .Type "feat" }}**New Feature** {{ end }}{{ .Subject }} by {{ .Author.Name}}
{{ end -}}
{{ end -}}
{{ end -}}