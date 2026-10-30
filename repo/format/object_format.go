package format

// ObjectFormat describes the format of objects in a repository.
type ObjectFormat struct {
	Splitter string `json:"splitter,omitempty"` // splitter used to break objects into pieces of content
}
