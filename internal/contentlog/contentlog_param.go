package contentlog

// ParamWriter must be implemented by all types that write a parameter ("key":value)to the JSON writer.
type ParamWriter interface {
	WriteValueTo(sw *JSONWriter)
}
