package policy

// OptionalBool provides convenience methods for manipulating optional booleans.
type OptionalBool bool

// OrDefault returns the value of the boolean or provided default if it's nil.
func (b *OptionalBool) OrDefault(def bool) bool {
	if b == nil {
		return def
	}

	return bool(*b)
}

func newOptionalBool(b OptionalBool) *OptionalBool {
	return &b
}
