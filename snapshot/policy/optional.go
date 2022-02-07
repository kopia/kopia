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

// OptionalInt provides convenience methods for manipulating optional integers.
type OptionalInt int

// OrDefault returns the value of the integer or provided default if it's nil.
func (b *OptionalInt) OrDefault(def int) int {
	if b == nil {
		return def
	}

	return int(*b)
}

func newOptionalInt(b OptionalInt) *OptionalInt {
	return &b
}
