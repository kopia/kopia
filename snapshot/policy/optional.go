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

// NewOptionalBool provides an OptionalBool pointer.
func NewOptionalBool(b OptionalBool) *OptionalBool {
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

// OptionalInt64 provides convenience methods for manipulating optional integers.
type OptionalInt64 int64

// OrDefault returns the value of the integer or provided default if it's nil.
func (b *OptionalInt64) OrDefault(def int64) int64 {
	if b == nil {
		return def
	}

	return int64(*b)
}

func newOptionalInt64(b OptionalInt64) *OptionalInt64 {
	return &b
}
