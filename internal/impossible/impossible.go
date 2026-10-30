// Package impossible provides PanicOnError which panics on impossible conditions.
package impossible

// PanicOnError panics if the err != nil.
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
