// Package scrubber contains helpers that remove sensitive information from Go structs before it's presented to users.
package scrubber

import (
	"reflect"
	"strings"
)

// ScrubSensitiveData returns a copy of a given value with sensitive fields scrubbed.
// Fields are marked as sensitive with truct field tag `kopia:"sensitive"`.
func ScrubSensitiveData(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Ptr:
		return ScrubSensitiveData(v.Elem()).Addr()

	case reflect.Struct:
		res := reflect.New(v.Type()).Elem()

		for i := 0; i < v.NumField(); i++ {
			fv := v.Field(i)

			sf := v.Type().Field(i)

			if sf.Tag.Get("kopia") == "sensitive" {
				if sf.Type.Kind() == reflect.String {
					res.Field(i).SetString(strings.Repeat("*", fv.Len()))
				}
			} else if sf.IsExported() {
				res.Field(i).Set(fv)
			}
		}

		return res

	default:
		panic("Unsupported type: " + v.String())
	}
}
