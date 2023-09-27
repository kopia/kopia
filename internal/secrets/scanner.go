// Package secrets keeps user-supplied secrets.
package secrets

// Context manager to hold globally accessible EncryptedToken.

import (
	"reflect"
)

// EvaluateSecrets will use reflect to locate all *Secret items within the 'search' interface and evaluate each value.
func EvaluateSecrets(search interface{}, signingKeyPtr **EncryptedToken, password string) error {
	var signingKey *EncryptedToken

	if *signingKeyPtr != nil {
		signingKey = *signingKeyPtr
	} else {
		signingKey = NewSigningKey(DefaultAlgorithm, DefaultKeyDerivation)
		*signingKeyPtr = signingKey
	}

	found := make(chan *Secret)

	go wrapFindSecretElements(search, found)

	for secret := range found {
		if err := secret.Evaluate(signingKey, password); err != nil {
			return err
		}
	}

	return nil
}

func wrapFindSecretElements(data interface{}, found chan *Secret) {
	findSecretElements(data, found)
	close(found)
}

func findSecretElements(data interface{}, found chan *Secret) {
	switch d := data.(type) {
	case *Secret:
		found <- d
		return
	default:
		break
	}

	dataType := reflect.TypeOf(data)

	switch dataType.Kind() {
	case reflect.Struct:
		value := reflect.ValueOf(data)
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if field.CanInterface() {
				findSecretElements(field.Interface(), found)
			}
		}
	case reflect.Slice:
		value := reflect.ValueOf(data)

		for i := 0; i < value.Len(); i++ {
			element := value.Index(i)
			if element.CanInterface() {
				findSecretElements(element.Interface(), found)
			}
		}
	case reflect.Ptr:
		value := reflect.ValueOf(data)
		data1 := value.Elem()

		if data1.IsValid() && data1.CanInterface() {
			findSecretElements(data1.Interface(), found)
		}
	default: // ok
	}
}
