// Package secrets keeps user-supplied secrets.
package secrets

// Context manager to hold globally accessible EncryptedToken.

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
)

type secretKey string

const secretCtxKey secretKey = "secretManager"

// Manager holds a context for the EncryptedToken.
type secretsManager struct {
	Token *EncryptedToken
}

// WithManager returns a derived context with associated manager.
func WithManager(ctx context.Context) context.Context {
	mgr := secretsManager{}
	return context.WithValue(ctx, secretCtxKey, &mgr)
}

// EvaluateSecrets will use reflect to locate all *Secret items within the 'search' interface and evaluate each value.
func EvaluateSecrets(ctx context.Context, search interface{}, encryptedToken *EncryptedToken, password string) error {
	mgr := secretsManagerFromContext(ctx)
	if mgr == nil {
		return errors.New("Context does not contain a secretsManager")
	}

	if encryptedToken != nil {
		mgr.Token = encryptedToken
	} else {
		token, err := CreateToken(password)
		if err != nil {
			return err
		}
		mgr.Token = token
	}

	found := make(chan *Secret)

	go wrapFindSecretElements(search, found)

	for secret := range found {
		secret.Evaluate(mgr.Token, password) //nolint:errcheck
	}

	return nil
}

// GetToken will return the EncryptedToken from the context manager.
func GetToken(ctx context.Context, password string) *EncryptedToken {
	mgr := secretsManagerFromContext(ctx)
	if mgr == nil {
		return nil
	}

	if mgr.Token == nil && password != "" {
		token, err := CreateToken(password)
		if err == nil {
			mgr.Token = token
		}
	}

	return mgr.Token
}

func secretsManagerFromContext(ctx context.Context) *secretsManager {
	v := ctx.Value(secretCtxKey)
	if v == nil {
		return nil
	}

	return v.(*secretsManager) //nolint:forcetypeassert
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
			field := value.Field(i).Interface()
			findSecretElements(field, found)
		}
	case reflect.Slice:
		value := reflect.ValueOf(data)

		for i := 0; i < value.Len(); i++ {
			element := value.Index(i).Interface()
			findSecretElements(element, found)
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
