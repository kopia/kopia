package blob

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/kopia/kopia/internal/crypto"
)

var (
	errConfigMustBePointerToStruct = errors.New("config must be a pointer to a struct")
	errErrorGeneratingSalt         = errors.New("error generating salt")
	errInvalidEncryptedData        = errors.New("invalid encrypted data")
)

// To maintain backwards compatibility, we tag encrypted data with "enc:".
const (
	encryptionPrefix    = "enc:"
	saltSize            = 16
	encryptionKeyLength = 32
)

var sensitiveFieldEncryptionKeyPurpose = []byte("sensitive-field-encryption")

func deriveEncryptionKey(password string) ([]byte, error) {
	key, err := crypto.DeriveKeyFromMasterKey([]byte(password), nil, sensitiveFieldEncryptionKeyPurpose, encryptionKeyLength)
	if err != nil {
		return nil, fmt.Errorf("deriving encryption key: %w", err)
	}

	return key, nil
}

// EncryptSensitiveFields iterates over the fields of a struct and encrypts the ones tagged as "sensitive".
func EncryptSensitiveFields(config any, password, storageType string) error {
	// If storage type is rclone, encryption is not impelmented currently. This would add an extra level of complexity.
	if storageType == "rclone" {
		return nil
	}

	// Ensure config is a pointer to a struct.
	if reflect.TypeOf(config).Kind() != reflect.Ptr {
		return errConfigMustBePointerToStruct
	}

	// Derive the encryption key from the provided password.
	encryptionKey, err := deriveEncryptionKey(password)
	if err != nil {
		return err
	}

	// Get the reflect.Value and reflect.Type of the struct.
	val := reflect.ValueOf(config).Elem()
	typ := val.Type()

	// Iterate over each field in the struct.
	for i := range val.NumField() {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Check if the field has the "sensitive" tag.
		sensitiveTag := fieldType.Tag.Get("kopia")
		if sensitiveTag == "sensitive" {
			if err := encryptSensitiveField(field, encryptionKey); err != nil {
				return err
			}
		}
	}

	return nil
}

func encryptSensitiveField(field reflect.Value, encryptionKey []byte) error {
	// Handle different field kinds.
	switch field.Kind() {
	case reflect.String:
		// Get the plaintext string value.
		plaintext := field.String()
		// If plaintext is empty or already encrypted, skip.
		if plaintext == "" || strings.HasPrefix(plaintext, encryptionPrefix) {
			return nil
		}

		// Encrypt the string.
		encrypted, err := encryptString(plaintext, encryptionKey)
		if err != nil {
			return err
		}

		// Set the field with the encrypted string.
		field.SetString(encrypted)

	case reflect.Slice:
		// Check if it's a byte slice (like json.RawMessage).
		if field.Type().Elem().Kind() == reflect.Uint8 { // json.RawMessage is []byte
			// Get the plaintext byte slice.
			plaintext := field.Bytes()
			// If plaintext is empty or already encrypted, skip.
			if len(plaintext) == 0 || strings.HasPrefix(string(plaintext), encryptionPrefix) {
				return nil
			}

			// Encrypt the string representation of the byte slice.
			encrypted, err := encryptString(string(plaintext), encryptionKey)
			if err != nil {
				return err
			}

			// Set the field with the encrypted byte slice.
			field.SetBytes([]byte(encrypted))
		}
	default:
		// Ignore other types
	}

	return nil
}

// DecryptSensitiveFields iterates over the fields of a struct and decrypts the ones tagged as "sensitive".
func DecryptSensitiveFields(config any, password, storageType string) error {
	// If storage type is rclone, decryption is not implemented.
	if storageType == "rclone" {
		return nil
	}

	// Ensure config is a pointer to a struct.
	if reflect.TypeOf(config).Kind() != reflect.Ptr {
		return errConfigMustBePointerToStruct
	}

	// Derive the encryption key from the provided password.
	encryptionKey, err := deriveEncryptionKey(password)
	if err != nil {
		return err
	}

	// Get the reflect.Value and reflect.Type of the struct.
	val := reflect.ValueOf(config).Elem()
	typ := val.Type()

	// Iterate over each field in the struct.
	for i := range val.NumField() {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Check if the field has the "sensitive" tag.
		sensitiveTag := fieldType.Tag.Get("kopia")
		if sensitiveTag == "sensitive" {
			if err := decryptSensitiveField(field, encryptionKey); err != nil {
				return err
			}
		}
	}

	return nil
}

func decryptSensitiveField(field reflect.Value, encryptionKey []byte) error {
	// Handle different field kinds.
	switch field.Kind() {
	case reflect.String:
		// Get the encrypted string value.
		encrypted := field.String()
		// If the string is not prefixed with encryptionPrefix, skip.
		if !strings.HasPrefix(encrypted, encryptionPrefix) {
			return nil
		}

		// Decrypt the string.
		decrypted, err := decryptString(encrypted, encryptionKey)
		if err != nil {
			return err
		}

		// Set the field with the decrypted string.
		field.SetString(decrypted)
	case reflect.Slice:
		// Check if it's a byte slice (like json.RawMessage).
		if field.Type().Elem().Kind() == reflect.Uint8 { // json.RawMessage is []byte
			// Get the encrypted byte slice.
			encrypted := field.Bytes()
			// If the byte slice is empty or not prefixed with encryptionPrefix, skip.
			if len(encrypted) == 0 || !strings.HasPrefix(string(encrypted), encryptionPrefix) {
				return nil
			}

			// Decrypt the string representation of the byte slice.
			decrypted, decryptionErr := decryptString(string(encrypted), encryptionKey)
			if decryptionErr != nil {
				// If decryption fails, it might be a legitimate JSON.
				// We can't be sure, so we leave it as is.
				//nolint:nilerr
				return nil
			}

			// check if decrypted is valid json
			var js json.RawMessage

			_ = json.Unmarshal([]byte(decrypted), &js) // Attempt to unmarshal, but ignore error
			field.SetBytes([]byte(decrypted))
		}
	default:
		// Ignore other types
	}

	return nil
}

func encryptString(plaintext string, encryptionKey []byte) (string, error) {
	// Generate a random salt for encryption.
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", errErrorGeneratingSalt
	}

	// Encrypt the plaintext using AES-256 GCM with the derived key and salt.
	encrypted, err := crypto.EncryptAes256Gcm([]byte(plaintext), encryptionKey, salt)
	if err != nil {
		return "", fmt.Errorf("encrypting string: %w", err)
	}

	// Prepend the encryption prefix, base64 encode the salt and encrypted data, and return.
	combined := make([]byte, 0, saltSize+len(encrypted))
	combined = append(combined, salt...)
	combined = append(combined, encrypted...)

	return encryptionPrefix + base64.StdEncoding.EncodeToString(combined), nil
}

func decryptString(encrypted string, encryptionKey []byte) (string, error) {
	// Decode the base64 string after removing the encryption prefix.
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encrypted, encryptionPrefix))
	if err != nil {
		return "", fmt.Errorf("decoding base64: %w", err)
	}

	// Check if the decoded data is long enough to contain the salt.
	if len(decoded) < saltSize {
		return "", errInvalidEncryptedData
	}

	// Extract the salt and the encrypted data.
	salt := decoded[:saltSize]
	data := decoded[saltSize:]

	// Decrypt the data using AES-256 GCM with the derived key and extracted salt.
	decrypted, err := crypto.DecryptAes256Gcm(data, encryptionKey, salt)
	if err != nil {
		return "", fmt.Errorf("decrypting string: %w", err)
	}

	// Return the decrypted string.
	return string(decrypted), nil
}
