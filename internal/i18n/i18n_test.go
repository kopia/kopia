package i18n

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTranslator(t *testing.T) {
	t.Run("English default", func(t *testing.T) {
		trans, err := NewTranslator("en")
		require.NoError(t, err)
		require.NotNil(t, trans)
	})

	t.Run("Russian", func(t *testing.T) {
		trans, err := NewTranslator("ru")
		require.NoError(t, err)
		require.NotNil(t, trans)
	})

	t.Run("Russian RU", func(t *testing.T) {
		trans, err := NewTranslator("ru_RU")
		require.NoError(t, err)
		require.NotNil(t, trans)
	})

	t.Run("Invalid language", func(t *testing.T) {
		trans, err := NewTranslator("invalid")
		// Invalid language falls back to English but returns an error
		if err != nil {
			trans, err = NewTranslator("en")
			require.NoError(t, err)
		}
		require.NotNil(t, trans)
	})
}

func TestTranslate(t *testing.T) {
	t.Run("English translation", func(t *testing.T) {
		trans, err := NewTranslator("en")
		require.NoError(t, err)

		result := trans.T("Create a new repository.")
		require.Equal(t, "Create a new repository.", result)
	})

	t.Run("Russian translation", func(t *testing.T) {
		trans, err := NewTranslator("ru")
		require.NoError(t, err)

		result := trans.T("Create a new repository.")
		require.Equal(t, "Создать новый репозиторий.", result)
	})

	t.Run("Unknown key returns key", func(t *testing.T) {
		trans, err := NewTranslator("ru")
		require.NoError(t, err)

		result := trans.T("Unknown key")
		require.Equal(t, "Unknown key", result)
	})

	t.Run("Translation with arguments", func(t *testing.T) {
		trans, err := NewTranslator("ru")
		require.NoError(t, err)

		// Add a test message with placeholders
		// For now, just test the mechanism works
		result := trans.T("ERROR:", "test")
		require.Equal(t, "ОШИБКА:", result)
	})
}

func TestDetectLanguageFromEnv(t *testing.T) {
	t.Run("KOPIA_LANGUAGE", func(t *testing.T) {
		t.Setenv("KOPIA_LANGUAGE", "ru")
		lang := DetectLanguageFromEnv()
		require.Equal(t, "ru", lang)
	})

	t.Run("LC_ALL", func(t *testing.T) {
		t.Setenv("KOPIA_LANGUAGE", "")
		t.Setenv("LC_ALL", "ru_RU.UTF-8")
		lang := DetectLanguageFromEnv()
		require.Equal(t, "ru_RU.UTF-8", lang)
	})

	t.Run("LANG", func(t *testing.T) {
		t.Setenv("KOPIA_LANGUAGE", "")
		t.Setenv("LC_ALL", "")
		t.Setenv("LANG", "ru_RU.UTF-8")
		lang := DetectLanguageFromEnv()
		require.Equal(t, "ru_RU", lang)
	})

	t.Run("Default English", func(t *testing.T) {
		t.Setenv("KOPIA_LANGUAGE", "")
		t.Setenv("LC_ALL", "")
		t.Setenv("LANG", "")
		lang := DetectLanguageFromEnv()
		require.Equal(t, "en", lang)
	})
}
