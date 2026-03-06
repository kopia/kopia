// Package i18n provides internationalization support for Kopia CLI.
package i18n

import (
	"embed"
	"fmt"
	"os"
	"strings"
	"sync"

	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

//go:embed locales/*.yaml
var localeFiles embed.FS

// GlobalTranslator is the global translator instance for the application.
var (
	GlobalTranslator *Translator
	translatorMu     sync.RWMutex
)

// Translator provides translation functionality.
type Translator struct {
	lang     language.Tag
	messages map[string]string
}

// SetGlobalTranslator sets the global translator instance.
func SetGlobalTranslator(t *Translator) {
	translatorMu.Lock()
	defer translatorMu.Unlock()
	GlobalTranslator = t
}

// GetGlobalTranslator returns the global translator instance.
func GetGlobalTranslator() *Translator {
	translatorMu.RLock()
	defer translatorMu.RUnlock()
	if GlobalTranslator == nil {
		// Return English translator as default
		t, _ := NewTranslator("en")
		return t
	}
	return GlobalTranslator
}

// T is a convenience function that translates using the global translator.
func T(msgID string, args ...any) string {
	return GetGlobalTranslator().Translate(msgID, args...)
}

// NewTranslator creates a new translator for the specified language.
func NewTranslator(lang string) (*Translator, error) {
	tag, err := parseLanguage(lang)
	if err != nil {
		return nil, err
	}

	messages, err := loadMessages(tag)
	if err != nil {
		return nil, err
	}

	return &Translator{
		lang:     tag,
		messages: messages,
	}, nil
}

// parseLanguage parses a language string into a language.Tag.
func parseLanguage(lang string) (language.Tag, error) {
	if lang == "" || lang == "en" || lang == "en-US" {
		return language.English, nil
	}

	if lang == "ru" || lang == "ru-RU" {
		return language.Russian, nil
	}

	// Try to parse the language tag
	tag, err := language.Parse(lang)
	if err != nil {
		return language.English, fmt.Errorf("invalid language tag: %w", err)
	}

	return tag, nil
}

// loadMessages loads messages from the embedded locale files.
func loadMessages(tag language.Tag) (map[string]string, error) {
	messages := make(map[string]string)

	// Determine the locale file to load
	lang := tag.String()

	// Try specific locale first, then base language
	filesToTry := []string{}
	if strings.HasPrefix(lang, "ru_") || strings.HasPrefix(lang, "ru-") || lang == "ru" {
		filesToTry = []string{"locales/ru.yaml"}
	} else {
		filesToTry = []string{"locales/en.yaml"}
	}

	for _, file := range filesToTry {
		data, err := localeFiles.ReadFile(file)
		if err != nil {
			continue
		}

		var localeData map[string]string
		if err := yaml.Unmarshal(data, &localeData); err != nil {
			return nil, fmt.Errorf("error parsing locale file %s: %w", file, err)
		}

		for k, v := range localeData {
			messages[k] = v
		}
	}

	return messages, nil
}

// Translate translates a message ID to the current language.
// If no translation is found, returns the default English message.
func (t *Translator) Translate(msgID string, args ...any) string {
	msg, ok := t.messages[msgID]
	if !ok {
		// Return the message ID as fallback (usually English)
		return msgID
	}

	if len(args) == 0 {
		return msg
	}

	// Simple placeholder replacement: {0}, {1}, etc.
	for i, arg := range args {
		placeholder := fmt.Sprintf("{%d}", i)
		msg = strings.ReplaceAll(msg, placeholder, fmt.Sprintf("%v", arg))
	}

	return msg
}

// T is a shorthand for Translate.
func (t *Translator) T(msgID string, args ...any) string {
	return t.Translate(msgID, args...)
}

// GetLanguage returns the current language tag.
func (t *Translator) GetLanguage() language.Tag {
	return t.lang
}

// DetectLanguageFromEnv detects language from environment variables.
func DetectLanguageFromEnv() string {
	// Check KOPIA_LANGUAGE environment variable
	if lang := os.Getenv("KOPIA_LANGUAGE"); lang != "" {
		return lang
	}

	// Check LC_ALL
	if lang := os.Getenv("LC_ALL"); lang != "" {
		return lang
	}

	// Check LC_MESSAGES
	if lang := os.Getenv("LC_MESSAGES"); lang != "" {
		return lang
	}

	// Check LANG
	if lang := os.Getenv("LANG"); lang != "" {
		// Extract language part (e.g., "ru_RU.UTF-8" -> "ru")
		parts := strings.Split(lang, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}

	// Default to English
	return "en"
}
