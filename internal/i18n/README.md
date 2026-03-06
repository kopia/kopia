# Internationalization (i18n) for Kopia

This package provides internationalization support for Kopia CLI.

## Features

- Support for multiple languages
- Automatic language detection from environment variables
- Command-line flag for language selection (`--language`)
- Embedded locale files in binary
- Simple placeholder substitution for dynamic messages

## Supported Languages

- `en` - English (default)
- `ru` - Russian

## Usage

### Environment Variable

```bash
export KOPIA_LANGUAGE=ru
kopia snapshot list
```

### Command-line Flag

```bash
kopia --language=ru repository status
```

### Automatic Detection

The system automatically detects language from environment variables in this order:
1. `KOPIA_LANGUAGE`
2. `LC_ALL`
3. `LC_MESSAGES`
4. `LANG`

## Adding Translations

### For Translators

1. Edit the locale file in `internal/i18n/locales/<lang>.yaml`
2. Add translations in format: `"English text": "Translated text"`
3. Run tests: `go test ./internal/i18n/...`

### For Developers

Use the translator in your code:

```go
import "github.com/kopia/kopia/internal/i18n"

// Using global translator
msg := i18n.T("Create a new repository.")

// Using app translator
msg := app.T("Commands to manipulate repository.")
```

## Tools

### Extract Translations

Extract translatable strings from source code:

```bash
go run ./tools/extracttranslations -source cli -output internal/i18n/locales
```

### Apply Translations

Apply translations from a dictionary file:

```bash
go run ./tools/applytranslations -dict translations.txt -locale internal/i18n/locales
```

Dictionary format (English|Russian):
```
Create a new repository.|Создать новый репозиторий.
Connect to a repository.|Подключиться к репозиторию.
```

## File Format

Locale files use YAML format:

```yaml
"Create a new repository.": "Создать новый репозиторий."
"Path to the repository": "Путь к репозиторию"
```

## Testing

```bash
go test ./internal/i18n/...
```

## Contributing Translations

1. Create `internal/i18n/locales/<lang>.yaml` for your language
2. Add translations for all keys in `en.yaml`
3. Update `parseLanguage()` in `i18n.go` to recognize your language
4. Submit a pull request

## Architecture

```
internal/i18n/
├── i18n.go              # Core translation logic
├── i18n_test.go         # Tests
└── locales/
    ├── en.yaml          # English (base)
    └── ru.yaml          # Russian
```

## Known Limitations

- **kingpin**: Basic messages (usage, flags, args) remain in English as kingpin doesn't support i18n
- Command descriptions are set at initialization time, before translator is ready
- Full i18n support requires updating all CLI command files to use `svc.T()`

## License

Same as Kopia project.
