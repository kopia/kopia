---
applyTo: '**/*.go,/go.{mod,sum}'
description: Development instructions for Go code in Kopia. Use when writing, or refactoring Go code; designing Go packages; writing code comments; or writing tests.
excludeAgent: "code-review"

paths:
  - "**/*.go"
  - "go.{mod,sum}"

---

## Development

- Primary platform: Linux
- Building requires Go toolchain with the version specified in `go.mod` file
- Do not modify `.gitignore` files

### Code Navigation

ALWAYS use `gopls` LSP tool for Go symbol lookups: definitions, references, implementations, callers. Do NOT use grep for Go symbols; `gopls` is accurate where grep produces false positives and misses generated code.
- `gopls` is a deferred tool: call `ToolSearch` with `query: "select:LSP"` to load its schema before the first use in a session. - Available operations:
   - `goToDefinition`
   - `findReferences`
   - `goToImplementation`
   - `incomingCalls`
   - `outgoingCalls`

### Making Code Changes

1. Set up environment  after cloning the git repository with `make -j4 ci-setup`; this installs `golangci-lint` linter, `gotestsum`, and other required tools

2. Make changes to Go files

3. Build and Lint iteratively
    Duration: ~3-7 minutes
    ```bash
    # Fast build without UI
    make install-noui
    ~/go/bin/kopia --help
    make lint
    ```

4. Unit tests for changes: `make -j4 test`

### Testing

- `make check-locks`
- Race detector test
   ```bash
   make install-race
   make test UNIT_TEST_RACE_FLAGS=-race UNIT_TESTS_TIMEOUT=1200s
   ```
- Integration tests:
    ```bash
    # Build integration test executable
    make build-integration-test-binary
    # Set KOPIA_INTEGRATION_EXE with the path to the kopia executable to use in integration tests
    KOPIA_INTEGRATION_EXE=<path_to_integration_executable> make integration-tests
    ```
- `make endurance-tests` - Long-running endurance tests (1 hour timeout)
- `make stress-test` - Stress tests (1 hour timeout)
- `make compat-tests` - Compatibility tests with older Kopia versions
- `make htmlui-e2e-test` - HTML UI end-to-end tests (10 minutes timeout)

### Dependency Management

- Avoid introducing new dependencies
- Use Go modules, manage dependencies with `go get` and `go mod tidy`

### Formatting

- Indent with tabs
- Keep line length reasonable, consider readability
- Add blank lines to separate logical groups of code, adhering to the linter constraints
- Ensure the file ends with a trailing newline `\n`
- Format with `gofumpt`
- Sort `import` with `goimports`; order: standard, default, localmodule
- Linter: `golangci-lint`,  configuration: `.golangci.yml`, run with: `make lint`
- Auto-fix linting issues: `make lint-fix`

### Git Commits

- Keep commits focused and atomic
- Write meaningful and descriptive commit messages
- Do not commit executables or binary artifacts to git
- Create an initial empty commit with title 'plan(ai): summary' and include the task summary in the commit message

#### Pre-Commit Checks

- Changes are formatted with `gofumpt`
- `make lint vet` passes (3-4 minutes)
- `make test` passes (2-4 minutes)
- License check: `make license-check`
- Review diffs
