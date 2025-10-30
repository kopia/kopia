# Kopia Copilot Instructions

## When reviewing code, focus on:

### Security Critical Issues
- Check for hardcoded secrets, API keys, or credentials
- Verify proper input validation and sanitization
- Review authentication and authorization logic

### Code Quality Essentials
- Functions should be focused and appropriately sized
- Use clear, descriptive naming conventions
- Ensure proper error handling throughout

### Performance Issues
- Spot inefficient loops and algorithmic issues
- Check for memory leaks and resource cleanup
- Review caching opportunities for expensive operations

## Review Style
- Be specific and actionable in feedback
- Explain the rationale behind recommendations
- Acknowledge good patterns when you see them
- Ask clarifying questions when code intent is unclear

## Review Test Coverage
- Ensure there are tests that cover and exercise the new or changed functionality

Always prioritize security vulnerabilities and performance issues that could impact users.

Always suggest changes to improve readability.

## Repository Overview

Kopia is a fast and secure open-source backup/restore tool written in **Go** that creates encrypted snapshots and saves them to remote  storage. The repository is approximately 15MB with ~1,000 Go files.

**Key Technologies:**
- **Backend:** Go (primary language)
- **Build System:** GNU Make with cross-platform support (Windows/Linux/macOS/ARM)
- **UI:** React-based HTML UI (embedded via go:embed, source at github.com/kopia/htmlui)
- **Desktop App:** Electron-based KopiaUI wrapper
- **Website:** Hugo static site generator

## Build Commands

### Setup (Required Before Building)
```bash
make -j4 ci-setup
```
**Time:** ~30-60 seconds
**What it does:** Downloads Go modules, installs build tools (gotestsum, golangci-lint, hugo, node), and installs npm dependencies for the app. **ALWAYS run this after cloning or when build tools are missing.**

## Linting

**Run linter:**
```bash
make lint
```
**Time:** ~3-4 minutes
**Linter:** golangci-lint with timeout of 1200s
**Config:** `.golangci.yml` (extensive configuration with 40+ enabled linters)

**Auto-fix linting issues:**
```bash
make lint-fix
```

**Check code locks:**
```bash
make check-locks
```
**Note:** Not available on linux/arm64 or linux/arm.

**Check JavaScript/TypeScript formatting (in app directory):**
```bash
make check-prettier
```

**Important:** Linting is **NOT** run on linux/arm64 or linux/arm platforms to avoid issues.

### Building Kopia CLI

**Build without UI (faster for testing):**
```bash
make install-noui
```
**Output:** `~/go/bin/kopia`
**Time:** ~5-10 seconds
**Use this for:** Testing CLI changes that don't involve the UI.

**Race detector build:**
```bash
make install-race
```
**Use this for:** Debugging race conditions.

**Full build with embedded HTML UI:**
```bash
make install
```
**Output:** `~/go/bin/kopia`
**Time:** ~10-20 seconds
**Note:** Embeds HTML UI from github.com/kopia/htmluibuild dependency.

### Building KopiaUI Desktop App

**Prerequisites:** Must build kopia CLI first (creates embedded binary).

```bash
make kopia-ui
```
**Output:** `dist/kopia-ui/` directory with platform-specific installers
**Time:** ~2-5 minutes
**Note:** Only works on amd64 architectures. On Linux, may require xvfb for headless testing.

## Testing

### Unit Tests (Standard)
```bash
make test
```
**Time:** ~2-4 minutes
**Runs:** All unit tests with gotestsum, excludes TestIndexBlobManagerStress
**Timeout:** 1200s (20 minutes) per test
**Format:** pkgname-and-test-fails

### Unit Tests with Coverage
```bash
make test-with-coverage
```
**Output:** `coverage.txt`
**Time:** ~3-5 minutes
**Note:** Used by Code Coverage workflow. Sets KOPIA_COVERAGE_TEST=1 and GOEXPERIMENT=nocoverageredesign.

### Index Blob Tests (Separate)
```bash
make test-index-blob-v0
```
**Runs:** TestIndexBlobManagerStress (excluded from standard tests due to duration)

### Integration Tests
```bash
make build-integration-test-binary  # Build test binary first
make integration-tests
```
**Time:** ~5-10 minutes
**Requires:** KOPIA_INTEGRATION_EXE environment variable

### CI Test Suites
```bash
make ci-tests  # Runs: vet + test
make ci-integration-tests  # Runs: robustness-tool-tests + socket-activation-tests
```

### Provider Tests (Cloud Storage)
```bash
make provider-tests PROVIDER_TEST_TARGET=...
```
**Time:** 15 minutes timeout
**Requires:** KOPIA_PROVIDER_TEST=true, credentials for storage backend, rclone binary
**Note:** Tests various cloud storage providers (S3, Azure, GCS, etc.)

### Other Test Types
- `make compat-tests` - Compatibility tests with older Kopia versions
- `make endurance-tests` - Long-running endurance tests (1 hour timeout)
- `make robustness-tests` - Robustness testing with FIO
- `make stress-test` - Stress tests (1 hour timeout)
- `make htmlui-e2e-test` - HTML UI end-to-end tests (10 minutes timeout)

**Race Detector Tests:**
```bash
make test UNIT_TEST_RACE_FLAGS=-race UNIT_TESTS_TIMEOUT=1200s
```

## Common Issues & Workarounds

### Build Issues

1. **Missing build tools error:** Always run `make -j4 ci-setup` first after cloning.

2. **Go version mismatch:** Kopia requires the Go toolchain with the version specified in go.mod. The `go-version-file` is used in GitHub Actions.

3. **Platform-specific builds:**
   - macOS: Creates universal binaries (AMD64 + ARM64) with `lipo`
   - Windows: Requires chocolatey packages: make, zip, unzip, curl
   - Linux ARM: Uses goreleaser for multi-arch builds on AMD64 host

4. **KopiaUI build failures on ARM:** KopiaUI (Electron app) only builds on amd64. The build is skipped on ARM architectures.

5. **Linting on ARM:** Linting and check-locks are skipped on linux/arm64 and linux/arm due to tool compatibility.

### Test Issues

1. **Test timeouts:** Default unit test timeout is 1200s (20 minutes). For race detector tests, explicitly set `UNIT_TESTS_TIMEOUT=1200s`.

2. **Parallel execution:** Tests use `-parallel` flag (8 on amd64, 2 on ARM). Adjust with `PARALLEL` variable if needed.

3. **Integration test binary:** Must build integration test binary explicitly with `make build-integration-test-binary` before running integration tests.

4. **Provider tests require environment:** Provider tests need KOPIA_PROVIDER_TEST=true and rclone binary available.

### Environment Variables

**Important variables for CI/tests:**
- `UNIX_SHELL_ON_WINDOWS=true` - Required for Windows builds
- `KOPIA_COVERAGE_TEST=1` - Enable coverage testing
- `KOPIA_INTEGRATION_EXE` - Path to integration test binary
- `TESTING_ACTION_EXE` - Path to testing action binary
- `KOPIA_PROVIDER_TEST=true` - Enable provider tests
- `RCLONE_EXE` - Path to rclone binary for provider tests

## Project Structure

### Root Directory Files
- `main.go` - Entry point, uses kingpin for CLI parsing
- `Makefile` - Primary build system (GNU Make)
- `go.mod` / `go.sum` - Go module dependencies
- `.golangci.yml` - Linter configuration (extensive rules)
- `.gitignore` - Excludes dist/, .tools/, node_modules/, coverage files
- `BUILD.md` - Detailed build architecture documentation
- `README.md` - General project information

### Source Directories

**`cli/`** - CLI command implementations (~200 command files)
- Each command is in a separate file (e.g., `command_snapshot_create.go`)
- Uses kingpin v2 for command-line parsing
- Main entry via `app.go`

**`repo/`** - Repository management and storage backends
- `repo/blob/` - Storage provider implementations (s3, azure, gcs, filesystem, etc.)
- `repo/content/` - Content-addressable storage layer
- `repo/format/` - Repository format and versioning
- `repo/manifest/` - Manifest management
- `repo/object/` - Object storage layer

**`fs/`** - Filesystem abstraction layer
- `fs/localfs/` - Local filesystem implementation
- Supports snapshots, restore, and filesystem walking

**`snapshot/`** - Snapshot creation and management
- `snapshot/snapshotmaintenance/` - Snapshot GC and maintenance
- `snapshot/upload/` - Upload logic and parallelization

**`internal/`** - Internal packages (74 subdirectories)
- Utilities and shared code not for external use
- Examples: cache, crypto, compression, auth, server, etc.

**`tests/`** - Integration and end-to-end tests
- `tests/end_to_end_test/` - E2E test scenarios
- `tests/robustness/` - Robustness testing framework
- `tests/tools/` - Test utilities and helpers

**`tools/`** - Build and release tools
- `tools/gettool/` - Tool downloader (downloads versioned binaries)
- Various publishing scripts (apt, rpm, docker, homebrew)
- `tools/.tools/` - Downloaded build tools (gitignored)

**`app/`** - Electron-based desktop application (KopiaUI)
- Node.js project with package.json
- Uses Electron Builder for packaging
- Resources in `app/resources/` and `app/public/`
- Embeds kopia server binary from `dist/kopia_*/kopia`

**`site/`** - Hugo-based website (kopia.io)
- Build with `make -C site build` or `make website`
- Auto-generates CLI docs to `site/content/docs/Reference/Command-Line/`
- Development server: `make -C site server` (http://localhost:1313)

### Configuration Files

- `.golangci.yml` - Linter config with 40+ enabled linters, custom rules
- `.codecov.yml` - Code coverage reporting config
- `.goreleaser.yml` - Release automation config
- `.github/workflows/*.yml` - GitHub Actions workflows (19 workflow files)

## GitHub Actions Workflows

### Pull Request Checks (Always Run)

1. **make.yml (Build)** - Builds on Windows/Linux/macOS/ARM
   - Runs: `make ci-setup` → `make ci-build`
   - Timeout: 40 minutes
   - Creates artifacts: binaries, installers, packages

2. **tests.yml** - Unit and integration tests on all platforms
   - Runs: `make ci-setup` → `make test-index-blob-v0` → `make ci-tests` → `make ci-integration-tests`
   - Uploads logs to artifacts

3. **lint.yml** - Linting on ubuntu-latest and macos-latest
   - Runs: `make lint` → `make check-locks` → `make check-prettier`
   - Includes govulncheck for vulnerability scanning

4. **code-coverage.yml** - Code coverage on ubuntu-latest
   - Runs: `make test-with-coverage`
   - Uploads to Codecov

5. **race-detector.yml** - Race condition detection
   - Runs: `make test UNIT_TEST_RACE_FLAGS=-race UNIT_TESTS_TIMEOUT=1200s`

### Additional Workflows
- `providers-core.yml` / `providers-extra.yml` - Cloud storage provider tests
- `compat-test.yml` - Compatibility with older Kopia versions
- `stress-test.yml` - Stress testing
- `endurance-test.yml` - Long-running endurance tests
- `license-check.yml` - License compliance checking
- `dependency-review.yml` - Dependency security review
- `check-pr-title.yml` - PR title format validation

### Workflow Tips
- **Build artifacts** are uploaded and can be downloaded from workflow runs
- **Logs** are uploaded to `.logs/**/*.log` on test failures
- **Concurrency:** All workflows use `cancel-in-progress: true` for the same ref
- **Scheduling:** Some workflows run weekly (Mondays at 8AM)

## Development Workflow

### Making Code Changes

1. **Setup environment:**
   ```bash
   make -j4 ci-setup
   ```

2. **Make your changes** to Go files

3. **Build and test iteratively:**
   ```bash
   make install-noui  # Fast build without UI
   ~/go/bin/kopia --help  # Test your changes
   ```

4. **Lint your changes:**
   ```bash
   make lint
   ```

5. **Run relevant tests:**
   ```bash
   make test  # Unit tests
   ```

6. **For HTML UI changes:**
   - UI source is in separate repo: github.com/kopia/htmlui
   - Pre-built UI imported from: github.com/kopia/htmluibuild
   - To test local UI changes: `make install-with-local-htmlui-changes` (requires 3 repos checked out side-by-side)

### Pre-Commit Checklist
- [ ] `make lint` passes (3-4 minutes)
- [ ] `make test` passes (2-4 minutes)
- [ ] Changes are formatted (gofumpt, gci enabled in linter)
- [ ] New packages: License check with `make license-check`

### Code Style
- Uses golangci-lint with formatters: gci, gofumpt
- Imports organized: standard, default, localmodule
- No `time.Now()` outside clock/timetrack packages - use `clock.Now()`
- No `time.Since()` - use `timetrack.Timer.Elapsed()`
- No `filepath.IsAbs()` - use `ospath.IsAbs()` for Windows UNC support
- Tests use the `stretchr/testify` packages

## Key Dependencies

**Go modules (from go.mod):**
- Cloud providers: Azure SDK, AWS SDK (via minio), Google Cloud Storage
- CLI: alecthomas/kingpin/v2
- Compression: klauspost/compress, klauspost/pgzip
- Testing: stretchr/testify, chromedp (for E2E)
- Observability: Prometheus client, OpenTelemetry
- UI: github.com/kopia/htmluibuild (pre-built React app)

**Node.js dependencies (app/package.json):**
- electron-builder - Desktop app packaging
- electron-updater - Auto-updates
- React (via htmluibuild) - UI framework

## Important Notes

1. **Do not modify go.mod/go.sum manually** - Use `go get` to update dependencies. CI runs `git checkout go.mod go.sum` after ci-setup to revert local changes from tool downloads.

2. **Build artifacts in dist/** - Gitignored. Contains platform-specific binaries and installers after `make ci-build` or `make goreleaser`.

3. **Tools directory** - `tools/.tools/` is gitignored and populated by `make ci-setup`. Contains downloaded versions of gotestsum, linter, node, hugo, etc.

4. **HTML UI is separate** - The HTML UI is maintained in github.com/kopia/htmlui and imported as a pre-built module. Don't try to find UI source in this repo.

5. **Platform differences:**
   - macOS: Creates universal binaries, requires Xcode command line tools
   - Windows: Requires chocolatey tools, uses PowerShell for some commands
   - Linux ARM: Builds via goreleaser on AMD64 host, creates ARM packages

6. **Parallelism:** Makefiles use `-j4` for parallel execution. Tests use `-parallel 8` on amd64, `-parallel 2` on ARM.

7. **Test binary paths:**
   - Integration: `dist/testing_$(GOOS)_$(GOARCH)/kopia.exe`
   - UI embedded: `dist/kopia_$(GOOS)_$(GOARCH)/kopia` (or universal binary on macOS)

8. **Timeout configuration:**
   - Linter: 1200s (20 minutes)
   - Unit tests: 1200s (20 minutes)
   - Integration tests: 300s (5 minutes)
   - Provider tests: 15 minutes
   - Stress/endurance: 3600s (1 hour)

9. **Required tools installed by ci-setup:**
   - gotestsum - Test runner with better output
   - golangci-lint - Linter
   - node - JavaScript runtime for app builds
   - hugo - Static site generator for website

10. **Trust these instructions** - These instructions have been validated by running all commands. Only search for additional information if something fails or if these instructions are incomplete or incorrect.
