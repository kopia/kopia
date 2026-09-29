---
applyTo: '**/*.go,/go.{mod,sum}'
description: Development instructions for Go code in Kopia. Use when writing, or refactoring Go code; designing Go packages; writing code comments; or writing tests.
excludeAgent: "code-review"

---

## Package Declaration Rules

- When editing an existing `.go` file:
  - Preserve the existing `package` declaration - do not add another one
- When creating a new `.go` file:
  - Before writing any code, check what package name other `.go` files in the same directory use, use the SAME package name as existing files in that directory, if it is a new directory, use the directory name as the package name
  - Write exactly one `package [name]` line at top of file
- When using file creation or replacement tools:
  - Always verify the target file doesn't already have a `package` declaration before adding one


## GitHub Actions Workflows

- Location: `.github/workflows/*.yml`
- Build artifacts are uploaded and can be downloaded from workflow runs
- Logs are uploaded to `.logs/**/*.log` on test failures
- Concurrency: All workflows use `cancel-in-progress: true` for the same ref

### Pull Request Checks (Always Run)

- `make.yml`: Builds on Windows/Linux/macOS/ARM
   - Runs: `make ci-setup`; `make ci-build`
   - Creates artifacts: binaries, installers, packages

- `tests.yml`: Unit and integration tests on all platforms
   - Runs: `make ci-setup`; `make test-index-blob-v0`; `make ci-tests`; `make ci-integration-tests`
   - Uploads logs to artifacts

- `lint.yml` - Linting on ubuntu-latest and macos-latest
   - Runs: `make lint`; `make check-locks`; `make check-prettier`
   - Includes govulncheck for vulnerability scanning

- `race-detector.yml` - Race condition detection
   - Runs: `make test UNIT_TEST_RACE_FLAGS=-race UNIT_TESTS_TIMEOUT=1200s`

### Additional Workflows

- `compat-test.yml` - Compatibility with older Kopia versions
- `stress-test.yml` - Stress testing
- `endurance-test.yml` - Long-running endurance tests
- `license-check.yml` - License compliance checking
- `dependency-review.yml` - Dependency security review
- `check-pr-title.yml` - PR title format validation

### Timeout configuration

- Linter: 1200s (20 minutes)
- Unit tests: 1200s (20 minutes)
- Integration tests: 300s (5 minutes)
- Stress/endurance: 3600s (1 hour)

## Other Instructions

- Do not modify anything under `site/` (including `site/go.mod` and `site/go.sum`)
