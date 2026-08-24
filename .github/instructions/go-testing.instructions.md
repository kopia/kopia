---
applyTo: '**/*.go'
description: Go code conventions in Kopia codebase. Use when writing, reviewing, or refactoring Go tests.

paths:
  - "**/*.go"

---

## Testing

### Test Organization

- Name test files with `_test.go` suffix
- Keep tests in the same package for white-box testing and use `_test` package name suffix for black-box testing
- Place test files next to the code they test

### Writing Tests

- Use table-driven tests for multiple test cases
- Name tests descriptively using `Test_functionName_scenario`
- Use subtests with `t.Run` for better organization
- Test both success and error cases
- Have separate top-level tests for the success and error cases
- Use `stretchr/testify/require` package for checking expected results

### Test Helpers

- Mark helper functions with `t.Helper()`
- Create test fixtures for complex setup
- Use `testing.TB` interface for functions used in tests and benchmarks
- Clean up resources using `t.Cleanup()`
