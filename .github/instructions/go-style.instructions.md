---
applyTo: '**/*.go'
description: Go code style and conventions in Kopia codebase. Use when writing, reviewing, or refactoring Go code; designing Go packages; handling errors; writing code comments; or writing tests.

paths:
  - "**/*.go"

---

# Go code in Kopia

## General Instructions

- Write clear, simple, self-documenting code with descriptive names
- Favor clarity and simplicity over cleverness, follow the principle of least surprise
- Follow idiomatic Go practices and standards.
- Reduce nesting and indentation:
  - Prefer early return over if-else chains; use `if condition { return }` pattern to avoid else blocks
  - Keep the non-error path left-aligned
- Avoid introducing new dependencies
- Avoid using emoji in code, comments and documentation
- Close and release resources such as files, network connections and memory buffers

## Naming

- Use allowed ASCII for identifiers, avoid using non-ASCII characters in identifiers

### Package

- Lowercase, single-word package names
- Avoid underscores, hyphens, or mixedCaps
- Choose names that describe what the package provides, not what it contains
- Avoid generic names like `util`, `common`, or `base`
- Each `.go` file must have exactly one `package` line, never have multiple `package` declarations in a file

### Variables and Functions

- Use mixedCaps or MixedCaps (camelCase) rather than underscores
- Keep names short but descriptive
- Use single-letter variables only for short scopes (like loop indices)
- Exported names start with a capital letter
- Unexported names start with a lowercase letter
- Avoid using the same name for the package and a type, (e.g., avoid `http.HTTPServer`, prefer `http.Server`)

### Interfaces

- Name interfaces with -er suffix when possible (e.g., `Reader`, `Writer`, `Formatter`)
- Name single-method interfaces after method name (e.g., method `Read`, interface `Reader`)
- Keep interfaces small

### Constants

- Exported constants: MixedCaps starting with a capital letter
- Unexported constants mixedCaps starting with a lowercase letter
- Group related constants with `const` blocks
- Use typed constants for type safety

## Code Comments and Documentation

### Comments

- Write self-documenting code; prefer clear variable names, function names, and code structure over comments and write no comments by default
- Comments explain rationale, not how it is done unless the implementation is complex
- Write comments only when necessary to explain constraints, subtlety, complex implementation, non-obvious behavior, workarounds for specific bugs
- Write comments and documentation in English, avoid emoji
- Use line comments (`//`) for most comments
- Keep comments close to implementation

### Documentation

- Document meaning and usage of structs, interfaces and fields
- Document exported symbols with clear, concise explanations
- Document the invariants expected when calling a function, the change of state
  if any, and the expected state invariant when a function returns.
- Start documentation with symbol name
- Package comments start with "Package [name]"
- Document configuration options
- Examples when helpful
- Update documentation when code changes

## Type Safety and Language Features

- Prefer specific types or generic type parameters with constraints. If an unconstrained type is required, use `any` rather than `interface{}`
- Leverage and prefer the Go standard library when functionality exists instead of writing custom implementations (e.g., use `strings.Builder` for string concatenation, `filepath.Join` for path construction)
- Use `defer` for cleanup

### Type Definitions

- Define types to add meaning and type safety
- Prefer explicit type conversions
- Use type assertions carefully and check whether the assertion succeeds using the second return value
- Prefer generics over unconstrained types; when an unconstrained type is truly needed, use the predeclared alias `any` instead of `interface{}`
- Make the zero value useful

### Interfaces and Composition

- Accept interfaces, return concrete types
- Keep interfaces small (1-3 methods is ideal)
- Define interfaces close to where they're used, not where they're implemented
- Don't export interfaces unless necessary
- Use embedding for composition

### Package Organization

- Group related functionality into packages
- Put reusable packages in `internal/` if possible
- Avoid circular dependencies

### Pointers vs Values Parameters and Receivers

- Consistent receiver type, either pointer receivers or value receivers for all receiver functions of given type
- Pointer receivers for large structs or when modifying receiver, value receivers for small immutable structs
- Prefer value parameters over pointer parameters
- Pointer parameters when parameter large or need to modify argument
- Consider zero value when choosing pointer vs value arguments

### JSON APIs

- Use struct tags to specify lower `camelCase` field names in JSON marshaling
- Validate input data
- Use pointers for optional fields
- Consider using `json.RawMessage` for delayed parsing
- Handle JSON marshaling errors

## Error Handling Patterns

### Error Handling and Propagation

- Name error variables `err`
- Check errors immediately after function call
- Don't ignore errors using `_` unless valid reason (explain why)
- Don't log and return errors (choose one)
- Place error returns as the last return value
- Preserve error chains, wrap errors with context message using `errors.Wrap()`
- Error checking with `errors.Is` and `errors.As`

### Creating Errors

- Create and export sentinel error variables with `errors.New` when checking for specific errors is needed
- Error messages: lowercase, descriptive, don't end with punctuation
- Use structured errors with fields

### Error Lists and Multiple Errors

- Use `errors.Join()` for collecting multiple errors (nil-safe)
- Handle validation scenarios with error accumulation

## Concurrency

### Goroutines

- Use `sync.WaitGroup` or channels to wait for goroutines
- Avoid creating goroutines in libraries; prefer letting the caller control concurrency
- If you must create goroutines in libraries, provide clear documentation and cleanup mechanisms
- Avoid goroutine leaks by ensuring cleanup, determine how a goroutine will exit

### Channels

- Use channels to communicate between goroutines
- Use `select` for non-blocking operations
- Close channels from the sender side, not the receiver
- Don't communicate by sharing memory; share memory by communicating
- Use buffered channels when capacity is known upfront

### Synchronization

- Keep critical sections small
- Choose between channels and mutexes based on the use case: use channels for communication, mutexes for protecting state
- Use `sync.Mutex` for protecting shared state
- Use `sync.RWMutex` when there are many readers
- Use `sync.Once` for one-time initialization

## Memory Management

- Minimize allocations in hot paths
- Reuse objects when a large number of those are allocated (consider `sync.Pool`)
- Use value receivers for small structs
- Preallocate slices when size is known
- Avoid unnecessary string-byte conversions


## I/O: Readers and Buffers

- Most `io.Reader` streams are consumable once; reading advances state. Do not assume a reader can be re-read without special handling
- If you must read data multiple times, buffer it once and recreate readers on demand:
	- Use `io.ReadAll` (or a limited read) to obtain `[]byte`, then create fresh readers via `bytes.NewReader(buf)` or `bytes.NewBuffer(buf)` for each reuse
	- For strings, use `strings.NewReader(s)`;
	- `Seek(0, io.SeekStart)` rewinds `*bytes.Reader`
- For HTTP requests, do not reuse a consumed `req.Body`. Instead:
	- Keep the original payload as `[]byte` and set `req.Body = io.NopCloser(bytes.NewReader(buf))` before each send
	- Prefer configuring `req.GetBody` so the transport can recreate the body for redirects/retries: `req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil }`
- To duplicate a stream while reading, use `io.TeeReader` (copy to a buffer while passing through) or write to multiple sinks with `io.MultiWriter`
- Reusing buffered readers: call `(*bufio.Reader).Reset(r)` to attach to a new underlying reader; do not expect it to "rewind" unless the source supports seeking
- For large payloads, avoid unbounded buffering; consider streaming, `io.LimitReader`, or on-disk temporary storage to control memory

- Use `io.Pipe` to stream without buffering the whole payload:
	- Write to `*io.PipeWriter` in a separate goroutine while the reader consumes
	- Always close the writer; use `CloseWithError(err)` on failures
	- `io.Pipe` is for streaming, not rewinding or making readers reusable

- Warning: When using `io.Pipe` (especially with multipart writers), all writes must be performed in strict, sequential order. Do not write concurrently or out of order—multipart boundaries and chunk order must be preserved. Out-of-order or parallel writes can corrupt the stream and result in errors.

- Streaming multipart/form-data with `io.Pipe`:
	- `pr, pw := io.Pipe()`; `mw := multipart.NewWriter(pw)`; use `pr` as the HTTP request body
	- Set `Content-Type` to `mw.FormDataContentType()`
	- In a goroutine: write all parts to `mw` in the correct order; on error `pw.CloseWithError(err)`; on success `mw.Close()` then `pw.Close()`
	- Do not store request/in-flight form state on a long-lived client; build per call
	- Streamed bodies are not rewindable; for retries/redirects, buffer small payloads or provide `GetBody`

## Security Best Practices

### Input Validation

- Use strong typing to prevent invalid states
- Validate all external input
- Validate and escape data for different contexts (HTML, SQL, shell)
- Sanitize file paths from user input
- Sanitize input before SQL queries

### Cryptography

- Use standard library crypto packages
- Use crypto/rand for random number generation
- Use TLS for network communication
- Never store plain-text passwords
- Store password hashes using functions designed for password hashing, such as PBKDF2 and scrypt

## Constraints

- Use `clock.Now()` instead of `time.Now()`
- Use `timetrack.Timer.Elapsed()` instead of `time.Since()`
- No `filepath.IsAbs()` - use `ospath.IsAbs()` for Windows UNC support

## Pitfalls to Avoid

- Ignoring race conditions
- Creating goroutine leaks
- Modifying maps concurrently
- Using global variables unnecessarily
- Not considering the zero value of types
- Creating duplicate `package` declarations, this is a compile error; always check existing files before adding package declarations

## References

- Linter configuration: `.golangci.yml`
- https://go.dev/doc/effective_go
- https://go.dev/wiki/CodeReviewComments
- https://google.github.io/styleguide/go/guide
