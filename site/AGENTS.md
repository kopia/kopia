Directory `site/` has project website files

- Do not modify any file in this directory or its sub-directories
- Never delete nor modify the `go.mod` and `go.sum` files
- Build with `make -C site build` or `make website`
- Generates CLI docs to `site/content/docs/Reference/Command-Line/`
- Development server: `make -C site server` (http://localhost:1313)
