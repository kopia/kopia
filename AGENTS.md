# AGENTS.md

## Commit Message Format

Commit titles must match the format validated by `.github/workflows/check-pr-title.yml`:

```
<type>(<scope>)!: <description>
```

- `type` must be one of: `feat`, `fix`, `breaking`, `build`, `chore`, `docs`, `style`, `refactor`, `test`
- `scope` must be one of: `ci`, `cli`, `deps-dev`, `deps`, `general`, `infra`, `kopiaui`, `lint`, `notifications`, `providers`, `repository`, `server`, `site`, `snapshots`, `testing`, `ui`
- `!` before the colon is optional and marks a breaking change
- The description starts with a lowercase letter and is not sentence-case

Pick the scope from the area of the change:

| Scope | Applies to |
| --- | --- |
| `cli` | `cli/` |
| `repository` | `repo/` (blob layer, repository logic) |
| `providers` | storage provider implementations under `repo/blob/*` provider packages (azure, s3, gcs, ...), `kopia/` provider config |
| `server` | `server/` |
| `snapshots` | `snapshot/`, `fs/` |
| `testing` | `tests/`, test infrastructure |
| `ui` | `kopia-ui/` |
| `kopiaui` | embedded htmlui |
| `site` | `site/` (documentation site) |
| `notifications` | `notification/` |
| `deps` | runtime dependency updates (`go.mod`) |
| `deps-dev` | dev dependency updates |
| `ci` / `infra` | `.github/`, `Makefile`, build tooling |
| `lint` | lint configuration |
| `general` | anything not covered above |

Examples:

```
refactor(repository): remove Azure SDK dependency for Locked retention mode
fix(cli): reject negative values for --parallel flags
build(deps): bump the common-golang-dependencies group with 10 updates
```
