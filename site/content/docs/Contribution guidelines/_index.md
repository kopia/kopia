---
title: "Contribution Guidelines"
linkTitle: "Contribution Guidelines"
toc_hide: true
---

> NOTE: Those guidelines are preliminary and will change as the project grows and expands in scope.

## Contacting Developers

* Using [Slack](https://slack.kopia.io) is the quickest way to get in touch with developers.

## Submitting issues

* If you find a bug or have a feature request, please submit an issue in the main Kopia project at https://github.com/kopia/kopia/issues.
* To keep all issues in one central location, please do not create issues directly in the `kopia/htmlui` (or other auxiliary) project.

## Security issues

* If you find a security issue that you want to disclose privately, please contact `security@kopia.io` or send a direct message on Slack.

## Submitting code via Pull Requests

* We follow the [Github Pull Request Model](https://help.github.com/en/articles/about-pull-requests) for all contributions.
* For large bodies of work, we recommend creating an issue and labelling it `design` outlining the feature that you wish to build, and describing how it will be implemented. This gives a chance for review to happen early, and ensures no wasted effort occurs.
* For new features, documentation must be included.
* Once review has occurred, please rebase your PR down to a single commit. This will ensure a nice clean Git history.
* Pull Requests must go through a number of CI checks before being approved. Before submitting a PR, it's a good idea to run some of these checks locally:
  - `make ci-tests`
  - `make lint-all` (if developing cross-platform code)
  - `make goreleaser`
* When creating a PR, the title should match the pattern `<type>`(`<domain>`): `<description>` where:
  - `<type>` is one of: `feat`, `fix`, `breaking`, `build`, `chore`, `docs`, `style`, `refactor`, `test`.
  - `<domain>` is one of: `kopiaui`, `cli`, `ui`, `repository`, `snapshots`, `server`, `providers`, `deps`, `deps-dev`, `site`, `ci`, `infra`, `general`.
  - `<description>` is a clear description of a PR.
  - Follow the pattern precisely, as the title-checker cares about capitalization parentheses, and spaces.
  - For example: `feat(cli): Add new policy rule --new-feature-x to enable using feature x`.
