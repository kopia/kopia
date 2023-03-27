---
title: Compatibility
linkTitle: Compatibility
weight: 70
---

## Compatibility

Kopia uses [semantic versioning](https://semver.org).

In order to ensure that snapshots created with Kopia will be available as the project evolves, starting with `v0.3.0` release Kopia is offering the following compatibility promise:

1. Each version of Kopia will be able to read snapshots created using current and **at least one previous version** of the software. The *previous version* is to be interpreted as:

  - for releases with major version == `v0` (i.e. `v0.x.y`), *previous version* means previous *minor version* (`v0.(x-1).*`)

  - for releases with major version == `v1`, *previous version* is the *last minor release* of the `v0` major version.

  - for releases with major version >= `v2` (i.e. `vx.y.z`), *previous version* means previous *major* version (`v(x-1)`).

2. While not explicitly guaranteed, it is possible and likely that Kopia will be able to read (but not necessarily write) snapshots created with even older versions of software than explicitly guaranteed. For example, it's likely that Kopia `v0.6.0` will read snapshots created using `v0.3.0`, even though it's three releases behind.

3. In order to avoid corrupting data, Kopia will refuse to mutate repositories it's not designed to safely handle. That means, it will typically only allow reading, but not writing older versions of repository, unless explicitly documented and tested.

4. Kopia may support for writing old repository formats on a best-effort basis.

  - For example Kopia `v0.4.x` may write using repository format created by `v0.3.x` in a way that `v0.3.x` versions will continue to understand it.

5. Each version of Kopia will offer migration mechanism to bring old readable repository format to the current format and thus enable full read-write operation.

6. Kopia will never upgrade old repository format to a new version without explicit human action.

