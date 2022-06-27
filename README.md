Kopia
=====
//
![Kopia](icons/kopia.svg)
[![Build Status](https://github.com/kopia/kopia/workflows/Build/badge.svg)](https://github.com/kopia/kopia/actions?query=workflow%3ABuild)
[![Slack](https://img.shields.io/badge/discuss-slack-blue.svg)](https://slack.kopia.io/) 
[![GoDoc](https://godoc.org/github.com/kopia/kopia/repo?status.svg)](https://godoc.org/github.com/kopia/kopia/repo)
[![Coverage Status](https://codecov.io/gh/kopia/kopia/branch/master/graph/badge.svg?token=CRK4RMRFSH)](https://codecov.io/gh/kopia/kopia)[![Go Report Card](https://goreportcard.com/badge/github.com/kopia/kopia)](https://goreportcard.com/report/github.com/kopia/kopia)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

> _n._
>
> 1. _[copy, replica](https://en.wikipedia.org/wiki/Replica) (Polish)_
> 2. _[lance, spear](https://en.wikipedia.org/wiki/Kopia)_
> 3. _[fast and secure backup tool](https://kopia.io)_


Kopia is a simple, cross-platform tool for managing encrypted backups in the cloud. It provides fast, incremental backups, secure, client-side end-to-end encryption, compression and data deduplication.

Unlike other cloud backup solutions, the user is in full control of the backup storage and responsible for purchasing one of the cloud storage products (such as [Google Cloud Storage](https://cloud.google.com/storage/)), which offer great durability and availability for the data.

Kopia in action
---

Using `kopia` command line tool:

[![asciicast](https://asciinema.org/a/ykx6uzEhKY3451fWEnX9nm9uo.svg)](https://asciinema.org/a/ykx6uzEhKY3451fWEnX9nm9uo)

Kopia UI - experimental user interface

[![Kopia UI Tutorial](https://img.youtube.com/vi/sHJjSpasWIo/0.jpg)](https://www.youtube.com/watch?v=sHJjSpasWIo)

Getting Started
---
See [Documentation](https://kopia.io/docs/) for more information.

Building Kopia
---
See [Build Infrastructure](BUILD.md) for more information on building Kopia and working with the source code.

Licensing
---
Kopia is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full license text.

Contribution Guidelines
---

Kopia is open source and contributions are welcome. For more information on how to contribute see the [Contribution Guidelines](https://kopia.io/docs/contribution-guidelines/).

Reporting Security Issues
---
If you find a security issue you'd like to disclose privately, please contact `kopia-pmc@googlegroups.com` or via direct message to maintainers on [Slack](https://slack.kopia.io).


Disclaimer
---

Kopia is a personal project and is not affiliated with, supported or endorsed by Google.

Cryptography Notice
---

  This distribution includes cryptographic software. The country in
  which you currently reside may have restrictions on the import,
  possession, use, and/or re-export to another country, of encryption
  software. BEFORE using any encryption software, please check your
  country's laws, regulations and policies concerning the import,
  possession, or use, and re-export of encryption software, to see if
  this is permitted. See <http://www.wassenaar.org/> for more
  information.

  The U.S. Government Department of Commerce, Bureau of Industry and
  Security (BIS), has classified this software as Export Commodity
  Control Number (ECCN) 5D002.C.1, which includes information security
  software using or performing cryptographic functions with symmetric
  algorithms. The form and manner of this distribution makes it
  eligible for export under the License Exception ENC Technology
  Software Unrestricted (TSU) exception (see the BIS Export
  Administration Regulations, Section 740.13) for both object code and
  source code.

[![Netlify Status](https://api.netlify.com/api/v1/badges/6b5c1fe4-a0da-4e7e-939b-ff1105251985/deploy-status)](https://app.netlify.com/sites/kopia/deploys)
