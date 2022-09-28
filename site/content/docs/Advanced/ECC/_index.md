---
title: "Error Correction Algorithm"
linkTitle: "Error Correction Algorithm"
weight: 13
---

Starting with v0.12.0, Kopia supports the use of error correction using the Reed-Solomon error correction algorithm. Error correction in Kopia is used to mitigate the likelihood that your snapshots become corrupt due to storage errors caused by the hardware your snapshots are saved on (like bitflips). Most, if not all, cloud storage platforms use their own error correction, so using Kopia's error correction for cloud repositories may be overkill. However, the choice is yours.

If you use error correction in Kopia, the `Error Correction Overhead` option controls how much extra storage space is needed for the error correction code. So, for example, if you pick the `1%` option, then your repository will use 1% more storage space with error correction enabled compared to without error correction.

Currently, if you want to use Reed-Solomon error correction with Kopia, you must create a new repository and enable the option when you create the new repository, because there is not yet a way to enable the feature for an existing repository.

**This feature is currently experimental.** Use at your own risk. You can read more about how the Reed-Solomon algorithm works at [Wikipedia](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction).
