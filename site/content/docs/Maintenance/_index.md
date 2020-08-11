---
title: "Maintenance"
linkTitle: "Maintenance"
weight: 45
---

Kopia repositories require periodic maintenance to ensure best possible performance and optimal storage usage.

Starting with v0.6.0 the repository maintenance is automatic and will happen occasionally when `kopia` command-line client is used. This document describes maintenance functionality in greater detail.

## Maintenance Tasks

Kopia uses the following types of maintenance tasks:

* **Quick Maintenance Tasks** are primarily responsible for keeping the number of frequently accessed blobs (`q` and `n`) low to ensure good performance.

  Quick Maintenance will never delete any metadata from the repository without ensuring that another copy of the same metadata exists. Quick Maintenance Tasks are enabled by default and will execute approximately every hour. 
  
  While the user can disable quick maintenance, it's not recommended as it will lead to reduced performance.

* **Full Maintenance Tasks** are responsible for keeping the repository compact and eliminate deleted files that the user no longer wishes to store.

  The most important task is Snapshot GC which marks for deletion all contents that are no longer reachable from any of the active snapshots. Full Maintenance is also responsible for compaction of data pack blobs (`p`) after contents stored in them have been deleted. In the current version of Kopia, the full maintenance tasks must be run manually, but in future versions they will be scheduled to run automatically.

## Maintenance Task Ownership

For correctness reasons, Kopia requires that no more than one instance of certain maintenance operations runs at any given time. To achieve that, one repository `user@hostname` is designated as the Maintenance Owner. Other repository users will not attempt to run maintenance automatically and the designated user will attempt to do so after holding an exclusive lock.

To see the current maintenance owner use `kopia maintenance info` command:

```
$ kopia maintenance info
Owner: root@myhost
```

To change the maintenance owner to either current user or another user use `kopia maintenance set` command:

```
$ kopia maintenance set --owner=me
$ kopia maintenance set --owner=another@somehost
```

## Maintenance Task Scheduling

To enable or disable quick or full maintenance:

```
$ kopia maintenance set --enable-quick true
$ kopia maintenance set --enable-full true
```

To change the quick or full maintenance interval:

```
$ kopia maintenance set --quick-interval=2h
$ kopia maintenance set --full-interval=8h
```

It is also possible to pause quick or full maintenance for some time so that it automatically resumes after specified time elapses. To change the quick or full maintenance for some time use:

```
$ kopia maintenance set --pause-quick=48h
$ kopia maintenance set --pause-full=268h
```

## Manually Running Maintenance

To run maintenance manually use `kopia maintenance run`:

```
# quick maintenance
$ kopia maintenance run

# full maintenance
$ kopia maintenance run --full
```

The current user must be the maintenance owner.

## Viewing Maintenance History

To view the history of maintenance operations use `kopia maintenance info`, which will display the history of last 5 maintenance runs.

