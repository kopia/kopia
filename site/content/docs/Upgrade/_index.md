---
title: "Upgrade to New Version"
linkTitle: "Upgrade to New Version"
weight: 6
---

### Upgrading Kopia to v0.9

Kopia v0.9 adds support for several new features thanks to a brand-new index format.

If your repository was created in a version older than v0.9, please follow the steps below to upgrade.

### Notes

It is critical to follow the process outlined before exactly and to verify that during the upgrade steps no instance of `kopia` is connected to the repository. 

This includes:

* `kopia` or `KopiaUI` running interactively on in scripts,
* running as a scheduled background tasks (e.g. using `crontab`),
* running in server mode either as the current user or system-wide daemon (e.g. using `systemd`),
* running in Docker containers and similar.

Also note, that after the upgrade, kopia v0.8 and earlier will not be able to open the repository anymore. Once upgraded all new v0.9 features will be supported except password change, which is only
available for newly-created repositories.

### Upgrade Process

1. Select one kopia client that will perform the upgrade, if there are more clients, pick the one that is currently the owner of maintenance process, which is typically the client that first created the repository.

2. Disconnect all other kopia clients:

* using CLI run:

```
$ kopia repository disconnect
```

* using KopiaUI, click `Repository` | `Disconnect`.  

* make sure to stop any running `kopia server` instances and disable all background kopia tasks, such as periodic snapshots in `crontab`.

3. Upgrade all kopia clients to the latest version >=v0.9.x

4. Using the designated `kopia` client, run:

```
$ kopia repository set-parameters --upgrade
```

5. Verify upgrade by:

```
$ kopia repository status
```

You should see `Format version:      2`

5. Reconnect kopia clients that were disconnected in step 2 and re-enable all disabled background jobs.

6. When in doubt, it's better not to guess, but post a question on https://kopia.discourse.group
