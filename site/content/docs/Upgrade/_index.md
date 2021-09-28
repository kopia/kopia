---
title: "Upgrade"
linkTitle: "Upgrade"
weight: 21
---

### Upgrading Kopia to v0.9

Kopia v0.9 adds support for several new features thanks to a brand-new index format.

If your repository was created in a version older than v0.9, please follow the steps below to upgrade.

### Notes

* After the upgrade, kopia v0.8 and earlier will not be able to open the repository anymore.

* It is very important to perform repository upgrade without other clients accessing repository to avoid data loss.

### Upgrade Process

1. Disconnect all but one kopia clients:

* using CLI run 

```
$ kopia repository disconnect
```

* using KopiaUI, click `Repository` | `Disconnect`.  

2. Upgrade all kopia executables to >=v0.9.0-rc1

3. Using the remaining connected `kopia` client, run:

```
$ kopia repository set-parameters --upgrade
```

4. Verify upgrade by:

```
$ kopia repository status
```

You should see `Format version:      2`

5. Reconnect kopia clients that were disconnected in step 1

After upgrade all new v0.9 features will be supported except password change.