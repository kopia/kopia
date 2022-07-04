---
title: "Mounting"
linkTitle: "Mounting"
weight: 5
---

Mounting allows you to map a content in Kopia repository into a directory in local filesystem and examine it using regular file commands or browser. This is currently the recommended way of restoring files from snapshots.

Mounting can be done repository-wise or content-wise.

When the special path `all` is used, the whole repository with its latest snapshot version is mounted:

```shell
$ mkdir /tmp/mnt
$ kopia mount all /tmp/mnt &
$ ls -l /tmp/mnt/
total 119992
-rw-r--r--  1 jarek  staff      1101 May  9 22:33 CONTRIBUTING.md
-rw-r--r--  1 jarek  staff     11357 May  9 22:33 LICENSE
-rw-r--r--  1 jarek  staff      1613 Jun 22 19:01 Makefile
-rw-r--r--  1 jarek  staff      2286 May  9 22:33 README.md
drwxr-xr-x  1 jarek  staff     11264 May  9 22:33 assets
drwxr-xr-x  1 jarek  staff      6275 Jun  2 23:08 cli2md
-rw-r--r--  1 jarek  staff      3749 May 14 19:00 config.toml
drwxr-xr-x  1 jarek  staff    879721 Jun 22 20:15 content
-rwxr-xr-x  1 jarek  staff       727 May  9 22:33 deploy.sh
drwxr-xr-x  1 jarek  staff      1838 May 14 19:00 layouts
drwxr-xr-x  1 jarek  staff  13682567 Jun 22 18:57 node_modules
-rw-r--r--  1 jarek  staff     94056 Jun 22 18:57 package-lock.json
-rw-r--r--  1 jarek  staff       590 May  9 22:33 package.json
drwxr-xr-x  1 jarek  staff   7104710 Jun 22 19:01 public
drwxr-xr-x  1 jarek  staff    904965 Jun 22 20:13 resources
drwxr-xr-x  1 jarek  staff  38701570 Jun  1 20:11 themes
$ umount /tmp/mnt
```

If the whole repository is not needed, you could mount a specific directory only:

```shell
$ kopia snapshot list
foo@bar:/home/foo/kopia
  2020-05-01 00:00:00 UTC kb9a8420bf6b8ea280d6637ad1adbd4c5 85.2 MB drwxrwxrwx files:75 dirs:2 (daily-3)
  + 2 identical snapshots until 2020-05-01 00:00:00 UTC
...

$ mkdir /tmp/mnt
$ kopia mount kb9a8420bf6b8ea280d6637ad1adbd4c5 /tmp/mnt &
$ ls -l /tmp/mnt/
total 119992
-rw-r--r--  1 foo  staff      1101 May  9 22:33 CONTRIBUTING.md
-rw-r--r--  1 foo  staff     11357 May  9 22:33 LICENSE
-rw-r--r--  1 foo  staff      1613 Jun 22 19:01 Makefile
-rw-r--r--  1 foo  staff      2286 May  9 22:33 README.md
drwxr-xr-x  1 foo  staff     11264 May  9 22:33 assets
drwxr-xr-x  1 foo  staff      6275 Jun  2 23:08 cli2md
-rw-r--r--  1 foo  staff      3749 May 14 19:00 config.toml
drwxr-xr-x  1 foo  staff    879721 Jun 22 20:15 content
-rwxr-xr-x  1 foo  staff       727 May  9 22:33 deploy.sh
drwxr-xr-x  1 foo  staff      1838 May 14 19:00 layouts
drwxr-xr-x  1 foo  staff  13682567 Jun 22 18:57 node_modules
-rw-r--r--  1 foo  staff     94056 Jun 22 18:57 package-lock.json
-rw-r--r--  1 foo  staff       590 May  9 22:33 package.json
drwxr-xr-x  1 foo  staff   7104710 Jun 22 19:01 public
drwxr-xr-x  1 foo  staff    904965 Jun 22 20:13 resources
drwxr-xr-x  1 foo  staff  38701570 Jun  1 20:11 themes
$ umount /tmp/mnt
```

## Windows

On Windows, the mounting is done with `net use` on a WebDAV server. To unmount, press Ctrl-C at the prompt:

```shell
PS> kopia mount all Z:
Mounted 'all' on Z:
Press Ctrl-C to unmount.

(Press Ctrl-C)

Unmounting...
Unmounted.
```

If for some reason you lost the prompt, unmounting can be done by using the "Disconnect" command in Explorer drive menu, or a `net use` command:

```shell
PS> net use Z: /delete
Z: was deleted successfully.
```

If you encounter an error with status "2", one possible cause is the "WebClient" service being unable to start. It could be disabled, or its dependencies have problem to start, etc. You can check the service status in the "Services" administrative tool. Windows 2012 not install WebClient Service by default.

```shell
PS> kopia mount all Z:  # unable to mount
kopia.exe: error: mount error: unable to mount webdav server as drive letter: unable to run 'net use' (): error running 'net use': exit status 2, try --help

PS> sc start WebClient  # attempt to start the service failed
[SC] StartService FAILED 1058:

The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.

PS> sc config WebClient start= demand  # change the service from Disabled to Demand start. can also be done in "Services"
[SC] ChangeServiceConfig SUCCESS

PS> kopia mount all Z:  # mount successful
Mounted 'all' on Z:
Press Ctrl-C to unmount.
```
