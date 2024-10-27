---
title: "Getting Started Guide"
linkTitle: "Getting Started Guide"
weight: 15
---

This guide will walk you through installing Kopia and setting up Kopia to backup/restore your data. Make sure to familiarize yourself with Kopia [features](../features/) before following this guide, so that you understand the appropriate terminology. As a reminder:

* A `snapshot` is a [point-in-time backup](../features#backup-files-and-directories-using-snapshots) of your files/directories; each snapshot contains the files/directories that you can [restore when you need to](../features#restore-snapshots-using-multiple-methods).
* A `repository` is the storage location where your snapshots are saved; Kopia supports [cloud/remote, network, and local storage locations](../features#save-snapshots-to-cloud-network-or-local-storage) and all repositories are [encrypted](../features/#end-to-end-zero-knowledge-encryption) with a password that you designate.
* A `policy` is a set of rules that tells Kopia how to create/manage snapshots; this includes features such as [compression, snapshot retention, and scheduling when to take automatically snapshots](../features#policies-control-what-and-how-filesdirectories-are-saved-in-snapshots).

## Download and Installation

Read the [download and installation guide](../installation/) to learn how to download & install Kopia. As a reminder, Kopia comes in two variants: [command-line interface (CLI)](../installation/#two-variants-of-kopia) and [graphical user interface (GUI)](../installation/#two-variants-of-kopia). Pick the one you like the most.

## Setting Up Kopia

Once you have installed Kopia, setting up Kopia is quite easy but varies depending on if you are using Kopia CLI or Kopia GUI (also known as `KopiaUI`).

### Kopia GUI (`KopiaUI`)

Setting up Kopia via the GUI is very easy. 

#### Creating and Connecting to a Repository

When you run `KopiaUI` for the first time, you will need to create a `repository`. You will see all supported [repository types](../repositories/) on-screen within the program interface. Pick the one you want and follow the on-screen directions to get it setup; you will need to enter various different details about the storage location that you selected, and you will pick a password that will be used to encrypt all the snapshots that you store in the repository. (As a reminder, Kopia uses [end-to-end zero knowledge encryption](../features#end-to-end-zero-knowledge-encryption), so your password is never sent anywhere and it never leaves your machine!) You can also name the repository whatever you want. 

**There is absolutely no way to restore snapshots (i.e., your backed up files/directories) from a repository if you forget your password, so do not forget it and keep it secure!** 

> NOTE: Remember, before you use Kopia, you need to provision, setup, and pay (the storage provider) for whatever storage location you want to use; Kopia will not do that for you. After you have done that, you can create a `repository` for that storage location in Kopia. For example, if you want to use `Backblaze B2`, you need to create a Backblaze account, create a B2 bucket, and get the access keys for the bucket; then you can use the `Backblaze B2` repository option in `KopiaUI` to create a repository.

#### Defining Snapshot Policy and Creating New Snapshot

Once you have created a repository, you can start backing up your files/directories by creating a new `policy` in `KopiaUI`. You can do this from the `Policies` tab and the process, again, is quite straightforward: enter the `directory` which contains the files you want to backup (you can either manually type in the `directory path` or browse for the `directory`), hit the `Set Policy` button, choose your policy settings from the on-screen options (all policy options are fairly self-explanatory), and hit the `Save Policy` button. Kopia will then automatically begin taking the snapshot following the settings you set for the policy. 

After the initial snapshot, for every snapshot afterwards Kopia will rescan the file/directories and [only upload file content that has changed](../features/#backup-files-and-directories-using-snapshots). All snapshots in Kopia are [always incremental](../features/#backup-files-and-directories-using-snapshots); a snapshot will only upload files/file contents that are not in the repository yet, which saves storage space and upload time. This even applies to files that were moved or renamed. In fact, if two computers have exactly the same file and both computers are backing up to the same `repository`, the file will still be stored only once.

> PRO TIP: If you pick a value for `Snapshot Frequency` when creating a `policy`, then Kopia will automatically take snapshots at that frequency (e.g., every one hour or whatever value you pick), and you do not need to remember to manually run the snapshot. If you do not pick a `Snapshot Frequency`, then Kopia will not automatically take snapshots, and you need to manually run snapshots from the `Snapshots` tab (just click the `Snapshot Now` button as needed).

Note that you can set policies at two levels in `KopiaUI` -- at the `global` level, where the settings are applied by default to all policies that do not define their own settings, or at the individual `policy` level, where the settings are applied only to that particular policy. By default, all new policies are set to inherit settings from the `global` policy. The `global` policy is the one that says `*` for `Username`, `Host`, and `Path`.

> PRO TIP: Kopia does not currently support the ability to save one snapshot to multiple different repositories. However, you can use `KopiaUI` to connect to multiple different repositories simultaneously and create identical policies for each repository, which essentially achieves the same outcome of saving one snapshot to multiple different repositories. Connecting to more than one repository in `KopiaUI` is easy: just right-click the icon of the desktop application and select `Connect To Another Repository...`. Currently, this is only available in the desktop version of `KopiaUI` and not the web-based `KopiaUI`. However, if you are using the web-based `KopiaUI`, you can manually run multiple instances of `KopiaUI` to achieve the same outcome.

#### Restoring Files/Directories from Snapshots

When you want to restore your files/directories from a snapshot, you can do so from the `Snapshots` tab in `KopiaUI`. Just click the `Path` for the files/directories you want to restore and then find the specific `snapshot` you want to restore from. You will then be given the option to either 

* `Mount` the snapshot as a local drive so that you can browse, open, and copy any files/directories from the snapshot to your local machine;
* `Restore` all the contents of the snapshot to a local or network location;
* or download individual files from the snapshot (which can be done by browsing the snapshot contents from inside `KopiaUI` and clicking on the file you want to download).

You can restore files/directories using either of these options.

#### Video Tutorial

Here is a video tutorial on how to use `KopiaUI` (note that the video is of an older version of `KopiaUI` and the interface is different in the current version of `KopiaUI`, but the main principles of how to use `KopiaUI` are the same):

{{< youtube sHJjSpasWIo >}}

### Kopia CLI

Setting up Kopia via the CLI follows similar steps as the GUI, but obviously requires using command-line rather than a graphical user interface.

> NOTE: This guide focuses on simple scenarios. You can learn more about all the command-line features in the [command-line reference page](../reference/command-line/).

#### Creating a Repository

The first thing you need to do is create a `repository`. For a full list of supported types of repositories that you can create, see the [repositories page](../repositories).

To create a repository, use one of the [subcommands](../reference/command-line/common/#commands-to-manipulate-repository) of `kopia repository create` and follow the on-screen instructions. When creating the repository, you must provide a password that will be used to encrypt all the snapshots and their contents in the repository. (As a reminder, Kopia uses [end-to-end zero knowledge encryption](../features#end-to-end-zero-knowledge-encryption), so your password is never sent anywhere and it never leaves your machine!)

**There is absolutely no way to restore snapshots (i.e., your backed up files/directories) from a repository if you forget your password, so do not forget it and keep it secure!** 

As an example, if you want to create a repository in a locally-mounted or network-attached filesystem, you would run the following command:

```shell
$ kopia repository create filesystem --path /tmp/my-repository
```
You can read more about all the supported `kopia repository create` commands for different repositories from the [repositories page](../repositories).

> NOTE: Remember, before you use Kopia, you need to provision, setup, and pay (the storage provider) for whatever storage location you want to use; Kopia will not do that for you. After you have done that, you can create a `repository` for that storage location in Kopia. For example, if you want to use `Backblaze B2`, you need to create a Backblaze account, create a B2 bucket, and get the access keys for the bucket; then you can use the [`kopia repository create b2` command](../reference/command-line/common/repository-create-b2/) to create a repository. 

#### Connecting to Repository

To connect to a repository after you have created it or to connect to an existing repository, simply use one of the [subcommands](../reference/command-line/common/#commands-to-manipulate-repository) of `kopia repository connect` instead of `kopia repository create`. You can connect as many computers as you like to the same repository, even simultaneously.

For example:

```shell
$ kopia repository connect filesystem --path /tmp/my-repository
```

#### Creating Initial Snapshot

Let's create our first snapshot. That's as simple as pointing `kopia snapshot create` to the directory that contains the files/directories you want to backup, but note you need to make sure to be connected to a repository first (see above). We will create the snapshot of the source code of Kopia itself:

```shell
$ kopia snapshot create $HOME/Projects/github.com/kopia/kopia
```

After completion, Kopia prints the identifier of the root of the snapshot, which starts with `k`:

```
uploaded snapshot 9a622e33ab134ef440f76ed755f79c2f
  (root kfe997567fb1cf8a13341e4ca11652f70) in 1m42.044883302s
```

#### Incremental Snapshots

Let's take the snapshot of the same files/directories again. To do so, just rerun the same `kopia snapshot create` command...

```shell
$ kopia snapshot create $HOME/Projects/github.com/kopia/kopia
```
...and Kopia will rescan the file/directories and [only upload the file content that has changed](../features/#backup-files-and-directories-using-snapshots). Assuming we did not make any changes to the files/directories, the snapshot root will be identical, because all object identifiers in Kopia are derived from contents of the underlying data:

```
uploaded snapshot 8a45c3b079cf5e7b99fb855a3701607a
  (root kfe997567fb1cf8a13341e4ca11652f70) in 563.670362ms
```

Notice that snapshot creation was nearly instantaneous. This is because Kopia did not have to upload almost any files to the repository, except tiny piece of metadata about the snapshot itself.

All snapshots in Kopia are [always incremental](../features/#backup-files-and-directories-using-snapshots); a snapshot will only upload files/file contents that are not in the repository yet, which saves storage space and upload time. This even applies to files that were moved or renamed. In fact, if two computers have exactly the same file and both computers are backing up to the same `repository`, the file will still be stored only once.

#### Managing Snapshots

We can see the history of snapshots of a directory using `kopia snapshot list`:

```
$ kopia snapshot list $HOME/Projects/github.com/kopia/kopia
jarek@jareks-mbp:/Users/jarek/Projects/Kopia
  2019-06-22 20:15:51 PDT kb9a8420bf6b8ea280d6637ad1adbd4c5 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-5)
  + 1 identical snapshots until 2019-06-22 20:15:57 PDT
  2019-06-22 20:21:39 PDT kbb7dd85a55ca79f282b59b57e5f9c479 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-3)
  2019-06-22 20:21:42 PDT ke2e07d38a8a902ad07eda5d2d0d3025d 61.4 MB drwxr-xr-x files:12500 dirs:798 (latest-2)
  + 1 identical snapshots until 2019-06-22 20:21:44 PDT
```

To compare contents of two snapshots, use `kopia diff`:

```
$ kopia diff kb9a8420bf6b8ea280d6637ad1adbd4c5 ke2e07d38a8a902ad07eda5d2d0d3025d
changed ./content/docs/Getting started/_index.md at 2019-06-22 20:21:30.176230323 -0700 PDT (size 5346 -> 6098)
```

We can list the contents of the directory using `kopia ls`:

```
$ kopia ls -l kb9a8420bf6b8ea280d6637ad1adbd4c5
-rw-r--r--         6148 2019-06-22 19:01:45 PDT aea2fe8e5ed3104806957f48648c957e   .DS_Store
-rw-r--r--           78 2019-05-09 22:33:06 PDT c829f2205d0ba889ebb354464e14c97a   .gitignore
-rw-r--r--         1101 2019-05-09 22:33:06 PDT 5c4da68139ab0a92a56c334988c75e2a   CONTRIBUTING.md
-rw-r--r--        11357 2019-05-09 22:33:06 PDT 28614f260fab7463e3cd9c410a501c3f   LICENSE
-rw-r--r--         1613 2019-06-22 19:01:17 PDT 5c1f9d67a2b1e2d34fc121ba774266b4   Makefile
-rw-r--r--         2286 2019-05-09 22:33:06 PDT 83a5b758d8409550010786e254096606   README.md
drwxr-xr-x        11264 2019-05-09 22:33:06 PDT kc76b1a9ddf378f803f1710df1150ded6  assets/
drwxr-xr-x         6275 2019-06-02 23:08:14 PDT kf3b4b410df41570345dbc2a8043ee29b  cli2md/
-rw-r--r--         3749 2019-05-14 19:00:21 PDT 8c9e27bed2f577b31b07b07da4bdfffb   config.toml
drwxr-xr-x       879721 2019-06-22 20:15:45 PDT k24eb31a05b81d1a83c47c40a4f7b9f0e  content/
-rwxr-xr-x          727 2019-05-09 22:33:06 PDT 2c08f511019f1f5f45f889909c755a9b   deploy.sh
drwxr-xr-x         1838 2019-05-14 19:00:21 PDT k024f1106e0cd56e2c6611cf884a30894  layouts/
drwxr-xr-x     13682567 2019-06-22 18:57:48 PDT k181d6990e75dd783bd50dae36591622a  node_modules/
-rw-r--r--        94056 2019-06-22 18:57:49 PDT ed474fb638d2a3b1c528295d1586466a   package-lock.json
-rw-r--r--          590 2019-05-09 22:33:06 PDT ee85ae1f1cdb70bbd9e335be9762c251   package.json
drwxr-xr-x      7104710 2019-06-22 19:01:38 PDT keb814d92fe795b96795d5bdbfa816ad6  public/
drwxr-xr-x       904965 2019-06-22 20:13:56 PDT k7bf88a7ca076b03f0dafc93ab5fa2263  resources/
drwxr-xr-x     38701570 2019-06-01 20:11:32 PDT kdb9f41fc8db5c45b1aec06df001be995  themes/
```

For each file/directory in a directory, Kopia stores its name, size, attributes and object ID which has the contents of the file or directory.

To examine contents of files, use `kopia show` while passing the object identifier of the file or directory you want to examine:

```shell
$ kopia show 8c9e27bed2f577b31b07b07da4bdfffb
```

Directories are stored as JSON objects, so it's possible to see their contents as if they were regular files using `kopia content show` along with the the directory's object identifier (the `-j` option displays pretty-printed JSON):

```shell
$ kopia content show -j kb9a8420bf6b8ea280d6637ad1adbd4c5
```

This command returns:

```json
{
  "stream": "kopia:directory",
  "entries": [
    {
      "name": "assets",
      "type": "d",
      "mode": "0755",
      "mtime": "2019-05-14T18:24:15-07:00",
      "uid": 501,
      "gid": 20,
      "obj": "kc76b1a9ddf378f803f1710df1150ded6",
      "summ": {
        "size": 11264,
        "files": 2,
        "dirs": 3,
        "maxTime": "2019-05-09T22:33:06-07:00"
      }
    },
    ...
    {
      "name": "package.json",
      "type": "f",
      "mode": "0644",
      "size": 590,
      "mtime": "2019-05-09T22:33:06-07:00",
      "uid": 501,
      "gid": 20,
      "obj": "ee85ae1f1cdb70bbd9e335be9762c251"
    }
  ],
  "summary": {
    "size": 61414615,
    "files": 12500,
    "dirs": 798,
    "maxTime": "2019-06-22T20:15:45.301289096-07:00"
  }
}
```

#### Mounting Snapshots and Restoring Files/Directories from Snapshots

We can [mount](../mounting/) the contents of a snapshot as a local filesystem and examine it using regular file commands to examine the contents using the `kopia mount` command:

```shell
$ mkdir /tmp/mnt
$ kopia mount kb9a8420bf6b8ea280d6637ad1adbd4c5 /tmp/mnt &
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
Mounting is currently the recommended way of restoring files/directories from snapshots. However, you can also use the [`kopia snapshot restore` command](../reference/command-line/common/snapshot-restore/) to restore files/directories from snapshots.

#### Policies

Policies can be used to specify how Kopia snapshots are taken and retained. We can define various different `policy` options, including:

- which files to ignore
- how many hourly, daily, weekly, monthly and yearly snapshots to maintain
- how frequently snapshots should be made
- whether to compress files or not

To learn read more about what `policy` options are available, see the [Kopia `policy` command help docs](../reference/command-line/common/#commands-to-manipulate-snapshotting-policies).

Each `repository` has a `global` policy, which contains the defaults used for all policies if a specific policy does not define its own settings. We can examine the `global` policy by using `kopia policy show --global`:

```
$ kopia policy show --global
Policy for (global):
Keep:
  Annual snapshots:    3           (defined for this target)
  Monthly snapshots:  24           (defined for this target)
  Weekly snapshots:   25           (defined for this target)
  Daily snapshots:    14           (defined for this target)
  Hourly snapshots:   48           (defined for this target)
  Latest snapshots:   10           (defined for this target)

Files policy:
  No ignore rules.
  Read ignore rules from files:
    .kopiaignore                   (defined for this target)
```

We can change policy settings using the [`kopia policy set` command](../reference/command-line/common/policy-set/). This command allows you to change the `global` policy or change specific policies for a 'user@host', a '@host', a 'user@host:path', or a particular directory. For example, here we tell Kopia to set the policy to ignore two directories from being included in the snapshot of `jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site`:

```
$ kopia policy set --add-ignore public/ --add-ignore node_modules/ .
Setting policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
 - adding public/ to ignored files
 - adding node_modules/ to ignored files
```

Now when taking snapshot of `jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site`, the directories `public/` and `node_modules/` will be skipped.

The [`kopia policy set` command help docs](../reference/command-line/common/policy-set/) provide more information about all the policy options you have. As another example, we can set a maximum number of weekly snapshots:

```
$ kopia policy set --keep-weekly 30 .
Setting policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
 - setting number of weekly backups to keep to 30.
```

If you want to examine the policy for a particular directory, use [`kopia policy show`](../reference/command-line/common/policy-show/):

```
$ kopia policy show .
Policy for jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site:
Keep:
  Annual snapshots:    3           inherited from (global)
  Monthly snapshots:  24           inherited from (global)
  Weekly snapshots:   30           (defined for this target)
  Daily snapshots:    14           inherited from (global)
  Hourly snapshots:   48           inherited from (global)
  Latest snapshots:   10           inherited from (global)

Files policy:
  Ignore rules:
    dist/                          (defined for this target)
    node_modules/                  (defined for this target)
    public/                        (defined for this target)
  Read ignore rules from files:
    .kopiaignore                   inherited from (global)
```

To list all policies for a `repository`, we can use [`kopia policy list`](../reference/command-line/common/policy-list/):

```
$ kopia policy list
7898f47e36bad80a6d5d90f06ef16de6 (global)
63fc854c283ad63cafbca54eaa4509e9 jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site
2339ab4739bb29688bf26a3a841cf68f jarek@jareks-mbp:/Users/jarek/Projects/Kopia/site/node_modules
```

Finally, you can also import and export policies using the [`kopia policy import`](../reference/command-line/common/policy-import/) and [`kopia policy export`](../reference/command-line/common/policy-export/) commands:

```
$ kopia policy import --from-file import.json
$ kopia policy export --to-file export.json
```

In the above example, `import.json` and `export.json` share the same format, which is a JSON map of policy identifiers to defined policies, for example:

```
{
  "(global)": {
    "retention": {
      "keepLatest": 10,
      "keepHourly": 48,
      ...
    },
    ...
  },
  "foo@bar:/home/foobar": {
     "retention": {
      "keepLatest": 5,
      "keepHourly": 24,
      ...
    },
    ...
  }
}
```

You can optionally limit which policies are imported or exported by specifying the policy identifiers as arguments to the `kopia policy import` and `kopia policy export` commands:

```
$ kopia policy import --from-file import.json "(global)" "foo@bar:/home/foobar"
$ kopia policy export --to-file export.json "(global)" "foo@bar:/home/foobar"
```

Both commands support using stdin/stdout:

```
$ cat file.json | kopia policy import
$ kopia policy export > file.json
```

You can use the `--delete-other-policies` flag to delete all policies that are not imported. This command would delete any policy besides `(global)` and `foo@bar:/home/foobar`:

```
$ kopia policy import --from-file import.json --delete-other-policies "(global)" "foo@bar:/home/foobar"
```

#### Examining Repository Structure

Kopia CLI provides low-level commands to examine the contents of repository, perform maintenance actions, and get deeper insight into how the data is laid out.

> REMINDER: This guide does not cover all of the commands available via Kopia CLI. Refer to the [command-line reference](../reference/command-line/) page to learn about all the available commands.

##### BLOBs

We can list the files in a repository using `kopia blob ls`, which shows how Kopia manages snapshots. We can see that repository contents are grouped into pack files (starting with `p`) and indexed using index files (starting with `n`). Both index and pack files are [encrypted](../features/#end-to-end-zero-knowledge-encryption):

```
$ kopia blob ls
kopia.repository                           636 2019-06-22 20:03:25 PDT
n16f6b7257610be4826396ce2be1fb302       667941 2019-06-22 20:05:30 PDT
n71861ad243bea4c2010001cf5cba1fbe          110 2019-06-22 20:03:29 PDT
p0b31020332e1fe97856faf8aa9c5cf4c     21015032 2019-06-22 20:05:13 PDT
p0df646619c8d1a25415b1b9dc9329d19     22659618 2019-06-22 20:04:36 PDT
p1bde010809b285b657cc130c34c01687     21606909 2019-06-22 20:04:30 PDT
p4dd3b780360b6889e172776fa65be836     23688119 2019-06-22 20:04:23 PDT
p549295a15c0f481ab46f4e4f18372bfc      6936758 2019-06-22 20:05:30 PDT
p61418c5b66c2ca06a51c4af3205e7006         4232 2019-06-22 20:03:29 PDT
p8a6e4f70c0fbbfda99b2e1100647606f     22930102 2019-06-22 20:04:43 PDT
p8f482528fff99ad5c23264873582545f     21261488 2019-06-22 20:05:21 PDT
p946d1fe528bde84d826cf177168d07b3     21015794 2019-06-22 20:04:15 PDT
pa0223f20fc6776b24d25b122cc9ac5f3     26497385 2019-06-22 20:05:07 PDT
pca01737879ac83f9e613c11f52d58349     21520893 2019-06-22 20:04:59 PDT
pcc4bc6fc2b960091755dc0c4704669f1     21656682 2019-06-22 20:05:27 PDT
pdec2a5b599acef1e1ce13ace25e914cd     21332440 2019-06-22 20:04:02 PDT
pf0239202fea978abf1cae4ca45d395b1     23756797 2019-06-22 20:03:54 PDT
pf1d1f00141f830ee34c89797e56011c2     23663212 2019-06-22 20:04:09 PDT
pfbaba06c70b6aed2c0ed3aa9c709dc47     22823458 2019-06-22 20:04:50 PDT
```

##### Content-Addressable Block Storage

To list individual contents stored in a repository, use [`kopia content list`](../reference/command-line/advanced/content-list/):

```shell
$ kopia content list
00020136867452b90a3c65a029e7d08c
00029ee689919a13dbf2d588c0986530
00075db1b05e83ba529bc0e06de76ca0
00077bc1c333ddc475d66c45d4645210
0009d14c0a50ceca3519d522137b3a68
000df512dd55ac9749dd0670efa49c7d
001130562b64643ca255396a95a9f2a4
001375d9065eef57b0f21dfe11590227
0015561b6e70feff623eede49f95da73
00166bad1a69484992e32dbd2b869a8a
001828cc591d0bec1301b15266c5c530
...
kfec1215488eaf6acef4558e87ff343a9
kfec7264b36fabdf072de939f622b4452
kff0a0d64969cde306f4dd2c95ca2df6f
kff646ec33a3c24701aabb22f62d60e43
kff74067fa7a2bf14681aee73eb08330d
kff8824282ccc64f68b7b39aacdbb6ceb
kff99aac1cd37371cfc521753e2a1d424
m08ef40b314fb7f08c7be222a79485cc1
m259ec63a4a0137b7ce2801cc47012ffa
m5cf33f9416a435478dea4040b8049f51
m81bd005052e582e821df831c36138d76
m831980997bceabebefa095914a600a1b
m8401800f69795ed0137365c3e6f627bc
...
```

##### Manifest Storage

To list manifests (snapshot manifests and policies) stored in a repository, use [`kopia manifest list`](../reference/command-line/advanced/manifest-list/):

```
$ kopia manifest list
7898f47e36bad80a6d5d90f06ef16de6        170 2019-06-22 20:03:29 PDT type:policy policyType:global
9a622e33ab134ef440f76ed755f79c2f        802 2019-06-22 20:05:28 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia username:jarek
2d73b31af65d4ac7196641eeea9c475c        755 2019-06-22 20:15:53 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
8a45c3b079cf5e7b99fb855a3701607a        762 2019-06-22 20:15:58 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
ed30d264bcf795bd6648540fdeebdb31        761 2019-06-22 20:21:39 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
a1646120c7a2450cd9e77fd98369d260        761 2019-06-22 20:21:43 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
38f1987e1ea434e161111abce86212ed        761 2019-06-22 20:21:45 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
2339ab4739bb29688bf26a3a841cf68f         63 2019-06-22 21:19:41 PDT type:policy hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site/node_modules policyType:path username:jarek
856ed6f6cc5cd522d23718e6315cf51e        757 2019-06-22 21:20:37 PDT type:snapshot hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site username:jarek
63fc854c283ad63cafbca54eaa4509e9        102 2019-06-22 21:22:20 PDT type:policy hostname:jareks-mbp path:/Users/jarek/Projects/Kopia/site policyType:path username:jarek
```

To examine individual manifests, use [`kopia manifest show`](../reference/command-line/advanced/manifest-show/):

```shell
$ kopia manifest show 2d73b31af65d4ac7196641eeea9c475c
```
```json
// id: 2d73b31af65d4ac7196641eeea9c475c
// length: 755
// modified: 2019-06-22 20:15:53 PDT
// label path:/Users/jarek/Projects/Kopia/site
// label type:snapshot
// label username:jarek
// label hostname:jareks-mbp
{
  "source": {
    "host": "jareks-mbp",
    "userName": "jarek",
    "path": "/Users/jarek/Projects/Kopia/site"
  },
  "description": "",
  "startTime": "2019-06-22T20:15:51.603328-07:00",
  "endTime": "2019-06-22T20:15:53.038247-07:00",
  "stats": {
    "content": {
      "readBytes": 863,
      "decryptedBytes": 799,
      "hashedBytes": 63685985,
      "readContents": 2,
      "hashedContents": 13315
    },
    "dirCount": 798,
    "fileCount": 12500,
    "totalSize": 61414615,
    "excludedFileCount": 0,
    "excludedTotalSize": 0,
    "excludedDirCount": 0,
    "cachedFiles": 0,
    "nonCachedFiles": 12500,
    "readErrors": 0
  },
  "rootEntry": {
    "name": "site",
    "type": "d",
    "mode": "0755",
    "mtime": "2019-06-22T19:01:45.936555202-07:00",
    "uid": 501,
    "gid": 20,
    "obj": "kb9a8420bf6b8ea280d6637ad1adbd4c5",
    "summ": {
      "size": 61414615,
      "files": 12500,
      "dirs": 798,
      "maxTime": "2019-06-22T20:15:45.301289096-07:00"
    }
  }
}
```

##### Cache

For better performance, Kopia maintains local cache directory where most-recently used blocks are stored. You can examine the cache by using [`kopia cache info`](../reference/command-line/advanced/cache-info/):

```
$ kopia cache info
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/contents: 3 files 7 KB (limit 5.2 GB)
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/indexes: 12 files 670.8 KB
/Users/jarek/Library/Caches/kopia/e470f963ef9528a1/metadata: 2006 files 3.9 MB (limit 0 B)
```

To clear the cache, use [`kopia cache clear`](../reference/command-line/advanced/cache-clear/):

```
$ kopia cache clear
```

To set caching parameters, such as maximum size of each cache, use [`kopia cache set`](../reference/command-line/advanced/cache-set/):

```
$ kopia cache set --metadata-cache-size-mb=500
21:38:25.024 [kopia/cli] changing metadata cache size to 500 MB
```
More information on `cache` commands is available in the [help docs](../reference/command-line/advanced/#commands-to-manipulate-local-cache).
