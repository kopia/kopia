# Kopia Policy
Policies are a useful tool to help manage repositories. You can configure things like how many snapshots are retained, files that are ignored, compression algorithms, etc...

## Sources

Policies can be applied to individual sources, or globally. A source is a single path on a single host. For example `user@host1:/home/user` is a different source to `user@host2:/home/user`. Each host can have policies defined, and the entire repository can have a global policy. This creates a hierarchy in which policy is inherited from the next highest policy. There is also a default policy for newly created repositories. The hierarchy will be as follows

Default Policy
> Global Policy
>> Host level policy
>>> Source level policy

In this system, policies are inherited from the next highest level, in  much the same way as Windows does permissions or GP. Any setting that is not explicitly defined in a lower level is inherited from the next highest level, until the default is reached. To edit a specific policy `$ kopia policy edit` is used. This takes a source as an argument, or if `--global` is given, then the global policy is edited. This will open your default text editor in the terminal where you can edit the policy in JSON format. When editing is finished save the file and close the editor. Kopia will then upload and apply the policy to the repository. Policies are always checked and updated at the start of a snapshot create.


## Retention Policy
Snapshots are retained based on the defined retention policy. Snapshots are assigned tags starting with the most recent and moving backwards in time. When there are no tags left, the snapshot expires and is deleted. Tags are assigned based on the time period that has passed and the count of each type.

Tags are assigned as follows:

    latest-N will be assigned to all snapshots going backwards where the most recent snapshot gets latest-1, previous is latest-2 and so on until the set number is reached.

    hourly-N will be assigned to the latest snapshot within each respective hour, going back no more than M hours where M is defined by --keep-hourly parameter.

    daily-N will be assigned to the latest snapshot within each respective day, going back no more than M days.

    Same for Weekly, Monthly and Annual

    For retention purposes, a week starts on Monday and ends on Sunday.

```
Annual snapshots:                     0
  Monthly snapshots:                    1
  Weekly snapshots:                     1
  Daily snapshots:                      4
  Hourly snapshots:                     1
  Latest snapshots:                     1
  Ignore identical snapshots:       false
```

```
  2023-06-11 22:00:00 - (daily-4)
  2023-06-12 22:00:00 - (daily-3)
  2023-06-13 22:00:00 - (daily-2)
  2023-06-14 22:00:00 - (latest-1,hourly-1,daily-1,weekly-1,monthly-1)
```

Similarly daily-N is assigned to the latest snapshot within each day:
```
2001-01-01 14:01 - not daily because 15:55 was done the same day
2001-01-01 15:55 - daily-5
2001-01-02 14:01 - daily-4
2001-01-03 15:55 - daily -3
2001-01-04 14:01 - not daily because 15:01 was done the same day
2001-01-04 15:01 - daily-2
2001-01-05 15:55 - daily-1
```
Same rule applies to weekly, monthly, quarterly and annual retention rules - each of them retains latest snapshot within a given time period up to a defined limit.

> Note: the limit applies to both count and time, so if you have keep-hourly 48, it will keep no more than 48 hourly snapshots going back no more than 48 hours since the latest snapshot.

## Ignore Files Policy
We can define files that are ignored based on patterns specified. These rules are given using regular expressions. Ignore rules can also be defined in a .kopiaignore file stored in the top level directory of what is being snapshotted. For more about ignore policy see [advanced ignore](../advanced/kopiaignore/)

`$ kopia snapshot ~/`

Would mean the .kopiaignore file goes in the ~/ directory. The formating for these files is the same as a .gitignore file.

Scan one file system only when set to false will allow kopia to cross filesystem boundrys. Set this to true to disable.

```
Files policy:
  Ignore cache directories:          true
  No ignore rules:
  Read ignore rules from files:           
    .kopiaignore
  Scan one filesystem only:         false
```

## Error Handling
There are 3 options regarding error handling that can be defined in a policy. These control the behavior of a snapshotting client during creation when errors are encountered.

```
Error handling policy:
  Ignore file read errors:           false
  Ignore directory read errors:      false
  Ignore unknown types:               true
```
`Ignore file and read errors` {default false} will ignore file read errors and continue during a snapshot when set to true.

`Ignore directory read errors` {default false} Will ignore directory read errors and continue when set to true. Directory errors can be caused by permissions when the user running kopia doesn't have access to a directory.

`Ignore unkonwn types` - how unknown files are handled. Mostly for special types of files like devices in /dev/ on Linux of socket files.

## Scheduling
This feature is mostly for kopiaUI and kopiaServer users, and defines how often snapshots will be created for each source. It defaults to none, as when running from the cli it is assumed that other mechanisms will be used to create snapshots on a schedule. This could be things like cron or the windows task scheduler.

## Uploads
This controls how many parallel uploads can occur during snapshot creation.

## Compression
This defines what type of compression is used. This can be changed at any point in time as the deduplication happens before compression, and changes won't affect already saved files. The algorithm can be set using Compressor: Options can be zstd,
```
Compression:
  Compressor:                         zstd
  Compress all files except the following extensions:
    .mkv
    .mp4
  Compress files of all sizes.
```

Files can be defined as ignore for compression. Use the same syntax as ignore policies. This can be used to reduce client workload when dealing with certain un-compressable file types, such as video. A compression of these defined files won't be done.

`Compress files of x size` Can be used to define a range of file sizes to be compressed.
