---
title: "What is a Repository Server?"
linkTitle: "What is a Repository Server?"
weight: 7
---

By default, every user of Kopia repository directly connects to an underlying storage using read-write access. If the users who share repository don't completely trust each other, some malicious actors can delete repository data structures causing data loss for others.

Repository Server allows an instance of kopia to proxy access to the underlying storage and has Kopia clients proxy all access through it, only requiring username and password to talk to server without any knowledge of
repository storage credentials. 

In repository server mode, each user is limited to seeing their own snapshots and policy manifest without being able to access those from another user account. 

>NOTE: Only snapshot and policy manifests are access-controlled, not the underlying contents. If two users shared the same file, it will be backed using exactly the same content IDs. The consequence of this is that if a third user can guess the content ID of files in the repository, they will be able to access the files. Because content IDs are one-way salted hashes of contents, in principle it should be impossible to guess content ID without possessing original content.

## Starting Repository Server

Repository Server should be started on a dedicated server in LAN, such that all clients can directly connect to it.

Before we can start repository server, we must first create a list of usernames and passwords that will be allowed access.

## Configuring Allowed Users - Kopia v0.8

Starting in Kopia v0.8, allowed repository users can be configured using `kopia server user` commands. Each user is identified by its lowercase `username@hostname` where hostname by default is the name of the computer the client is connecting from (without domain name suffix).

To add a user:

```shell
$ kopia server user add myuser@mylaptop
Enter new password for user myuser@mylaptop: 
Re-enter new password for verification: 

Updated user credentials will take effect in 5-10 minutes or when the server is restarted.
To refresh credentials in a running server use 'kopia server refresh' command.
```

Other commands are also available:

* `kopia server user list` - lists user accounts
* `kopia server user set` - changes password
* `kopia server user delete` - deletes user account

## Configuring Allowed Users - Kopia v0.7

>NOTE: This method is still supported in v0.8, but it's recommended to use `kopia server user` to manage users
instead.

In Kopia v7.0 the user list must be put in a text file formatted using the [htpasswd](https://httpd.apache.org/docs/2.4/programs/htpasswd.html) utility from Apache. 

Each username must be named:

```
<client-username>@<client-host-name-lowercase-without-domain>
```

where `<client-host-name-lowercase-without-domain>` is the host name of the client computer in **lowercase and without domain component**. On Linux clients, the client host name can be found with the following command. Notice that this is *not* the server host name, but the client host name instead.

```shell
$ cat /etc/hostname
```

>NOTE: The use of htpasswd tool is only temporary. In future releases Kopia may introduce its own password management options and ACL management without having to rely on external utility.

To create password file for two users:

```shell
$ htpasswd -c password.txt user1@host1
New password: 
Re-type new password: 
Adding password for user user1@host1

$ htpasswd password.txt user2@host1
New password: 
Re-type new password: 
Adding password for user user2@host1
```

The contents of password file will store hashes of provided passwords and not passwords themselves:

```
user1@host1:$apr1$8yF23v/9$Su9NdzkBp.r456/qcgJBF.
user2@host1:$apr1$KbY23BUf$xtzBjaMnrOBOfPePIfS//.
```

It is recommended to use TLS to ensure security of connections to the server.

### Auto-Generated TLS Certificate

To start repository server with auto-generated TLS certificate for the first time:

```shell
kopia server start --htpasswd-file ~/password.txt --tls-generate-cert --tls-cert-file ~/my.cert --tls-key-file ~/my.key --address 0.0.0.0:51515
```

or when using repository users in Kopia v0.8 and newer:

```shell
kopia server start --tls-generate-cert --tls-cert-file ~/my.cert --tls-key-file ~/my.key --address 0.0.0.0:51515
```


This will generate TLS certificate and key files and store them in the provided paths (`~/my.cert` and `~/my.key` respectively). It will also print certificate SHA256 fingerprint, which will be used later:

```shell
SERVER CERT SHA256: 48537cce585fed39fb26c639eb8ef38143592ba4b4e7677a84a31916398d40f7
```

Note that when starting the server again the `--tls-generate-cert` must be omitted, otherwise the server will fail to start.

### Custom TLS Certificates

If a user has obtained custom certificate (for example from LetsEncrypt or another CA), using it is simply a matter of providing PEM-formatted certificate and key files on server startup.

To get SHA256 certificate of existing file use:

```shell
$ openssl x509 -in ~/my.cert -noout -fingerprint -sha256 | sed 's/://g' | cut -f 2 -d =
48537CCE585FED39FB26C639EB8EF38143592BA4B4E7677A84A31916398D40F7
```

### On Client Computer

Assuming we're on another machine running as `user1@host1`, we can now run the following command to connect to the repository (notice we're using fingerprint obtained before without `:` separators)

```shell
kopia repository connect server --url https://<address>:51515 \
  --server-cert-fingerprint 48537cce585fed39fb26c639eb8ef38143592ba4b4e7677a84a31916398d40f7
```

Once connected, all snapshot and policy `kopia` commands should work for the current user, but low-level commands such as `repo status` will fail:

```shell
$ kopia repository status
kopia: error: operation supported only on direct repository, try --help
```

You can override username and hostname with this command :
```shell
$ kopia repo connect server --url=http://11.222.111.222:51515 --override-username=johndoe --override-hostname=my-laptop
```

## Server Access Control (ACL)

Kopia server will check permissions when users try to access contents and manifests based on rules we call ACLs (access control list).

Prior to v0.8, the rules were non-configurable and each user could only read and write their own
snapshot manifests. Starting in Kopia v0.8 the ACLs can be controlled by using `kopia server acl` commands. 

If no ACLs are explicitly defined, Kopia will use a set of built-in access control rules, which grants all authenticated users identified by `username@hostname` ability to:

* read and write policies for `username@hostname` and `username@hostname:/path`,
* read and write snapshots for `username@hostname:/path`,
* read `global` policy,
* read `host`-level policies for their own `hostname`,
* read and write `user`-level policies for their own `username@hostname`,
* read and write their own `user` account `username@hostname` (to be able to change password),
* read objects if they know their object IDs
* write new `content` to the repository

### Access control for individual files or directories

Kopia does not currently perform access control checks to verify that a user trying to access file or directory by object ID is the original owner of the file (because of Kopia's deduplication, two different users who have the same file will get the same object ID when snapshotting it).

This means that any user who knows of a valid object ID will get be able to restore its contents (by `kopia restore <object-id>` or `kopia show <object-id>` etc.). 

Users who currently are (or previously were) in possession of a file will be able to easily determine its object ID from one of the snapshot manifests, but it's impractical for other users to guess 128-bit or 256-bit object identifiers.

On the flip side, this allows easy sharing of files between users simply by exchanging object IDs and letting another user restore the object (either a single file or an entire directory) from the repository.

### Customizing ACL rules

Sometimes we want to be able to customize those rules, for example to allow some users to modify
`global` or `host`-level policies, to let one user see another user's snapshots, etc.

To enable ACL mode, run:

```shell
$ kopia server acl enable
```
 
This will install ACL entries matching the default rules. Let's take a look at the rules installed:

```shell
$ kopia server acl list
id:c95e5a13b0b0fc874e550677f5e89262 user:*@* access:APPEND target:type=content
id:633f998af93e00c715e6417cb41df0af user:*@* access:READ target:type=policy,policyType=global
id:de21ac7a55ec1d9cf939b4cb158cc85a user:*@* access:READ target:type=policy,hostname=OWN_HOST,policyType=host
id:2314795d54060ab84cdc7fcb6e6953c8 user:*@* access:FULL target:type=policy,username=OWN_USER,hostname=OWN_HOST
id:0245e5bc1eca296e775e137a9587d09e user:*@* access:FULL target:type=snapshot,hostname=OWN_HOST,username=OWN_USER
id:fb043d9e77299e182e9297d0f4601d9b user:*@* access:FULL target:type=user,username=OWN_USER@OWN_HOST
```

As you can see, all rules have unique identifiers (different for each repository) and each rule defines:

* The `user` the rule applies to (could be a wildcard pattern containing an asterisk (*) instead of `username` or `hostname`)
* The `access` level:
  * `READ` - allows reading but not writing
  * `APPEND` - allows reading and writing, but not deleting
  * `FULL` - allows full read/write/delete access
* The `target`, which specifies the manifests the rule applies to.
  
  The target specification consists of `key=value` pairs which must match the corresponding manifest labels. Each target must have a `type` label and (optionally) other labels that are type-specific.

Supported types are:

* `snapshot` with optional labels `username`, `hostname` and `path`
* `policy` with optional labels `username`, `hostname`, `path` and `policyType` (which must be one of `global`, `user`, `host` or `path`)
* `user` with optional label `username`
* `acl`

Only labels specified will be matched. The label values can be literals or one of two special values:

* `OWN_USER` - the user's own `username`
* `OWN_HOST` - the user's own `hostname`

### Defining ACL rules

To define an ACL rule use: 
```shell
$ kopia server acl add --user U --access A --target T
```

For example:

1. To grant user `alice@wonderland` the ability to modify global policy:

```shell
$ kopia server acl add --user alice@wonderland \
      --access FULL --target type=policy,policyType=global
```

2. To allow all users to see all snapshots in the system:

```shell
$ kopia server acl add --user "*@*" --access READ --target type=snapshot
```

3. To give `princess@tall-tower` the ability to modify that hosts' policy:

```shell
$ kopia server acl add --user "princess@tall-tower" --access FULL \
    --target type=policy,policyType=host,hostname=tall-tower
```

4. To give `admin@somehost` the ability to add new user accounts and change passwords for existing users:

```shell
$ kopia server acl add --user "admin@somehost" --access FULL --target type=user
```

5. To give `superadmin@somehost` the ability to define new ACLs:

```shell
$ kopia server acl add --user "superadmin@somehost" \
    --access FULL --target type=acl
```
### Deleting ACL rules

To delete a single ACL rule, use `kopia server acl remove` passing the identifier of the entry:

```shell
$ kopia server acl remove 77fb0a5706ddbd9848294b36aae47079
```

To delete all ACL rules and revert to default set of rules, use:

```shell
$ kopia server acl remove --all
```

Both commands default to preview mode and must be confirmed by passing `--delete` for safety.

## Reloading server configuration 

Kopia server will refresh its configuration by fetching it from repository periodically. To speed up this process after changing access control rules, adding or modifying users or to simply force server to discover new snapshots or policies, you may want to run:

```shell
$ kopia server refresh \
  --address=https://server:port [--server-cert-fingerprint=FINGERPRINT] \
  --server-username=U --server-password=P
```

To authenticate, pass any valid username and password configured on the server.

Alternatively on Linux/macOS you can send `SIGHUP` signal to a running Kopia process to trigger a refresh.

```shell
$ ps awx | grep kopia
74189 s003  U+     0:19.19 kopia server start ...

$ kill -SIGHUP 74189
```

or simply:

```
$ killall -SIGHUP kopia
```

## Kopia behind a reverse proxy

Kopia server can be run behind a reverse proxy. Here a working example for nginx.

```shell
server {
  listen 443 ssl http2;
  server_name mydomain.com;

  ssl_certificate_key /path/to/your/key.key;
  ssl_certificate /path/to/your/cert.crt;

  client_max_body_size 0;  # Allow unlimited upload size

  location / {
   grpc_pass grpcs://localhost:51515; # Adapt if your kopia is running on another server
  }
}
```

Make sure you use a recent nginx version (>=1.16) and you start your kopia server with a certificate (`--insecure` does not work), e.g.

```shell
kopia server start --address 0.0.0.0:51515 --tls-cert-file ~/my.cert --tls-key-file ~/my.key
```

You can now connect to your kopia server via reverse proxy with your domain: `mydomain.com:443`.
