---
title: "Repository Server"
linkTitle: "Repository Server"
weight: 44
---

By default every user of Kopia repository directly connects to a underlying storage using read-write access. If the users who share repository don't completely trust each other, some malicious actors can delete repository data structures causing data loss for others.

Repository Server allows an instance of kopia to proxy access to the underlying storage and has Kopia clients proxy all access through it, only requiring username and password to talk to server without any knowledge of
repository storage credentials. 

In repository server mode, each user is limited to seeing their own snapshots and policy manifest without being able to access those from another user account. 

>NOTE: Only snapshot and policy manifests are access-controlled, not the underlying contents. If two users shared the same file, it will be backed using exactly the same same content IDs. The consequence of this is that if a third user can guess the content ID of files in the repository, they will be able to access the files. Because content IDs are one-way salted hashes of contents, in principle it should be impossible to guess content ID without possessing original content.

## Starting Repository Server

Repository Server should be started on a dedicated server in LAN, such that all clients can directly connect to it.

Before we can start repository server, we must first create a list of usernames and passwords that will be allowed access. The list must be put in a text file formatted using the [htpasswd](https://httpd.apache.org/docs/2.4/programs/htpasswd.html) utility from Apache. Each username must be named:

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

```
kopia server start --htpasswd-file ~/password.txt --tls-generate-cert --tls-cert-file ~/my.cert --tls-key-file ~/my.key --address 0.0.0.0:51515
```

This will generate TLS certificate and key files and store them in the provided paths (`~/my.cert` and `~/my.key` respectively). It will also print certificate SHA256 fingerprint, which will be used later:

```
SERVER CERT SHA256: 48537cce585fed39fb26c639eb8ef38143592ba4b4e7677a84a31916398d40f7
```

Note that when starting the server again the `--tls-generate-cert` must be omitted, otherwise the server will fail to start.

### Custom TLS Certificates

If a user has obtained custom certificate (for example from LetsEncrypt or another CA), using it is simply a matter of providing PEM-formatted certificate and key files on server startup.

To get SHA256 certificate of existing file use:

```
$ openssl x509 -in ~/my.cert -noout -fingerprint -sha256 | sed 's/://g' | cut -f 2 -d =
48537CCE585FED39FB26C639EB8EF38143592BA4B4E7677A84A31916398D40F7
```

### On Client Computer

Assuming we're on another machine running as `user1@host1`, we can now run the following command to connect to the repository (notice we're using fingerprint obtained before without `:` separators)

```
kopia repository connect server --url https://<address>:51515 \
  --server-cert-fingerprint 48537cce585fed39fb26c639eb8ef38143592ba4b4e7677a84a31916398d40f7
```

Once connected, all snapshot and policy `kopia` commands should work for the current user, but low-level commands such as `repo status` will fail:

```
$ kopia repository status
kopia: error: operation supported only on direct repository, try --help
```

You can override username and hostname with this command :
```shell
$ kopia repo connect server --url=http://11.222.111.222:51515 --override-username=johndoe --override-hostname=my-laptop
```
