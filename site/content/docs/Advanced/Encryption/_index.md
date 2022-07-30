---
title: "Encryption"
linkTitle: "Encryption"
weight: 40
---

### Format Blob Encryption

Kopia uses a standard envelope encryption technique to de-couple the repository passphrase from the keys used for encrypting and authenticating the contents of the repository.

Each repository has a format blob containing configuration parameters for the repository. The format blob itself is serialized and persistent as JSON, such as the one below.

```json
{
  "tool": "https://github.com/kopia/kopia",
  "buildVersion": "v0.3.0",
  "buildInfo": "unknown",
  "uniqueID": "bm90IGEgZ29vZCByYW5kb20gaW5pdGlhbCB2YWx1ZQ==",
  "keyAlgo": "scrypt-65536-8-1",
  "version": "1",
  "encryption": "AES256_GCM",
  "encryptedBlockFormat": "ZSxqt/gXwVPS6pNvFZOOHlCKr18TmziuUnN8nnuvwQ/+mjbcvEHUfKS11RJl/sWrIOyiYqpSwAZt
BzOxQAUWQt7vHc2ZT4Y75ODrPHhzd0CcBlqHa3HSx8pxIXqpgMy5K3xDvIkBN1qdb/cTyU5s9lZ2
J+rBh2CGQ3phIlKFCCuS3lgB+rnYRrExGg5rm4BUmZZWHfBPQ7QiOJNg86PK+n///dejsAA/+FBj
qIODf7Gr3ppaGZeAHYJuz0BkRSPC4XIAgFj1yPo="
}
```

The corresponding Go struct is the following:

```go
type formatBlob struct {
	Tool         string `json:"tool"`
	BuildVersion string `json:"buildVersion"`
	BuildInfo    string `json:"buildInfo"`

	UniqueID               []byte `json:"uniqueID"`
	KeyDerivationAlgorithm string `json:"keyAlgo"`

	Version              string                  `json:"version"`
	EncryptionAlgorithm  string                  `json:"encryption"`
	EncryptedFormatBytes []byte                  `json:"encryptedBlockFormat,omitempty"`
	UnencryptedFormat    *format.RepositoryObjectFormat `json:"blockFormat,omitempty"`
}
```

* `Version` identifies the version of the format blob.
* The `tool` and `buildInfo` fields are informational
* `buildVersion` is the version of Kopia, which is also indirectly used as the repository format version.
* `UniqueID` is a randomly generated identifier for the repository. This is also used as the input for various encryption operations.
* `keyAlgo` identifies the password-based key derivation function (PBKDF). Only _scrypt_ with the currently recommended cost parameters (N=65536, r=8, p=1) is supported at the moment. The main purpose of this field is to be able to change and extend the key derivation algorithm in the future. For example, by increasing the cost parameters for _scrypt_ or using a different algorithm altogether.
* `encryption` identifies the encryption algorithm that was used to encrypt the encryptedBlockFormat field.
* `encryptedBlockFormat` is a ciphertext containing among others, the encryption secrets and parameters used for encrypting the repository content. Below is additional information about its plaintext content and how it is encrypted.
* Alternatively, the unencrypted block format parameters can be specified in the the `blockFormat` field.

The `formatBlob.EncryptedBlockFormat` field is the result of encrypting a JSON-serialized version of the `EncryptedRepositoryConfig` struct shown below. The plaintext version contains the parameters for performing block chunking, as well as for encrypting and authenticating "content" objects.


```go
type EncryptedRepositoryConfig struct {
	Format RepositoryObjectFormat `json:"format"`
}

type RepositoryObjectFormat struct {
	format.ContentFormat
	format.ObjectFormat
}
```

```go
package content

// ContentFormat describes the rules for formatting contents in repository.
type ContentFormat struct {
	Version     int    `json:"version,omitempty"`     // version number, must be "1"
	Hash        string `json:"hash,omitempty"`        // identifier of the hash algorithm used
	Encryption  string `json:"encryption,omitempty"`  // identifier of the encryption algorithm used
	HMACSecret  []byte `json:"secret,omitempty"`      // HMAC secret used to generate encryption keys
	MasterKey   []byte `json:"masterKey,omitempty"`   // master encryption key (SIV-mode encryption only)
	MaxPackSize int    `json:"maxPackSize,omitempty"` // maximum size of a pack object
}
```

```go
package object

type Format struct {
	Splitter string `json:"splitter,omitempty"` // splitter used to break objects into pieces of content
}
```

The ciphertext in `formatBlob.encryptedBlockFormat` is obtained by:

1. Serializing to JSON the populated encryptedRepositoryConfig struct.
2. Encrypting the resulting byte stream with the algorithm specified in `formatBlob.encryption`. At the moment, `AES256_GCM` is used by default, which is both encrypted and authenticated. The input parameters are:
    * "Plaintext": serialized JSON byte stream
    * "IV": randomly generated and prepended to the ciphertext in `formatBlob.encryptedBlockFormat`
    * Key: 256-bit AES encryption key derived from the passphrase, details below.
    * Additional Data (AD): Used as "authentication data", the value is derived from the passphrase as well. See below.

The passphrase is used to generate the encryption key and _additional data_ (AD) for the encryption and decryption of `formatBlob.encryptedBlockFormat` ciphertext as follows:

* A master key (Km) is derived from the password by using (a) the password-based key derivation function specified in `formatBlob.keyAlgo`, and (b) `formatBlob.UniqueID` as the salt. The resulting key is 32-bytes long (256 bits). `Km = PBKDF( passphrase, formatBlob.UniqueID, … cost parameters)`.
* The AES-256 encryption key (Ke) is derived from Km by using a hash-based key derivation function (HKDF), with SHA256 as the hash. `Ke = HKDF(SHA256, Km, formatBlob.UniqueID, "AES", 32)`
* The additional data (AD) is derived using an HKDF as follows: `AD = HKDF(SHA256, Km, formatBlob.UniqueID, "CHECKSUM", 32)`
