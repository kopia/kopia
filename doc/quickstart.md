Quick Start
===

Kopia is a simple tool for managing encrypted backups in the cloud.

Key Concepts
---

* **Repository** is a [Content-Addressable Store](https://en.wikipedia.org/wiki/Content-addressable_storage) of files and directories (known as Objects) identified by their **Object IDs**.

    - Object ID is comprised of the type, the identifier of the block (Block ID) where the contents are stored and the encryption key. Example block ID:

    ```
    D7ce4f067c179664...e746337031644a.715b50351785a6...439637a2c8e50c7
    |\------------------------------/ \------------------------------/
    type          block id                      encryption key
    ```

    - Block IDs are derived from the contents of objects by using cryptographic hash, typically as one of [SHA-2](https://en.wikipedia.org/wiki/SHA-2) hash functions.
    - Encryption key is also derived from the contents of the object - this technique is known as [Convergent Encryption](https://en.wikipedia.org/wiki/Convergent_encryption)
    - Identical objects will have the same ID and thus will be stored only once with the same encryption key.
    - A person with access to the object can easily compute its Object ID (including encryption key), but the knowledge of block id is not enough to be able to retrieve the content.
    - Repository can be shared among many users as long as they all can compute the same object IDs

* **Vault** securely stores Object IDs, encrypted with user-specific password or passphrase
* **Blob Storage** stores unstructured, blocks of binary large objects (BLOBs).
  Supported backends include:

    - [Google Cloud Storage](https://cloud.google.com/storage/) (GCS)
    - [Amazon Simple Storage Service](https://aws.amazon.com/s3/) (S3)
    - Local or remote filesystem

Object IDs
---

There are three types of object IDs:

* **Data** or **Direct** object where the entire object is stored in a single block
* **List** objects, that store list of object IDs
* **Inline Text** and **Inline Binary** objects, that represent the contents of the very short files directly in the object IDs, encoded either as text or base64

Some examples (note that block IDs and encryption keys have been shortened for illustration purposes):

* `D7ce4f067c.715b503c8` - is a *Data* object stored in block `7ce4f067c` and encrypted with key `715b503c8`

* `L43637a2c8.715b50351` - is a *List* object stored in block `43637a2c8` and encrypted with key `715b50351`

* `Tquick brown fox` - is an *Inline Text* object representing the text `quick brown fox`

* `BAQIDBA` - is an *Inline Binary* object representing base-64 encoded bytes: `01 02 03 04`

Object Types
---

* **File** contents are store as binary
* **Directory** contents are stored as JSON-encoded objects storing file or subdirectory metadata and Object IDs of file/subdirectory contents.

