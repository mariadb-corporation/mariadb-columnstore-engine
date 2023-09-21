Storage manager `Meta versioning` - is approach to startup any node from any available version.
CS uses StorageManager to communicate(send/receive data) with a S3 bucket.
StorageManager designed as a plugin which intercepts `write`, `open`, `read`, `append` etc. functions
and applies its own logic.
StroageManager locally has only 3 types of files - `journal`, `meta`, `cache`.
`Journal files` - files to be stored into S3 bucket by background `Synchronizer` thread.
`Meta files` - files which map CS .cdf files(data files) to S3 object files.
`Cache` - files which a cached locally.


`Create meta version.`

At certain intervals:
1. CMAPI sends command to ControllerNode to freeze any operation.
2. CMAPI sends command to create a meta version for each SM in cluster.
3. SM takes current one store it locally with name `metaversion_i`.
4. SM compresses and put on S3.
5. CMAPI sends command to Controller node to unfreeze.


```mermaid
sequenceDiagram
    participant MetaVersionNew
    participant StorageManager
    participant S3Bucket
    participant ControllerNode
    participant CMAPI
    CMAPI->>ControllerNode: Send command to freeze (similar to readonly)
    CMAPI->StorageManager: Send command to create a new meta version
    StorageManager->>MetaVersionNew: Create new meta version
    StorageManager->>S3Bucket: Pack and put file with meta version on S3
    StorageManager->>CMAPI: Send command with success/failure
    CMAPI->>ControllerNode: Send command command to unfreeze
```

`DML command`
1. SM intercepts `DML command`.
2. Checks `object` locally and on S3.
3. Updates `given` object or create new one.
4. Puts object on S3.
5. Checks old objects UID in previous meta versions, if object exists put UID into local file otherwise delete it from S3.

```mermaid
sequenceDiagram
    participant MetaVersion
    participant StorageManager
    participant S3Bucket
    participant ColumnstoreEngine
    participant MetaData
    participant MetaVersionObjects
    ColumnstoreEngine->>StorageManager: Handle `write(filename, offset)` to the file
    StorageManager->>MetaData: Translate `filename, offset` to meta `file, offset` and find related object's UIDs
    StorageManager->>S3Bucket: Request object from S3
    S3Bucket->>StorageManager: Send object to remote node in a cluster with SM running
    StorageManager->>StorageManager: Update data in given object. Create new UID
    StorageManager->>S3Bucket: Put updated object on S3 with new UID
    StorageManager->>MetaVersion: Check old object UID in meta version
    StorageManager->>S3Bucket: Delete old object if not exist in MetaVersion
    StorageManager->>MetaVersionObjects: Save UID of the old object to the file
    StorageManager->>ColumnstoreEngine: Return success
```

`On startup with meta version.`
1. Downloads meta version from S3 and unpacks it.

```mermaid
sequenceDiagram
    participant MetaVersion
    participant StorageManager
    participant S3Bucket
    StorageManager->>S3Bucket: Request meta version.
    S3Bucket->>StorageManager: Send meta version to worker node.
    StorageManager->>MetaVersion: Unpack metaversion.
```

`Remove meta version.`
1. Collects all objects UID related to a meta version i.(They all placed in the file)
2. Removes all objects from S3 bucket
3. Removes meta version i and all from i - 1 to 1 locally and from S3 bucket.

```mermaid
sequenceDiagram
    participant MetaVersion_i
    participant MetaVersionObjects_i
    participant StorageManager
    participant S3Bucket
    StorageManager->>MetaVersionObjects_i: Collect all objects UIDs.
    StorageManager->>S3Bucket: Remove all objects described in meta version object file.
    StorageManager->>MetaVersion_i: Remove meta version `i` and all previous from `i - 1` to 1.
    StorageManager->>S3Bucket: Remove meta version[i, 1] from S3.
```
