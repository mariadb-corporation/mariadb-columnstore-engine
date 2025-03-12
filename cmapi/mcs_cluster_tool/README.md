# `mcs`

The  MCS  Command  Line  Interface is a unified tool to manage your MCS services

**Usage**:

```console
$ mcs [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `backup`: Backup Columnstore and/or MariDB data.
* `dbrm_backup`: Columnstore DBRM Backup.
* `restore`: Restore Columnstore (and/or MariaDB) data.
* `dbrm_restore`: Restore Columnstore DBRM data.
* `cskeys`: Generates a random AES encryption key and init vector and writes them to disk.
* `cspasswd`: Encrypt a Columnstore plaintext password using the encryption key in the key file.
* `help-all`: Show help for all commands in man page style.
* `status`: Get status information.
* `stop`: Stop the Columnstore cluster.
* `start`: Start the Columnstore cluster.
* `restart`: Restart the Columnstore cluster.
* `node`: Cluster nodes management.
* `set`: Set cluster parameters.
* `cluster`: MariaDB Columnstore cluster management...
* `cmapi`: CMAPI itself related commands.

## `mcs backup`

Backup Columnstore and/or MariDB data.

**Usage**:

```console
$ mcs backup [OPTIONS]
```

**Options**:

* `-bl, --backup-location TEXT`: What directory to store the backups on this machine or the target machine.
Consider write permissions of the scp user and the user running this script.
Mariadb-backup will use this location as a tmp dir for S3 and remote backups temporarily.
Example: /mnt/backups/  [default: /tmp/backups/]
* `-bd, --backup-destination TEXT`: Are the backups going to be stored on the same machine this script is running on or another server - if Remote you need to setup scp=Options: &quot;Local&quot; or &quot;Remote&quot;  [default: Local]
* `-scp TEXT`: Used only if --backup-destination=&quot;Remote&quot;.
The user/credentials that will be used to scp the backup files
Example: &quot;centos@10.14.51.62&quot;
* `-bb, --backup-bucket TEXT`: Only used if --storage=S3
Name of the bucket to store the columnstore backups.
Example: &quot;s3://my-cs-backups&quot;
* `-url, --endpoint-url TEXT`: Used by on premise S3 vendors.
Example: &quot;http://127.0.0.1:8000&quot;
* `-s, --storage TEXT`: What storage topogoly is being used by Columnstore - found in /etc/columnstore/storagemanager.cnf.
Options: &quot;LocalStorage&quot; or &quot;S3&quot;  [default: LocalStorage]
* `-i, --incremental TEXT`: Adds columnstore deltas to an existing full backup. Backup folder to apply increment could be a value or &quot;auto_most_recent&quot; - the incremental backup applies to last full backup.
* `-P, --parallel INTEGER`: Determines if columnstore data directories will have multiple rsync running at the same time for different subfolders to parallelize writes. Ignored if &quot;-c/--compress&quot; argument not set.  [default: 4]
* `-ha, --highavilability / -no-ha, --no-highavilability`: Hint wether shared storage is attached @ below on all nodes to see all data
HA LocalStorage ( /var/lib/columnstore/dataX/ )
HA S3           ( /var/lib/columnstore/storagemanager/ )  [default: no-ha]
* `-f, --config-file TEXT`: Path to backup configuration file to load variables from - relative or full path accepted.
* `-sbrm, --skip-save-brm / -no-sbrm, --no-skip-save-brm`: Skip saving brm prior to running a backup - ideal for dirty backups.  [default: no-sbrm]
* `-spoll, --skip-polls / -no-spoll, --no-skip-polls`: Skip sql checks confirming no write/cpimports running.  [default: no-spoll]
* `-slock, --skip-locks / -no-slock, --no-skip-locks`: Skip issuing write locks - ideal for dirty backups.  [default: no-slock]
* `-smdb, --skip-mariadb-backup / -no-smdb, --no-skip-mariadb-backup`: Skip running a mariadb-backup for innodb data - ideal for incremental dirty backups.  [default: no-smdb]
* `-sb, --skip-bucket-data / -no-sb, --no-skip-bucket-data`: Skip taking a copy of the columnstore data in the bucket.  [default: no-sb]
* `-nb, --name-backup TEXT`: Define the name of the backup - default: $(date +%m-%d-%Y)  [default: 03-20-2025]
* `-c, --compress TEXT`: Compress backup in X format - Options: [ pigz ].
* `-q, --quiet / -no-q, --no-quiet`: Silence verbose copy command outputs.  [default: no-q]
* `-nv-ssl, --no-verify-ssl / -v-ssl, --verify-ssl`: Skips verifying ssl certs, useful for onpremise s3 storage.  [default: v-ssl]
* `-pi, --poll-interval INTEGER`: Number of seconds between poll checks for active writes &amp; cpimports.  [default: 5]
* `-pmw, --poll-max-wait INTEGER`: Max number of minutes for polling checks for writes to wait before exiting as a failed backup attempt.  [default: 60]
* `-r, --retention-days INTEGER`: Retain backups created within the last X days, default 0 == keep all backups.  [default: 0]
* `-aro, --apply-retention-only`: Only apply retention policy to existing backups, does not run a backup.
* `-li, --list`: List backups.
* `--help`: Show this message and exit.

## `mcs dbrm_backup`

Columnstore DBRM Backup.

**Usage**:

```console
$ mcs dbrm_backup [OPTIONS]
```

**Options**:

* `-i, --interval INTEGER`: Number of minutes to sleep when --mode=loop.  [default: 90]
* `-r, --retention-days INTEGER`: Retain dbrm backups created within the last X days, the rest are deleted  [default: 7]
* `-bl, --backup-location TEXT`: Path of where to save the dbrm backups on disk.  [default: /tmp/dbrm_backups]
* `-m, --mode TEXT`: &quot;loop&quot; or &quot;once&quot; ; Determines if this script runs in a forever loop sleeping -i minutes or just once.  [default: once]
* `-nb, --name-backup TEXT`: Define the prefix of the backup - default: dbrm_backup+date +%Y%m%d_%H%M%S  [default: dbrm_backup]
* `-ssm, --skip-storage-manager / -no-ssm, --no-skip-storage-manager`: Skip backing up storagemanager directory.  [default: no-ssm]
* `-q, --quiet / -no-q, --no-quiet`: Silence verbose copy command outputs.  [default: no-q]
* `-li, --list`: List backups.
* `--help`: Show this message and exit.

## `mcs restore`

Restore Columnstore (and/or MariaDB) data.

**Usage**:

```console
$ mcs restore [OPTIONS]
```

**Options**:

* `-l, --load TEXT`: What date folder to load from the backup_location.
* `-bl, --backup-location TEXT`: Where the backup to load is found.
Example: /mnt/backups/  [default: /tmp/backups/]
* `-bd, --backup_destination TEXT`: Is this backup on the same or remote server compared to where this script is running.
Options: &quot;Local&quot; or &quot;Remote&quot;  [default: Local]
* `-scp, --secure-copy-protocol TEXT`: Used only if --backup-destination=RemoteThe user/credentials that will be used to scp the backup files.Example: &quot;centos@10.14.51.62&quot;
* `-bb, --backup-bucket TEXT`: Only used if --storage=S3
Name of the bucket to store the columnstore backups.
Example: &quot;s3://my-cs-backups&quot;
* `-url, --endpoint-url TEXT`: Used by on premise S3 vendors.
Example: &quot;http://127.0.0.1:8000&quot;
* `-s, --storage TEXT`: What storage topogoly is being used by Columnstore - found in /etc/columnstore/storagemanager.cnf.
Options: &quot;LocalStorage&quot; or &quot;S3&quot;  [default: LocalStorage]
* `-dbs, --dbroots INTEGER`: Number of database roots in the backup.  [default: 1]
* `-pm, --nodeid TEXT`: Forces the handling of the restore as this node as opposed to whats detected on disk.
* `-nb, --new-bucket TEXT`: Defines the new bucket to copy the s3 data to from the backup bucket. Use -nb if the new restored cluster should use a different bucket than the backup bucket itself.
* `-nr, --new-region TEXT`: Defines the region of the new bucket to copy the s3 data to from the backup bucket.
* `-nk, --new-key TEXT`: Defines the aws key to connect to the new_bucket.
* `-ns, --new-secret TEXT`: Defines the aws secret of the aws key to connect to the new_bucket.
* `-P, --parallel INTEGER`: Determines number of decompression and mdbstream threads. Ignored if &quot;-c/--compress&quot; argument not set.  [default: 4]
* `-ha, --highavilability / -no-ha, --no-highavilability`: Flag for high available systems (meaning shared storage exists supporting the topology so that each node sees all data)  [default: no-ha]
* `-cont, --continue / -no-cont, --no-continue`: This acknowledges data in your --new_bucket is ok to delete when restoring S3. When set to true skips the enforcement that new_bucket should be empty prior to starting a restore.  [default: no-cont]
* `-f, --config-file TEXT`: Path to backup configuration file to load variables from - relative or full path accepted.
* `-smdb, --skip-mariadb-backup / -no-smdb, --no-skip-mariadb-backup`: Skip restoring mariadb server via mariadb-backup - ideal for only restoring columnstore.  [default: no-smdb]
* `-sb, --skip-bucket-data / -no-sb, --no-skip-bucket-data`: Skip restoring columnstore data in the bucket - ideal if looking to only restore mariadb server.  [default: no-sb]
* `-c, --compress TEXT`: Hint that the backup is compressed in X format. Options: [ pigz ].
* `-q, --quiet / -no-q, --no-quiet`: Silence verbose copy command outputs.  [default: no-q]
* `-nv-ssl, --no-verify-ssl / -v-ssl, --verify-ssl`: Skips verifying ssl certs, useful for onpremise s3 storage.  [default: v-ssl]
* `-li, --list`: List backups.
* `--help`: Show this message and exit.

## `mcs dbrm_restore`

Restore Columnstore DBRM data.

**Usage**:

```console
$ mcs dbrm_restore [OPTIONS]
```

**Options**:

* `-bl, --backup-location TEXT`: Path of where dbrm backups exist on disk.  [default: /tmp/dbrm_backups]
* `-l, --load TEXT`: Name of the directory to restore from -bl
* `-ns, --no-start`: Do not attempt columnstore startup post dbrm_restore.
* `-sdbk, --skip-dbrm-backup / -no-sdbk, --no-skip-dbrm-backup`: Skip backing up dbrms before restoring.  [default: sdbk]
* `-ssm, --skip-storage-manager / -no-ssm, --no-skip-storage-manager`: Skip backing up storagemanager directory.  [default: ssm]
* `-li, --list`: List backups.
* `--help`: Show this message and exit.

## `mcs cskeys`

This utility generates a random AES encryption key and init vector
and writes them to disk. The data is written to the file &#x27;.secrets&#x27;,
in the specified directory. The key and init vector are used by
the utility &#x27;cspasswd&#x27; to encrypt passwords used in Columnstore
configuration files, as well as by Columnstore itself to decrypt the
passwords.

WARNING: Re-creating the file invalidates all existing encrypted
passwords in the configuration files.

**Usage**:

```console
$ mcs cskeys [OPTIONS] [DIRECTORY]
```

**Arguments**:

* `[DIRECTORY]`: The directory where to store the file in.  [default: /var/lib/columnstore]

**Options**:

* `-u, --user TEXT`: Designate the owner of the generated file.  [default: mysql]
* `--help`: Show this message and exit.

## `mcs cspasswd`

Encrypt a Columnstore plaintext password using the encryption key in
the key file.

**Usage**:

```console
$ mcs cspasswd [OPTIONS]
```

**Options**:

* `--password TEXT`: Password to encrypt/decrypt  [required]
* `--decrypt`: Decrypt an encrypted password instead.
* `--help`: Show this message and exit.

## `mcs help-all`

Show help for all commands in man page style.

**Usage**:

```console
$ mcs help-all [OPTIONS]
```

## `mcs status`

Get status information.

**Usage**:

```console
$ mcs status [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `mcs stop`

Stop the Columnstore cluster.

**Usage**:

```console
$ mcs stop [OPTIONS]
```

**Options**:

* `-i, --interactive / -no-i, --no-interactive`: Use this option on active cluster as interactive stop waits for current writes to complete in DMLProc before shutting down. Ensuring consistency, preventing data loss of active writes.  [default: no-interactive]
* `-t, --timeout INTEGER`: Time in seconds to wait for DMLproc to gracefully stop.Warning: Low wait timeout values could result in data loss if the cluster is very active.In interactive mode means delay time between promts.  [default: 15]
* `--help`: Show this message and exit.

## `mcs start`

Start the Columnstore cluster.

**Usage**:

```console
$ mcs start [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `mcs restart`

Restart the Columnstore cluster.

**Usage**:

```console
$ mcs restart [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

## `mcs node`

Cluster nodes management.

**Usage**:

```console
$ mcs node [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `add`: Add nodes to the Columnstore cluster.
* `remove`: Remove nodes from the Columnstore cluster.

### `mcs node add`

Add nodes to the Columnstore cluster.

**Usage**:

```console
$ mcs node add [OPTIONS]
```

**Options**:

* `--node TEXT`: node IP, name or FQDN. Can be used multiple times to add several nodes at a time.  [required]
* `--help`: Show this message and exit.

### `mcs node remove`

Remove nodes from the Columnstore cluster.

**Usage**:

```console
$ mcs node remove [OPTIONS]
```

**Options**:

* `--node TEXT`: node IP, name or FQDN. Can be used multiple times to remove several nodes at a time.  [required]
* `--help`: Show this message and exit.

## `mcs set`

Set cluster parameters.

**Usage**:

```console
$ mcs set [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `mode`: Set Columnstore cluster mode.
* `api-key`: Set API key for communication with cluster...
* `log-level`: Set logging level on all cluster nodes for...

### `mcs set mode`

Set Columnstore cluster mode.

**Usage**:

```console
$ mcs set mode [OPTIONS]
```

**Options**:

* `--mode TEXT`: cluster mode to set. &quot;readonly&quot; or &quot;readwrite&quot; are the only acceptable values.  [required]
* `--help`: Show this message and exit.

### `mcs set api-key`

Set API key for communication with cluster nodes via API.

WARNING: this command will affect API key value on all cluster nodes.

**Usage**:

```console
$ mcs set api-key [OPTIONS]
```

**Options**:

* `--key TEXT`: API key to set.  [required]
* `--help`: Show this message and exit.

### `mcs set log-level`

Set logging level on all cluster nodes for develop purposes.

WARNING: this could dramatically affect the number of log lines.

**Usage**:

```console
$ mcs set log-level [OPTIONS]
```

**Options**:

* `--level TEXT`: Logging level to set.  [required]
* `--help`: Show this message and exit.

## `mcs cluster`

MariaDB Columnstore cluster management command line tool.

**Usage**:

```console
$ mcs cluster [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `status`: Get status information.
* `stop`: Stop the Columnstore cluster.
* `start`: Start the Columnstore cluster.
* `restart`: Restart the Columnstore cluster.
* `node`: Cluster nodes management.
* `set`: Set cluster parameters.

### `mcs cluster status`

Get status information.

**Usage**:

```console
$ mcs cluster status [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `mcs cluster stop`

Stop the Columnstore cluster.

**Usage**:

```console
$ mcs cluster stop [OPTIONS]
```

**Options**:

* `-i, --interactive / -no-i, --no-interactive`: Use this option on active cluster as interactive stop waits for current writes to complete in DMLProc before shutting down. Ensuring consistency, preventing data loss of active writes.  [default: no-interactive]
* `-t, --timeout INTEGER`: Time in seconds to wait for DMLproc to gracefully stop.Warning: Low wait timeout values could result in data loss if the cluster is very active.In interactive mode means delay time between promts.  [default: 15]
* `--help`: Show this message and exit.

### `mcs cluster start`

Start the Columnstore cluster.

**Usage**:

```console
$ mcs cluster start [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `mcs cluster restart`

Restart the Columnstore cluster.

**Usage**:

```console
$ mcs cluster restart [OPTIONS]
```

**Options**:

* `--help`: Show this message and exit.

### `mcs cluster node`

Cluster nodes management.

**Usage**:

```console
$ mcs cluster node [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `add`: Add nodes to the Columnstore cluster.
* `remove`: Remove nodes from the Columnstore cluster.

#### `mcs cluster node add`

Add nodes to the Columnstore cluster.

**Usage**:

```console
$ mcs cluster node add [OPTIONS]
```

**Options**:

* `--node TEXT`: node IP, name or FQDN. Can be used multiple times to add several nodes at a time.  [required]
* `--help`: Show this message and exit.

#### `mcs cluster node remove`

Remove nodes from the Columnstore cluster.

**Usage**:

```console
$ mcs cluster node remove [OPTIONS]
```

**Options**:

* `--node TEXT`: node IP, name or FQDN. Can be used multiple times to remove several nodes at a time.  [required]
* `--help`: Show this message and exit.

### `mcs cluster set`

Set cluster parameters.

**Usage**:

```console
$ mcs cluster set [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `mode`: Set Columnstore cluster mode.
* `api-key`: Set API key for communication with cluster...
* `log-level`: Set logging level on all cluster nodes for...

#### `mcs cluster set mode`

Set Columnstore cluster mode.

**Usage**:

```console
$ mcs cluster set mode [OPTIONS]
```

**Options**:

* `--mode TEXT`: cluster mode to set. &quot;readonly&quot; or &quot;readwrite&quot; are the only acceptable values.  [required]
* `--help`: Show this message and exit.

#### `mcs cluster set api-key`

Set API key for communication with cluster nodes via API.

WARNING: this command will affect API key value on all cluster nodes.

**Usage**:

```console
$ mcs cluster set api-key [OPTIONS]
```

**Options**:

* `--key TEXT`: API key to set.  [required]
* `--help`: Show this message and exit.

#### `mcs cluster set log-level`

Set logging level on all cluster nodes for develop purposes.

WARNING: this could dramatically affect the number of log lines.

**Usage**:

```console
$ mcs cluster set log-level [OPTIONS]
```

**Options**:

* `--level TEXT`: Logging level to set.  [required]
* `--help`: Show this message and exit.

## `mcs cmapi`

CMAPI itself related commands.

**Usage**:

```console
$ mcs cmapi [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--help`: Show this message and exit.

**Commands**:

* `is-ready`: Check CMAPI is ready to handle requests.

### `mcs cmapi is-ready`

Check CMAPI is ready to handle requests.

**Usage**:

```console
$ mcs cmapi is-ready [OPTIONS]
```

**Options**:

* `--node TEXT`: Which node to check the CMAPI is ready to handle requests.  [default: 127.0.0.1]
* `--help`: Show this message and exit.
