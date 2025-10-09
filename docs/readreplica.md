# MariaDB ColumnStore Read Replicas

## Overview

The read replicas feature in [MariaDB ColumnStore](https://mariadb.com/docs/columnstore/) enables horizontal scaling of read performance by incorporating read-only nodes into a multi-node cluster. These replicas differ from standard ColumnStore nodes by not running the WriteEngineServer process, which means they cannot handle write operations directly. Instead, any write queries attempted on a replica are automatically forwarded to a read-write (RW) node. Replicas utilize shared storage with other nodes in the cluster, ensuring data consistency without duplication. A key requirement is maintaining at least one RW node; a cluster consisting solely of read replicas is not operational and cannot process reads or writes. Currently, read-only nodes are incompatible with S3 as the storage backend. Additionally, there is no automatic promotion of a read replica to RW mode if the only RW node fails, which could lead to temporary downtime until manual intervention.

## Key Features

- **Horizontal Read Scaling**: Adds compute power for handling more read-intensive queries without impacting write performance.
- **Write Forwarding**: Ensures writes on replicas are redirected to RW nodes, maintaining data integrity.
- **Shared Storage**: Replicas access the same DBRoots as RW nodes, promoting efficiency and reducing storage overhead.

### Add Read Replica

To introduce a read-only node for scaling reads:  
`sudo mcs node add --read-replica --node <private-ip>`

### Remove Node

To safely remove any node (RW or replica) from the cluster:  
`sudo mcs node remove --node <private-ip>`  
This reassigns resources as needed without cluster disruption.

### Verify Status

Monitor the cluster's health and node roles:  
`sudo mcs cluster status`

## Limitations

- Node addition is restricted to private IPs only for now.
- Incompatible with S3 storage, limiting use to shared file systems.
- No automatic failover or promotion mechanism if the sole RW node goes down, requiring manual recovery.
- At least one RW node must always be present for the cluster to function properly, supporting both read and write operations.

## How To

### Prerequisites

Ensure shared storage is mounted on all nodes (at `/var/lib/columnstore/data1` for non-S3 configuration), to ensure data consistency across RW nodes and read replicas.  
Refer to the official [MariaDB ColumnStore Architectural Overview](https://mariadb.com/docs/columnstore/architecture/columnstore-architectural-overview#shared-local-storage) for exact mount points and shared storage setup.

### Installation and Setup

1. **Set Up MariaDB Repository**  
   Run the following to add the MariaDB repository (example using version 10.6; adjust for the latest stable version):  
   `curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | sudo bash -s -- --mariadb-server-version=10.6`  
   See the [MariaDB Package Repository Setup and Usage](https://mariadb.com/docs/server/server-management/install-and-upgrade-mariadb/installing-mariadb/binary-packages/mariadb-package-repository-setup-and-usage) for more details.

2. **Install Packages**  
   On all nodes:  
   For RPM-based systems:  
   `sudo dnf install -y MariaDB-server MariaDB-columnstore-engine MariaDB-columnstore-cmapi`  
   Refer to the [MariaDB ColumnStore Quick Start Guide](https://mariadb.com/resources/blog/mariadb-columnstore-quick-start-guide/) for additional context.

   For DEB-based systems:  
   `sudo apt update`  
   `sudo apt install -y mariadb-server mariadb-plugin-columnstore mariadb-columnstore-cmapi`

3. **Start and Enable Services**  
   `sudo systemctl start mariadb`  
   `sudo systemctl enable mariadb`  
   `sudo systemctl start mariadb-columnstore-cmapi`  
   `sudo systemctl enable mariadb-columnstore-cmapi`

4. **Configure the Initial RW Node**  
   On the primary RW node, set up the cluster API key (replace `your-api-key-here` with a secure key):  
   `sudo mcs cluster set api-key --key your-api-key-here`

5. **Add the Initial RW Node to the Cluster**  
   From the primary RW node:  
   `sudo mcs node add --node <private-ip-of-rw-node>`

6. **Add Read Replica Nodes**  
   From the primary RW node, add each read replica:  
   `sudo mcs node add --read-replica --node <private-ip-of-replica>`

7. **Verify the Cluster**  
   Check the status to ensure nodes are added and the cluster is healthy:  
   `sudo mcs cluster status`

For setting up MaxScale for query routing or handling failover, check the official [MariaDB ColumnStore documentation](https://mariadb.com/docs/columnstore/). Ensure at least one RW node is always present, as replicas cannot operate independently.