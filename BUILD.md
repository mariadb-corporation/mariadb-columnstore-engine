## Checking out the source

MariaDB Columnstore (MCS) is not meant to be built independently outside of the MariaDB server. Therefore you need to checkout the [server](https://github.com/MariaDB/server) code as a first step.

You can clone the MariaDB server repository from GitHub using a SSH connection:

```bash
git clone git@github.com:MariaDB/server.git

```

or via HTTPS:

```bash
git clone https://github.com/MariaDB/server.git

```

(Note, that if you are a first time contributor and don’t have any access rights to the server repository, you might want to create a fork of the MariaDB server repository, if you also want to contribute to the server and not only to MCS.)

MariaDB server contains many git submodules that need to be checked out with:

```bash
git submodule update --init --recursive --depth=1

```

The submodule initialisation also checks out this MCS repository. Again, if you don’t have developer rights to the MCS repository and want to contribute, you should create a fork of this repository and change the remote of the already cloned MCS repository.

If you want to switch the remote to your fork, you can achieve this by:

```bash
cd server/storage/columnstore/columnstore
git remote -v #this should return 
							#origin	https://github.com/mariadb-corporation/mariadb-columnstore-engine.git (fetch)
							#origin	https://github.com/mariadb-corporation/mariadb-columnstore-engine.git (push)
git remote remove origin
git remote add origin <HTTPS-URL-TO-YOUR-FORK>
```

As a next step check out the MCS branch you want to build and install:

```bash
cd server/storage/columnstore/columnstore
git checkout <columnstore-branch-you-want-to-build>
git config --global --add safe.directory `pwd`
```

## Build

Regarding dependencies: If this is the first time building MCS on your system you should either use the `./install-deps.sh` script or pass `--install-deps` to the `bootstrap_mcs.sh` script.

For development convenience, building the MariaDB server with MCS can be done with:

```
sudo -E build/bootstrap_mcs.sh
```

Tested for: Ubuntu:20.04/22.04, CentOS:7, Debian:10/11, RockyLinux:8