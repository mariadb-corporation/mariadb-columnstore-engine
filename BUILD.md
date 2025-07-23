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
git submodule update --init --recursive

```

The submodule initialisation also checks out this MCS repository. Again, if you don’t have developer rights to the MCS repository and want to contribute, you should create a fork of this repository and change the remote of the already cloned MCS repository.

If you want to switch the remote to your fork, you can achieve this by:

```bash
cd server/storage/columnstore/columnstore
git remote -v #this should return: origin https://github.com/mariadb-corporation/mariadb-columnstore-engine.git (fetch)
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

The `bootstrap_mcs.sh` script can now do **two** main things:

1. **Build & install** ColumnStore into your system
```bash
cd server/storage/columnstore/columnstore

sudo build/bootstrap_mcs.sh  --install-deps
```

2. **Build native OS packages** (RPM or DEB)

```bash
cd server/storage/columnstore/columnstore
sudo build/bootstrap_mcs.sh  --install-deps --build-packages
# → find your .rpm/.deb files in the build directory
```
Note: Packages can be built only for the OS you’re on—for so for example if you are running --build-packages on Rocky Linux it will produce RPMs for Rocky.
You can see the full options list in the script itself

> **Supported distros:**  
> Ubuntu:20.04/22.04/24.04, Debian:11/12, Rocky Linux:8/9



