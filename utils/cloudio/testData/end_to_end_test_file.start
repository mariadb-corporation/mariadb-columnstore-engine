# MariaDB ColumnStore Server (version 1.2)

This is the server part of MariaDB ColumnStore 1.2.
MariaDB ColumnStore 1.2 is an GA of MariaDB ColumnStore.
It is built by porting InfiniDB 4.6.7 on MariaDB 10.3 and adding entirely
new features not found anywhere else.

## MariaDB ColumnStore Engine (version 1.2)

MariaDB ColumnStore also requires the matching engine version. This can be found at https://github.com/mariadb-corporation/mariadb-columnstore-engine.

Always match the server engine / git branch with the engine git branch.

Currently building has only been certified on CentOS 6 and 7, Ubuntu 16.04, Debain 8, and SUSE 12.. 
Building on other platforms will be certified in a later release.

## Issue tracking

Issue tracking of MariaDB ColumnStore happens in JIRA, https://jira.mariadb.org/browse/MCOL

### The structure of this repository is:

* Branch "master" - this is the latest released version of the source code.  Each major release is tagged.
* Branch "develop-1.0" - this is the 1.0 legacy mainline development branch.
* Branch "develop-1.1" - this is the 1.1 mainline development branch.
* Branch "develop" - this is the 1.2 unstable development branch.
* Branch "mcol-xxx" - these are specific bug and feature branches. These are merged into development which is merged to master.

MariaDB ColumnStore server and the engine are in separate repositories.

## Contributing

To contribute to ColumnStore please see the [Contributions Documentation](CONTRIBUTING.md).

## Build dependencies

### Boost Libraries

MariaDB ColumnStore requires that the boost package of 1.53 or newer is installed for both building and executing

For CentOS 7, Ubuntu 16, Debian 8, SUSE 12 and other newer OS's, you can just install the boost packages via yum or apt-get.

```bash
yum install boost-devel
```

or

```bash
apt-get install libboost-dev-all
```
or

```bash
SUSEConnect -p sle-sdk/12.2/x86_64

zypper install boost-devel
```

For CentOS 6, you can either download and install the MariaDB Columnstore Centos 6 boost library package or install the boost source of 1.55 and build it to generate the required libraries. That means both the build and the install machines require this.

Downloading and installing the MariaDB Columnstore Centos 6 boost library package is documented here:

https://mariadb.com/kb/en/mariadb/preparing-for-columnstore-installation/#boost-libraries

Downloading and build the boost libraries:

NOTE: This means that the "Development Tools" group install be done prior to this.

```bash
yum groupinstall "Development Tools"
yum install cmake
```

Here is the procedure to download and build the boost source:

```bash
cd /usr/

wget http://sourceforge.net/projects/boost/files/boost/1.55.0/boost_1_55_0.tar.gz

tar zxvf boost_1_55_0.tar.gz

cd boost_1_55_0

./bootstrap.sh --with-libraries=atomic,date_time,exception,filesystem,iostreams,locale,program_options,regex,signals,system,test,thread,timer,log --prefix=/usr

./b2 install

ldconfig
```

### For CentOS

These packages need to be install along with the group development packages:

```bash
yum groupinstall "Development Tools"
yum install bison ncurses-devel readline-devel perl-devel openssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel python-devel ruby-devel tree wget pam-devel snappy-devel
```

### For Ubuntu 16/18

```bash
apt-get install build-essential automake libboost-all-dev bison cmake libncurses5-dev libreadline-dev libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev libsnappy-dev
```
### For Debian 8

```bash
apt-get install build-essential automake libboost-all-dev bison cmake libncurses5-dev libreadline-dev libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev libkrb5-dev libsnappy-dev
```

### For Debian 9

```bash
apt-get install build-essential automake libboost-all-dev bison cmake libncurses5-dev libreadline-dev libperl-dev libssl1.0-dev libxml2-dev libkrb5-dev flex libpam-dev libkrb5-dev libsnappy-dev libgmp-dev
apt-get install libxml2-dev lmodern openjdk-8-jdk openssl pkg-config scons texlive-latex-base texlive-latex-base-doc thrift-compiler ttf-dejavu ttf-dejavu-extra unixodbc-dev uuid-dev xfonts-encodings xfonts-utils zlib1g-dev
```
### For Suse 12

These packages need to be install along with the group development packages:

```bash
zypper se -t pattern devel
zypper install bison ncurses-devel readline-devel perl-devel openssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel python-devel ruby-devel tree wget pam-devel snappy-devel
```


## Building master branch

The current master branch is the released version.

## Building develop branch

The develop branch is used for develop updates

Building can be done as a non-root user. If you do a "build install", it will install the binaries in `/usr/local/mariadb/columnstore`
and the use of sudo is required.

To build the current development branch binaries only (Engine checkout inside Server):
```bash
git clone https://github.com/mariadb-corporation/mariadb-columnstore-server.git
cd mariadb-columnstore-server
git checkout develop # switch to develop code
cmake . -DCMAKE_INSTALL_PREFIX=/usr/local/mariadb/columnstore/mysql
make -jN # N is the number of concurrent build processes and should likely be the number of cores available
sudo make install
git clone https://github.com/mariadb-corporation/mariadb-columnstore-engine.git
cd mariadb-columnstore-engine
git checkout develop
cmake .
make -jN # same as above with respect to concurrent processes
sudo make install
```

To build the current development branch binaries and packages only (Engine checkout inside Server):
```bash
git clone https://github.com/mariadb-corporation/mariadb-columnstore-server.git
cd mariadb-columnstore-server
git checkout develop # switch to develop code
run cmake
For RPMs:
cmake . -DWITH_READLINE=1 -DRPM=centos6 -DPLUGIN_CONNECT=NO -DWITH_WSREP=OFF -DINSTALL_LAYOUT=RPM -DCMAKE_INSTALL_PREFIX=/usr/local/mariadb/columnstore/mysql  -DCPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION=/usr/local
For DEBIANs:
cmake . -DWITH_READLINE=1 -DDEB=xenial -DPLUGIN_CONNECT=NO -DWITH_WSREP=OFF -DINSTALL_LAYOUT=DEB -DCMAKE_INSTALL_PREFIX=/usr/local/mariadb/columnstore/mysql/  -DCPACK_DEB_EXCLUDE_FROM_AUTO_FILELIST_ADDITION=/usr/local
make -jN # N is the number of concurrent build processes and should likely be the number of cores available
sudo make install
make package
git clone https://github.com/mariadb-corporation/mariadb-columnstore-engine.git
cd mariadb-columnstore-engine
git checkout develop
run cmake 
For RPMs"
cmake . -DRPM=centos6
For DEBIANs:
cmake . -DDEB=xenial
make -jN # same as above with respect to concurrent processes
sudo make install
make package
```

With the engine checked out in a separate location the following values need to be set by cmake command.

```bash
SERVER_BUILD_INCLUDE_DIR=Path to the server build include directory.
SERVER_SOURCE_ROOT_DIR=Path the directory the server source checked out from github.
```

### Examples

Engine not located inside server:

```bash
git clone https://github.com/mariadb-corporation/mariadb-columnstore-server.git
git clone https://github.com/mariadb-corporation/mariadb-columnstore-engine.git
cd mariadb-columnstore-server
cmake . -DCMAKE_INSTALL_PREFIX=/usr/local/mariadb/columnstore/mysql
make -jN # N is the number of concurrent build processes and should likely be the number of cores available
sudo make install
cd ../mariadb-columnstore-engine
cmake . -DSERVER_BUILD_INCLUDE_DIR=../mariadb-columnstore-server/include -DSERVER_SOURCE_ROOT_DIR=../mariadb-columnstore-server
make -jN # same as above with respect to concurrent processes
sudo make install
```

Build out-of-source:

```bash
git clone https://github.com/mariadb-corporation/mariadb-columnstore-server.git
git clone https://github.com/mariadb-corporation/mariadb-columnstore-engine.git
mkdir buildServer
cd buildServer
cmake ../mariadb-columnstore-server -DCMAKE_INSTALL_PREFIX=/usr/local/mariadb/columnstore/mysql
make -jN # N is the number of concurrent build processes and should likely be the number of cores available
sudo make install
cd ..
mkdir buildEngine
cd buildEngine
cmake ../mariadb-columnstore-engine -DSERVER_BUILD_INCLUDE_DIR=../buildServer/include -DSERVER_SOURCE_ROOT_DIR=../mariadb-columnstore-server
make -jN # same as above with respect to concurrent processes
sudo make install
```

To build a debug version
  * Add `-DCMAKE_BUILD_TYPE=debug -DPLUGIN_TOKUDB=NO` to each of the cmake commands in the build process
  * Do not mix release and debug versions of server and engine

To develop a new branch/feature/pull request
  * Fork the server repo from github `mariadb-corporation/mariadb-columnstore-server`
  * Fork the engine report from github `mariadb-corporation/mariadb-columnstore-engine`
  * `git checkout develop  #branch in server`
  * `git submodule update --init`
  * `git branch new-branch-name` (this can be in engine or server code)
  * `git checkout new-branch-name`
  * Edit source files
  * `git commit -m 'meaningful checkin comment'`
  * `git push -u origin new-branch-name`
  * Issue pull request for merge from new-branch-name into develop
  * MariaDB ColumnStore team will evaluate the changes and may request further development or changes before merge 

## Run dependencies

## For CentOS

For CentOS 6 follow the install procedure for boost from the build Dependecy section above, with CentOS 7 you can just do:

```bash
yum install boost
```

In addition these packages need to be install:

```bash
yum install expect perl perl-DBI openssl zlib file sudo libaio rsync snappy net-tools nmap
```

## For Ubuntu 16/18 

These packages need to be installed:

```bash
apt-get install expect perl openssl file sudo libdbi-perl libboost-all-dev libreadline-dev rsync libsnappy1v5 net-tools nmap
```

## For Debian 8

These packages need to be installed:

```bash
apt-get install expect perl openssl file sudo libdbi-perl libboost-all-dev libreadline-dev rsync libsnappy1 net-tools nmap
```
## For Debian 9

These packages need to be installed:

```bash
apt-get install expect perl openssl file sudo libdbi-perl libboost-all-dev libreadline-dev rsync  net-tools libsnappy1v5 libreadline5 nmap
```
## For SUSE 12

These packages need to be installed:

SUSEConnect -p sle-sdk/12.2/x86_64
zypper install boost-devel

```bash
zypper install expect perl perl-DBI openssl file sudo libaio1 rsync net-tools libsnappy1 nmap
```

## MariaDB ColumnStore utilizes the System Logging for logging purposes

So you will want to make sure that one of these system logging packages is installed:

  syslog, rsyslog, or syslog-ng

## Configure and Starting of MariaDB ColumnStore

Follow the binary package install instructions in the ColumnStore Getting Starter Guide:

  https://mariadb.com/kb/en/mariadb/columnstore-getting-started/

Commands to run as root user:

```bash
cd /usr/local/mariadb/columnstore/bin/
./post-install
./postConfigure
```
