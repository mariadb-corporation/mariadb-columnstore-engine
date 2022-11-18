# CMAPI REST server
[![Build Status](https://ci.columnstore.mariadb.net/api/badges/mariadb-corporation/mariadb-columnstore-cmapi/status.svg)](https://ci.columnstore.mariadb.net/mariadb-corporation/mariadb-columnstore-cmapi)

## Overview
This RESTfull server enables multi-node setups for MCS.

## Requirements

See requirements.txt file.

All the Python packages prerequisits are shipped with a pre-built Python enterpreter.

## Usage

To run the server using defaults call:
```sh
python3 -m cmapi_server
```
There is a configuration server inside cmapi_server.

## Testing

To launch the integration and unit tests use unittest discovery mode.
```sh
python3 -m unittest discover -v mcs_node_control
python3 -m unittest discover -v cmapi_server
python3 -m unittest discover -v failover
```

mcs_control_node unit tests ask for root privileges and additional systemd unit
to run smoothly.

## Build packages

Packages have bundled python interpreter and python dependencies.

## Get dependencies

# get portable python
wget -qO- https://cspkg.s3.amazonaws.com/python-dist-no-nis.tar.gz | tar xzf - -C ./

# install python dependencies
python/bin/pip3 install -t deps --only-binary :all -r requirements.txt

## RPM

```sh
./cleanup.sh
yum install -y wget cmake make rpm-build
cmake -DRPM=1 .
make package
```

## DEB

```sh
./cleanup.sh
DEBIAN_FRONTEND=noninteractive apt update && apt install -y cmake make
cmake -DDEB=1 .
make package
```
