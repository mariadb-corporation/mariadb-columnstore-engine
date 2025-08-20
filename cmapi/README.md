# CMAPI REST server
[![Build Status](https://ci.columnstore.mariadb.net/api/badges/mariadb-corporation/mariadb-columnstore-cmapi/status.svg)](https://ci.columnstore.mariadb.net/mariadb-corporation/mariadb-columnstore-cmapi)

## Overview
This RESTful server enables multi-node setups for MCS.

## Requirements

See requirements.txt file.

All the Python packages prerequisites are shipped with a pre-built Python interpreter.

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
wget -qO- https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.9.13+20220802-x86_64_v2-unknown-linux-gnu-pgo+lto-full.tar.zst | tar --use-compress-program=unzstd -xf - -C ./ && \
mv python pp && mv pp/install python && rm -rf pp

There is a script dev_tools/activate that works like virtualenv activate (you can use it to work with portable Python like with virtualenv).

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
