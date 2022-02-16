This is MariaDB Columnstore
=====================

To build MCS from source you will need:

  * modern linux distribution. e.g. Ubuntu 20
  * modern C++ compiler: clang version greater than 12 or gcc version greater than 11

Clone or download this repository.

    git clone https://github.com/MariaDB/server

Update Sumbodules
    git submodule update --init --recursive

Run the bootstrap
    /some/path/server/storage/columnstore/columnstore/build/bootstrap_mcs.sh
bootstrap_mcs.sh could be run with --help for list of flags
