This is MariaDB Columnstore
=====================

To build MCS from source you will need:

  * modern linux distribution. e.g. Ubuntu 20

Clone or download this repository.

    git clone https://github.com/MariaDB/server

Edit a bootstrap script to fix paths to MariaDB repo cloned.
    vim /some/path/server/storage/columnstore/columnstore/build/bootstrap_mcs.sh

Run the bootstrap
    /some/path/server/storage/columnstore/columnstore/build/bootstrap_mcs.sh
