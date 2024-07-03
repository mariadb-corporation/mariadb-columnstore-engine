Without limitation of generality let's set we want to install packages from nightly build
https://ci.columnstore.mariadb.net/mariadb-corporation/mariadb-columnstore-engine/9294/1/11

First go to the build step 'publish pkg url', copy the <url>
like https://cspkg.s3.amazonaws.com/index.html?prefix=stable-23.10/cron/9294/10.6-enterprise/amd64/centos7/

remove index.html?prefix= and last two pathes from url and
run
``export PACKAGES_URL=https://cspkg.s3.amazonaws.com/stable-23.10/cron/9294/10.6-enterprise`` and
run ``export OS=centos7``


get the ``https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/develop/setup-repo.sh``
using wget or curl

run ``bash setup-repo.sh``

for rocky linux run ``yum install epel-release procps``

then install columnstore
like
``yum install MariaDB-columnstore-engine.x86_64 MariaDB-columnstore-cmapi.x86_64`` for rockys
or
``apt install mariaDB-columnstore-cmapi mariadb-plugin-columnstore` for debians``






