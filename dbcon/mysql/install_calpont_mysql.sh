#!/bin/bash
#
# $Id$
#

prefix=/usr/local
installdir=$prefix/mariadb/columnstore
rpmmode=install
pwprompt=" "

for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/mariadb/columnstore
	elif [ `expr -- "$arg" : '--rpmmode='` -eq 10 ]; then
		rpmmode="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--tmpdir='` -eq 9 ]; then
		tmpdir="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

mysql --force --user=root mysql 2> ${tmpdir}/mysql_install.log <<EOD
INSTALL PLUGIN columnstore SONAME 'libcalmysql.so';
INSTALL PLUGIN columnstore_tables SONAME 'is_columnstore_tables.so';
INSTALL PLUGIN columnstore_columns SONAME 'is_columnstore_columns.so';
INSTALL PLUGIN columnstore_extents SONAME 'is_columnstore_extents.so';
INSTALL PLUGIN columnstore_files SONAME 'is_columnstore_files.so';
INSERT INTO mysql.func VALUES ('calgetstats',0,'libcalmysql.so','function'),('calsettrace',2,'libcalmysql.so','function'),('calsetparms',0,'libcalmysql.so','function'),('calflushcache',2,'libcalmysql.so','function'),('calgettrace',0,'libcalmysql.so','function'),('calgetversion',0,'libcalmysql.so','function'),('calonlinealter',2,'libcalmysql.so','function'),('calviewtablelock',0,'libcalmysql.so','function'),('calcleartablelock',0,'libcalmysql.so','function'),('callastinsertid',2,'libcalmysql.so','function'),('calgetsqlcount',0,'libcalmysql.so','function'),('idbpm',2,'libcalmysql.so','function'),('idbdbroot',2,'libcalmysql.so','function'),('idbsegment',2,'libcalmysql.so','function'),('idbsegmentdir',2,'libcalmysql.so','function'),('idbextentrelativerid',2,'libcalmysql.so','function'),('idbblockid',2,'libcalmysql.so','function'),('idbextentid',2,'libcalmysql.so','function'),('idbextentmin',0,'libcalmysql.so','function'),('idbextentmax',0,'libcalmysql.so','function'),('idbpartition',0,'libcalmysql.so','function'),('idblocalpm',2,'libcalmysql.so','function'),('mcssystemready',2,'libcalmysql.so','function'),('mcssystemreadonly',2,'libcalmysql.so','function'),('mcssystemprimary',2,'libcalmysql.so','function'),('regr_avgx',1,'libregr_mysql.so','aggregate'),('regr_avgy',1,'libregr_mysql.so','aggregate'),('regr_count',2,'libregr_mysql.so','aggregate'),('regr_slope',1,'libregr_mysql.so','aggregate'),('regr_intercept',1,'libregr_mysql.so','aggregate'),('regr_r2',1,'libregr_mysql.so','aggregate'),('corr',1,'libregr_mysql.so','aggregate'),('regr_sxx',1,'libregr_mysql.so','aggregate'),('regr_syy',1,'libregr_mysql.so','aggregate'),('regr_sxy',1,'libregr_mysql.so','aggregate'),('covar_pop',1,'libregr_mysql.so','aggregate'),('covar_samp',1,'libregr_mysql.so','aggregate'),('distinct_count',2,'libudf_mysql.so','aggregate'),('caldisablepartitions',0,'libcalmysql.so','function'),('calenablepartitions',0,'libcalmysql.so','function'),('caldroppartitions',0,'libcalmysql.so','function'),('calshowpartitions',0,'libcalmysql.so','function'),('caldroppartitionsbyvalue',0,'libcalmysql.so','function'),('caldisablepartitionsbyvalue',0,'libcalmysql.so','function'),('calenablepartitionsbyvalue',0,'libcalmysql.so','function'),('calshowpartitionsbyvalue',0,'libcalmysql.so','function');

CREATE DATABASE IF NOT EXISTS infinidb_vtable;
CREATE DATABASE IF NOT EXISTS infinidb_querystats;
CREATE TABLE IF NOT EXISTS infinidb_querystats.querystats
(
  queryID bigint NOT NULL AUTO_INCREMENT,
  sessionID bigint DEFAULT NULL,
  host varchar(50),
  user varchar(50),
  priority char(20),
  queryType char(25),
  query varchar(8000),
  startTime timestamp NOT NULL,
  endTime timestamp NOT NULL,
  \`rows\` bigint,
  errno int,
  phyIO bigint,
  cacheIO bigint,
  blocksTouched bigint,
  CPBlocksSkipped bigint,
  msgInUM bigint,
  msgOutUm bigint,
  maxMemPct int,
  blocksChanged bigint,
  numTempFiles bigint,
  tempFileSpace bigint,
  PRIMARY KEY (queryID)
);

CREATE TABLE IF NOT EXISTS infinidb_querystats.user_priority
(
  host varchar(50),
  user varchar(50),
  priority char(20)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE IF NOT EXISTS infinidb_querystats.priority
(
  priority char(20) primary key,
  priority_level int
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

insert ignore into infinidb_querystats.priority values ('High', 100),('Medium', 66), ('Low', 33);
EOD

mysql --user=root  mysql 2>/dev/null <$installdir/mysql/syscatalog_mysql.sql
mysql --user=root  mysql 2>/dev/null <$installdir/mysql/calsetuserpriority.sql
mysql --user=root  mysql 2>/dev/null <$installdir/mysql/calremoveuserpriority.sql
mysql --user=root  mysql 2>/dev/null <$installdir/mysql/calshowprocesslist.sql
mysql --user=root  mysql 2>/dev/null <$installdir/mysql/columnstore_info.sql

