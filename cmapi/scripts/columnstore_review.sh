#!/bin/bash
# columnstore_review.sh
# script by Edward Stoever for MariaDB support
# Contributors: Allen Herrera
#               Patrizio Tamorri
VERSION=1.4.13

function prepare_for_run() {
  unset ERR
  if [ -n "$USER_PROVIDED_OUTPUT_PATH" ] && [ ! -d "$USER_PROVIDED_OUTPUT_PATH" ]; then 
    printf "The directory $USER_PROVIDED_OUTPUT_PATH does not exist.\n\n"
    exit 1
  fi

  if [ -n "$USER_PROVIDED_OUTPUT_PATH" ]; then
    OUTDIR=$USER_PROVIDED_OUTPUT_PATH/columnstore_review
    TARDIR=$USER_PROVIDED_OUTPUT_PATH
  else
    OUTDIR=/tmp/columnstore_review
    TARDIR=/tmp
  fi
  mkdir -p $OUTDIR
  WARNFILE=$OUTDIR/cs_warnings.out
  if [ $EM_CHECK ]; then
    EMOUTDIR=$OUTDIR/em; mkdir -p $EMOUTDIR
    OUTPUTFILE=$EMOUTDIR/$(hostname)_cs_em_check.txt
  else
    OUTPUTFILE=$OUTDIR/$(hostname)_cs_review.txt
  fi
  touch $WARNFILE || ERR=true
  touch $OUTPUTFILE || ERR=true
  if [ $ERR ]; then
     ech0 'Cannot create temporary output. Exiting'; exit 1;
  fi
  truncate -s0 $WARNFILE
  truncate -s0 $OUTPUTFILE
}

function exists_mariadb_installed() {
  ls /usr/sbin/mariadbd 1>/dev/null 2>/dev/null || echo 'MariaDB Server is not installed.' >> $WARNFILE
}

function exists_columnstore_installed() {
  ls /usr/bin/mariadb-columnstore-start.sh 1>/dev/null 2>/dev/null && CS_INSTALLED=true || echo 'Mariadb-Columnstore is not installed.' >> $WARNFILE
}

function exists_cmapi_installed() {
  ls /usr/share/columnstore/cmapi 1>/dev/null 2>/dev/null && CMAPI_INSTALLED=true || echo 'Mariadb-Columnstore-cmapi is not installed.' >> $WARNFILE
}

function exists_cmapi_running() {
  if [[ "$(ps -ef | grep cmapi_server | grep -v "grep" |wc -l)" == "0" ]]; then echo 'Mariadb-Columnstore-cmapi is not running.' >> $WARNFILE; else CMAPI_CONNECT=true; fi
}

function exists_mariadbd_running() {
  if [[ "$(ps -ef | grep mariadbd | grep -v "grep" |wc -l)" == "0" ]]; then echo 'Mariadb Server is not running.' >> $WARNFILE; else MARIADB_RUNNING=true; fi
}

function exists_columnstore_running() {

  if [[ "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then 
    echo 'There are no Mariadb-Columnstore processes running.' >> $WARNFILE; 
  else
    CS_RUNNING=true
  fi
}

function exists_iptables_rules() {
  if [ "$(id -u)" -eq 0 ] && [ $(which iptables 2>/dev/null) ]; then
    if [[ $(iptables -S | grep -v ACCEPT) ]]; then echo 'Iptables rules exist.' >> $WARNFILE; fi
  fi
}

function exists_ip6tables_rules() {
  if [ "$(id -u)" -eq 0 ] &&  [ $(which ip6tables 2>/dev/null) ]; then
    if [[ $(ip6tables -S | grep -v ACCEPT) ]]; then echo 'Ip6tables rules exist.' >> $WARNFILE; fi
  fi
}

function exists_zombie_processes() {
  ZOMBIES=$(ps -ef | grep "\<defunct\>" | grep -v 'grep' | wc -l)
  if [[ "$ZOMBIES" == "0" ]]; then return; fi
  if [[ "$ZOMBIES" == "1"  ]]; then echo "There is 1 zombie process." >> $WARNFILE; return; fi
  echo "There are $ZOMBIES zombie processes." >> $WARNFILE
}

function exists_https_proxy() {
  if [ $CMAPI_CONNECT ]; then
    mcspm1host=$(mcsGetConfig PMS1 IPaddr)
    if [[ "$(timeout 4 curl -v https://$mcspm1host:8640 2>&1 | grep proxy | wc -l)" != "0" ]]; then echo 'Https_proxy is configured.' >> $WARNFILE; fi
  else
    if [[ $(env| grep https_proxy) ]]; then echo 'Https_proxy is set.' >> $WARNFILE; fi
  fi
}

function exists_cmapi_cert_expired() {
  if [ ! $CMAPI_INSTALLED ]; then return; fi
  CERTFILE='/usr/share/columnstore/cmapi/cmapi_server/self-signed.crt'
  if [[ ! -f $CERTFILE  ]]; then echo "The certificate file $CERTFILE does not exist." >> $WARNFILE; return; fi
  certdate=$(date -d "$(openssl x509 -enddate -noout -in $CERTFILE | sed 's/.*=//')" +%s)
  nowdate=$(date +%s)
  if (($certdate<=$nowdate)); then echo "The certificate $CERTFILE for cmapi https is expired." >> $WARNFILE; fi
}

function exists_client_able_to_connect_with_socket() {
  if [ "$(id -u)" -eq 0 ]; then
    if [ -n "$SUDO_USER" ]; then
      RUNAS="$(whoami) (sudo)"
    else 
      RUNAS="$(whoami)"
    fi
  else 
    RUNAS="$(whoami)"
  fi

  OUTP="Columnstore Review Script Version $VERSION. Script by Edward Stoever for MariaDB Support. Script started at: "
  HDR=$(mariadb -ABNe "select concat('$OUTP', now())" 2>/dev/null || echo "$OUTP$(date +'%Y-%m-%d %H:%M:%S - No DB connection.')")
  if [[ ! "$HDR" =~ .*"No DB connection".* ]]; then CAN_CONNECT=true; fi
  print_color "$HDR\n"
  ech0 "Run as $RUNAS."
  ech0
}

function exists_corefiles() {
COREFILE_DIR=/var/log/mariadb/columnstore/corefiles
COREFILE_COUNT=$(find $COREFILE_DIR -type f| wc -l)
  if [ "$COREFILE_COUNT" -gt "0" ]; then
    if [ "$COREFILE_COUNT" == "1" ]; then 
      echo "There is 1 file in the Columnstore Core file directory $COREFILE_DIR." >> $WARNFILE;
    else
      echo "There are $COREFILE_COUNT files in the Columnstore Core file directory $COREFILE_DIR." >> $WARNFILE;
    fi
  fi
}

function exists_errors_pushing_config() {
  MESSAGESFILE=$(find /var/log -name messages -type f | head -1)
  if [ $MESSAGESFILE ]; then
    PUSHING_ERRORS=$(grep -i "Got an unexpected error pushing new config to" $MESSAGESFILE |wc -l)
  fi
  
  if [ "$PUSHING_ERRORS" -gt "0" ]; then
    if [ "$PUSHING_ERRORS" == "1" ]; then
      echo "There is 1 error related to pushing new config to cmapi in $MESSAGESFILE." >> $WARNFILE;
    else
      echo "There are $PUSHING_ERRORS errors related to pushing new config to cmapi in $MESSAGESFILE." >> $WARNFILE;
    fi
  fi
}

function exists_columnstore_tmp_files_wrong_owner() {
  if [ -f /mnt/skysql/podinfo/namespace ] && [ -f /mnt/skysql/podinfo/podname ]; then return; fi #if SkySQL, ignore this
  TMP_DIR=$(mcsGetConfig SystemConfig SystemTempFileDir)
  if [ -d $TMP_DIR ]; then
    DIR_OWNER=$(stat -c '%U' $TMP_DIR)
  else
    return # directory does not exist, so OK.
  fi
  if [ "$DIR_OWNER" != "mysql" ]; then echo "The directory $TMP_DIR is not owned by mysql. See MCOL-4866."  >> $WARNFILE; fi
}

function exists_selinux_not_disabled() {
  if [ $(which getenforce 2>/dev/null) ]; then
    SELINUX=$(getenforce 2>/dev/null);
    if [ ! -z $SELINUX ] && [ ! "$SELINUX" == "Disabled" ]; then echo "SELINUX status: $SELINUX" >> $WARNFILE;  fi
  fi
}

function exists_mysql_user_with_proper_uid() {
  MYSQL_UID=$(id -u mysql 2>/dev/null) || echo 'OS user mysql does not exist.' >> $WARNFILE;
  THIS_SYS_UID_MAX=$(grep SYS_UID_MAX /etc/login.defsx 2>/dev/null | awk '{print $2}')
  if ! [[ $THIS_SYS_UID_MAX =~ '^[0-9]+$' ]] ; then THIS_SYS_UID_MAX=999; fi
  if [ "$MYSQL_UID" -gt "$THIS_SYS_UID_MAX" ]; then echo 'OS user mysql has high UID number. See MCOL-5216.' >> $WARNFILE; fi
}

function exists_cs_error_log() {
  CS_ERR_LOG=/var/log/mariadb/columnstore/err.log
  if [ ! -f $CS_ERR_LOG ]; then echo "There is no columnstore error log file: $CS_ERR_LOG."  >> $WARNFILE; unset CS_ERR_LOG; fi
}

function exists_error_log() {
#  if [ ! $CAN_CONNECT ]; then return; fi
  set_log_error
  if [ ! $LOG_ERROR ]; then echo 'MariaDB Server log_error is not set.' >> $WARNFILE; return; fi
  if [ ! -f $LOG_ERROR ]; then echo 'MariaDB Server log_error is set but file is not found.' >> $WARNFILE; unset LOG_ERROR; fi
#  unset DATADIR FULL_PATH
}

function exists_abandoned_DDL_log(){
set_data1dir
  DDL_FAILURES=$(find $DATA1DIR/systemFiles/dbrm/ -name "DDL*" -type f | wc -l)
  if [ "$DDL_FAILURES" -gt "0" ]; then
    if [ "$DDL_FAILURES" == "1" ]; then
      echo "There is 1 DDL file in $DATA1DIR/systemFiles/dbrm. The existence of this file could block DDL commands from completing. Ref: CS0545308" >> $WARNFILE;
    else
      echo "There are $DDL_FAILURES DDL files in $DATA1DIR/systemFiles/dbrm. The existence of these could block DDL commands from completing. Ref: CS0545308" >> $WARNFILE;
    fi
  fi 
}

function exists_querystats_enabled() {
   if [ ! $CS_INSTALLED ]; then return; fi
   MCSQUERYSTATS=$(mcsGetConfig QueryStats Enabled 2>&1)
  if [ "$MCSQUERYSTATS" == "Y" ]; then
    return;
  else
    echo "QueryStats Enabled = $MCSQUERYSTATS" >> $WARNFILE
  fi
}
  
function exists_diskbased_hashjoin() {
   if [ ! $CS_INSTALLED ]; then return; fi
   MCSHASHJOIN=$(mcsGetConfig HashJoin AllowDiskBasedJoin 2>&1)
  if [ "$MCSHASHJOIN" == "Y" ]; then
    return;
  else
    echo "HashJoin AllowDiskBasedJoin = $MCSHASHJOIN" >> $WARNFILE
  fi
}

function exists_crossenginesupport() {
  if [ ! $CS_INSTALLED ]; then return; fi
  CROSSENGINE_HOST=$(mcsGetConfig CrossEngineSupport Host)
  CROSSENGINE_PORT=$(mcsGetConfig CrossEngineSupport Port)
  CROSSENGINE_USER=$(mcsGetConfig CrossEngineSupport User)
  CROSSENGINE_PW=$(mcsGetConfig CrossEngineSupport Password)
  if [ -z $CROSSENGINE_HOST ]; then NOTCROSSENGINESUPPORT=true; fi
  if [ -z $CROSSENGINE_PORT ]; then NOTCROSSENGINESUPPORT=true; fi
  if [ -z $CROSSENGINE_USER ]; then NOTCROSSENGINESUPPORT=true; fi
  if [ -z $CROSSENGINE_PW ]; then NOTCROSSENGINESUPPORT=true; fi

  if [ $NOTCROSSENGINESUPPORT ]; then 
    echo "CrossEngineSupport is not enabled." >> $WARNFILE
  fi
}

function exists_minimal_columnstore_running() {
  if [ ! $CS_RUNNING ]; then return; fi
  CONTROLLERNODEPID=$(pidof /usr/bin/controllernode)
  WORKERNODEPID=$(pidof /usr/bin/workernode)
  PRIMPROCPID=$(pidof /usr/bin/PrimProc)
  WRITEENGINESERVERPID=$(pidof /usr/bin/WriteEngineServer)
  DMLPROCPID=$(pidof /usr/bin/DMLProc)
  DDLPROCPID=$(pidof /usr/bin/DDLProc)
  EXEMGRPID=$(pidof /usr/bin/ExeMgr)
  if [ $CAN_CONNECT ]; then
    ISTWENTYSOMETHING=$(mariadb -ABNe "show global status like 'Columnstore_version';" | awk '{print $2}'| awk '{print substr ($0, 0, 2)}')
    if [[ ! "$ISTWENTYSOMETHING" =~ ^[2][2-9]$ ]]; then # version is not in the twenties
       if [ -z $EXEMGRPID ]; then echo "Columnstore processes running but ExeMgr process is not." >> $WARNFILE; fi
    fi
  fi
  if [ -z $WORKERNODEPID ]; then echo "Columnstore processes running but workernode process is not." >> $WARNFILE; fi
  if [ -z $PRIMPROCPID ]; then echo "Columnstore processes running but PrimProc process is not." >> $WARNFILE; fi
  if [ -z $WRITEENGINESERVERPID ]; then echo "Columnstore processes running but WriteEngineServer process is not." >> $WARNFILE; fi
  if [ $CONTROLLERNODEPID ]; then
    if [ -z $DMLPROCPID ]; then echo "Columnstore processes running but DMLProc process is not." >> $WARNFILE; fi
    if [ -z $DDLPROCPID ]; then echo "Columnstore processes running but DDLProc process is not." >> $WARNFILE; fi
  fi
}

function exists_errors_in_cmapi_server_log() {
  CMAPI_ERRORS_FOUND=$(find /var/log/mariadb/columnstore -name "cmapi_server.log*" -type f ! -name "*gz" | xargs grep "\[ERROR\]"|sed 's/.*\[E/\  [E/'|sort|uniq|wc -l)
    if [ ! "$CMAPI_ERRORS_FOUND" == "0" ]; then
  CMAPI_ERRORS_LIST=$(find /var/log/mariadb/columnstore -name "cmapi_server.log" -type f ! -name "*gz" | xargs grep "\[ERROR\]"|sed 's/.*\[E/\  [E/'|sort|uniq)
  echo "CMAPI Errors found:" >> $WARNFILE
  echo "$CMAPI_ERRORS_LIST" >> $WARNFILE
    fi
}

function exists_history_of_delete_dml() {
   DELETES_FOUND=$(find /var/log/mariadb/columnstore/ -name "debug.log*" ! -name "*gz" |xargs grep dmlpackageproc|grep 'Start SQL statement' |grep -i delete |wc -l)
   if [ ! "$DELETES_FOUND" == "0" ]; then
      if [ "$DELETES_FOUND" == "1" ]; then
        echo "There is 1 DML delete found in recent logs." >> $WARNFILE
	  else
        DELETES_FOUND=$(printf "%'d" $DELETES_FOUND)
        echo "There are $DELETES_FOUND DML deletes found in recent logs." >> $WARNFILE
	  fi
   fi
}


function exists_history_of_update_dml() {
   UPDATES_FOUND=$(find /var/log/mariadb/columnstore/ -name "debug.log*" ! -name "*gz" |xargs grep dmlpackageproc|grep 'Start SQL statement' |grep -i update |wc -l)
   if [ ! "$UPDATES_FOUND" == "0" ]; then
      if [ "$UPDATES_FOUND" == "1" ]; then
        echo "There is 1 DML update found in recent logs." >> $WARNFILE
	  else
        UPDATES_FOUND=$(printf "%'d" $UPDATES_FOUND)
        echo "There are $UPDATES_FOUND DML updates found in recent logs." >> $WARNFILE
	  fi
   fi
}

function exists_history_of_file_does_not_exist() {
   FILE_DOES_NOT_EXIST=$(find /var/log/mariadb/columnstore/ -name "debug.log*" ! -name "*gz" | xargs grep 'Data file does not exist' | wc -l)
   if [ ! "$FILE_DOES_NOT_EXIST" == "0" ]; then
     echo "Errors found in debug logs: file does not exist. Run an extent map check." >> $WARNFILE
   fi
}

function exists_compression_header_errors() {
   ERROR_READING=$(find /var/log/mariadb/columnstore/ -name "crit.log*" ! -name "*gz" | xargs grep 'Error reading compression header' | wc -l)
   if [ ! "$ERROR_READING" == "0" ]; then
     echo "Errors found in crit logs: reading compression header. Check for possible data file corruption." >> $WARNFILE
   fi
}

function exists_history_failed_getSystemState() {
   return; # This is inconsequential. Not going to do anything here until further notice. 
   FAILED_GETSYSTEMSTATE=$(find /var/log/mariadb/columnstore/ -name "debug.log*" ! -name "*gz" | xargs grep 'SessionManager::getSystemState() failed (network)' | wc -l)
   if [ ! "$FAILED_GETSYSTEMSTATE" == "0" ]; then
     echo "Errors found in debug logs: getSystemState failed. MariaDB and Columnstore may not be communicating." >> $WARNFILE
   fi
}

function exists_error_opening_file() {
   ERROR_OPENING=$(find /var/log/mariadb/columnstore/ -name "crit.log*" ! -name "*gz" | xargs grep 'Error opening file' | wc -l)
   if [ ! "$ERROR_OPENING" == "0" ]; then
     echo "Errors found in crit logs: opening file. Check for possible file permission and/or ownership problems." >> $WARNFILE
   fi
}

function exists_readtomagic_errors() {
   return; # This is inconsequential. Not going to do anything here until further notice.
   READTOMAGIC=$(find /var/log/mariadb/columnstore/ -name "crit.log*" ! -name "*gz" | xargs grep 'readToMagic' | wc -l)
   if [ ! "$READTOMAGIC" == "0" ]; then
     echo "Errors found in crit logs: readToMagic. Most readToMagic errors are inconsequential." >> $WARNFILE
   fi
}

function exists_bad_magic_errors() {
   BADMAGIC=$(find /var/log/mariadb/columnstore/ -name "crit.log*" ! -name "*gz" | xargs grep 'Bad magic' | wc -l)
   if [ ! "$BADMAGIC" == "0" ]; then
     echo "Errors found in crit logs: Bad Magic." >> $WARNFILE
   fi
}

function exists_got_signal_errors() {
   set_log_error; 
   if [ ! $LOG_ERROR ]; then return; fi
   if [ ! -f $LOG_ERROR ]; then return; fi
   GOTSIGNAL=$(tail -100000 $LOG_ERROR | grep 'got signal'|wc -l)
   if [ ! "$GOTSIGNAL" == "0" ]; then
     echo "This MariaDB instance has crashed recently." >> $WARNFILE
   fi
}

function exists_unknown_ref_item() {
   set_log_error;
   if [ ! $LOG_ERROR ]; then return; fi
   if [ ! -f $LOG_ERROR ]; then return; fi
   GOTUNKNOWN=$(tail -100000 $LOG_ERROR | grep 'UNKNOWN REF Item'|wc -l)
   if [ ! "$GOTUNKNOWN" == "0" ]; then
     echo "Unknown ref item error found in error log. Mariadb server version may not be fully compatible with columnstore version." >> $WARNFILE
   fi
}

function exists_replica_not_started() {
  if [ ! $CAN_CONNECT ]; then return; fi
  SLAVE_STATUS=$(mariadb -Ae "show all slaves status\G"| grep Slave_SQL_Running_State | head -1 | xargs)
  if [ "$SLAVE_STATUS" == "Slave_SQL_Running_State:" ]; then echo "Replication is not running." >> $WARNFILE; fi
}

function exists_does_not_exist_in_columnstore() {
   IDB2006=$(find /var/log/mariadb/columnstore/ -name "warning.log*" ! -name "*gz" | xargs grep 'IDB-2006' | wc -l)
   if [ ! "$IDB2006" == "0" ]; then
     echo "Errors found in warning logs: IDB-2006 does not exist in Columnstore. MariaDB may be out of sync with Columnstore or there may be a problem with the extent map." >> $WARNFILE
   fi
}

function exists_holding_table_lock() {
   IDB2009=$(find /var/log/mariadb/columnstore/ -name "err.log*" ! -name "*gz" | xargs grep 'IDB-2009' | wc -l)
   if [ ! "$IDB2009" == "0" ]; then
     echo "Errors found in err logs: IDB-2009 Unable to perform the update operation because DMLProc is currently holding the table lock." >> $WARNFILE
   fi
}

function exists_data1_dir_wrong_access_rights() {
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then return; fi

  set_data1dir
  if [ -L $DATA1DIR ]; then return; fi # if a symbolic link, then ignore this
  DATA1DIR_ACCESS_RIGHTS=$(stat -c "%a" $DATA1DIR)
  if [ ! "$DATA1DIR_ACCESS_RIGHTS" == "1755" ]; then
    echo "The octal for directory $DATA1DIR should be 1755 and it is not." >> $WARNFILE
  fi
}

function exists_systemFiles_dir_wrong_access_rights() {
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then return; fi

  set_data1dir
  SYSTEMFILES=$DATA1DIR/systemFiles
  SYSTEMFILES_ACCESS_RIGHTS=$(stat -c "%a" $SYSTEMFILES)
  if [ ! "$SYSTEMFILES_ACCESS_RIGHTS" == "1755" ]; then
    echo "The octal for directory $SYSTEMFILES should be 1755 and it is not." >> $WARNFILE
  fi 
}

function exists_any_idb_errors() {
   ANY_IDB=$(grep -oh "IDB-[0-9][0-9][0-9][0-9]" /var/log/mariadb/columnstore/*.log | sort | uniq |sed -z 's/\n/, /g;s/,$/\n/' | sed 's/, *$//g')
   if [ "$ANY_IDB" ]; then
     echo "The following IDB errors exist in the logs: $ANY_IDB." >> $WARNFILE
   fi
}

function exists_version_buffer_overflow() {
   MCS2008=$(find /var/log/mariadb/columnstore/ -name "err.log*" ! -name "*gz" | xargs grep 'The version buffer overflowed' | wc -l)
   if [ ! "$MCS2008" == "0" ]; then
     echo "Errors found in err logs: The version buffer overflowed. Increase VersionBufferFileSize or limit the rows to be processed." >> $WARNFILE
   fi
}

function exists_any_mcs_errors() {
   ANY_MCS=$(grep -oh "MCS-[0-9][0-9][0-9][0-9]" /var/log/mariadb/columnstore/*.log | sort | uniq |sed -z 's/\n/, /g;s/,$/\n/' | sed 's/, *$//g')
   if [ "$ANY_MCS" ]; then
     echo "The following MCS errors exist in the logs: $ANY_MCS." >> $WARNFILE
   fi
}

function exists_erroneous_module() {
  if [ ! -f /var/lib/columnstore/local/module ]; then
    echo "The file /var/lib/columnstore/local/module does not exist." >> $WARNFILE;
    return;
  fi
  LOCAL_MODULE=$(cat /var/lib/columnstore/local/module | xargs)
  CMD=$(printf ${LOCAL_MODULE}_WriteEngineServer)
  IP_SHOULD_BE=$(mcsGetConfig $CMD IPAddr)
  if [ ! "$(ip a | grep $IP_SHOULD_BE)" ]; then 
    echo "module file shows $LOCAL_MODULE which does not match IP Address in Columnstore.xml." >> $WARNFILE;
  fi
}

function exists_cpu_unsupported_2208() {
  if [ -z $(cat /proc/cpuinfo | grep -E 'sse4_2|neon|asimd' | head -c1) ]; then
    echo "The machine CPU lacks of support for Intel SSE4.2 or ARM Advanced SIMD, required for Columnstore 22.08+." >> $WARNFILE
  fi
}

function exists_symbolic_links_in_columnstore_dir() {
  set_data1dir
  COLUMNSTOREDIR=$(dirname $DATA1DIR)
  SYMLINKS=$(find $COLUMNSTOREDIR -maxdepth 2 -type l | wc -l)
  if [ ! "$SYMLINKS" == "0" ]; then
      if [ "$SYMLINKS" == "1" ]; then
        echo "There is 1 symbolic link found in $COLUMNSTOREDIR." >> $WARNFILE
      else
        SYMLINKS=$(printf "%'d" $SYMLINKS)
        echo "There are $SYMLINKS symbolic links found in $COLUMNSTOREDIR." >> $WARNFILE
      fi
  fi
}


function get_out_of_sync_tables() {
SQL="SELECT
A.TABLE_SCHEMA AS 'SCHEMA NAME', A.TABLE_NAME AS 'TABLE NAME',
if(C.ENGINE IS NULL,'No','Yes') AS 'TABLE EXISTS, NOT COLUMNSTORE ENGINE'
FROM information_schema.COLUMNSTORE_TABLES A
left join information_schema.TABLES C
ON A.TABLE_SCHEMA=C.TABLE_SCHEMA AND A.TABLE_NAME=C.TABLE_NAME
WHERE NOT EXISTS
(SELECT 'x' FROM information_schema.TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
AND B.ENGINE='Columnstore')
ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME;"

  OUT_OF_SYNC_TABLES="$(timeout 3 mariadb -v -v -v -Ae "$SQL" | sed -nr '/^(\+|^\|)/p')"
}

function get_out_of_sync_tables_count() {
SQL="SELECT COUNT(*)
FROM information_schema.COLUMNSTORE_TABLES A
WHERE NOT EXISTS
(SELECT 'x' FROM information_schema.TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
AND B.ENGINE='Columnstore');"

  COLUMNSTORE_TABLES_NOT_IN_MARIABD=$(timeout 3 mariadb -ABNe "$SQL")

}

function exists_schema_out_of_sync() {
if [ ! $CAN_CONNECT ]; then return; fi
if [ ! $CS_RUNNING ]; then return; fi
get_out_of_sync_tables_count
if [ ! $COLUMNSTORE_TABLES_NOT_IN_MARIABD ]; then return; fi
if [ ! "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ]; then
get_out_of_sync_tables
  echo >> $WARNFILE
  echo "The following tables exist as Columnstore extents but not in Mariadb as Columnstore tables." >> $WARNFILE
  echo  "$OUT_OF_SYNC_TABLES" >> $WARNFILE
  echo "Run columnstore_review.sh --schemasync for options to fix out-of-sync tables." >> $WARNFILE
  echo >> $WARNFILE
fi
}

function get_no_extents_count() {
SQL="SELECT COUNT(*)
FROM information_schema.TABLES A
WHERE A.ENGINE='Columnstore'
AND A.TABLE_SCHEMA<>'calpontsys'
AND NOT EXISTS
(SELECT 'x' FROM information_schema.COLUMNSTORE_TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME)
ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME;"

  NO_EXTENTS_COUNT=$(timeout 3 mariadb -ABNe "$SQL")

}

function get_columnstore_tables_with_no_columnstore_extents() {
SQL="SELECT
A.TABLE_SCHEMA AS 'SCHEMA NAME', A.TABLE_NAME AS 'TABLE NAME'
FROM information_schema.TABLES A
WHERE A.ENGINE='Columnstore'
AND A.TABLE_SCHEMA<>'calpontsys'
AND NOT EXISTS
(SELECT 'x' FROM information_schema.COLUMNSTORE_TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME)
ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME;"

  NO_CS_EXTENTS="$(timeout 3 mariadb -v -v -v -Ae "$SQL" | sed -nr '/^(\+|^\|)/p')"
}

exists_columnstore_tables_with_no_extents() {
if [ ! $CAN_CONNECT ]; then return; fi
if [ ! $CS_RUNNING ]; then return; fi
get_no_extents_count
if [ ! $NO_EXTENTS_COUNT ]; then return; fi
if [ ! "$NO_EXTENTS_COUNT" == "0" ]; then
get_columnstore_tables_with_no_columnstore_extents
  echo >> $WARNFILE
  echo "The following tables exist in MariaDB as Columnstore tables but do have have columnstore extents." >> $WARNFILE
  echo  "$NO_CS_EXTENTS" >> $WARNFILE
  echo >> $WARNFILE
fi
}

function exists_storage_manager_errors() {
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ ! "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then return; fi

  STORAGEMANAGER_LOG=/var/log/mariadb/columnstore/storagemanager.log
  if [ -f $STORAGEMANAGER_LOG ]; then
    STORAGE_MANAGER_ERRORS=$(grep -i error $STORAGEMANAGER_LOG |grep -v Retrying |wc -l)
	if [ ! "$STORAGE_MANAGER_ERRORS" == "0" ]; then 
	  echo "Look for errors in $STORAGEMANAGER_LOG" >> $WARNFILE
	fi
  fi 
}

function exists_flooding_query_in_debug() {
   if [ ! -f /var/log/mariadb/columnstore/debug.log ]; then return; fi
   FLOOD_CLIENT=$(grep -i -E "SELECT\ +\*\ +FROM" /var/log/mariadb/columnstore/debug.log | grep -vi WHERE | grep -vi LIMIT | wc -l)
   if [ ! "$FLOOD_CLIENT" == "0" ]; then
     echo "Check the debug log for queries with no WHERE clause or LIMIT clause. A query that request a massive dataset has the potential to flood the client connection which can bring down the server." >> $WARNFILE
   fi
}

function report_skysql() {
  if [ ! -f /mnt/skysql/podinfo/namespace ] && [ ! -f /mnt/skysql/podinfo/podname ]; then return; fi
  print_color "### SkySQL ###\n"
  ech0 "$(cat /mnt/skysql/podinfo/namespace), $(cat /mnt/skysql/podinfo/podname)"
  ech0
}

function report_top_for_mysql_user() {
  if [ ! $MARIADB_RUNNING ] && [ ! $CS_RUNNING ]; then return; fi
  print_color "### Top for mysql user processes ###\n"
  TOP="$(top -bc -n1 -u mysql)"
  if [ ! -z "$TOP" ]; then ech0 "$TOP"; else print0 "No results from top to show.\n"; fi
  ech0
}

function report_last_10_error_log_error() {
  if [ ! $LOG_ERROR ]; then return; fi
  print_color "### Last 10 Errors in MariaDB Server Log ###\n"
  LAST_10=$(tail -5000 $LOG_ERROR | grep -i error | tail -10)
  if [ ! -z "$LAST_10" ]; then print0 "$LAST_10\n"; else print0 "No recent errors found in MariaDB Server error log.\n"; fi
  ech0
}

function report_last_5_columnstore_err_log() {
  CS_ERR_LOG=/var/log/mariadb/columnstore/err.log
  if [ ! -f $CS_ERR_LOG ]; then return; fi
  print_color "### Last 5 Lines of Columnstore Error Log ###\n"
  LAST_5=$(tail -5 $CS_ERR_LOG | awk '{$1=$1};1')
  if [ ! -z "$LAST_5" ]; then print0 "$LAST_5\n"; else print0 "Nothing found in Columnstore Error Log.\n"; fi
  ech0
} 

function report_cs_table_locks() {
  if [ ! $CS_RUNNING ]; then return; fi
  print_color "### Columnstore Table Locks ###\n"
  TABLE_LOCKS=$(timeout 5 viewtablelock 2>/dev/null)
  if [ ! -z "$TABLE_LOCKS" ]; then print0 "$TABLE_LOCKS\n"; else print0 "The command viewtablelock timed out or an error occurred.\n"; fi
  ech0
}

function report_machine_architecture() {
   print_color "### Machine Architecture ###\n"
   if [ "$(grep docker /proc/1/cgroup 2>/dev/null)" ]; then
     ARCH="Docker Container"
   elif [ "$(grep kube /proc/1/cgroup 2>/dev/null)" ]; then
     ARCH="Kubernetes"
   else
     ARCH=$(hostnamectl | grep -E "(Static|Chassis|Virtualization|Operat|Kernel|Arch)" 2>/dev/null)
   fi
   if [ ! -z "$ARCH" ]; then print0 "$ARCH\n"; else print0 "Architecture not available.\n"; fi
   ech0
}

function report_cpu_info() {
   print_color "### CPU Information ###\n"
   CPUINFO=$(lscpu | grep -E "(op-mode|ddress|hread|ocket|name|MHz|vendor|cache)" 2>/dev/null)
   if [ ! -z "$CPUINFO" ]; then print0 "$CPUINFO\n"; else print0 "CPU info is not available.\n"; fi
   ech0
}

function report_disk_space() {
   print_color "### Storage (excluding tmpfs) ###\n"
   DFH=$(df -h | grep -v "tmpfs")
   if [ ! -z "$DFH" ]; then ech0 "$DFH"; else print0 "Free space info is not available.\n"; fi
   ech0
}

function report_memory_info() {
   print_color "### Memory Information ###\n"
   MEMINFO=$(free -h 2>/dev/null)
   if [ ! -z "$MEMINFO" ]; then print0 "$MEMINFO\n"; else print0 "Memory info is not available.\n"; fi
   ech0
}

function report_columnstore_mounts() {
  set_data1dir
  CSDIR=$(basename $(dirname $DATA1DIR))
  print_color "### Columnstore Mounts ###\n"
  COLSTORE_MOUNTS=$(mount | grep $CSDIR)
  if [ ! -z "$COLSTORE_MOUNTS" ]; then print0 "$COLSTORE_MOUNTS\n"; 
  else 
  for ((i=1;i<=9;i++)); do
  DDIR=$(mcsGetConfig SystemConfig DBRoot${i} 2>/dev/null)
    if [ ! -z $DDIR ]; then
      if [ -L $DDIR ]; then
         if [ $SYMLINKSINPLACE ]; then ech0 '--'; fi
         RDLINK=$(readlink $DDIR)
         ech0 "$DDIR is a symbolic link to $RDLINK"
         COLSTORE_MOUNTS=$(mount | grep $RDLINK)
         if [ ! -z "$COLSTORE_MOUNTS" ]; then print0 "$COLSTORE_MOUNTS\n"; fi
         SYMLINKSINPLACE=true
      fi
    fi
  done
  if [ ! $SYMLINKSINPLACE ]; then print0 "No file system mount with directory name like $CSDIR.\n"; fi
  fi
  ech0
}

function report_storage_type() {
  if [ ! $CS_INSTALLED ]; then return; fi
  print_color "### Storage Type ###\n"
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ ! -z "$STORAGE_TYPE" ]; then print0 "$STORAGE_TYPE\n"; else print0 "Storage type not found.\n"; fi
  ech0
}

function report_listening_procs() {
  print_color "### Listening Processes ###\n"
  LISTENING=$(lsof -i | grep LISTEN | grep -E "(mariadb|workernod|controlle|PrimProc|ExeMgr|WriteEngi|DMLProc|DDLProc|python3)")
  if [ ! -z "$LISTENING" ]; then print0 "$LISTENING\n"; else print0 "No mariadb processes listening.\n"; fi
  ech0
}

function report_columnstore_query_count() {
if [ $CS_RUNNING ] && [ $CAN_CONNECT ]; then
  print_color "### Columnstore Queries ###\n"
  ech0 "$(timeout 3 mariadb -ABNe 'select calgetsqlcount();')"
  ech0
fi
}

function report_host_datetime() {
  print_color "### Host DateTime ###\n"
  DT=$(date +"%Y-%m-%d %H:%M:%S" 2>/dev/null);
  if [ ! -z "$DT" ]; then print0 "$DT\n"; else print0 "The command date failed.\n"; fi
  ech0
}

function report_local_hostname_and_ips() {
  print_color "### Hostname / IPs ###\n"
  ech0 "$(hostname -f 2>/dev/null) / $(hostname -I 2>/dev/null)" || ech0 "The command hostname failed.\n"
  ech0
}

function report_db_processes(){
  if [ ! $CAN_CONNECT ]; then return; fi
  print_color "### MariaDB Database Connections ###\n"
  PROCS=$(mariadb -v -v -v -Ae "select count(*) as 'HOW_MANY', user as 'USER' from information_schema.processlist where user<>'system user' AND host<>'localhost' group by user;" | sed -nr '/^(\+|^\|)/p')
  if [ ! -z "$PROCS" ]; then print0 "$PROCS\n"; else print0 "No connections.\n"; fi
  ech0
}

function report_slave_status(){
  if [ ! $CAN_CONNECT ]; then return; fi
  print_color "### Slave Status ###\n"
  SLAVE_STATUS=$(mariadb -Ae "show all slaves status\G"| grep Slave_SQL_Running_State | head -1 | xargs)
  if [ "$SLAVE_STATUS" == "Slave_SQL_Running_State:" ]; then print0 "Replication is not running.\n"; ech0; return; fi
  if [ ! -z "$SLAVE_STATUS" ]; then print0 "$SLAVE_STATUS\n"; else print0 "Not a slave.\n"; fi
  ech0
}

function report_slave_hosts() {
   if [ ! $CAN_CONNECT ]; then return; fi
   print_color "### Replicas ###\n"
   REPLICAS=$(mariadb -v -v -v -Ae "show slave hosts;" | sed -nr '/^(\+|^\|)/p')
   if [ ! -z "$REPLICAS" ]; then print0 "$REPLICAS\n"; else print0 "No replicas found.\n"; fi
   ech0
}

function report_columnstore_tables() {
   if [ ! $CAN_CONNECT ]; then return; fi
   print_color "### Columnstore Tables ###\n"
   TABLESUMMARY=$(mariadb -v -v -v -Ae "select TABLE_SCHEMA as 'SCHEMA', count(*) as 'COLUMNSTORE_TABLES' from information_schema.tables where ENGINE='Columnstore' group by table_schema order by table_schema;" | sed -nr '/^(\+|^\|)/p')
   TABLECOUNT=$(mariadb -v -v -v -Ae "select count(*) as 'TABLE COUNT', engine as 'ENGINE' from information_schema.tables where table_schema not in ('sys','information_schema','mysql','performance_schema') group by engine;" | sed -nr '/^(\+|^\|)/p') 
   if [ ! -z "$TABLESUMMARY" ]; then print0 "$TABLESUMMARY\n"; fi
   if [ ! -z "$TABLECOUNT" ]; then print0 "$TABLECOUNT\n"; fi
   ech0
}

function report_topology() {
  if [ ! $CS_INSTALLED ]; then return; fi
  PM1=$(mcsGetConfig pm1_WriteEngineServer IPAddr)
  PM2=$(mcsGetConfig pm2_WriteEngineServer IPAddr)
  print_color "### Columnstore Topology ###\n"
  if [ ! "$PM1" == "127.0.0.1" ] && [ ! -z $PM2 ]; then
    THISISCLUSTER=true
    print0 "Cluster\n"
    THISHOSTIPS="$(hostname -i| awk '{print $1}')"
    for ((i=1;i<=9;i++)); do
      IPADDRESS=$(mcsGetConfig pms${i} ipaddr)
      if [ $IPADDRESS ]; then
        unset THISHOST
        if [[ "$THISHOSTIPS" == *"$IPADDRESS"* ]]; then THISHOST="(This host)"; fi
        ech0 "pm$i $IPADDRESS $THISHOST"
      fi
    done
  else
    print0 "Not a cluster\n"
  fi
  unset PM1 PM2
  ech0
}

function report_cmapi_failover_enabled() {
  if [ ! $CMAPI_INSTALLED ]; then return; fi
  print_color "### CMAPI Auto-Failover ###\n"
  
  if [ ! -f /etc/columnstore/cmapi_server.conf ]; then 
   print0 "cmapi_server.conf does not exist.\n"; return;
  fi
  
  AUTO_FAILOVER_STATE=$(my_print_defaults --defaults-file=/etc/columnstore/cmapi_server.conf application | grep -i failover | tail -1 | sed 's/-//g')
  if [ ! -z $AUTO_FAILOVER_STATE ]; then
    print0 "$AUTO_FAILOVER_STATE\n"
    ech0
  else
    print0 "Enabled\n"
    ech0
  fi  
}


function report_dbroots() {
  if [ ! $CS_INSTALLED ]; then return; fi
  print_color "### DBRoots ###\n"
  DBROOTS=$(mcsGetConfig -a | grep ModuleDBRootID | grep -v unassigned || ech0 'Something went wrong querying DBRoots.')
  print0 "$DBROOTS\n"
  ech0
}

function report_brm_saves_current() {
  if [ ! $CS_INSTALLED ]; then return; fi
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs | awk '{print tolower($0)}')
  if [ ! "$STORAGE_TYPE" == "localstorage" ]; then return; fi
  print_color "### Current BRM Files ###\n"
  DATA1DIR=$(mcsGetConfig SystemConfig DBRoot1 2>/dev/null) || DATA1DIR=/var/lib/columnstore/data1
  BRMSAV=$(cat $DATA1DIR/systemFiles/dbrm/BRM_saves_current || ech0 'BRM_saves_current does not exist.')
  print0 "$BRMSAV\n"
  ech0
}

function report_calpontsys_exists() {
  if [ ! $CAN_CONNECT ]; then return; fi
  print_color "### Calpontsys Schema ###\n"
  IS_CALPONTSYS=$(mariadb -ABNe "select 'yes' from information_schema.schemata where schema_name='calpontsys' limit 1;")
  IS_CALPONTSYS_SYSTABLE=$(mariadb -ABNe "select 'yes' from information_schema.tables where table_schema='calpontsys' and table_name='systable' limit 1;")
  IS_CALPONTSYS_SYSCOLUMN=$(mariadb -ABNe "select 'yes' from information_schema.tables where table_schema='calpontsys' and table_name='syscolumn' limit 1;")
  if [ "$IS_CALPONTSYS" == "yes" ]; then print0 "The database schema calpontsys exists.\n"; else print0 "The schema calpontsys does not exist.\n"; return; fi
  if [ "$IS_CALPONTSYS_SYSTABLE" == "yes" ]; then print0 "The table calpontsys.systable exists.\n"; else print0 "The table calpontsys.systable does not exist.\n"; fi
  if [ "$IS_CALPONTSYS_SYSCOLUMN" == "yes" ]; then print0 "The table calpontsys.syscolumn exists.\n"; else print0 "The table calpontsys.syscolumn does not exist.\n"; fi
  ech0
}

function displaytime() {
  local T=$1
  local W=$((T/60/60/24/7))
  local D=$((T/60/60/24%7))
  local H=$((T/60/60%24))
  local M=$((T/60%60))
  (( $W > 1 )) && printf '%d weeks, ' $W
  (( $W == 1 )) && printf '%d week, ' $W
  (( $D > 1 )) && printf '%d days, ' $D
  (( $D == 1 )) && printf '%d day, ' $D
  (( $H > 1 )) && printf '%d hours, ' $H
  (( $H == 1 )) && printf '%d hour, ' $H
  (( $M > 1 )) && printf '%d minutes ' $M 
  (( $M == 1 )) && printf '%d minute ' $M
  (( $M == 0 )) && printf '0 minutes ' $M
}

function report_uptime() {
  print_color "### UPTIME ###\n"
  ech0 "Host uptime:               $(uptime -p)"
  if [ ! $CAN_CONNECT ]; then ech0; return; fi
  SCNDS=$(mariadb -ABNe "show global status like 'Uptime';" | awk '{print $2}')
  ech0 "Mariadb uptime:            up $(displaytime $SCNDS)"
  CMAPIPID=$(pidof /usr/share/columnstore/cmapi/python/bin/python3)
  if [ "$CMAPIPID" ]; then
    CMAPI_UPTIME=$(ps -p $CMAPIPID -o etimes | tail -1 | xargs)
    ech0 "Cmapi uptime:              up $(displaytime $CMAPI_UPTIME)"
  fi
  WORKERNODEPID=$(pidof /usr/bin/workernode)
  if [ "$WORKERNODEPID" ]; then
     WORKERNODE_UPTIME=$(ps -p $WORKERNODEPID -o etimes | tail -1 | xargs)
     echo "Workernode uptime:         up $(displaytime $WORKERNODE_UPTIME)"
  fi  
  CONTROLLERNODEPID=$(pidof /usr/bin/controllernode)
  if [ "$CONTROLLERNODEPID" ]; then
     CONTROLLERNODE_UPTIME=$(ps -p $CONTROLLERNODEPID -o etimes | tail -1 | xargs)
     echo "Controlernode uptime:      up $(displaytime $CONTROLLERNODE_UPTIME)"
  fi
  PRIMPROCPID=$(pidof /usr/bin/PrimProc)
  if [ "$PRIMPROCPID" ]; then
     PRIMPROC_UPTIME=$(ps -p $PRIMPROCPID -o etimes | tail -1 | xargs)
     echo "Primproc uptime:           up $(displaytime $PRIMPROC_UPTIME)"
  fi  
  EXEMGRPID=$(pidof /usr/bin/ExeMgr)
  if [ "$EXEMGRPID" ]; then
     EXEMGR_UPTIME=$(ps -p $EXEMGRPID -o etimes | tail -1 | xargs)
     echo "ExeMgr uptime:             up $(displaytime $EXEMGR_UPTIME)"
  fi  
  WRITEENGINESERVERPID=$(pidof /usr/bin/WriteEngineServer)
  if [ "$WRITEENGINESERVERPID" ]; then
     WRITEENGINESERVER_UPTIME=$(ps -p $WRITEENGINESERVERPID -o etimes | tail -1 | xargs)
     echo "WriteEngineServer uptime:  up $(displaytime $WRITEENGINESERVER_UPTIME)"
  fi
  DMLPROCPID=$(pidof /usr/bin/DMLProc)
  if [ "$DMLPROCPID" ]; then
     DMLPROC_UPTIME=$(ps -p $DMLPROCPID -o etimes | tail -1 | xargs)
     echo "DMLProc uptime:            up $(displaytime $DMLPROC_UPTIME)"
  fi  
  DDLPROCPID=$(pidof /usr/bin/DDLProc)
  if [ "$DDLPROCPID" ]; then
     DDLPROC_UPTIME=$(ps -p $DDLPROCPID -o etimes | tail -1 | xargs)
     echo "DDLProc uptime:            up $(displaytime $DDLPROC_UPTIME)"
  fi 
  ech0
}

function report_versions() {
   if [  $CAN_CONNECT ]; then
     print_color "### MariaDB Version ###\n"
     ech0 "$(mariadb -ABNe "select version();")"
     ech0
     if [ $CS_INSTALLED ]; then
       print_color "### Columnstore Version ###\n"
       ech0 "$(mariadb -ABNe "show global status like 'Columnstore_version';")"
       ech0 "$(mariadb -ABNe "show global status like 'Columnstore_commit_hash';")"
       ech0
     fi
   else
        print_color "### MariaDB Version ###\n"
        MDBVS="$(find /usr -name mariadbd -exec {} --version \; 2>/dev/null || echo 'MariaDB Version not available.')"
        print0 "$MDBVS\n"
        ech0
     if [ -f /usr/share/columnstore/releasenum ]; then
        print_color "### Columnstore Version ###\n"
        CSVS="$(cat /usr/share/columnstore/releasenum 2>/dev/null || echo 'Columnstore Version not available.')"
        print0 "$CSVS\n"
        ech0
     fi
   fi
   CMAPI_VERSION_FILE=/usr/share/columnstore/cmapi/VERSION
   if [ -f $CMAPI_VERSION_FILE ]; then
     . $CMAPI_VERSION_FILE
     if [ $CMAPI_VERSION_MAJOR ]; then
       print_color "### CMAPI Version ###\n"
       ech0 $CMAPI_VERSION_MAJOR.$CMAPI_VERSION_MINOR.$CMAPI_VERSION_PATCH
       ech0
     fi
   fi
}

function report_warnings() {
  print_color "### Warnings ###\n"
  WARNINGS=$(cat $WARNFILE)
  if [ ! -z "$WARNINGS" ]; then print0 "$WARNINGS\n"; else print0 "There are no warnings to report.\n"; fi
  ech0
}

function report_datadir_size() {
  set_log_error
  if [ ! $DATADIR ]; then return; fi
  if [ -d $DATADIR ]; then
    print_color "### Mariadb Datadir Size (File space usage) ###\n"
    ech0 "$(du -sh $DATADIR)"
    ech0
  fi
}

function set_data1dir() {
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then
    DATA1DIR=/var/lib/columnstore/storagemanager/metadata/data1
  else
    DATA1DIR=$(mcsGetConfig SystemConfig DBRoot1 2>/dev/null) || DATA1DIR=/var/lib/columnstore/data1
  fi
}

function set_log_error() {
  unset DATADIR LOG_ERROR FULL_PATH
  if [ $CAN_CONNECT ]; then
    LOG_ERROR=$(mariadb -ABNe "select @@log_error")
    DATADIR=$(mariadb -ABNe "select @@datadir" | sed 's:/*$::')
  else
    LOG_ERROR=$(my_print_defaults --mysqld| grep log_error | tail -1 | cut -d "=" -f2)
    DATADIR=$(my_print_defaults --mysqld| grep datadir | tail -1 | cut -d "=" -f2 | sed 's:/*$::')
  fi
  if [ -z "$LOG_ERROR" ]; then unset LOG_ERROR; return; fi;
  if [[ $LOG_ERROR =~ ^/.* ]]; then FULL_PATH=true; fi

  if [ ! $FULL_PATH ]; then
    LOG_ERROR=$(basename $LOG_ERROR)
    LOG_ERROR=$DATADIR/$LOG_ERROR
  fi
}

function dump_log () {
    name=$1
    path=$2
    journalctl -u "$name".service > $path/"${name}".log
}

function collect_logs() {

  if [ -n "$USER_PROVIDED_OUTPUT_PATH" ]; then
    TARPATH="$USER_PROVIDED_OUTPUT_PATH"
    LOGSOUTDIR="$USER_PROVIDED_OUTPUT_PATH/columnstore_review/logs_$(date +"%m-%d-%H-%M-%S")/$(hostname)"
  else
    TARPATH=/tmp
    LOGSOUTDIR=/tmp/columnstore_review/logs_$(date +"%m-%d-%H-%M-%S")/$(hostname)
  fi

  mkdir -p $LOGSOUTDIR || ech0 'Cannot create temporary directory for logs.';
  mkdir -p $LOGSOUTDIR/system
  mkdir -p $LOGSOUTDIR/mariadb
  mkdir -p $LOGSOUTDIR/columnstore
  mkdir -p $LOGSOUTDIR/systemd
  COMPRESSFILE=$(hostname)_$(date +"%Y-%m-%d-%H-%M-%S")_logs.tar.gz
  set_log_error
  if [ $LOG_ERROR ]; then
    OUTFILE=$LOGSOUTDIR/mariadb/$(hostname)_$(date +"%Y-%m-%d-%H-%M-%S")_$(basename $LOG_ERROR)
    FILTEREDFILE=$LOGSOUTDIR/mariadb/$(hostname)_$(date +"%Y-%m-%d-%H-%M-%S")_filtered_$(basename $LOG_ERROR)
    if [ -f $LOG_ERROR ]; then
      tail -100000 $LOG_ERROR > $OUTFILE
    fi
    if [ -f $LOG_ERROR ]; then
      tail -1000000 $LOG_ERROR | grep -iv "\[warning\]"|grep -iv "\[note\]"  > $FILTEREDFILE
    fi
  fi
  cp /etc/columnstore/Columnstore.xml $LOGSOUTDIR/columnstore
  tar cf $LOGSOUTDIR/columnstore/etc_columnstore.tar /etc/columnstore/ 1>/dev/null 2>&1
  dump_log "mariadb" $LOGSOUTDIR/columnstore/
  dump_log "mcs-ddlproc" $LOGSOUTDIR/columnstore/
  dump_log "mcs-dmlproc" $LOGSOUTDIR/columnstore/
  dump_log "mcs-loadbrm" $LOGSOUTDIR/columnstore/
  dump_log "mcs-primproc" $LOGSOUTDIR/columnstore/
  dump_log "mcs-workernode@1" $LOGSOUTDIR/columnstore/
  dump_log "mcs-workernode@2" $LOGSOUTDIR/columnstore/
  dump_log "mcs-writeengineserver" $LOGSOUTDIR/columnstore/
  dump_log "mcs-controllernode" $LOGSOUTDIR/columnstore/

  set_data1dir
  ls -lrt $DATA1DIR/systemFiles/dbrm > $LOGSOUTDIR/columnstore/ls_lrt_dbrm.txt
  if [ ! -z "$STORAGE_TYPE" ] && [ "$STORAGE_TYPE" == "S3" ]; then 
    dump_log "mcs-storagemanager" $LOGSOUTDIR/columnstore/
    smls /data1/systemFiles/dbrm/ >  $LOGSOUTDIR/columnstore/s3_dbrms.txt ;
    smcat /data1/systemFiles/dbrm/BRM_saves_current 2>/dev/null > $LOGSOUTDIR/columnstore/s3_BRM_saves_current ;
  fi


  # System Logs
  if [ -f "/proc/sys/kernel/threads-max" ]; then cp /proc/sys/kernel/threads-max $LOGSOUTDIR/system/kernal-threads-max; fi;
  if [ -f "/proc/sys/kernel/pid_max" ]; then cp /proc/sys/kernel/pid_max $LOGSOUTDIR/system/kernal-pid_max; fi;
  if [ -f "/proc/sys/vm/max_map_count" ]; then cp /proc/sys/vm/max_map_count $LOGSOUTDIR/system/kernal-max_map_count; fi;
  #   if [ -f "/var/log/messages" ]; then cp /var/log/messages* $LOGSOUTDIR/system; fi;  # TOO MUCH COLLECTED...
  find /var/log -name "messages*" -mtime -5 -type f -exec cp {} $LOGSOUTDIR/system \; 2>/dev/null

  if [ -f "/var/log/syslog" ]; then find /var/log/syslog -name syslog -type f -exec tail -10000 {} > $LOGSOUTDIR/system/syslog \;; fi;
  if [ -f "/var/log/daemon.log" ]; then find /var/log/daemon.log -name daemon.log -type f -exec tail -10000 {} > $LOGSOUTDIR/system/daemon.log \;; fi;
  if command -v ulimit >/dev/null 2>&1; then
      ulimit -a >  $LOGSOUTDIR/system/kernal-ulimits.txt
  fi
  
  cd /var/log/mariadb
  find /usr/lib -name "mcs*service" -exec cp {} $LOGSOUTDIR/systemd \;
  find /usr/lib -name "mariadb*service" -exec cp {} $LOGSOUTDIR/systemd \;

  ls -1 columnstore/*.log 2>/dev/null | cpio -pd $LOGSOUTDIR/ 2>/dev/null
  ls -1 columnstore/*z 2>/dev/null | cpio -pd $LOGSOUTDIR/ 2>/dev/null
  find columnstore/archive columnstore/install columnstore/trace -mtime -30 | cpio -pd $LOGSOUTDIR/ 2>/dev/null 
  #  find columnstore/cpimport -mtime -1 | cpio -pd $LOGSOUTDIR/ 2>/dev/null   # COLLECTS TOO MUCH
  find columnstore/cpimport -name "*.err" -size +0 -mtime -2 | cpio -pd $LOGSOUTDIR/ 2>/dev/null

  #collect ports Status
  unset SUPPRESS_CLOSED_PORTS
  check_ports > $LOGSOUTDIR/columnstore/$(hostname)_ports_check.txt 2>/dev/null


  if [ $CAN_CONNECT ]; then
    mariadb -ABNe "show global variables" > $LOGSOUTDIR/mariadb/$(hostname)_global_variables.txt 2>/dev/null
    mariadb -ABNe "show global status" > $LOGSOUTDIR/mariadb/$(hostname)_global_status.txt 2>/dev/null
  fi
  my_print_defaults --mysqld > $LOGSOUTDIR/mariadb/$(hostname)_my_print_defaults.txt 2>/dev/null 
  if [ -f $OUTPUTFILE ]; then cp $OUTPUTFILE $LOGSOUTDIR/; fi
  cd $LOGSOUTDIR/..
  tar -czf $TARPATH/$COMPRESSFILE ./*
  cd - 1>/dev/null 
  print_color "### COLLECTED LOGS FOR SUPPORT TICKET ###\n"
  ech0 "Attach the following tar file to your support ticket."
  if [ $THISISCLUSTER ]; then
    ech0 "Please collect logs with this script from each node in your cluster."
  fi
  FILE_SIZE=$(stat -c %s $TARPATH/$COMPRESSFILE)
  if (( $FILE_SIZE > 52428800 )); then
    print0 "The file $TARPATH/$COMPRESSFILE is larger than 50MB.\nPlease use MariaDB Large file upload at https://mariadb.com/upload/\nInform us about the upload in the support ticket.\n"
  fi 
  print0 "\nCreated: $TARPATH/$COMPRESSFILE\n"
  ech0
}

function empty_directories(){
  set_data1dir
  COLUMNSTOREDIR=$(dirname $DATA1DIR)
  print0 "Empty directories in $COLUMNSTOREDIR can be caused by failed runs of cpimport.\nThey are not necessarily a serious issue.\n"
  ech0
  print_color "### Empty Directories in $COLUMNSTOREDIR ###\n"
  EMPTY_DIRS=$(find $COLUMNSTOREDIR -type d -name "[0-9][0-9][0-9].dir" -empty)
  ech0 "$EMPTY_DIRS"
  ech0
  display_outputfile_message
}

function not_mysql_directories(){
  if [ -f /mnt/skysql/podinfo/namespace ] && [ -f /mnt/skysql/podinfo/podname ]; then print0 "This is not required for SkySQL instances.\n\n"; return; fi
  set_data1dir
  COLUMNSTOREDIR=$(dirname $DATA1DIR)
  print0 "There should not be any directores in $COLUMNSTOREDIR that are not owned by mysql.\n"
  ech0
  print_color "### Directories not owned by mysql ###\n"
  NOT_MYSQL_DIRS=$(find $COLUMNSTOREDIR/ ! -user mysql -type d)
  ech0 "$NOT_MYSQL_DIRS"
  ech0
  display_outputfile_message
}

function add_pscs_alias() {
CSALIAS=/etc/profile.d/columnstoreAlias.sh
if [ ! -f $CSALIAS ]; then
  TEMP_COLOR=lred; print_color "The file $CSALIAS is missing."; unset TEMP_COLOR; exit 0;
fi
PSCS=$(grep alias $CSALIAS | sed 's,=.*,,' | awk '{print $2}' | grep pscs | head -1)
if [ ! $PSCS ]; then
   print_color "### Adding pscs alias to $CSALIAS ###\n"
   echo "" >> $CSALIAS
   echo "# pscs by Edward Stoever for Mariadb Support" >> $CSALIAS
   printf "alias pscs=\'ps -ef | grep -E \"(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)\" | grep -v \"grep\"\'\n\n" >> $CSALIAS
   ech0 "The alias pscs was added to $CSALIAS."
   ech0 "To use the command, source the file $CSALIAS or logout and login."
else
  ech0 "The alias pscs is already included in $CSALIAS."
fi
ech0
}

function create_test_schema() {
KEEP_SCHEMA="${1:NO}"
if [[ "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
  ech0 "Columnstore is not running."; ech0; return;
fi
if [ ! $CAN_CONNECT ]; then
  ech0 "Cannot connect to database."; ech0; return;
fi
print_color "### Creating a Schema and Two Tables. ###\n"
ech0
print0 "If query succeeds, Columnstore is working.\n"
ech0
unset SCHEMA FILEAUTOMOBILES DATA1 SQL OUTDIR
OUTDIR=/tmp/columnstore_review; mkdir -p $OUTDIR
FILEAUTOMOBILES=$OUTDIR/automobiles.txt
FILEACCIDENTS=$OUTDIR/accindents.txt
DATA1="'Datsun','Zcar','dz01','1972',678954321,'2015-02-12 11:33:55'
'Mazda','MX-5 Miata','mm01','1978',93421329,'2016-01-19 15:01:23'
'Lada','Riva','lr01','1981',876323,'2016-03-10 08:38:51'
'Delorian','DSV-12','dd01','1979',10621,'2016-09-01 01:05:12'\n"
DATA2="'dd01','It was a complete smashup.','2020-09-02 05:09:32',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2020-10-01 12:11:31'
'mm01','It was a headon collision.','2021-01-01 06:06:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-01-02 18:11:44'
'lr01','It was a fender bender.','2021-05-22 09:23:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-06-01 08:31:18'
'dd01','Just a scratch.','2021-06-22 02:21:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-06-25 12:12:01',
'dd01','Fire damage caused by smoking.','2021-08-19 11:10:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-08-20 14:19:01'
'lr01','Dropped off cliff. Total loss.','2021-09-02 12:19:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-09-04 18:51:13'
'dz01','Complete and total smash up.','2021-12-04 18:18:00',$((101+RANDOM%(33000-101))).$((RANDOM%99)),'2021-12-05 15:50:09'\n"
if [ -f $FILEAUTOMOBILES ]; then truncate -s0 $FILEAUTOMOBILES; fi
if [ -f $FILEACCIDENTS ]; then truncate -s0 $FILEACCIDENTS; fi
printf "$DATA1" > $FILEAUTOMOBILES
printf "$DATA2" > $FILEACCIDENTS
SCHEMA=test_$(date +"%M%S")_$(echo $RANDOM | md5sum | head -c 5)_$(echo $RANDOM)_$(echo $RANDOM | md5sum | head -c 3)
SQL="create schema if not exists $SCHEMA; use $SCHEMA;
create table automobiles (make varchar(100),
                         model varchar(100),
                         model_code varchar(100),
                         year_introduced varchar(100),
                         qty_manufactured bigint,
                         data_entered datetime)
ENGINE=Columnstore DEFAULT CHARSET=utf8mb4;
create table accidents(model_code varchar(100),
                       description mediumtext,
                       occurred datetime,
                       cost float(10,2),
                       data_entered datetime)
ENGINE=Columnstore DEFAULT CHARSET=utf8mb4;"
mariadb -Ae "$SQL" || echo 'Something went wrong.'
cpimport -s "," -E "'" $SCHEMA automobiles $FILEAUTOMOBILES
cpimport -s "," -E "'" $SCHEMA accidents $FILEACCIDENTS
SQL="use $SCHEMA; select make,model, count(a.model_code) as accidents_per_model, concat('$',format(sum(cost),2)) as cost_of_all_accidents from
accidents a inner join automobiles b on a.model_code=b.model_code
group by  make, model;"
mariadb -Ae "$SQL" || ech0 'Something went wrong when querying test tables.'
if [ ! "$KEEP_SCHEMA" == "YES" ]; then
  SQL="drop schema $SCHEMA;"
  mariadb -Ae "$SQL" && print0 "Test Schema dropped.\n\n" || ech0 'Something went wrong when dropping schema.'
else
  ech0
fi
}

function ldli_test_schema() {
KEEP_SCHEMA="${1:NO}"

OUTDIR=/tmp/columnstore_review; mkdir -p $OUTDIR
FILEPRODUCE=$OUTDIR/produce.txt
FILEPRODUCE_LOSS=$OUTDIR/produce_loss.txt
FILEPRODUCE_SOLD=$OUTDIR/produce_sold.txt
if [ -f $FILEPRODUCE ]; then truncate -s0 $FILEPRODUCE; fi
if [ -f $FILEPRODUCE_LOSS ]; then truncate -s0 $FILEPRODUCE_LOSS; fi
if [ -f $FILEPRODUCE_SOLD ]; then truncate -s0 $FILEPRODUCE_SOLD; fi
DATA1="10,'white onions','standard white onions','2019-02-12 11:32:21'
11,'red onions','the sweet kind','2019-02-12 11:33:55'
12,'blueberries','best in summer','2019-02-12 11:35:51'
13,'bananas','purchased green but sold yellow','2019-02-12 11:41:50'"
DATA2="10,8.41,'2021-11-12 10:31:10',4,'given to charity before rotten.'
10,$((RANDOM%30)).$((RANDOM%99)),'2021-11-13 10:11:00',1,'given to charity before rotten.'
12,$((RANDOM%30)).$((RANDOM%99)),'2021-11-12 10:32:18',4,'given to charity before rotten.'
13,$((RANDOM%30)).$((RANDOM%99)),'2021-11-14 15:01:14',7,'given to charity before rotten.'
11,$((RANDOM%30)).$((RANDOM%99)),'2021-11-15 15:06:14',12,'discarded due to rot.'
11,$((RANDOM%30)).$((RANDOM%99)),'2021-11-16 14:02:11',10,'discarded due to rot.'
10,$((RANDOM%30)).$((RANDOM%99)),'2021-11-16 15:01:21',3,'discarded due to rot.'
12,$((RANDOM%50)).$((RANDOM%99)),'2021-11-16 15:03:19',11,'rotton so thrown out.'"
DATA3="11,'AA-000109654',4.31
12,'AA-000109654',$((RANDOM%20)).$((RANDOM%99))
13,'AA-000109654',$((RANDOM%10)).$((RANDOM%99))
10,'AA-000109655',$((RANDOM%20)).$((RANDOM%99))
11,'AA-000109655',$((RANDOM%10)).$((RANDOM%99))
13,'AA-000109655',$((RANDOM%30)).$((RANDOM%99))
10,'AA-000109656',$((RANDOM%10)).$((RANDOM%99))
11,'AA-000109656',$((RANDOM%30)).$((RANDOM%99))
13,'AA-000109657',$((RANDOM%10)).$((RANDOM%99))
13,'AA-000109657',$((RANDOM%40)).$((RANDOM%99))
11,'AA-000109661',$((RANDOM%10)).$((RANDOM%99))
11,'AA-000109662',$((RANDOM%40)).$((RANDOM%99))
11,'AA-000109663',$((RANDOM%10)).$((RANDOM%99))
12,'AA-000109664',$((RANDOM%50)).$((RANDOM%99))
12,'AA-000109665',$((RANDOM%10)).$((RANDOM%99))
10,'AA-000109665',$((RANDOM%50)).$((RANDOM%99))
11,'AA-000109667',$((RANDOM%10)).$((RANDOM%99))
13,'AA-000109669',$((RANDOM%60)).$((RANDOM%99))
13,'AA-000109672',$((RANDOM%10)).$((RANDOM%99))"
printf "$DATA1" > $FILEPRODUCE
printf "$DATA2" > $FILEPRODUCE_LOSS
printf "$DATA3" > $FILEPRODUCE_SOLD
unset DATA1 DATA2 DATA3
SCHEMA=test_$(date +"%M%S")_$(echo $RANDOM | md5sum | head -c 5)_$(echo $RANDOM)_$(echo $RANDOM | md5sum | head -c 3)
ech0 "Creating schema $SCHEMA."
SQL="create schema if not exists $SCHEMA; use $SCHEMA;
create table produce ( p_id integer unsigned, common_name varchar(100), description varchar(2000), first_entry datetime) engine=columnstore;
create table produce_loss ( p_id integer unsigned, estimated_value decimal(6,2), loss_dt datetime, kilos integer, notes varchar(1000)) engine=columnstore;
create table produce_sold (p_id integer unsigned, receipt_id varchar(30), gross_income decimal(6,2))engine=columnstore;"
mariadb -Ae "$SQL" || echo 'Something went wrong.'
ech0 "Loading columnstore tables with ldli syntax, columnstore_use_import_for_batchinsert=ALWAYS."
SQL="set columnstore_use_import_for_batchinsert=ALWAYS;
LOAD DATA LOCAL INFILE  '$FILEPRODUCE' INTO TABLE produce FIELDS TERMINATED BY ',' ENCLOSED BY \"\'\" LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE  '$FILEPRODUCE_LOSS' INTO TABLE produce_loss FIELDS TERMINATED BY ',' ENCLOSED BY \"\'\" LINES TERMINATED BY '\n';
LOAD DATA LOCAL INFILE  '$FILEPRODUCE_SOLD' INTO TABLE produce_sold FIELDS TERMINATED BY ',' ENCLOSED BY \"\'\" LINES TERMINATED BY '\n';"
mariadb $SCHEMA -Ae "$SQL"

SQL="SELECT A.p_id, A.common_name, SUM(B.estimated_value)*-1 AS "LOSS", SUM(C.gross_income) AS "GAIN", SUM(B.estimated_value)*-1+SUM(C.gross_income) AS "DIFFERENCE"
FROM produce A
INNER JOIN produce_loss B ON A.p_id=B.p_id
INNER JOIN produce_sold C ON A.p_id=C.p_id
GROUP BY A.p_id, A.common_name
ORDER BY p_id;"

mariadb $SCHEMA -Ae "$SQL"

if [ ! "$KEEP_SCHEMA" == "YES" ]; then
  SQL="drop schema $SCHEMA;"
  mariadb -Ae "$SQL" && print0 "Schema $SCHEMA dropped.\n\n" || ech0 'Something went wrong when dropping LDLI schema.'
else
  ech0
fi
}

function backup_dbrm() {
if [ -n "$USER_PROVIDED_OUTPUT_PATH" ]; then
  TARPATH="$USER_PROVIDED_OUTPUT_PATH"
else
  TARPATH=/tmp
fi

  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
  if [ "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then print0 "This is node uses S3 storage for Columnstore. Exiting.\n\n"; return; fi

if [[ ! "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
  CS_RUNNING=true
fi

if [ $CS_RUNNING ]; then
  TEMP_COLOR=lred; print_color "Columnstore processes are running.\n"; unset TEMP_COLOR
  ech0 "Consistency among the em files is only ensured when columnstore processes are stopped."
  ech0 "Type c to continue running the script taking a backup while columnstore is running."
  ech0 "Type any other key to exit."

  read -s -n 1 RESPONSE
  if [ ! "$RESPONSE" = "c" ]; then
    print0 "\n Exiting.\n\n";  exit 0
  fi
fi
  if [ $CS_RUNNING ]; then
    COMPRESSFILE=$(hostname)_r_$(date +"%Y-%m-%d-%H-%M-%S")_dbrm.tar.gz
  else
    COMPRESSFILE=$(hostname)_s_$(date +"%Y-%m-%d-%H-%M-%S")_dbrm.tar.gz
  fi
  set_data1dir
  cd $DATA1DIR/systemFiles
  tar -czf $TARPATH/$COMPRESSFILE ./dbrm
  cd - 1>/dev/null
  print_color "### DBRM EXTENT MAP BACKUP ###\n"
  ech0 "Files in dbrm directory backed up to compressed archive $TARPATH/$COMPRESSFILE"
  ech0 "Files in /tmp can be deleted on reboot. It is recommended to move the archive to a safe location."
  ech0
}

function oid_missing_file() {
  MISSINGFILE=$(grep $1 $FILE_EM_FILES)
  FL=$(echo "$MISSINGFILE" | awk '{print $1}')
  OID=$(echo "$MISSINGFILE" | awk '{print $2}')
  printf "$(echo $MISSINGFILE | awk '{print $2}')," >> $MISSING_OIDS;
  ech0 "$FL   OID: $OID"
}

function extent_map_check() {
if [ $CAN_CONNECT ]; then 
  IS_SYSCOLUMN=$(mariadb -ABNe "select count(*) from information_schema.tables where table_schema='calpontsys' and table_name='syscolumn';");
else
  IS_SYSCOLUMN=0;
fi
STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
if [ ! "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "localstorage" ]; then
  TEMP_COLOR=lred; print_color "Storage type is $STORAGE_TYPE. "; unset TEMP_COLOR; print0 "This storage type is unupported for the extent map check. Try --s3check for a check of s3 storage instead.\n\n"; exit 0
fi

if [ ! $CAN_CONNECT ]; then
  TEMP_COLOR=lred; print_color "You cannot connect to the database. "; unset TEMP_COLOR; 
  print0 "An extent map check can be performed without listing effected database objects.\n"
fi

print_color "### Extent Map Check ###\n"
CACHEFILE=$EMOUTDIR/cachetimestamp.txt
if [ -f $CACHEFILE ]; then
  DT=$(cat $CACHEFILE|head -1); FILE_EM=$EMOUTDIR/em_$DT.txt
  if [ -f $FILE_EM ]; then
    CACHETIMESTAMP=(cat $CACHEFILE); MOD_TIME=$(stat -c %Y $CACHEFILE); RIGHTNOW=$(date +%s); HOW_LONG=$(expr $RIGHTNOW - $MOD_TIME); NUM_MINS=$(expr $HOW_LONG / 60);
    if (( $NUM_MINS < 360 )); then # DO NOT OFFER TO USE CACHED EM OLDER THAN 6 HOURS
      ech0 "It is recommended to run this process infrequently. You have a cached extent map that you can use."
      TEMP_COLOR=lmagenta; print_color "Do you want to run this report from the cached extent map file that is from $NUM_MINS minutes ago?"; unset TEMP_COLOR;
      read -r -p " [y/N] " response
      if [[ $response == "y" || $response == "Y" || $response == "yes" || $response == "Yes" ]]; then USECACHED=true; else unset USECACHED; fi
    else
      unset USECACHED
    fi
  fi
fi

if [ ! $USECACHED ]; then
  if [[ "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
    TEMP_COLOR=lred; print_color "\nColumnstore must be running to perform a new extent map check.\n\n"; unset TEMP_COLOR; return
  fi
fi

if [ $USECACHED ]; then
  print0 "\nUsing cached extent map file to generate report. Please wait while report is generated.\n";
  DT=$(cat $CACHEFILE|head -1)
else
  DT=$(date +"%m_%d_%H_%M_%S")
  printf $DT > $CACHEFILE
fi

if [ ! $USECACHED ]; then
ech0
if [ $CAN_CONNECT ] && [ "$IS_SYSCOLUMN" == "1" ]; then
  COLUMNSTORE_COLCOUNT=$(mariadb -ABNe "select count(1) from calpontsys.syscolumn;"); COLUMNSTORE_COLCOUNT=$(printf "%'d" $COLUMNSTORE_COLCOUNT)
  print0 "Your instance has $COLUMNSTORE_COLCOUNT Columnstore columns. There will likely be more than 2 files on disk for each column.\n"
fi
print0 "On very large databases, this process can take a long time.\n"
print0 "It is recommended that you allow this process to complete.\n"
print0 "Killing the editem process while it is running can damage the extent map.\n"
TEMP_COLOR='lred'; print_color "Do you want to run this process?"; unset TEMP_COLOR
  read -r -p " [y/N] " response
   if [[ $response == "y" || $response == "Y" || $response == "yes" || $response == "Yes" ]]; then CONTINUE=true; else ech0 "exiting..."; exit 0; fi
   if [ $CONTINUE ]; then print0 "\nGenerating a new report. This may take a while.\n"; fi;
fi

FILEPL=$EMOUTDIR/convertem.pl
FILE_EM=$EMOUTDIR/em_$DT.txt
DT=$(date +"%m_%d_%H_%M_%S")
FILE_EM_FILES=$EMOUTDIR/em_files_$DT.txt; touch $FILE_EM_FILES; truncate -s0 $FILE_EM_FILES
FILE_ALL_FILES=$EMOUTDIR/all_files_$DT.txt; touch $FILE_ALL_FILES; truncate -s0 $FILE_ALL_FILES
MISSING_FILES=$EMOUTDIR/missing_files_$DT.txt; touch $MISSING_FILES; truncate -s0 $MISSING_FILES
MISSING_OIDS=$EMOUTDIR/oids_missing_files_$DT.txt; touch $MISSING_OIDS; truncate -s0 $MISSING_OIDS

set_data1dir
COLUMNSTOREDIR=$(dirname $DATA1DIR)
PLSCRIPT="my (\$pn) = \$ARGV[0];
@parts = split(/\|/, \$pn);
if (@parts <= 7) { exit; }
\$oid = \$parts[2];
\$seqnum = \$parts[6];
\$partnum = \$parts[5];
\$dbroot = \$parts[7];
\$z1 = \$oid >> 24;
\$dir1 = sprintf('%03d',\$z1);
\$t2 = \$oid & hex '0x00FF0000';
\$z2 = \$t2 >> 16;
\$dir2 = sprintf('%03d',\$z2);
\$t3 = \$oid & hex '0x0000FF00';
\$z3 = \$t3 >> 8;
\$dir3 = sprintf('%03d',\$z3);
\$z4 = \$oid & hex '0x000000ff';
\$dir4 = sprintf('%03d',\$z4);
\$dir5 = sprintf('%03d',\$partnum);
\$dir6 = sprintf('%03d',\$seqnum);
\$fl = '$COLUMNSTOREDIR/data'.\$dbroot.'/'.\$dir1.'.dir/'.\$dir2.'.dir/'.\$dir3.'.dir/'.\$dir4.'.dir/'.\$dir5.'.dir/FILE'.\$dir6.'.cdf';
print \$fl,' ',\$oid,\"\n\";"

if [ ! $USECACHED ]; then
  printf '%s\n' "$PLSCRIPT" > $FILEPL
  TEMP_COLOR=blue; print_color "Starting extent map to text file.\n"; unset TEMP_COLOR
  editem -i > $FILE_EM 2>/dev/null || echo 'An error has occurred running editem.';
  TEMP_COLOR=lblue; print_color "Extent map to text file completed.\n"; unset TEMP_COLOR
fi

  APPROX_FILE_SIZE=$(wc -c < $FILE_EM)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    if [ ! $USECACHED ]; then 
      TEMP_COLOR=lred; print_color "The output for editem is too small. "; unset TEMP_COLOR;
      print0 "Something went wrong. Check that columnstore processes are running properly.\n"; 
    else
      TEMP_COLOR=lred; print_color "The cached extent map is too small. "; unset TEMP_COLOR;
      print0 "Do not use the cached extent map file. Check that columnstore processes are running properly.\n"
    fi
    exit 0;
  fi

  cat $FILE_EM | while read line
  do
     perl $FILEPL $line >> $FILE_EM_FILES
  done
  
  APPROX_FILE_SIZE=$(wc -c < $FILE_EM_FILES)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    TEMP_COLOR=lred; print_color "The output for extent map files is too small. "; unset TEMP_COLOR; print0 "Something went wrong. Exiting\n\n"; exit 0;
  fi
  set_data1dir
  COLUMNSTOREDIR=$(dirname $DATA1DIR)
  find -L $COLUMNSTOREDIR -iname "FILE[0-9][0-9][0-9].cdf" -type f >> $FILE_ALL_FILES

  APPROX_FILE_SIZE=$(wc -c < $FILE_ALL_FILES)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    TEMP_COLOR=lred; print_color "The output for all files is too small. "; unset TEMP_COLOR; print0 "Something went wrong. Exiting\n\n"; exit 0;
  fi

TEMP_COLOR=blue; print_color "Beginning process to compare text files with grep. This can take a while.\n"; unset TEMP_COLOR
ORPHAN_COUNT=$(grep -F -x -v -f  <(cat $FILE_EM_FILES | awk '{print $1}') $FILE_ALL_FILES |wc -l)
if [ $ORPHAN_COUNT == "1" ]; then
  ORPHANED_MSG='There is 1 orphaned file:'; else ORPHANED_MSG="There are $ORPHAN_COUNT orphaned files:"
fi

MISSING_COUNT=$(grep -F -x -v -f  $FILE_ALL_FILES <(cat $FILE_EM_FILES | awk '{print $1}')|wc -l)
if [ $MISSING_COUNT == "1" ]; then
  MISSING_MSG='There is 1 missing file:'; else MISSING_MSG="There are $MISSING_COUNT missing files:"
fi

  print_color "\n### Summary ###\n"
  EM_COUNT=$(cat $FILE_EM_FILES | sort | uniq | wc -l) # Careful here, some single files have more than one entry
  ALL_FILE_COUNT=$(wc -l < $FILE_ALL_FILES);
  EM_COUNT=$(printf "%'d" $EM_COUNT); ALL_FILE_COUNT=$(printf "%'d" $ALL_FILE_COUNT);
  print0 "There are $EM_COUNT files registered in the extent map. There are $ALL_FILE_COUNT eligible columnstore extent files.\n"
  ech0

print_color "### Orphaned Files ###\n"
if [ "$ORPHAN_COUNT" -gt "0" ]; then
  ech0 "Orphaned files are those that exist on the file system but do not exist in the Columnstore extent map."
   TEMP_COLOR=lred; print_color "Mariadb Support does not recommend that orphaned files be deleted.\n"; unset TEMP_COLOR
  ech0
  if [ $USECACHED ]; then
    print0 "When a cached extent map is compared to the current file system it is possible that this script\nreports orphaned files that are not actually orphaned. This is due to tables and columns added\nsince the cached extent map was created.\n"
  fi
  ech0 "$ORPHANED_MSG"
  ech0 "$(grep -F -x -v -f  <(cat $FILE_EM_FILES | awk '{print $1}') $FILE_ALL_FILES)"
  ech0
else
  ech0 "There are no orphaned files."
  ech0
fi

print_color "### Missing files ###\n"
if [ "$MISSING_COUNT" -gt "0" ]; then
  ech0 "Missing files are those expected by the extent map but are not found on disk."
  ech0
  if [ $USECACHED ]; then
    print0 "When a cached extent map is compared to the current file system it is possible that this script\nreports missing files that are not actually missing. This is due to tables being dropped\nsince the cached extent map was created.\n"
  fi
  ech0 "$MISSING_MSG"

  grep -F -x -v -f  $FILE_ALL_FILES <(cat $FILE_EM_FILES | awk '{print $1}') > $MISSING_FILES

  cat $MISSING_FILES | while read line
  do
    oid_missing_file $line
  done
  OIDLIST=$(cat $MISSING_OIDS | sed 's/,*$//g')
  ech0
# assume errors on the next query:
  SQL="select \`schema\`,tablename,columnname, objectid, dictobjectid, listobjectid, treeobjectid
       from calpontsys.syscolumn
       where objectid in ($OIDLIST) or dictobjectid in ($OIDLIST) or listobjectid in ($OIDLIST) or treeobjectid in ($OIDLIST);"
  if [ $CAN_CONNECT ] && [ "$IS_SYSCOLUMN" == "1" ]; then
    MISSINGOIDSREPORT=$(mariadb -v -v -v -Ae "$SQL" | sed -nr '/^(\+|^\|)/p') 2>/dev/null || ERR=true
    if [ ! $ERR ]; then
      print_color "### Database Objects Effected by Missing Files ###\n"
      print0 "$MISSINGOIDSREPORT\n"
    fi
  fi
  ech0
else
  ech0 "There are no missing files."
  ech0
fi
display_outputfile_message
}

function s3_check() {
STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
if [ ! "$(echo $STORAGE_TYPE | awk '{print tolower($0)}')" == "s3" ]; then
  TEMP_COLOR=lred; print_color "Storage type is $STORAGE_TYPE. "; unset TEMP_COLOR; print0 "This storage type is unupported for the s3 check. Try --emcheck for a standard extent map check instead.\n\n"; exit 0;
fi

if [ ! $(which jq 2>/dev/null) ] || [ ! $(which s3cmd 2>/dev/null) ]; then
   print0 "The full functionality of the S3 check requires that you install jq and s3cmd.\nType c to continue running the script without full functionality.\n\n"
   read -s -n 1 RESPONSE
   if [ ! "$RESPONSE" == "c" ]; then
    print0 "\n Exiting.\n\n";   exit 0
  fi
fi

if [ ! $CAN_CONNECT ]; then
  TEMP_COLOR=lred; print_color "You cannot connect to the database. "; unset TEMP_COLOR;
  print0 "An s3 check can be performed without listing effected database objects.\n"
fi

print_color "### S3 Check ###\n"
CACHEFILE=$S3OUTDIR/cachetimestamp.txt
if [ -f $CACHEFILE ]; then
  DT=$(cat $CACHEFILE|head -1); FILE_EM=$S3OUTDIR/em_$DT.txt
  if [ -f $FILE_EM ]; then
    CACHETIMESTAMP=(cat $CACHEFILE); MOD_TIME=$(stat -c %Y $CACHEFILE); RIGHTNOW=$(date +%s); HOW_LONG=$(expr $RIGHTNOW - $MOD_TIME); NUM_MINS=$(expr $HOW_LONG / 60);
    if (( $NUM_MINS < 360 )); then # DO NOT OFFER TO USE CACHED EM OLDER THAN 6 HOURS
      ech0 "It is recommended to run this process infrequently. You have a cached S3 extent map that you can use."
      TEMP_COLOR=lmagenta; print_color "Do you want to run this report from the cached S3 extent map file that is from $NUM_MINS minutes ago?"; unset TEMP_COLOR;
      read -r -p " [y/N] " response
      if [[ $response == "y" || $response == "Y" || $response == "yes" || $response == "Yes" ]]; then USECACHED=true; else unset USECACHED; fi
    else
      unset USECACHED
    fi
  fi
fi

if [ ! $USECACHED ]; then
  if [[ "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
    TEMP_COLOR=lred; print_color "\nColumnstore must be running to perform a new S3 extent map check.\n\n"; unset TEMP_COLOR; return
  fi
fi

if [ $USECACHED ]; then
  print0 "\nUsing cached extent map file to generate report. Please wait while report is generated.\n";
  DT=$(cat $CACHEFILE|head -1)
else
  DT=$(date +"%m_%d_%H_%M_%S")
  printf $DT > $CACHEFILE
fi

if [ ! $USECACHED ]; then
ech0

if [ $CAN_CONNECT ]; then
  IS_SYSCOLUMN=$(mariadb -ABNe "select count(*) from information_schema.tables where table_schema='calpontsys' and table_name='syscolumn';");
else
  IS_SYSCOLUMN=0;
fi

if [ $CAN_CONNECT ] && [ "$IS_SYSCOLUMN" == "1" ]; then
  COLUMNSTORE_COLCOUNT=$(mariadb -ABNe "select count(1) from calpontsys.syscolumn;"); COLUMNSTORE_COLCOUNT=$(printf "%'d" $COLUMNSTORE_COLCOUNT)
  print0 "Your instance has $COLUMNSTORE_COLCOUNT Columnstore columns. There will likely be more than 2 files on disk for each column.\n"
fi
print0 "On very large databases, this process can take a long time.\n"
print0 "It is recommended that you allow this process to complete.\n"
print0 "Killing the editem process while it is running can damage the extent map.\n"
TEMP_COLOR='lred'; print_color "Do you want to run this process?"; unset TEMP_COLOR
  read -r -p " [y/N] " response
   if [[ $response == "y" || $response == "Y" || $response == "yes" || $response == "Yes" ]]; then CONTINUE=true; else ech0 "exiting..."; exit 0; fi
   if [ $CONTINUE ]; then print0 "\nGenerating a new report. This may take a while.\n"; fi;
fi

FILEPL=$S3OUTDIR/convertem.pl
FILE_EM=$S3OUTDIR/em_$DT.txt
DT=$(date +"%m_%d_%H_%M_%S")
FILE_EM_FILES=$S3OUTDIR/em_files_$DT.txt; touch $FILE_EM_FILES; truncate -s0 $FILE_EM_FILES
FILE_ALL_META_FILES=$S3OUTDIR/all_meta_files_$DT.txt; touch $FILE_ALL_META_FILES; truncate -s0 $FILE_ALL_META_FILES
MISSING_META_FILES=$S3OUTDIR/missing_meta_files_$DT.txt; touch $MISSING_META_FILES; truncate -s0 $MISSING_META_FILES
MISSING_OIDS=$S3OUTDIR/oids_missing_meta_files_$DT.txt; touch $MISSING_OIDS; truncate -s0 $MISSING_OIDS
ALL_META_KEYS=$S3OUTDIR/all_meta_keys_$DT.txt; touch $ALL_META_KEYS; truncate -s0 $ALL_META_KEYS
S3_KEYS=$S3OUTDIR/s3_keys_$DT.txt; touch $S3_KEYS; truncate -s0 $S3_KEYS

set_data1dir
COLUMNSTOREDIR="$(dirname $DATA1DIR)/storagemanager/metadata"
PLSCRIPT="my (\$pn) = \$ARGV[0];
@parts = split(/\|/, \$pn);
if (@parts <= 7) { exit; }
\$oid = \$parts[2];
\$seqnum = \$parts[6];
\$partnum = \$parts[5];
\$dbroot = \$parts[7];
\$z1 = \$oid >> 24;
\$dir1 = sprintf('%03d',\$z1);
\$t2 = \$oid & hex '0x00FF0000';
\$z2 = \$t2 >> 16;
\$dir2 = sprintf('%03d',\$z2);
\$t3 = \$oid & hex '0x0000FF00';
\$z3 = \$t3 >> 8;
\$dir3 = sprintf('%03d',\$z3);
\$z4 = \$oid & hex '0x000000ff';
\$dir4 = sprintf('%03d',\$z4);
\$dir5 = sprintf('%03d',\$partnum);
\$dir6 = sprintf('%03d',\$seqnum);
\$fl = '$COLUMNSTOREDIR/data'.\$dbroot.'/'.\$dir1.'.dir/'.\$dir2.'.dir/'.\$dir3.'.dir/'.\$dir4.'.dir/'.\$dir5.'.dir/FILE'.\$dir6.'.cdf.meta';
print \$fl,' ',\$oid,\"\n\";"

if [ ! $USECACHED ]; then
  printf '%s\n' "$PLSCRIPT" > $FILEPL
  TEMP_COLOR=blue; print_color "Starting extent map to text file.\n"; unset TEMP_COLOR
  editem -i > $FILE_EM 2>/dev/null || echo 'An error has occurred running editem.';
  TEMP_COLOR=lblue; print_color "Extent map to text file completed.\n"; unset TEMP_COLOR
fi

  APPROX_FILE_SIZE=$(wc -c < $FILE_EM)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    if [ ! $USECACHED ]; then
      TEMP_COLOR=lred; print_color "The output for editem is too small. "; unset TEMP_COLOR;
      print0 "Something went wrong. Check that columnstore processes are running properly.\n";
    else
      TEMP_COLOR=lred; print_color "The cached extent map is too small. "; unset TEMP_COLOR;
      print0 "Do not use the cached extent map file. Check that columnstore processes are running properly.\n"
    fi
    exit 0;
  fi

  cat $FILE_EM | while read line
  do
     perl $FILEPL $line >> $FILE_EM_FILES
  done

  APPROX_FILE_SIZE=$(wc -c < $FILE_EM_FILES)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    TEMP_COLOR=lred; print_color "The output for extent map files is too small. "; unset TEMP_COLOR; print0 "Something went wrong. Exiting\n\n"; exit 0;
  fi
  find -L $COLUMNSTOREDIR -iname "FILE[0-9][0-9][0-9].cdf.meta" -type f >> $FILE_ALL_META_FILES

HOST_BASE=$(grep endpoint /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
REGION=$(grep region /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
ACCESS_KEY=$(grep aws_access_key_id /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
SECRET_KEY=$(grep aws_secret_access_key /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
HOST_BUCKET=$(grep bucket /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | xargs)
S3_CONFIG=$S3OUTDIR/.s3cfg; touch $S3_CONFIG; truncate -s0 $S3_CONFIG
echo "[default]" > $S3_CONFIG
echo "host_base = $HOST_BASE" >> $S3_CONFIG
echo "region = $REGION" >> $S3_CONFIG
echo "access_key = $ACCESS_KEY" >> $S3_CONFIG
echo "secret_key = $SECRET_KEY" >> $S3_CONFIG
echo "host_bucket = $HOST_BUCKET" >> $S3_CONFIG

unset NO_JQ NO_S3CMD

if [ $(which jq 2>/dev/null) ]; then
  find -L $COLUMNSTOREDIR -iname "FILE[0-9][0-9][0-9].cdf.meta" -type f -exec jq -r '.objects[].key' {} \; >> $ALL_META_KEYS
else
  NO_JQ=true;
fi
if [ $(which s3cmd 2>/dev/null) ]; then
  s3cmd --config=$S3_CONFIG ls s3://$HOST_BUCKET | grep "FILE[0-9][0-9][0-9].cdf" | grep -o '[^/]*$' >> $S3_KEYS
else
  NO_S3CMD=true;
fi

  APPROX_FILE_SIZE=$(wc -c < $FILE_ALL_META_FILES)
  if [ "$APPROX_FILE_SIZE" -lt "1000" ]; then
    TEMP_COLOR=lred; print_color "The output for all files is too small. "; unset TEMP_COLOR; print0 "Something went wrong. Exiting\n\n"; exit 0;
  fi

TEMP_COLOR=blue; print_color "Beginning process to compare text files with grep. This can take a while.\n"; unset TEMP_COLOR
ORPHAN_COUNT=$(grep -F -x -v -f  <(cat $FILE_EM_FILES | awk '{print $1}') $FILE_ALL_META_FILES |wc -l)
if [ $ORPHAN_COUNT == "1" ]; then
  ORPHANED_MSG='There is 1 orphaned meta file:'; else ORPHANED_MSG="There are $ORPHAN_COUNT orphaned meta files:"
fi

MISSING_COUNT=$(grep -F -x -v -f  $FILE_ALL_META_FILES <(cat $FILE_EM_FILES | awk '{print $1}')|wc -l)
if [ $MISSING_COUNT == "1" ]; then
  MISSING_MSG='There is 1 missing meta file:'; else MISSING_MSG="There are $MISSING_COUNT missing meta files:"
fi

  print_color "\n### Summary ###\n"
  EM_COUNT=$(cat $FILE_EM_FILES | sort | uniq | wc -l) # Careful here, some single files have more than one entry
  ALL_FILE_COUNT=$(wc -l < $FILE_ALL_META_FILES);
  EM_COUNT=$(printf "%'d" $EM_COUNT); ALL_FILE_COUNT=$(printf "%'d" $ALL_FILE_COUNT);
  print0 "There are $EM_COUNT files registered in the extent map. There are $ALL_FILE_COUNT eligible columnstore extent files.\n"
  ech0

print_color "### Orphaned Meta Files ###\n"
if [ "$ORPHAN_COUNT" -gt "0" ]; then
  ech0 "Orphaned meta files are those that exist on the file system but do not exist in the Columnstore extent map."
   TEMP_COLOR=lred; print_color "Mariadb Support does not recommend that orphaned meta files be deleted.\n"; unset TEMP_COLOR
  ech0
  if [ $USECACHED ]; then
    print0 "When a cached extent map is compared to the current file system it is possible that this script\nreports orphaned meta files that are not actually orphaned. This is due to tables and columns added\nsince the cached extent map was created.\n"
  fi
  ech0 "$ORPHANED_MSG"
  ech0 "$(grep -F -x -v -f  <(cat $FILE_EM_FILES | awk '{print $1}') $FILE_ALL_META_FILES)"
  ech0
else
  ech0 "There are no orphaned meta files."
  ech0
fi

print_color "### Missing Meta Files ###\n"
if [ "$MISSING_COUNT" -gt "0" ]; then
  ech0 "Missing meta files are those expected by the extent map but are not found on disk."
  ech0
  if [ $USECACHED ]; then
    print0 "When a cached extent map is compared to the current file system it is possible that this script\nreports missing meta files that are not actually missing. This is due to tables being dropped\nsince the cached extent map was created.\n"
  fi
  ech0 "$MISSING_MSG"

  grep -F -x -v -f  $FILE_ALL_META_FILES <(cat $FILE_EM_FILES | awk '{print $1}') > $MISSING_META_FILES

  cat $MISSING_META_FILES | while read line
  do
    oid_missing_file $line
  done
  OIDLIST=$(cat $MISSING_OIDS | sed 's/,*$//g')
  ech0
# assume errors on the next query:
  SQL="select \`schema\`,tablename,columnname, objectid, dictobjectid, listobjectid, treeobjectid
       from calpontsys.syscolumn
       where objectid in ($OIDLIST) or dictobjectid in ($OIDLIST) or listobjectid in ($OIDLIST) or treeobjectid in ($OIDLIST);"
  if [ $CAN_CONNECT ] && [ "$IS_SYSCOLUMN" == "1" ]; then
    MISSINGOIDSREPORT=$(mariadb -v -v -v -Ae "$SQL" | sed -nr '/^(\+|^\|)/p') 2>/dev/null || ERR=true
    if [ ! $ERR ]; then
      print_color "### Database Objects Effected by Missing Meta Files ###\n"
      print0 "$MISSINGOIDSREPORT\n"
    fi
  fi
  ech0
else
  ech0 "There are no missing files."
  ech0
fi
print_color "### Orphaned S3 Keys ###\n"
if [ $NO_JQ ] || [ $NO_S3CMD ]; then
  ech0 "Install jq and s3cmd for this section of the report."
  ech0
else
  ORPHANED_S3_COUNT=$(grep -F -x -v -f $ALL_META_KEYS $S3_KEYS |wc -l)
  if [ "$ORPHANED_S3_COUNT" == "1" ]; then ORPHANED_MSG="There is 1 orphaned s3 key."; else ORPHANED_MSG="There are $ORPHANED_S3_COUNT orphaned s3 keys."; fi
  if [ "$ORPHANED_S3_COUNT" -gt "0" ]; then
    ech0 "Orphaned s3 Keys are those that exist in s3 storage but do not have a meta key reference."
    TEMP_COLOR=lred; print_color "Mariadb Support does not recommend that orphaned s3 keys be deleted.\n"; unset TEMP_COLOR
    ech0
    ech0 "$ORPHANED_MSG"
    ech0 "$(grep -F -x -v -f $ALL_META_KEYS $S3_KEYS)"
    ech0
  else
    ech0 "There are no orphaned s3 keys."
    ech0
  fi
fi

print_color "### Missing S3 Keys ###\n"
if [ $NO_JQ ] || [ $NO_S3CMD ]; then
  ech0 "Install jq and s3cmd for this section of the report."
  ech0
else
  MISSING_S3_COUNT=$(grep -F -x -v -f $S3_KEYS $ALL_META_KEYS |wc -l)
  if [ "$MISSING_S3_COUNT" == "1" ]; then MISSING_S3_MSG="There is 1 missing s3 key."; else MISSING_S3_MSG="There are $MISSING_S3_COUNT missing s3 keys."; fi
  if [ "$MISSING_S3_COUNT" -gt "0" ]; then
    ech0 "Missing s3 Keys are those that have a meta key reference but do not exist in s3 storage."
    ech0
    ech0 "$MISSING_S3_MSG"
    ech0 "$(grep -F -x -v -f $S3_KEYS $ALL_META_KEYS)"
    ech0
  else
    ech0 "There are no missing s3 keys."
    ech0
  fi
fi

display_outputfile_message
}



function schema_sync() {
ls /usr/bin/mariadb-columnstore-start.sh 1>/dev/null 2>/dev/null && CS_INSTALLED=true
if [ ! $CS_INSTALLED ]; then return; fi
if [ ! $CAN_CONNECT ]; then return; fi
if [[ ! "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
  CS_RUNNING=true
fi
if [ ! $CS_RUNNING ]; then ech0 "Columnstore is not running."; return; fi

PM1=$(mcsGetConfig pm1_WriteEngineServer IPAddr)
PM2=$(mcsGetConfig pm2_WriteEngineServer IPAddr)

if [ ! "$PM1" == "127.0.0.1" ] && [ ! -z $PM2 ]; then
  THISISCLUSTER=true
fi
if [ $CAN_CONNECT ]; then
get_out_of_sync_tables_count
fi

if [ ! $COLUMNSTORE_TABLES_NOT_IN_MARIABD ]; then return; fi
if [ ! "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ]; then
  get_out_of_sync_tables
  ech0 "The following tables exist as Columnstore extents but not in Mariadb as Columnstore tables."
  ech0  "$OUT_OF_SYNC_TABLES" 
fi

SLAVE_STATUS=$(mariadb -Ae "show all slaves status\G"| grep Slave_SQL_Running_State | head -1 | xargs)
if [ "$SLAVE_STATUS" == "Slave_SQL_Running_State:" ] && [ ! "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ]; then
  TEMP_COLOR=lcyan; print_color "Replication is not running. The best solution for this node may be to repair replication.\n\n";unset TEMP_COLOR;
fi

IS_THIS_A_SLAVE=$(mariadb -Ae "show all slaves status\G"| grep Slave_SQL_Running_State | head -1 |awk '{print $1}')
if [ "$IS_THIS_A_SLAVE" == "Slave_SQL_Running_State:" ]; then IS_THIS_A_SLAVE=true; else unset IS_THIS_A_SLAVE; fi

NO_APPROPRIATE_OPTIONS=true

print0 "Use this routine to dynamically generate a script. There are three methods to choose from:\n"
ech0 "----------------"
if [ ! $THISISCLUSTER ]; then
  TEMP_COLOR=lred; print_color "Option 1 is useful for Columnstore clusters only.\n"; unset TEMP_COLOR
else
  print0 "Respond with "; TEMP_COLOR=lcyan; print_color "1"; unset TEMP_COLOR
  print0 " to create a SQL script from mariadb-dump (mysqldump).\n"

  print0 "Use option 1 on a node with all the tables defined correctly that primary/master.\nThis option is to be used when a replica is missing table definitions because replication broke.\nTransfer the resulting script to the replica and run it there.\n"
  if [ ! "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ] || [ $IS_THIS_A_SLAVE ]; then
    TEMP_COLOR=lred; print_color "Option 1 is not appropriate on this node. "; unset TEMPCOLOR
    if [ $IS_THIS_A_SLAVE ]; then TEMP_COLOR=lmagenta; print_color "This is a replica.\n"  unset TEMP_COLOR; else print0 "\n"; fi
  else 
    TEMP_COLOR=lgreen; print_color "Option 1 is appropriate on this node.\n"; unset TEMP_COLOR NO_APPROPRIATE_OPTIONS; ENABLE_OP1=true
  fi
fi
ech0 "----------------"
print0 "Respond with "; TEMP_COLOR=lcyan; print_color "2"; unset TEMP_COLOR;
print0 " to generate a SQL script that will redefine out-of-sync columnstore tables in MariaDB.\n"
if [ "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ]; then
  TEMP_COLOR=lred; print_color "Option 2 is not appropriate on this node.\n"; unset TEMP_COLOR
else
  TEMP_COLOR=lgreen; print_color "Option 2 is appropriate on this node.\n"; unset TEMP_COLOR NO_APPROPRIATE_OPTIONS; ENABLE_OP2=true
fi

ech0 "----------------"
print0 "Respond with "; TEMP_COLOR=lcyan; print_color "3"; unset TEMP_COLOR;
print0 " to generate a SQL script that can be used to permanently delete out-of-sync columnstore extents.\n"
if [ "$COLUMNSTORE_TABLES_NOT_IN_MARIABD" == "0" ]; then
  TEMP_COLOR=lred; print_color "Option 3 is not appropriate on this node.\n"; unset TEMP_COLOR
else
  TEMP_COLOR=lgreen; print_color "Option 3 is appropriate on this node.\n"; unset TEMP_COLOR NO_APPROPRIATE_OPTIONS; ENABLE_OP3=true
fi
ech0 "----------------"

if [ $NO_APPROPRIATE_OPTIONS ]; then print0 "No appropriate options for this node. Exiting.\n\n"; exit 0; fi

TEMP_COLOR=lgreen; print_color "Choose one of these options:\n"; unset TEMP_COLOR
if [ $ENABLE_OP1 ]; then
  TEMP_COLOR=lcyan; print_color "  1"; unset TEMP_COLOR; ech0 ") Generate SQL script from mariadb-dump to resync columnstore tables on another node."
fi
if [ $ENABLE_OP2 ]; then
  TEMP_COLOR=lcyan; print_color "  2"; unset TEMP_COLOR; ech0 ") Generate SQL script that can be used to re-sync columnstore tables."
fi
if [ $ENABLE_OP3 ]; then
  TEMP_COLOR=lcyan; print_color "  3"; unset TEMP_COLOR; ech0 ") Generate SQL script that can be used to permanently delete out-of-sync columnstore extents."
fi
TEMP_COLOR=lcyan; print_color "  4"; unset TEMP_COLOR; ech0 ") Do nothing, exit." 

read yourchoice
case $yourchoice in
  1) ech0 "Option 1 chosen"; if [ ! $ENABLE_OP1 ]; then ech0 "Exiting."; exit 0; fi; mariadb_dump_to_resync_columnstore_tables;;
  2) ech0 "Option 2 chosen"; if [ ! $ENABLE_OP2 ]; then ech0 "Exiting."; exit 0; fi; generate_sql_to_resync_columnstore_tables;;
  3) ech0 "Option 3 chosen"; if [ ! $ENABLE_OP3 ]; then ech0 "Exiting."; exit 0; fi; generate_sql_to_delete_out_of_sync_columnstore_extents;;
  *) exit 0;;
esac

}

function mariadb_dump_to_resync_columnstore_tables() {
  SQLFILE=/tmp/rebuild_schema_sync_only.sql
  TEMPSCRIPT=/tmp/mariadb-dump_table_definitons.sh

  printf "/* Dynamically generated SQL to restore lost columnstore tables on cluster replica. */\n/* Script by Edward Stoever for MariaDB Support */\n" > $SQLFILE
  printf "#!/bin/bash\n# Script by Edward Stoever for MariaDB Support.\n" > $TEMPSCRIPT
  mariadb -ABNe "select distinct concat('echo \"SET sql_log_bin = OFF;\" >> $SQLFILE; ')as txt" >> $TEMPSCRIPT
  mariadb -ABNe "select distinct concat('echo \"CREATE SCHEMA IF NOT EXISTS ',TABLE_SCHEMA,';\" >> $SQLFILE; ')as txt from information_schema.tables where engine='Columnstore'" >> $TEMPSCRIPT
  mariadb -ABNe "select concat('echo \"USE ',TABLE_SCHEMA,';\" >> $SQLFILE; ','mariadb-dump ', TABLE_SCHEMA,' ',TABLE_NAME,' --no-data --skip-lock-tables --compact >> $SQLFILE')as txt from information_schema.tables where engine='Columnstore'" >> $TEMPSCRIPT
  chmod 750 $TEMPSCRIPT
# RUN THE TEMPSCRIPT:
  $TEMPSCRIPT
# ELIMINATE TABLE COMMENTS
  perl -p -i -e "s/ COMMENT=\'(.*?)\'//g" $SQLFILE
# ELIMINATE IN-LINE COMMENTS
  perl -p -i -e "s/\/\*(.*?)\*\/;//g" $SQLFILE
# ADD IN SHEMA SYNC ONLY
  perl -p -i -e "s/ENGINE=Columnstore/ENGINE=Columnstore COMMENT=\'SCHEMA SYNC ONLY\'/g" $SQLFILE
# ADD IF NOT EXISTS
  perl -p -i -e "s/CREATE TABLE/CREATE TABLE IF NOT EXISTS/g" $SQLFILE
# INFORM USER:
  echo "Please review the SQL file $SQLFILE before running it on the replica/slave."
}


function generate_sql_to_resync_columnstore_tables(){
ech0
TEMP_COLOR=lcyan; print_color "###### INDICATE CHARACTER SET #######\n"; unset TEMP_COLOR
ech0 "UTF8MB4 is a multi-byte character set. A varchar(2000) column, which is the maximum for UTF8MB4, can"
ech0 "store 2000 characters per string field, up to 8000 bytes. UTF8MB4 is contemporary and Columnstore's default."
ech0
ech0 "Latin1 is a single-byte character set. A varchar(8000) column, which is the maximum for Latin1, can"
ech0 "store 8000 characters per string field, up to 8000 bytes. Latin1 is legacy."
ech0 
ech0 "This script will generate SQL commands based on saved definitions of columnstore columns."
ech0 "Columnstore saves strings as bytes, not keeping information about the default character set of a table."
ech0 "Only the MariaDB dictionary stores the default character set. For these tables this detail has not been saved."
ech0
TEMP_COLOR=lgreen; print_color "Choose one of these options:\n"; unset TEMP_COLOR
TEMP_COLOR=lcyan; print_color "  1"; unset TEMP_COLOR; ech0 ") generate SQL to recreate tables as DEFAULT CHARSET=UTF8MB4"
TEMP_COLOR=lcyan; print_color "  2"; unset TEMP_COLOR; ech0 ") generate SQL to recreate tables as DEFAULT CHARSET=Latin1"

read yourchoice

case $yourchoice in
  1) echo "Option 1 chosen."; CHARSET='UTF8MB4';;
  2) echo "Option 2 chosen."; CHARSET='LATIN1';;
  *) exit 0;;
esac

  OUTPUTSQLFILE=/tmp/$(hostname)_resync_columnstore_tables_$CHARSET.sql
  echo "-- This SQL script was generated from columnstore_review.sh --schemasync" > $OUTPUTSQLFILE
  echo "-- By Edward Stoever for MariaDB Support" >> $OUTPUTSQLFILE
  echo "-- Please review this script carefully before running." >> $OUTPUTSQLFILE
  SQL="SELECT DISTINCT CONCAT('CREATE SCHEMA IF NOT EXISTS columnstore_review;') AS txt
       FROM information_schema.TABLES A
       INNER JOIN information_schema.COLUMNSTORE_TABLES B
       ON A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
       WHERE A.ENGINE<>'Columnstore';"
  mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE
  SQL="SELECT CONCAT('RENAME TABLE \`',A.TABLE_SCHEMA,'\`.\`',A.TABLE_NAME,'\` TO \`columnstore_review\`.\`',A.TABLE_NAME,'\`;') AS txt
       FROM information_schema.TABLES A
       INNER JOIN information_schema.COLUMNSTORE_TABLES B
       ON A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
       WHERE A.ENGINE<>'Columnstore';"
  mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE
if [ "$CHARSET" == "UTF8MB4" ]; then
  SQL="SELECT 
CONCAT('create schema if not exists \`',TABLE_SCHEMA,'\`; create table if not exists \`',TABLE_SCHEMA,'\`.\`',TABLE_NAME,'\` (', 
  GROUP_CONCAT('\`',COLUMN_NAME,'\` ',
    case DATA_TYPE 
    when 'medint' then CONCAT('mediumint')
    when 'int' then CONCAT('integer')
    when 'utinyint' then CONCAT('tinyint unsigned')
    when 'usmallint' then CONCAT('smallint unsigned')
    when 'umedint' then CONCAT('mediumint unsigned')
    when 'uint32_t' then CONCAT('integer unsigned')
    when 'ubigint' then CONCAT('bigint unsigned')
    when 'decimal' then CONCAT(DATA_TYPE,'(',NUMERIC_PRECISION,',',NUMERIC_SCALE,')')
    when 'varchar' then CONCAT(DATA_TYPE,'(',floor(COLUMN_LENGTH/4),')')
    when 'char' then CONCAT(DATA_TYPE,'(',floor(COLUMN_LENGTH/4),')')
    when 'text' then 
	   case COLUMN_LENGTH
	   when 255 then CONCAT('tinytext')
	   when 65535 then CONCAT('text')
	   ELSE CONCAT('longtext')
	   END
    when 'blob' then 
	   case COLUMN_LENGTH
	   when 255 then CONCAT('tinyblob')
	   when 65535 then CONCAT('blob')
	   ELSE CONCAT('longblob')
	   END
    else CONCAT(DATA_TYPE) 
	 END,
	 CONCAT(' DEFAULT ',COALESCE(CONCAT('''',COLUMN_DEFAULT,''''), 'NULL'))
  ORDER BY COLUMN_POSITION SEPARATOR ',')
,') ENGINE=columnstore DEFAULT CHARSET=utf8mb4 COMMENT=''SCHEMA SYNC ONLY'';') as txt
FROM information_schema.COLUMNSTORE_COLUMNS A 
WHERE NOT EXISTS
(SELECT 'x' FROM information_schema.TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
AND B.ENGINE='Columnstore')
GROUP BY TABLE_NAME, TABLE_SCHEMA
ORDER BY TABLE_SCHEMA, TABLE_NAME;"
fi 
if [ "$CHARSET" == "LATIN1" ]; then
  SQL="SELECT
CONCAT('create schema if not exists \`',TABLE_SCHEMA,'\`; create table if not exists \`',TABLE_SCHEMA,'\`.\`',TABLE_NAME,'\` (',
  GROUP_CONCAT('\`',COLUMN_NAME,'\` ',
    case DATA_TYPE
    when 'medint' then CONCAT('mediumint')
    when 'int' then CONCAT('integer')
    when 'utinyint' then CONCAT('tinyint unsigned')
    when 'usmallint' then CONCAT('smallint unsigned')
    when 'umedint' then CONCAT('mediumint unsigned')
    when 'uint32_t' then CONCAT('integer unsigned')
    when 'ubigint' then CONCAT('bigint unsigned')
    when 'decimal' then CONCAT(DATA_TYPE,'(',NUMERIC_PRECISION,',',NUMERIC_SCALE,')')
    when 'varchar' then CONCAT(DATA_TYPE,'(',COLUMN_LENGTH,')')
    when 'char' then CONCAT(DATA_TYPE,'(',COLUMN_LENGTH,')')
    when 'text' then
           case COLUMN_LENGTH
           when 255 then CONCAT('tinytext')
           when 65535 then CONCAT('text')
           ELSE CONCAT('longtext')
           END
    when 'blob' then
           case COLUMN_LENGTH
           when 255 then CONCAT('tinyblob')
           when 65535 then CONCAT('blob')
           ELSE CONCAT('longblob')
           END
    else CONCAT(DATA_TYPE)
         END,
         CONCAT(' DEFAULT ',COALESCE(CONCAT('''',COLUMN_DEFAULT,''''), 'NULL'))
  ORDER BY COLUMN_POSITION SEPARATOR ',')
,') ENGINE=columnstore DEFAULT CHARSET=latin1 COMMENT=''SCHEMA SYNC ONLY'';') as txt
FROM information_schema.COLUMNSTORE_COLUMNS A
WHERE NOT EXISTS
(SELECT 'x' FROM information_schema.TABLES B WHERE
A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
AND B.ENGINE='Columnstore')
GROUP BY TABLE_NAME, TABLE_SCHEMA
ORDER BY TABLE_SCHEMA, TABLE_NAME;"
fi

mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE
print0 "SQL script saved to $OUTPUTSQLFILE\n"
print0 "Review the SQL script before running.\n"
ech0
}

function generate_sql_to_delete_out_of_sync_columnstore_extents(){
  OUTPUTSQLFILE=/tmp/$(hostname)_delete_out_of_sync_columnstore_extents.sql
  echo "-- This SQL script was generated from columnstore_review.sh --schemasync" > $OUTPUTSQLFILE
  echo "-- By Edward Stoever for MariaDB Support" >> $OUTPUTSQLFILE
  echo "-- Please review this script carefully before running." >> $OUTPUTSQLFILE
  SQL="SELECT DISTINCT CONCAT('CREATE SCHEMA IF NOT EXISTS columnstore_review;') AS txt
       FROM information_schema.TABLES A
       INNER JOIN information_schema.COLUMNSTORE_TABLES B
       ON A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
       WHERE A.ENGINE<>'Columnstore';"
  mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE
  SQL="SELECT CONCAT('RENAME TABLE \`',A.TABLE_SCHEMA,'\`.\`',A.TABLE_NAME,'\` TO \`columnstore_review\`.\`',A.TABLE_NAME,'\`;') AS txt
       FROM information_schema.TABLES A
       INNER JOIN information_schema.COLUMNSTORE_TABLES B
       ON A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
       WHERE A.ENGINE<>'Columnstore';"
  mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE

  SQL="SELECT
       CONCAT('drop table if exists \`',TABLE_SCHEMA,'\`.\`',TABLE_NAME,'\`;') as txt
       FROM information_schema.COLUMNSTORE_COLUMNS A
       WHERE NOT EXISTS
      (SELECT 'x' FROM information_schema.TABLES B WHERE
       A.TABLE_SCHEMA=B.TABLE_SCHEMA AND A.TABLE_NAME=B.TABLE_NAME
       AND B.ENGINE='Columnstore')
      GROUP BY TABLE_NAME, TABLE_SCHEMA
      ORDER BY TABLE_SCHEMA, TABLE_NAME;"
  mariadb -ABNe "$SQL" >> $OUTPUTSQLFILE
  print0 "SQL script saved to $OUTPUTSQLFILE\n"
  print0 "Review the SQL script before running.\n"
  ech0

}

function ensure_owner_privs_of_tmp_dir() {
  if [ ! "$EUID" == "0" ]; then ech0 "This routine must be run as root."; return; fi
  ls /usr/bin/mariadb-columnstore-start.sh 1>/dev/null 2>/dev/null && CS_INSTALLED=true
  if [ ! $CS_INSTALLED ]; then return; fi
  if [ -f /mnt/skysql/podinfo/namespace ] && [ -f /mnt/skysql/podinfo/podname ]; then print0 "This solution is not required for SkySQL instances.\n\n"; return; fi
  TMP_DIR=$(mcsGetConfig SystemConfig SystemTempFileDir)
  SOMETHING_EXISTS=$(stat -c '%F' $TMP_DIR 2>/dev/null)
  if [ ! "$SOMETHING_EXISTS" == "directory" ] && [ ! -z "$SOMETHING_EXISTS" ]; then ech0 "$TMP_DIR exists but it is a $SOMETHING_EXISTS."; return; fi 
  if [ -d $TMP_DIR ]; then
    DIR_OWNER=$(stat -c '%U' $TMP_DIR)
	DIR_OCTAL=$(stat -c '%a' $TMP_DIR)
  fi
  BASE_DIR=$(ech0 $TMP_DIR | cut -d "/" -f2)
  
  if [ ! -d $TMP_DIR ]||[ ! "$DIR_OWNER" == "mysql" ]||[ ! "$DIR_OCTAL" == "755" ]; then 
    mkdir -p $TMP_DIR; 
    chown -R mysql:mysql $TMP_DIR; 
    chmod 755 $TMP_DIR; 
    ech0 "Created or modified directory $TMP_DIR with correct ownership and access rights.";
  fi

  PM1=$(mcsGetConfig pm1_WriteEngineServer IPAddr)
  PM2=$(mcsGetConfig pm2_WriteEngineServer IPAddr)
  if [ ! "$PM1" == "127.0.0.1" ] && [ ! -z $PM2 ]; then THISISCLUSTER=true; fi
  if [ ! "$THISISCLUSTER" ]; then print0 "This is not a columnstore cluster. This solution is necessary for Columnstore Clusters only.\n\n"; return; fi

  if [ ! -d /etc/tmpfiles.d ] && [ ! -d /usr/lib/tmpfiles.d ]; then 
    ech0 "Check whether this OS supports systemd-tmpfiles management of volatile and temporary files."; return;
  fi

  if [ -d /etc/tmpfiles.d ]; then 
    DIR1=true; 
    ALREADY_CONFIG=$(find /etc/tmpfiles.d -type f | xargs grep -l $TMP_DIR | head -1)
  fi
  if [ -d /usr/lib/tmpfiles.d ]; then 
    DIR2=true; 
	if [ -z $ALREADY_CONFIG ]; then
      ALREADY_CONFIG=$(find /usr/lib/tmpfiles.d -type f | xargs grep -l $TMP_DIR | head -1)
    fi
  fi

  if [ ! -z "$ALREADY_CONFIG" ]; then ech0 "The configuration has been completed already. Check the file $ALREADY_CONFIG"; return; fi
  
  if [ $DIR1 ]; then
    echo "d $TMP_DIR 0755 mysql mysql" > /etc/tmpfiles.d/columnstore_tmp_files.conf
	ech0 "Created the file /etc/tmpfiles.d/columnstore_tmp_files.conf"
  elif [ $DIR2 ]; then
    echo "d $TMP_DIR 0755 mysql mysql" > /usr/lib/tmpfiles.d/columnstore_tmp_files.conf
	ech0 "Created the file /usr/lib/tmpfiles.d/columnstore_tmp_files.conf"
  fi
  
}

# CHECK PORTS FUNCTIONS BY Patrizio Tamorri
function check_ports(){
	# Check if nmap is installed
	if ! command -v nmap &> /dev/null; then
		printf "nmap is not installed.\n\n"
		return
	fi

	# Define the ports to check
	ports="8600,8601,8602,8603,8604,8605,8606,8607,8608,8609,8610,8611,8612,8613,8614,8615,8616,8617,8618,8619,8620,8630,8700,8800,3306,8999"

	# Get the local node from the file
	my_node=$(cat /var/lib/columnstore/local/module)

	# Get the hostname and local IP address of the machine
	hostname=$(hostname)
	local_ip=$(hostname -I | awk '{print $1}')

	# Extract IPs from Columnstore.xml, handling special characters like \r, \n, and \t
	ips=$(grep -A 1 "_WriteEngineServer" /etc/columnstore/Columnstore.xml \
	| sed "/${my_node}_WriteEngineServer/,+0d" \
	| grep "<IPAddr>" \
	| tr -d '\r\n\t' \
	| sed -e 's/<IPAddr>//g' -e 's/<\/IPAddr>//g' -e 's/^[ \t]*//' -e 's/[ \t]*$//' \
	| sort -u)

	# Extract IPAddr:Port pairs from the XML file, removing \r, \n, and \t
	local_ports=$(grep -E "<IPAddr>|<Port>" /etc/columnstore/Columnstore.xml \
	| tr -d '\r\n\t' \
	| sed -e 's/<\/\?IPAddr>//g' -e 's/<\/\?Port>//g' \
	| awk 'NR%2{printf "%s:", $0; next;} 1')

	pass=true

	# Function to check if a port is available to use
	check_port_nmap_available_to_use() {
		ip=$1
		port=$2

		# Use nmap to check the port status
		result=$(nmap -T4 -p $port $ip | grep "$port" | awk '{print $2}')

		if [ "$result" = "open" ]; then
			echo "$ip:$port - Port is open: SUCCESS"
		elif [ "$result" = "closed" ]; then
			if [ ! ${SUPPRESS_CLOSED_PORTS} ]; then echo "$ip:$port - Port is closed and not firewalled"; fi
		elif [ "$result" = "filtered" ]; then
			echo "$ip:$port - Port is filtered (firewalled or blocked): ERROR"
			pass=false
		else
			echo "$ip:$port - Unknown port status: ERROR"
			pass=false
		fi
	}

	# Function to check if a port must be open
	check_port_nmap_must_be_opened() {
		ip=$1
		port=$2

		# Use nmap to check the port status
		result=$(nmap -T4 -p $port $ip | grep "$port" | awk '{print $2}')

		if [ "$result" = "open" ]; then
			echo "$ip:$port - Port is open: SUCCESS"
		elif [ "$result" = "closed" ]; then
			echo "$ip:$port - Port is closed and not firewalled: ERROR"
			pass=false
		elif [ "$result" = "filtered" ]; then
			echo "$ip:$port - Port is filtered (firewalled or blocked): ERROR"
			pass=false
		else
			echo "$ip:$port - Unknown port status: ERROR"
			pass=false
		fi
	}

	# Loop through each IP and check the ports
	for ipadd in $ips; do
		echo "Checking ports on $ipadd..."

		# Replace ipadd with 127.0.0.1 if it matches the local IP or hostname
		if [[ "$ipadd" == "$local_ip" || "$ipadd" == "$hostname" ]]; then
			ipadd="127.0.0.1"
		fi

		for port in ${ports//,/ }; do
			ip_port="$ipadd:$port"
			if [[ " ${local_ports[@]} " =~ " $ip_port " ]]; then
				check_port_nmap_must_be_opened $ipadd $port
			else
				check_port_nmap_available_to_use $ipadd $port
			fi
		done
	done

	# Final status report
	if [ "$pass" = true ]; then
		printf "All nodes passed the port test.\n\n"
	else
		printf "One or more nodes failed the port test. Please investigate.\n\n"
	fi


}

function clear_rollback() {
  unset ERR
  CLEAR_ROLLBACK_MESSAGE="It is recommended that you clear rollback files only when instructed to do so by Mariadb Support.\nType c to clear rollback files.\nType any other key to exit.\n"
  STORAGE_TYPE=$(grep service /etc/columnstore/storagemanager.cnf | grep -v "^\#" | grep "\=" | awk -F= '{print $2}' | awk '{print tolower($0)}' | xargs)
  DATA1DIR=$(mcsGetConfig SystemConfig DBRoot1 2>/dev/null) || DATA1DIR=/var/lib/columnstore/data1
  BRMSAV=$(cat $DATA1DIR/systemFiles/dbrm/BRM_saves_current | xargs)
if [[ ! "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then
  TEMP_COLOR=lred; print_color "Columnstore processes are running.\nYou may clear rollback fragments only when Columnstore processes are stopped."; unset TEMP_COLOR
  print0 "\nExiting.\n\n";  exit 0
fi

if [ "$STORAGE_TYPE" == "localstorage" ]; then 
  COUNTFILES=$(find $DATA1DIR/systemFiles \( -name "${BRMSAV}_vss" -o -name "${BRMSAV}_vbbm" \) -size +0 | wc -l)
  if [ "$COUNTFILES" == "0" ]; then
    TEMP_COLOR=lred; print_color "Rollback files are empty."; unset TEMP_COLOR
    print0 "\nExiting.\n\n";  exit 0
  fi
  
  print0 "$CLEAR_ROLLBACK_MESSAGE"

  read -s -n 1 RESPONSE
  if [ "$RESPONSE" == "c" ]; then
    ech0; ech0
    BRM_SAVES_BACKUP_FILE=$(hostname)_$(date +"%Y-%m-%d-%H-%M-%S")_BRM_saves.tar
    cd $DATA1DIR/systemFiles/dbrm/
    print0 "BRM_saves_current: ${BRMSAV}\n\nBacking up these files:\n"
    find . \( -name "${BRMSAV}_vss" -o -name "${BRMSAV}_vbbm" \) -exec tar -rvf /tmp/$BRM_SAVES_BACKUP_FILE {} \;
    ech0
    find . \( -name "${BRMSAV}_vss" -o -name "${BRMSAV}_vbbm" \) -size +0 -exec truncate -s0 {} \; || ERR=true
    COUNTFILES=$(find $DATA1DIR/systemFiles \( -name "${BRMSAV}_vss" -o -name "${BRMSAV}_vbbm" \) -size +0 | wc -l)
    if [ $ERR ] || [ "$COUNTFILES" != "0" ]; then
      ech0 "Something went wrong. Check the size of files ${BRMSAV}_vss and ${BRMSAV}_vbbm. Each file should be zero bytes in size."
      ls -lrt $DATA1DIR/systemFiles/dbrm
    else
      TEMP_COLOR=lcyan; print_color  "BRM_saves files backed up to /tmp/$BRM_SAVES_BACKUP_FILE.\nFiles cleared successfully.\n\n"; unset TEMP_COLOR
    fi
  else
    print0 "\nNothing done.\n\n"
  fi
  
fi

if [ "$STORAGE_TYPE" == "s3" ]; then 
  DBRM_TMP_DIR=/tmp/dbrm-before-clearing-$(date +"%Y-%m-%d-%H-%M-%S") || ERR=true
  print0 "$CLEAR_ROLLBACK_MESSAGE"
  read -s -n 1 RESPONSE
  if [ "$RESPONSE" == "c" ]; then
    ## REF: https://mariadbcorp.atlassian.net/wiki/spaces/Support/pages/1600094249/Stuck+load_brm+failed+rollback+of+a+transaction
    cd /var/lib/columnstore/storagemanager/metadata/data1/systemFiles/dbrm/ || ERR=true
    mkdir -p $DBRM_TMP_DIR || ERR=true
    find . | cpio -pd $DBRM_TMP_DIR || ERR=true
    # Clear vss and vbbm files
    rm -f BRM_saves_vss.meta BRM_saves_vbbm.meta || ERR=true
    touch BRM_saves_vss.meta || ERR=true;  chown mysql:mysql BRM_saves_vss.meta || ERR=true
    touch BRM_saves_vbbm.meta || ERR=true; chown mysql:mysql BRM_saves_vbbm.meta || ERR=true
    rm -rf /var/lib/columnstore/storagemanager/cache/data1/* || ERR=true
    mkdir /var/lib/columnstore/storagemanager/cache/data1/downloading || ERR=true
    chown mysql:mysql -R /var/lib/columnstore/storagemanager/cache || ERR=true
    
      if [ $ERR ]; then
        ech0 "Something went wrong."
      else
        TEMP_COLOR=lcyan; print_color "BRM_saves files backed up to $DBRM_TMP_DIR.\nFiles cleared successfully.\n\n"; unset TEMP_COLOR
      fi
    
  else
    print0 "\nNothing done.\n\n"
  fi
fi
}

function kill_columnstore(){
COUNT_ANY_STRAGGLERS=$(ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep" | wc -l)
PM1=$(mcsGetConfig pm1_WriteEngineServer IPAddr)
PM2=$(mcsGetConfig pm2_WriteEngineServer IPAddr)
if [ ! "$PM1" == "127.0.0.1" ] && [ ! -z $PM2 ]; then
  THISISCLUSTER=true
fi

if [ "$COUNT_ANY_STRAGGLERS" == "0" ]; then
  TEMP_COLOR=lred; print_color "Columnstore processes are not running.\n"; unset TEMP_COLOR
  clearShm
  TEMP_COLOR=lcyan; print_color "Columnstore shared memory cleared.\n";  unset TEMP_COLOR
  print0 "\nExiting.\n\n";  exit 0
fi

if [ $THISISCLUSTER ] && [ "$COUNT_ANY_STRAGGLERS" != "0" ]; then 
  TEMP_COLOR=lred; print_color "WARNING: This is a columnstore cluster and it is best to use cmapi commands to stop columnstore processes.\n"; unset TEMP_COLOR
fi


TEMP_COLOR=lcyan; print_color "Press c to stop all columnstore processes on this node.\n"; unset TEMP_COLOR

read -s -n 1 RESPONSE
  if [ "$RESPONSE" == "c" ]; then
  if [ "$COUNT_ANY_STRAGGLERS" != "0" ]; then
    ech0 "Attempting to gracefully stop mcs-ddlproc."
    systemctl stop mcs-ddlproc;
    ech0 "Attempting to gracefully stop mcs-dmlproc."
    systemctl stop mcs-dmlproc;
    systemctl stop mcs-exemgr 2>/dev/null; # if cs 6.4 and prior
    ech0 "Attempting to gracefully stop mcs-controllernode."
    systemctl stop mcs-controllernode;  
    ech0 "Attempting to gracefully stop mcs-storagemanager."
    systemctl stop mcs-storagemanager;
    ech0 "Attempting to gracefully stop mcs-primproc."
    systemctl stop mcs-primproc;
    ech0 "Attempting to gracefully stop mcs-writeengineserver."
    systemctl stop mcs-writeengineserver;
    ech0 "Attempting to gracefully stop mcs-workernode@1."
    systemctl stop mcs-workernode@1;
    ech0 "Attempting to gracefully stop mcs-workernode@2."
    systemctl stop mcs-workernode@2;
  fi

  COUNT_ANY_STRAGGLERS=$(ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep" | wc -l)
  if [ "$COUNT_ANY_STRAGGLERS" != "0" ]; then 
    ech0 "Remaining processes:"
    ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep"
    ech0 "Killing them..."
    ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep" | awk '{print $2}' | xargs kill -9
  fi

  COUNT_ANY_STRAGGLERS=$(ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep" | wc -l)
  
  if [ "$COUNT_ANY_STRAGGLERS" == "0" ]; then
    ech0 "No columnstore processes running."
    clearShm
    TEMP_COLOR=lcyan; print_color "Columnstore shared memory cleared.\n";  unset TEMP_COLOR
  else
    ech0 "After two attempts to kill all Columnstore processes, this is still running:"
    ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm)' | grep -v "grep"
  fi
else
  print0 "\nNothing done.\n\n"
fi
}

function display_outputfile_message() {
  echo "The output of this script is saved in the file $OUTPUTFILE"; echo;
}

function display_help_message() {
  set_data1dir
  COLUMNSTOREDIR=$(dirname $DATA1DIR) || COLUMNSTOREDIR=/var/lib/columnstore
  print_color "### HELP ###\n"
  print0 "This script is intended to be run while logged in as root. 
If database is up, this script will connect as root@localhost via socket.

Switches:
   --help             # display this message
   --version          # only show the header with version information
   --logs             # create a compressed archive of logs for MariaDB Support Ticket
   --path             # define the path for where to save files/tarballs and outputs of this script
   --backupdbrm       # takes a compressed backup of extent map files in dbrm directory
   --testschema       # creates a test schema, tables, imports, queries, drops schema
   --testschemakeep   # creates a test schema, tables, imports, queries, does not drop
   --ldlischema       # using ldli, creates test schema, tables, imports, queries, drops schema
   --ldlischemakeep   # using ldli, creates test schema, tables, imports, queries, does not drop
   --emptydirs        # searches $COLUMNSTOREDIR for empty directories
   --notmysqldirs     # searches $COLUMNSTOREDIR for directories not owned by mysql
   --emcheck          # Checks the extent map for orphaned and missing files
   --s3check          # Checks the extent map against S3 storage
   --pscs             # Adds the pscs command. pscs lists running columnstore processes 
   --schemasync       # Fix out-of-sync columnstore tables (CAL0009)
   --tmpdir           # Ensure owner of temporary dir after reboot (MCOL-4866 & MCOL-5242)
   --checkports       # Checks if ports needed by Columnstore are opened
   --eustack          # Dumps the stack of Columnstore processes
   --clearrollback    # Clear any rollback fragments from dbrm files
   --killcolumnstore  # Stop columnstore processes gracefully, then kill remaining processes

Color output switches:
   --color=none       # print headers without color
   --color=red        # print headers in color
                      # Options: [none,red,blue,green,yellow,magenta,cyan] prefix color with "l" for light\n"
  ech0
}

function ech0() {
 echo "$1"
 echo "$1" >> $OUTPUTFILE
}

function print0() {
 printf "$1"
 printf "$1" >> $OUTPUTFILE
}

function print_color () {
  if [ -z "$COLOR" ] && [ -z "$TEMP_COLOR" ]; then printf "$1"; printf "$1" >> $OUTPUTFILE; return; fi
  case "$COLOR" in
    default) i="0;36" ;;
    red)  i="0;31" ;;
    blue) i="0;34" ;;
    green) i="0;32" ;;
    yellow) i="0;33" ;;
    magenta) i="0;35" ;;
    cyan) i="0;36" ;;
    lred) i="1;31" ;;
    lblue) i="1;34" ;;
    lgreen) i="1;32" ;;
    lyellow) i="1;33" ;;
    lmagenta) i="1;35" ;;
    lcyan) i="1;36" ;;
    *) i="0" ;;
  esac
if [ $TEMP_COLOR ]; then
  case "$TEMP_COLOR" in
    default) i="0;36" ;;
    red)  i="0;31" ;;
    blue) i="0;34" ;;
    green) i="0;32" ;;
    yellow) i="0;33" ;;
    magenta) i="0;35" ;;
    cyan) i="0;36" ;;
    lred) i="1;31" ;;
    lblue) i="1;34" ;;
    lgreen) i="1;32" ;;
    lyellow) i="1;33" ;;
    lmagenta) i="1;35" ;;
    lcyan) i="1;36" ;;
    *) i="0" ;;
  esac
fi

  printf "\033[${i}m${1}\033[0m"
  printf "$1" >> $OUTPUTFILE
}

function get_eu_stack() {
  if ! command -v eu-stack &> /dev/null; then
    printf "\n[!] eu-stack not found. Please install eu-stack\n\n"
    ech0 "example: "
    ech0 "  yum install elfutils -y"
    ech0 "  apt-get install elfutils"
    ech0 
    exit 1; 
  fi  
  
  # Confirm CS online
  if [[ "$(ps -ef | grep -E "(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode)" | grep -v "grep"|wc -l)" == "0" ]]; then 
    printf "Columnstore processes are not running. EU Stack will not be collected.\n\n"
    exit 1;
  fi

  eu=$(which eu-stack)
  EU_FOLDER="$(hostname)_$(date +"%Y-%m-%d-%H-%M-%S")_eu_stack"
  if [ ! -d "$OUTDIR/$EU_FOLDER" ]; then mkdir -p "$OUTDIR/$EU_FOLDER"; fi

  $eu -p $(pidof PrimProc) > "$OUTDIR/$EU_FOLDER/eu-PrimProc.txt" ;
  $eu -p $(pidof DMLProc) > "$OUTDIR/$EU_FOLDER/eu-DMLProc.txt" ;
  $eu -p $(pidof DDLProc) > "$OUTDIR/$EU_FOLDER/eu-DDLProc.txt" ;
  $eu -p $(pidof mariadbd) > "$OUTDIR/$EU_FOLDER/eu-mariadbd.txt" ;
  $eu -p $(pidof WriteEngineServer) > "$OUTDIR/$EU_FOLDER/eu-WriteEngineServer.txt" ;
  $eu -p $(pidof controllernode) > "$OUTDIR/$EU_FOLDER/eu-controllernode.txt" ;
  $eu -p $(pidof workernode) > "$OUTDIR/$EU_FOLDER/eu-workernode.txt" ;
  cd $OUTDIR
  tar -czf "$OUTDIR/$EU_FOLDER.tar.gz" $EU_FOLDER/*

  if [ -f "$OUTDIR/$EU_FOLDER.tar.gz" ]; then
    print_color "### EU STACK COMPLETE ###\n"
  else
    print0 "EU Stack files not found.\n"
    exit 1;
  fi

  # cleanup
  mv "$OUTDIR/$EU_FOLDER.tar.gz" $TARDIR
  if [ -f "$TARDIR/$EU_FOLDER.tar.gz" ]; then
    print0 "Created: $TARDIR/$EU_FOLDER.tar.gz \n\n"
  else
    print0 "EU Stack files not found.\n"
    exit 1;
  fi
}

COLOR=default
for params in "$@"; do
  unset VALID;
  if [ "$params" == '--color=none' ]; then unset COLOR; VALID=true; fi
  if [ "$params" == '--color=red' ]; then COLOR=red; VALID=true; fi
  if [ "$params" == '--color=blue' ]; then COLOR=blue; VALID=true; fi
  if [ "$params" == '--color=green' ]; then COLOR=green; VALID=true; fi
  if [ "$params" == '--color=yellow' ]; then COLOR=yellow; VALID=true; fi
  if [ "$params" == '--color=magenta' ]; then COLOR=magenta; VALID=true; fi
  if [ "$params" == '--color=cyan' ]; then COLOR=cyan; VALID=true; fi
  if [ "$params" == '--color=lred' ]; then COLOR=lred; VALID=true; fi
  if [ "$params" == '--color=lblue' ]; then COLOR=lblue; VALID=true; fi
  if [ "$params" == '--color=lgreen' ]; then COLOR=lgreen; VALID=true; fi
  if [ "$params" == '--color=lyellow' ]; then COLOR=lyellow; VALID=true; fi
  if [ "$params" == '--color=lmagenta' ]; then COLOR=lmagenta; VALID=true; fi
  if [ "$params" == '--color=lcyan' ]; then COLOR=lcyan; VALID=true; fi
  if [ "$params" == '--color=cyan' ]; then COLOR=cyan; VALID=true; fi
  if [ "$params" == '--help' ]; then HELP=true; VALID=true; fi
  if [ "$params" == '--version' ]; then if [ ! $SKIP_REPORT ]; then DISPLAY_VERSION=true; fi; VALID=true; fi
  if [ "$params" == '--logs' ];    then if [ ! $SKIP_REPORT ]; then COLLECT_LOGS=true;    fi; VALID=true; fi
  if [[ "$params" == "--path"* ]];    then USER_PROVIDED_OUTPUT_PATH=$(echo "$params" | awk -F= '{print $2}'); VALID=true; fi
  if [ "$params" == '--backupdbrm' ]; then BACKUP_DBRM=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--testschema' ]; then TEST_SCHEMA=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--testschemakeep' ]; then TEST_SCHEMA_KEEP=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--ldlischema' ]; then LDLI_SCHEMA=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--ldlischemakeep' ]; then LDLI_SCHEMA_KEEP=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--emptydirs' ]; then EMPTY_DIRS=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--notmysqldirs' ]; then NOT_MYSQL_DIRS=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--emcheck' ]; then EM_CHECK=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--s3check' ]; then S3_CHECK=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--pscs' ]; then PSCS_ALIAS=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--schemasync' ]; then SCHEMA_SYNC=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--tmpdir' ]; then FIX_TMP_DIR=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--clearrollback' ]; then CLEARROLLBACK=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi
  if [ "$params" == '--checkports' ]; then SKIP_REPORT=true; CHECKPORTS=true;VALID=true; fi
  if [ "$params" == '--killcolumnstore' ]; then KILLCS=true; SKIP_REPORT=true; unset COLLECT_LOGS; VALID=true; fi  
  if [ "$params" == '--eustack' ]; then SKIP_REPORT=true; COLLECT_EU_STACK=true;VALID=true; fi
  if [ ! $VALID ]; then  INVALID_INPUT=$params; fi
done

prepare_for_run
exists_client_able_to_connect_with_socket
if [ $DISPLAY_VERSION ]; then exit 0; fi
if [ $INVALID_INPUT ]; then TEMP_COLOR=lred; print_color "Invalid parameter: ";ech0 $INVALID_INPUT; ech0; unset TEMP_COLOR; exit 1; fi
if [ $HELP ]||[ $INVALID_INPUT ]; then
  display_help_message
  exit 0
fi
if [ ! $SKIP_REPORT ]; then
exists_error_log

exists_cs_error_log
exists_columnstore_running
exists_mariadb_installed
exists_columnstore_installed
exists_cmapi_installed
exists_cmapi_running
exists_mariadbd_running
exists_https_proxy
exists_cmapi_cert_expired
exists_zombie_processes
exists_iptables_rules
exists_ip6tables_rules
exists_mysql_user_with_proper_uid
exists_columnstore_tmp_files_wrong_owner
exists_selinux_not_disabled
exists_corefiles
exists_abandoned_DDL_log
exists_replica_not_started
exists_querystats_enabled
exists_diskbased_hashjoin
exists_crossenginesupport
exists_minimal_columnstore_running
exists_errors_in_cmapi_server_log
exists_history_of_delete_dml
exists_history_of_update_dml
exists_history_of_file_does_not_exist
exists_compression_header_errors
exists_error_opening_file
exists_readtomagic_errors
exists_bad_magic_errors
exists_got_signal_errors
exists_unknown_ref_item
exists_errors_pushing_config
exists_history_failed_getSystemState
exists_does_not_exist_in_columnstore
exists_data1_dir_wrong_access_rights
exists_systemFiles_dir_wrong_access_rights
exists_holding_table_lock
exists_any_idb_errors
exists_any_mcs_errors
exists_version_buffer_overflow
exists_cpu_unsupported_2208
exists_symbolic_links_in_columnstore_dir
exists_schema_out_of_sync
exists_columnstore_tables_with_no_extents
exists_storage_manager_errors
exists_flooding_query_in_debug
exists_erroneous_module

TEMP_COLOR=lblue; print_color "===================== HOST =====================\n"; unset TEMP_COLOR
report_machine_architecture
report_cpu_info
report_memory_info
report_disk_space
report_uptime
report_local_hostname_and_ips
report_top_for_mysql_user
report_listening_procs
TEMP_COLOR=lblue; print_color "===================== MARIADB =====================\n"; unset TEMP_COLOR
report_versions
report_slave_hosts
report_slave_status
report_db_processes
report_datadir_size
report_skysql
TEMP_COLOR=lblue; print_color "===================== COLUMNSTORE =====================\n"; unset TEMP_COLOR
report_topology
report_cmapi_failover_enabled
report_dbroots
report_storage_type
report_columnstore_mounts
report_brm_saves_current
report_cs_table_locks
report_columnstore_query_count
report_calpontsys_exists
report_columnstore_tables
SUPPRESS_CLOSED_PORTS=true; check_ports
TEMP_COLOR=lblue; print_color "===================== LOGS =====================\n"; unset TEMP_COLOR
report_host_datetime
report_last_10_error_log_error
report_last_5_columnstore_err_log
TEMP_COLOR=lblue; print_color "===================== WARNINGS =====================\n"; unset TEMP_COLOR
report_warnings
fi

if [ $COLLECT_LOGS ]; then
  collect_logs
else
  if [ ! $SKIP_REPORT ]; then display_outputfile_message; fi
fi

if [ $BACKUP_DBRM ]; then
  backup_dbrm
fi

if [ $TEST_SCHEMA ]; then
  create_test_schema 
fi

if [ $TEST_SCHEMA_KEEP ]; then
  create_test_schema YES
fi

if [ $LDLI_SCHEMA ]; then
  ldli_test_schema
fi

if [ $LDLI_SCHEMA_KEEP ]; then
  ldli_test_schema YES
fi

if [ $EMPTY_DIRS ]; then
  empty_directories
fi

if [ $NOT_MYSQL_DIRS ]; then
  not_mysql_directories
fi

if [ $EM_CHECK ]; then
  extent_map_check
fi

if [ $S3_CHECK ]; then
  s3_check
fi

if [ $PSCS_ALIAS ]; then
  add_pscs_alias
fi

if [ $SCHEMA_SYNC ]; then
  schema_sync
fi

if [ $FIX_TMP_DIR ]; then
  ensure_owner_privs_of_tmp_dir
fi

if [ $CLEARROLLBACK ]; then
  clear_rollback
fi

if [ $CHECKPORTS ]; then
	check_ports
fi

if [ $KILLCS ]; then
  kill_columnstore
fi

if [ $COLLECT_EU_STACK ]; then
  get_eu_stack
fi