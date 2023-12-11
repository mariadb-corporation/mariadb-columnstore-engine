#!/bin/bash
########################################################################
# Copyright (c) 2020 MariaDB Corporation Ab
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file and at www.mariadb.com/bsl11.
#
# Change Date: 2024-07-10
#
# On the date above, in accordance with the Business Source License, use
# of this software will be governed by version 2 or later of the General
# Public License.
#
########################################################################
# Documentation:  bash mcs_backup_manager.sh help
# Version: 3.3
# 
# Backup Example
#   LocalStorage: sudo ./mcs_backup_manager.sh backup
#   S3:           sudo ./mcs_backup_manager.sh backup -bb s3://my-cs-backups
#
########################################################################
#
# Restore Example
#   LocalStorage: sudo ./mcs_backup_manager.sh restore -l <date>
#   S3:           sudo ./mcs_backup_manager.sh restore -bb s3://my-cs-backups -l <date> 
# 
########################################################################

start=`date +%s`
action=$1

print_action_help_text() {
    echo "
    MariaDB Columnstore Backup Manager
    
    Actions:

        backup                  Full & Incremental columnstore backup with additional flags to augment the backup taken
        restore                 Restore a backup taken with this script
        dbrm_backup             Quick hot backup of internal columnstore metadata only - only use under support recommendation

    Documentation:
        bash $0 <action> help

    Example:
        bash $0 backup help
    "
}

check_operating_system() {

    OPERATING_SYSTEM=$(awk -F= '/^ID=/{gsub(/"/, "", $2); print $2}' /etc/os-release)
    VERSION_EXACT=$(grep 'VERSION_ID=' /etc/os-release | awk -F= '{gsub(/"/, "", $2); print $2}')
    VERSION_MAJOR=$(grep 'VERSION_ID=' /etc/os-release | awk -F= '{gsub(/"/, "", $2); print $2}' |  awk -F. '{print $1}')

    # Supported OS
    case $OPERATING_SYSTEM in
        centos )
            return 1;
            ;;
        debian )
            return 1;
            ;;
        rocky )
            return 1;
            ;;
        ubuntu )
            return 1;
            ;;
        *)  # unknown option
            printf "\ncheck_operating_system: unknown os & version: $OPERATING_SYSTEM\n"
            exit 2;
    esac   
}

load_default_backup_variables()
{
    check_operating_system
    
    # What directory to store the backups on this machine or the target machine.
    # Consider write permissions of the scp user and the user running this script.
    # Mariadb-backup will use this location as a tmp dir for S3 and remote backups temporarily
    # Example: /mnt/backups/
    backup_location="/tmp/backups/"

    # Are the backups going to be stored on the same machine this script is running on or another server - if Remote you need to setup scp=
    # Options: "Local" or "Remote"
    backup_destination="Local"

    # Used only if backup_destination="Remote"
    # The user/credentials that will be used to scp the backup files
    # Example: "centos@10.14.51.62"
    scp=""

    # Only used if storage=S3
    # Name of the bucket to store the columnstore backups
    # Example: "s3://my-cs-backups"
    backup_bucket=""

    # OS specific Defaults
    case $OPERATING_SYSTEM in
        centos | rocky )
            MARIADB_SERVER_CONFIGS_PATH="/etc/my.cnf.d"
            ;;
        ubuntu | debian )
            MARIADB_SERVER_CONFIGS_PATH="/etc/mysql/mariadb.conf.d/"
            ;;
        *)  # unknown option
            handle_failed_dependencies "\nload_default_backup_variables: unknown os & version: $OPERATING_SYSTEM\n";
    esac  

    # Fixed Paths
    CS_CONFIGS_PATH="/etc/columnstore"
    DBRM_PATH="/var/lib/columnstore/data1/systemFiles/dbrm"
    STORAGEMANAGER_PATH="/var/lib/columnstore/storagemanager"
    STORAGEMANGER_CNF="$CS_CONFIGS_PATH/storagemanager.cnf"

    # Configurable Paths
    cs_metadata=$(grep ^metadata_path $STORAGEMANGER_CNF | cut -d "=" -f 2 | tr -d " ")
    cs_journal=$(grep ^journal_path $STORAGEMANGER_CNF  | cut -d "=" -f 2 | tr -d " ")
    cs_cache=$(grep -A25 "\[Cache\]" $STORAGEMANGER_CNF | grep ^path | cut -d "=" -f 2 | tr -d " ")

    # What storage topogoly is being used by Columnstore - found in /etc/columnstore/storagemanager.cnf
    # Options: "LocalStorage" or "S3" 
    storage=$(grep -m 1 "^service = " $STORAGEMANGER_CNF | awk '{print $3}')

    # Name of the existing bucket used in the cluster - found in /etc/columnstore/storagemanager.cnf
    # Example: "mcs-20211101172656965400000001"
    bucket=$( grep -m 1 "^bucket =" $STORAGEMANGER_CNF | awk '{print $3}')

    # modes ['direct','indirect'] - direct backups run on the columnstore nodes themselves. indirect run on another machine that has read-only mounts associated with columnstore/mariadb
    mode="direct"

    # Name of the Configuration file to load variables from 
    config_file=".cs-backup-config"

    # Track your write speed with "dstat --top-cpu --top-io"
    # Monitor individual rsyncs with ` watch -n0.25 "ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l; ps aux | grep 'rsync -a' | grep -vE 'color|grep' " `
    # Determines if columnstore data directories will have multiple rysnc running at the same time for different subfolders to parallelize writes
    parrallel_rsync=false
    # Directory stucture:    /var/lib/columnstore/data$dbroot/$dir1.dir/$dir2.dir/$dir3.dir/$dir4.dir/$partition.dir/FILE.$seqnum.cdf ;
    # DEPTH [1,2,3,4] determines at what level to begin (dir1/dir2/dir3/dir4) the number of PARALLEL_FOLDERS to issue
    DEPTH=3
    # PARALLEL_FOLDERS determines the number of parent directories to have rsync x PARALLEL_THREADS running at the same time ( for DEPTH=3 i.e /var/lib/columnstore/data$dbroot/$dir1.dir/$dir2.dir/$dir3.dir)
    PARALLEL_FOLDERS=4
    # PARALLEL_THREADS determines the number of threads to rsync subdirectories of the set DEPTH level ( for DEPTH=3 i.e /var/lib/columnstore/data$dbroot/$dir1.dir/$dir2.dir/$dir3.dir/X)
    # PARALLEL_THREADS is also used in for pigz compression parallelism
    PARALLEL_THREADS=4

    # Google Cloud
    # PARALLEL_UPLOAD=50
    RETRY_LIMIT=3

    # Other Variables
    #today=$(date +%m-%d-%Y-%H%M%S)
    today=$(date +%m-%d-%Y)
    HA=false
    if [ ! -f /var/lib/columnstore/local/module ]; then  pm="pm1"; else pm=$(cat /var/lib/columnstore/local/module);  fi;
    PM_NUMBER=$(echo "$pm" | tr -dc '0-9')
    if [[ -z $PM_NUMBER ]]; then PM_NUMBER=1; fi;
    
    #source_ips=$(grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}" /etc/columnstore/Columnstore.xml)
    #source_host_names=$(grep "<Node>" /etc/columnstore/Columnstore.xml)
    cmapi_key="$(grep "x-api-key" $CS_CONFIGS_PATH/cmapi_server.conf | awk  '{print $3}' | tr -d "'" )";
    endpoint="s3.us-east-1.amazonaws.com"
    final_message="Backup Complete"
    skip_save_brm=false
    skip_locks=false
    skip_polls=false
    skip_mdb=false
    skip_bucket_data=false
    quiet=false
    xtra_s3_args=""
    xtra_cmd_args=""
    rsync_flags=" -av";
    poll_interval=5
    poll_max_wait=60;

    # Compression Variables
    compress_format=""
    split_file_mdb_prefix="mariadb-backup.tar"
    split_file_cs_prefix="cs-localfiles.tar"
    timeout=999999
    logfile="backup.log"
    col_xml_backup="Columnstore.xml.source_backup"
    cmapi_backup="cmapi_server.conf.source_backup"

    # Used by on premise S3 vendors
    # Example: "http://127.0.0.1:8000"
    s3_url=""
    no_verify_ssl=false

    # Tracks if flush read lock has been run
    read_lock=false
    incremental=false
    mariadb_server_online=false
    columnstore_online=false

    # Number of DBroots
    # Integer usually 1 or 3
    if ! command -v xmllint > /dev/null; then
        confirm_xmllint_installed
    fi;
    DBROOT_COUNT=$(xmllint --xpath "string(//DBRootCount)" $CS_CONFIGS_PATH/Columnstore.xml)
    ASSIGNED_DBROOT=$(xmllint --xpath "string(//ModuleDBRootID$PM_NUMBER-1-3)" $CS_CONFIGS_PATH/Columnstore.xml)
}

parse_backup_variables()
{   
    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            backup)
                shift # past argument
                ;;
            -bl|--backuplocation)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -bd|--backupdestination)
                backup_destination="$2"
                shift # past argument
                shift # past value
                ;;
            -scp)
                scp="$2"
                shift # past argument
                shift # past value
                ;;
            -bb|--backupbucket)
                backup_bucket="$2"
                shift # past argument
                shift # past value
                ;;
            -url| --endpoint-url)
                s3_url="$2"
                shift # past argument
                shift # past value
                ;;
            -s|--storage)
                storage="$2"
                shift # past argument
                shift # past value
                ;;
            -i|--incremental)
                incremental=true
                today="$2"
                shift # past argument
                shift # past value
                ;;
            -P|--parallel)
                parrallel_rsync=true
                PARALLEL_THREADS="$2"
                shift # past argument
                shift # past value
                ;;
            -ha | --highavilability)
                HA=true
                shift # past argument
                ;;
            -f| --config-file)
                config_file="$2"
                shift # past argument
                shift # past value
                ;;
            -sbrm| --skip-save-brm)
                skip_save_brm=true
                shift # past argument
                ;;
            -spoll| --skip-polls)
                skip_polls=true
                shift # past argument
                ;;
            -slock| --skip-locks)
                skip_locks=true
                shift # past argument
                ;;
            -smdb | --skip-mariadb-backup)
                skip_mdb=true
                shift # past argument
                ;;
            -sb   | --skip-bucket-data)
                skip_bucket_data=true
                shift # past argument
                ;;
            -nb   | --name-backup)
                today="$2"
                shift # past argument
                shift # past value
                ;;
            -m    | --mode)
                mode="$2"
                shift # past argument
                shift # past value
                ;;
            -c    | --compress)
                compress_format="$2"
                shift # past argument
                shift # past value
                ;;
            -q    | --quiet)
                quiet=true
                shift # past argument
                ;;
            --no-verify-ssl)
                no_verify_ssl=true 
                shift # past argument
                ;;
            -pi| --poll-interval)
                poll_interval="$2"
                shift # past argument
                shift # past value
                ;;
            -pmw| --poll-max-wait)
                poll_max_wait="$2"
                shift # past argument
                shift # past value
                ;;
            -h|--help|-help|help)
                print_backup_help_text;
                exit 1;
                ;;
            *)  # unknown option
                handle_early_exit_on_backup "\nunknown flag: $1\n" true
        esac
    done

    # Load config file
    if [ -f $config_file ]; then
        source $config_file
    fi

    # Adjustment for indirect mode
    if [ $mode == "indirect" ]; then skip_locks=true; skip_save_brm=true; fi;
}

print_backup_help_text()
{
    echo "
    Columnstore Backup

        -bl   | --backup_location        directory where the backup will be saved
        -bd   | --backup_destination     if the directory is 'Local' or 'Remote' to this script
        -scp                             scp connection to remote server if -bd 'Remote'
        -bb   | --backup_bucket          bucket name for where to save S3 backups
        -url  | --endpoint-url           onprem url to s3 storage api example: http://127.0.0.1:8000
        --no-verify-ssl                  skips verifying ssl certs, useful for onpremise s3 storage
        -s    | --storage                the storage used by columnstore data 'LocalStorage' or 'S3'
        -i    | --incremental            adds columnstore deltas to an existing full backup
        -P    | --parallel               number of parallel rsync/compression threads to run
        -f    | --config-file            path to backup configuration file to load variables from
        -sbrm | --skip-save-brm          skip saving brm prior to running a backup - ideal for dirty backups
        -slock| --skip-locks             skip issuing write locks - ideal for dirty backups
        -spoll| --skip-polls             skip sql checks confirming no write/cpimports running
        -smdb | --skip-mariadb-backup    skip running a mariadb-backup for innodb data - ideal for incremental dirty backups
        -sb   | --skip-bucket-data       skip taking a copy of the columnstore data in the bucket
        -pi   | --poll-interval          number of seconds between poll checks for active writes & cpimports
        -pmw  | --poll-max-wait          max number of minutes for polling checks for writes to wait before exiting as a failed backup attempt
        -q    | --quiet                  silence verbose copy command outputs
        -c    | --compress               compress backup in X format - Options: [ pigz ]
        -nb   | --name-backup            define the name of the backup - default: date +%m-%d-%Y
        -ha   | --highavilability        Hint wether shared storage is attached @ below on all nodes to see all data
                                            HA LocalStorage ( /var/lib/columnstore/dataX/ )
                                            HA S3           ( /var/lib/columnstore/storagemanager/ )  

        Local Storage Examples:
            ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage
            ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage -P 8
            ./$0 backup -bl /tmp/backups/ -bd Local -s LocalStorage --incremental 02-18-2022
            ./$0 backup -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage

        S3 Examples:  
            ./$0 backup -bb s3://my-cs-backups -s S3
            ./$0 backup -bb s3://my-cs-backups -c pigz --quiet -sb
            ./$0 backup -bb gs://my-cs-backups -s S3 --incremental 02-18-2022
            ./$0 backup -bb s3://my-onpremise-bucket -s S3 -url http://127.0.0.1:8000
            
        Cron Example:
        */60 */24 * * *  root  bash /root/$0 -bb s3://my-cs-backups -s S3  >> /root/csBackup.log  2>&1
    ";
}

print_backup_variables()
{
    # Log Variables
    s1=20
    s2=20
    printf "\nBackup Variables\n"
    echo "--------------------------------------------------------------------------"
    if [ -f $config_file ]; then
        printf "%-${s1}s %-${s2}s\n" "Configuration File:" "$config_file";
    fi
    echo "--------------------------------------------------------------------------"
    printf "%-10s %-10s %-10s %-10s %-10s\n" "Skips:" "BRM($skip_save_brm)" "Locks($skip_locks)" "MariaDB($skip_mdb)" "Bucket($skip_bucket_data)";
    echo "--------------------------------------------------------------------------"
    printf "%-${s1}s  %-${s2}s\n" "Storage:" "$storage";
    printf "%-${s1}s  %-${s2}s\n" "Folder:" "$today";
    printf "%-${s1}s  %-${s2}s\n" "DB Root Count:" "$DBROOT_COUNT";
    printf "%-${s1}s  %-${s2}s\n" "PM:" "$pm";
    printf "%-${s1}s  %-${s2}s\n" "Highly Available:" "$HA";
    printf "%-${s1}s  %-${s2}s\n" "Incremental:" "$incremental";
    printf "%-${s1}s  %-${s2}s\n" "Timestamp:" "$(date +%m-%d-%Y-%H%M%S)";

    if [[ -n "$compress_format" ]]; then
        printf "%-${s1}s  %-${s2}s\n" "Compression:" "true";
        printf "%-${s1}s  %-${s2}s\n" "Compression Format:" "$compress_format";
        printf "%-${s1}s  %-${s2}s\n" "Compression Threads:" "$PARALLEL_THREADS";
    else 
        printf "%-${s1}s  %-${s2}s\n" "Parallel Enabled:" "$parrallel_rsync";
        if $parrallel_rsync ; then printf "%-${s1}s  %-${s2}s\n" "Parallel Threads:" "$PARALLEL_THREADS";   fi;
    fi

    if [ $storage == "LocalStorage" ]; then
        printf "%-${s1}s  %-${s2}s\n" "Backup Destination:" "$backup_destination";
        if [ $backup_destination == "Remote" ]; then   printf "%-${s1}s  %-${s2}s\n" "scp:" "$scp";  fi; 
        printf "%-${s1}s  %-${s2}s\n" "Backup Location:" "$backup_location";
    fi

    if [ $storage == "S3" ]; then 
        printf "%-${s1}s  %-${s2}s\n" "Active Bucket:" "$bucket";
        printf "%-${s1}s  %-${s2}s\n" "Backup Bucket:" "$backup_bucket";
    fi
    echo "--------------------------------------------------------------------------"
}

check_package_managers() {
    
    package_manager='';
    if command -v apt &> /dev/null ; then
        if ! command -v dpkg-query &> /dev/null ; then
            printf "[!!] Cant access dpkg-query\n"
            exit 1;
        fi
        package_manager="apt";
    fi

    if command -v yum &> /dev/null ; then
        package_manager="yum";
    fi    

    if [ $package_manager == '' ]; then 
        handle_failed_dependencies "[!!] No package manager found: yum or apt must be installed"
        exit 1;
    fi;
}

confirm_xmllint_installed() {
    
    if ! command -v xmllint > /dev/null; then
        printf "[!] xmllint not installed ... attempting auto install\n\n"
        check_package_managers
        case $package_manager in
            yum ) 
                install_command="yum install libxml2 -y";
                ;;
            apt )
                install_command="apt install libxml2-utils -y --quiet";
                ;;
            *)  # unknown option
                handle_failed_dependencies "[!!] package manager not implemented: $package_manager\n"
                exit 2;
        esac

        if ! eval $install_command; then
            handle_failed_dependencies "[!] Failed to install xmllint\nThis is required."
            exit 1;
        fi
    fi
}

confirm_rsync_installed() {

    if ! command -v rsync  > /dev/null; then
        printf "[!] rsync not installed ... attempting auto install\n\n"
        check_package_managers
        case $package_manager in
            yum ) 
                install_command="yum install rsync -y";
                ;;
            apt )
                install_command="apt install rsync -y --quiet";
                ;;
            *)  # unknown option
                log_debug_message "[!!] package manager not implemented: $package_manager\n"
                exit 2;
        esac

        if ! eval $install_command; then
            handle_failed_dependencies "[!] Failed to install rsync\nThis is required."
            exit 1;
        fi
    fi
}

confirm_mariadb_backup_installed() {

    if $skip_mdb ; then return; fi;

    if ! command -v  mariadb-backup  > /dev/null; then
        printf "[!]  mariadb-backup not installed ... attempting auto install\n\n"
        check_package_managers
        case $package_manager in
            yum ) 
                install_command="yum install MariaDB-backup -y";
                ;;
            apt )
                install_command="apt install mariadb-backup -y --quiet";
                ;;
            *)  # unknown option
                log_debug_message "[!!] package manager not implemented: $package_manager\n"
                exit 2;
        esac

        if ! eval $install_command; then
            handle_failed_dependencies "[!] Failed to install MariaDB-backup\nThis is required."
            exit 1;
        fi
    fi
}

confirm_pigz_installed() {

    if ! command -v pigz  > /dev/null; then
        printf "[!] pigz not installed ... attempting auto install\n\n"
        check_package_managers
        case $package_manager in
            yum ) 
                install_command="yum install pigz -y";
                ;;
            apt )
                install_command="apt install pigz -y --quiet";
                ;;
            *)  # unknown option
                log_debug_message "[!!] package manager not implemented: $package_manager\n"
                exit 2;
        esac

        if ! eval $install_command; then
            handle_failed_dependencies "[!] Failed to install pigz\nThis is required for '-c pigz'"
            exit 1;
        fi
    fi
}

check_for_dependancies() 
{
    # Check pidof works
    if [ $mode != "indirect" ] &&  ! command -v pidof > /dev/null; then
        handle_failed_dependencies "\n\n[!] Please make sure pidof is installed and executable\n\n"
    fi
    
    # used for save_brm and defining columnstore_user
    if ! command -v stat > /dev/null; then
        handle_failed_dependencies "\n\n[!] Please make sure stat is installed and executable\n\n"
    fi

    confirm_rsync_installed
    confirm_mariadb_backup_installed

    if [ $1 == "backup" ] && [ $mode != "indirect" ] && ! command -v dbrmctl  > /dev/null; then 
        handle_failed_dependencies "\n\n[!] dbrmctl unreachable to issue lock \n\n"
    fi

    if [ $storage == "S3" ]; then
        
        # Default cloud
        cloud="aws"
        
        # Critical argument for S3 - determine which cloud
        if [ -z "$backup_bucket" ]; then handle_failed_dependencies "\n undefined --backup_bucket: $backup_bucket \nfor examples see: ./$0 backup --help\n"; fi
        if [[ $backup_bucket == gs://* ]]; then
            cloud="gcp"; protocol="gs";
        elif [[ $backup_bucket == s3://* ]]; then
            cloud="aws"; protocol="s3";
        else 
            handle_failed_dependencies "\n Invalid --backup_bucket - doesnt lead with gs:// or s3:// - $backup_bucket\n";
        fi

        if [ $cloud == "gcp" ]; then
            # If GCP - Check gsutil installed
            gsutil=""
            protocol="gs"
            if command -v mcs_gsutil > /dev/null; then
                gsutil=$(which mcs_gsutil 2>/dev/null)
            elif ! command -v gsutil > /dev/null; then
                which gsutil
                echo "Hints:  
                curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-443.0.0-linux-x86_64.tar.gz
                tar -xvf google-cloud-cli-443.0.0-linux-x86_64.tar.gz
                ./google-cloud-sdk/install.sh -q
                sudo ln -s \$PWD/google-cloud-sdk/bin/gsutil /usr/bin/gsutil

                Then
                A)    gsutil config -a
                or B) gcloud auth activate-service-account --key-file=user-file.json
                "

                handle_failed_dependencies "\n\nPlease make sure gsutil cli is installed configured and executable\n"
            else 
                gsutil=$(which gsutil 2>/dev/null)
            fi
           
            # gsutil sytax for silent
            if $quiet; then xtra_s3_args+="-q"; fi
        else 

            # on prem S3 will use aws cli
            # If AWS - Check aws-cli installed
            awscli=""
            protocol="s3"
            if command -v mcs_aws > /dev/null; then
                awscli=$(which mcs_aws 2>/dev/null)
            elif ! command -v aws > /dev/null; then
                which aws
                echo "Hints:  
                curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"
                unzip awscliv2.zip
                sudo ./aws/install
                aws configure"

                handle_failed_dependencies "\n\n Please make sure aws cli is installed configured and executable\nSee existing .cnf aws credentials with:  grep aws_ $STORAGEMANGER_CNF \n\n"
            else
                awscli=$(which aws 2>/dev/null)
            fi
            
            # aws sytax for silent
            if $quiet; then xtra_s3_args+="--quiet"; fi
        fi;
    fi
}

validation_prechecks_for_backup() 
{
    echo "Prechecks ..."

    # Adjust rsync for incremental copy
    additional_rysnc_flags=""
    if $quiet; then xtra_cmd_args+="  2> /dev/null"; additional_rysnc_flags+=" -a"; else additional_rysnc_flags+=" -av "; fi
    if  $incremental ; then additional_rysnc_flags+=" --inplace --no-whole-file --delete"; fi;
    if [ -z "$mode" ]; then printf "\n[!!!] Required field --mode: $mode - is empty\n"; exit 1; fi
    if [ "$mode" != "direct" ] && [ "$mode" != "indirect" ] ; then printf "\n[!!!] Invalid field --mode: $mode\n"; exit 1; fi

    # Poll Variable Checks
    is_numerical_or_decimal "$poll_interval" "poll_interval"
    is_numerical_or_decimal "$poll_max_wait" "poll_max_wait"
    max_poll_attempts=$((poll_max_wait * 60 / poll_interval))
    if [ "$max_poll_attempts" -lt 1 ]; then max_poll_attempts=1; fi;

    # Detect if columnstore online
    if [ $mode == "direct" ]; then
        if [ -z $(pidof PrimProc) ] || [ -z $(pidof WriteEngineServer) ]; then printf " - Columnstore is OFFLINE \n"; columnstore_online=false; else printf " - Columnstore is ONLINE - safer if offline \n"; export columnstore_online=true; fi
    fi;

    # Validate compression option
    if [[ -n "$compress_format" ]]; then
        case $compress_format in
            pigz) 
                confirm_pigz_installed
                ;;
            *)  # unknown option
                handle_early_exit_on_backup "\n[!!!] Invalid field --compress: $compress_format\n\n" true
        esac
    fi;

    # If Remote save - Check ssh works to remote
    if [ $backup_destination == "Remote" ]; then
        if ssh -o ConnectTimeout=10 $scp echo ok 2>&1 ;then
            printf 'SSH Works\n'
        else
            handle_early_exit_on_backup "Failed Command: ssh $scp\n" true
        fi
    fi

    # Storage Based Checks
    if [ $storage == "LocalStorage" ]; then 

        # Incremental Job checks
        if  $incremental; then
            # Cant continue if this folder (which represents a full backup) doesnt exists
            if [ $backup_destination == "Local" ]; then 
                if [ -d $backup_location$today ]; then printf "[+] Full backup directory exists\n"; else  handle_early_exit_on_backup "[X] Full backup directory ($backup_location$today) DOES NOT exist \n\n" true; fi;
            elif [ $backup_destination == "Remote" ]; then
                if [[ `ssh $scp test -d $backup_location$today && echo exists` ]] ; then  printf "[+] Full backup directory exists\n"; else handle_early_exit_on_backup "[X] Full backup directory ($backup_location$today) DOES NOT exist on remote $scp \n\n" true;  fi
            fi
        fi
    elif [ $storage == "S3" ]; then
        
        # Adjust s3api flags for onpremise/custom endpoints
        add_s3_api_flags=""
        if [ ! -z "$s3_url" ];  then add_s3_api_flags+=" --endpoint-url $s3_url"; fi;
        if $no_verify_ssl; then add_s3_api_flags+=" --no-verify-ssl"; fi;
        
        # Validate addtional relevant arguments for S3
        if [ -z "$backup_bucket" ]; then echo "Invalid --backup_bucket: $backup_bucket - is empty"; exit 1; fi

        # Check cli access to bucket
        if [ $cloud == "gcp" ]; then
        
            if $gsutil ls $backup_bucket > /dev/null ; then
                printf " - Success listing backup bucket\n"
            else 
                printf "\n[X] Failed to list bucket contents... \nCheck $gsutil credentials: $gsutil config -a"
                handle_early_exit_on_backup "\n$gsutil ls $backup_bucket \n\n" true
            fi

        else 

            # Check aws cli access to bucket
            if $awscli $add_s3_api_flags s3 ls $backup_bucket > /dev/null ; then
                printf " - Success listing backup bucket\n"
            else 
                printf "\n[X] Failed to list bucket contents... \nCheck aws cli credentials: aws configure"
                handle_early_exit_on_backup "\naws $add_s3_api_flags s3 ls $backup_bucket \n" true
            fi
        fi;

         # Incremental Job checks
        if $incremental; then
            # Cant continue if this folder (which represents a full backup) doesnt exists
            if [ $cloud == "gcp" ]; then
                if [[ $( $gsutil ls $backup_bucket/$today | head ) ]]; then  printf "[+] Full backup directory exists\n"; else handle_early_exit_on_backup "[X] Full backup directory ($backup_bucket/$today) DOES NOT exist in GCS \nCheck - $gsutil ls $backup_bucket/$today | head \n\n" true;   fi  
            else
                if [[ $( $awscli $add_s3_api_flags s3 ls $backup_bucket/$today | head ) ]]; then  printf "[+] Full backup directory exists\n"; else handle_early_exit_on_backup "[X] Full backup directory ($backup_bucket/$today) DOES NOT exist in S3 \nCheck - aws $add_s3_api_flags s3 ls $backup_bucket/$today | head \n\n" true; fi  
            fi;
        fi
    else 
        handle_early_exit_on_backup "Invalid Variable storage: $storage" true
    fi
}

cs_read_only_wait_loop() {
    retry_limit=1800
    retry_counter=0
    read_only_mode_message="DBRM is currently Read Only!"
    current_status=$(dbrmctl status);
    printf " - Confirming CS Readonly...                          "
    while [ "$current_status" != "$read_only_mode_message" ]; do
        sleep 1
        printf "."
        current_status=$(dbrmctl status);
        if [ $? -ne 0 ]; then
            handle_early_exit_on_backup "\n[!] Failed to get dbrmctl status\n\n" 
        fi
        if [ $retry_counter -ge $retry_limit ]; then
            handle_early_exit_on_backup "\n[!] Set columnstore readonly wait retry limit exceeded: $retry_counter \n\n"
        fi

        ((retry_counter++))
    done
    printf "Done\n"
}

# Having columnstore offline is best for non-volatile backups  
# If online - issue a flush tables with read lock and set DBRM to readonly
issue_write_locks() 
{
    if [ $mode == "indirect" ] || $skip_locks; then printf "\n"; return; fi;
    if ! $skip_mdb && ! pidof mariadbd > /dev/null; then 
        handle_early_exit_on_backup "\n[X] MariaDB is offline ... Needs to be online to issue read only lock and to run mariadb-backup \n\n" true; 
    fi;

    printf "\nLocks \n"; 
    # Pre 23.10.2 CS startreadonly doesnt exist - so poll cpimports added to protect them
    if ! $skip_polls; then 
        poll_check_no_active_sql_writes
        poll_check_no_active_cpimports
    fi;

    ORIGINAL_READONLY_STATUS=$(mariadb -qsNe "select @@read_only;")
    # Set MariaDB Server ReadOnly Mode
    if pidof mariadbd > /dev/null; then
        printf " - Issuing read-only lock to MariaDB Server ...      ";
        if mariadb -e "FLUSH TABLES WITH READ LOCK;"; then
            mariadb -e "set global read_only=ON;"
            read_lock=true
            printf " Done\n"; 
        else    
            handle_early_exit_on_backup "\n[X] Failed issuing read-only lock\n\n" 
        fi 
    fi
    
    if [ $pm == "pm1" ]; then 
        
        # Set Columnstore ReadOnly Mode
        startreadonly_exists=$(dbrmctl -h 2>&1 | grep "startreadonly")
        printf " - Issuing read-only lock to Columnstore Engine ...   ";
        if ! $columnstore_online; then 
            printf "Skip since offline\n\n";
        elif [ $DBROOT_COUNT == "1" ] && [[ -n "$startreadonly_exists" ]]; then
            if dbrmctl startreadonly ; then
                cs_read_only_wait_loop
                printf " \n";
            else
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via dbrmctl startreadonly\n" 
            fi;

        elif [ $DBROOT_COUNT == "1" ]; then 
            if dbrmctl readonly ; then
                printf " \n";
            else 
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via dbrmctl readonly \n" 
            fi
        else 
            cmapiResponse=$(curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapi_key" --data '{"timeout":20, "mode": "readonly"}' -k);
            if [[ $cmapiResponse == '{"error":'* ]] ; then 
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via cmapi\n" 
            else
                printf "$cmapiResponse \n";
            fi
        fi
    else
        cs_read_only_wait_loop
        printf " \n";
    fi
}

poll_check_no_active_sql_writes() {
    printf " - Polling For Active Writes ...                      "
    query="SELECT COUNT(*) FROM information_schema.processlist where user != 'root' AND command = 'Query' AND ( info LIKE '%INSERT%' OR info LIKE '%UPDATE%' OR info LIKE '%DELETE%' OR info LIKE '%LOAD DATA%');"
    attempts=0
    no_writes=false
    while [ "$attempts" -lt "$max_poll_attempts" ]; do
        active_writes=$( mariadb -s -N -e "$query" )
        if [ "$active_writes" -le 0 ]; then
            printf "Done\n"
            no_writes=true
            break
        else
            # extra_info=$( mariadb -e "SELECT * FROM information_schema.processlist where user != 'root' AND command = 'Query' AND info LIKE '%INSERT%' OR info LIKE '%UPDATE%' OR info LIKE '%DELETE%' " )
            # echo "Active write operations detected: $active_writes"
            # echo "$extra_info"
            printf "."
            if ! $quiet; then printf "\nActive write operations detected: $active_writes"; fi;
            sleep "$poll_interval"
            ((attempts++))
        fi
    done

    if ! $no_writes; then
        handle_early_exit_on_backup "\n[X] Exceeded poll_max_wait: $poll_max_wait minutes\nActive write operations detected: $active_writes \n" true
    fi;
}

poll_check_no_active_cpimports() {
    printf " - Polling For Active cpimports ...                   "
    attempts=0
    no_cpimports=false
    if ! $columnstore_online ; then printf "Skip since offline\n"; return ; fi;
    while [ "$attempts" -lt "$max_poll_attempts" ]; do
        active_cpimports=$(viewtablelock 2>/dev/null )
        if [[ "$active_cpimports" == *"No tables are locked"* ]]; then
            printf "Done\n"
            no_cpimports=true
            break
        else 
            printf "."
            if ! $quiet; then printf "\n$active_cpimports"; fi;
            sleep "$poll_interval"
            ((attempts++))
        fi
    done

    if ! $no_cpimports; then
        handle_early_exit_on_backup "\n[X] Exceeded poll_max_wait: $poll_max_wait minutes\nActive cpimports\n$active_cpimports \n" true
    fi;
}

run_save_brm() 
{

    if $skip_save_brm || [ $pm != "pm1" ] || ! $columnstore_online || [ $mode == "indirect" ]; then return; fi;

    printf "\nBlock Resolution Manager\n"
    local tmpDir="/tmp/DBRMbackup-$today"
    if [ ! -d "$tmpDir" ]; then mkdir -p $tmpDir; fi;

    # Copy extent map locally just in case save_brm fails
    if [ $storage == "LocalStorage" ]; then 
        cp -R $DBRM_PATH/* $tmpDir
    elif [ $storage == "S3" ]; then 
        cp -R $STORAGEMANAGER_PATH/* $tmpDir

        # Base Set
        eval "smcat /data1/systemFiles/dbrm/BRM_saves_current > $tmpDir/BRM_saves_current $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_saves_journal > $tmpDir/BRM_saves_journal $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_saves_em > $tmpDir/BRM_saves_em $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_saves_vbbm > $tmpDir/BRM_saves_vbbm $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_saves_vss > $tmpDir/BRM_saves_vss $xtra_cmd_args"

        # A Set
        eval "smcat /data1/systemFiles/dbrm/BRM_savesA_em > $tmpDir/BRM_savesA_em $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_savesA_vbbm > $tmpDir/BRM_savesA_vbbm $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_savesA_vss > $tmpDir/BRM_savesA_vss $xtra_cmd_args"
        
        # B Set
        eval "smcat /data1/systemFiles/dbrm/BRM_savesB_em > $tmpDir/BRM_savesB_em $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_savesB_vbbm > $tmpDir/BRM_savesB_vbbm $xtra_cmd_args"
        eval "smcat /data1/systemFiles/dbrm/BRM_savesB_vss > $tmpDir/BRM_savesB_vss $xtra_cmd_args"
    fi

    printf " - Saving BRMs... \n"
    brmOwner=$(stat -c "%U" $DBRM_PATH/)
    if sudo -u $brmOwner /usr/bin/save_brm ; then 
        rm -rf $tmpDir
    else  
        printf "\n Failed: save_brm error - see /var/log/messages - backup @ $tmpDir \n\n"; 
        handle_early_exit_on_backup 
    fi
    
}

# Example: s3sync <from> <to> <success_message> <failed_message>
# Example: s3sync s3://$bucket $backup_bucket/$today/columnstoreData "Done - Columnstore data sync complete"
s3sync() 
{

    local from=$1
    local to=$2
    local success_message=$3
    local failed_message=$4
    local retries=${5:-0}
    local cmd=""
    
    if [ $cloud == "gcp" ]; then
        cmd="$gsutil $xtra_s3_args -m rsync -r -d $from $to"

        # gsutil throws WARNINGS if not directed to /dev/null
        if $quiet; then cmd+="  2>/dev/null"; fi
    else 
        # Default AWS
        cmd="$awscli $xtra_s3_args $add_s3_api_flags s3 sync $from $to"
    fi

    if eval $cmd; then
        if [ ! -z "$success_message" ]; then printf "$success_message"; fi;
    else
        if [ $retries -lt $RETRY_LIMIT ]; then 
            echo "$cmd"
            echo "Retrying: $retries"
            sleep 1;
            s3sync "$1" "$2" "$3" "$4" "$(($retries+1))"
        else
            if [ ! -z "$failed_message" ]; then printf "$failed_message"; fi;
            echo "Was Trying:   $cmd"
            echo "exiting ..."
            handle_early_exit_on_backup
        fi
    fi
}

# Example: s3cp <from> <to>
s3cp()
{
    local from=$1
    local to=$2
    local cmd=""
    
    if [ $cloud == "gcp" ]; then
        cmd="$gsutil $xtra_s3_args cp $from $to"
    else 
        # Default AWS
        cmd="$awscli $xtra_s3_args $add_s3_api_flags s3 cp $from $to"
    fi;

    $cmd

}

# Example: s3rm <path to delete> 
s3rm()
{
    local path=$1
    local cmd=""
    
    if [ $cloud == "gcp" ]; then
        cmd="$gsutil $xtra_s3_args -m rm -r $path"
    else 
        # Default AWS
        cmd="$awscli $xtra_s3_args $add_s3_api_flags s3 rm $path  --recursive"
    fi;

    $cmd
}

# Example: s3ls <path to list> 
s3ls()
{
    local path=$1
    local cmd=""
    
    if [ $cloud == "gcp" ]; then
        cmd="$gsutil ls $path"
    else 
        # Default AWS
        cmd="$awscli $add_s3_api_flags s3 ls $path"
    fi;

    $cmd
}

clear_read_lock() 
{

    if [ $mode == "indirect" ] || $skip_locks; then return; fi;

    printf "\nClearing Locks\n"; 
    # Clear MDB Lock 
    if pidof mariadbd > /dev/null && [ $read_lock ]; then
        printf " - Clearing read-only lock on MariaDB Server ...     ";
        if mariadb -e "UNLOCK TABLES;" && mariadb -qsNe "set global read_only=$ORIGINAL_READONLY_STATUS;"; then
            read_lock=false;
            printf " Done\n"
        else 
            handle_early_exit_on_backup "\n[X] Failed clearing readLock\n" true
        fi
    fi

    if [ $pm == "pm1" ]; then

        # Clear CS Lock
        printf " - Clearing read-only lock on Columnstore Engine ...  "; 
        if ! $columnstore_online; then 
            printf "Skip since offline\n"
        elif [ $DBROOT_COUNT == "1" ]; then 
            if dbrmctl readwrite ; then
                printf " ";
            else 
                handle_early_exit_on_backup "\n[X] Failed clearing columnstore BRM lock via dbrmctl\n" true;
            fi
        elif curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapi_key" --data '{"timeout":20, "mode": "readwrite"}' -k  ; then 
            printf " \n"
        else
            handle_early_exit_on_backup "\n[X] Failed clearing columnstore BRM lock\n" true;
        fi
    fi
    
} 

# handle_ called when certain checks/functionality fails
handle_failed_dependencies()
{
    printf "\nDependency Checks Failed: $1"
    alert $1
    exit 1;
}

# first argument is the error message
# 2nd argument true = skip clear_read_lock, false= dont skip
handle_early_exit_on_backup()
{   
    skip_clear_locks=${2:-false}
    if ! $skip_clear_locks; then clear_read_lock; fi;
    printf "\nBackup Failed: $1\n"
    alert $1
    exit 1;
}

handle_early_exit_on_restore()
{
    printf "\nRestore Failed: $1\n"
    alert $1
    exit 1;
}

handle_ctrl_c_backup() {
    echo "Ctrl+C captured. handle_early_exit_on_backup..."
    handle_early_exit_on_backup
}

alert() {
    # echo "Not implemented yet"
    # slack, email, webhook curl endpoint etc.
    return;
}

# initiate_rsync uses this to wait and monitor all running jobs
# deepParallelRsync uses this to limit the parallelism to a max of $PARALLEL_FOLDERS times $PARALLEL_THREADS
wait_on_rsync()
{
    local visualize=$1
    local pollSleepTime=$2
    local concurrentThreshHold=$3
    local dbrootToSync=$4
    local rsyncInProgress="$(ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l )";
    local total=0

    local w=0;
    while [ $rsyncInProgress -gt "$concurrentThreshHold" ]; do 
        if ! $quiet && $visualize && [ $(($w % 10)) -eq 0 ]; then 
            if [[ $total == 0 ]]; then 
                total=$(du -sh /var/lib/columnstore/data$dbrootToSync/) 
            else  
                echo -E "$rsyncInProgress rsync processes running ... seconds: $w"
                echo "Status: $(du -sh $backup_location$today/data$dbrootToSync/)"
                echo "Goal:   $total"
                echo "Monitor rsync: ps aux | grep 'rsync -a' | grep -vE 'color|grep' "
            fi;
        fi
        #ps aux | grep 'rsync -a' | grep -vE 'color|grep'    # DBEUG
        sleep $pollSleepTime
        rsyncInProgress="$(ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l )";
        ((w++))
    done
}

initiate_rsyncs() 
{
    local dbrootToSync=$1
    parallel_rysnc_flags=" -a "
    if $incremental ; then parallel_rysnc_flags+=" --inplace --no-whole-file --delete"; fi;
    
    deepParallelRsync /var/lib/columnstore/data$dbrootToSync 1 $DEPTH data$dbrootToSync &
    sleep 2;
    #jobs
    wait_on_rsync true 2 0 $dbrootToSync
    wait
}

# A recursive function that increments depthCurrent+1 each directory it goes deeper and issuing rsync on each directory remaing at the target depth  
# Example values:   
#   path: /var/lib/columnstore/data1
#   depthCurrent: 1
#   depthTarget: 3
#   depthCurrent: data1
deepParallelRsync() 
{
    path=$1
    depthCurrent=$2
    depthTarget=$3
    relativePath=$4
    # echo "DEBUG: 
    # path=$1
    # depthCurrent=$2
    # depthTarget=$3
    # relativePath=$4"

    for fullFilePath in $path/*; do

        local fileName=$(basename $fullFilePath)
        # echo "DEBUG - filename: $fileName"

        # If a directory, keep going deeper until depthTarget reached
        if [ -d $fullFilePath ]; then
            #echo "DEBUG - Directory $file"
            mkdir -p $backup_location$today/$relativePath/$fileName

            if [ $depthCurrent -ge $depthTarget ]; then
                #echo "DEBUG - copy to relative: $backup_location$today/$relativePath/"
                if ls $fullFilePath | xargs -P $PARALLEL_THREADS -I {} rsync $parallel_rysnc_flags $fullFilePath/{} $backup_location$today/$relativePath/$fileName ; then
                    echo "   + Completed: $backup_location$today/$relativePath/$fileName"
                else 
                    echo "Failed: $backup_location$today/$relativePath/$fileName"
                    exit 1;
                fi
                
            else
                # echo "DEBUG - Fork Deeper - $fullFilePath "    
                wait_on_rsync false "0.5" $PARALLEL_FOLDERS
                # Since target depth not reached, recursively call for each directory 
                deepParallelRsync $fullFilePath "$((depthCurrent+1))" $depthTarget "$relativePath/$fileName" &
            fi

        # If a file, rsync right away
        elif [ -f $fullFilePath ]; then
            rsync $additional_rysnc_flags $fullFilePath $backup_location$today/$relativePath/

        # If filename is * then the directory is empty 
        elif [ "$fileName" == "*" ]; then 
            # echo "DEBUG - Skipping $relativePath - empty";
            continue
        else
            printf "\n\n Warning - Unhandled - $fullFilePath \n\n";
        fi
    done
    wait
}

run_backup()
{   
    if [ $storage == "LocalStorage" ]; then 
        if [ $backup_destination == "Local" ]; then

            printf "\nLocal Storage Backup\n"

            # Check/Create Directories
            printf " - Creating Backup Directories...    "
            if [[ -n "$compress_format" ]]; then
                mkdir -p $backup_location$today
            else
                mkdir -p $backup_location$today/mysql
                mkdir -p $backup_location$today/configs 
                mkdir -p $backup_location$today/configs/mysql
            
                # Check/Create CS Data Directories
                i=1
                while [ $i -le $DBROOT_COUNT ]; do
                    if [[ $ASSIGNED_DBROOT == "$i" || $HA == true ]]; then mkdir -p $backup_location$today/data$i ; fi
                    ((i++))
                done 
            fi
            printf " Done\n"   
            
            # Backup Columnstore data
            i=1
            while [ $i -le $DBROOT_COUNT ]; do
                if [[ $ASSIGNED_DBROOT == "$i" || $HA == true ]]; then 
                    if [[ -n "$compress_format" ]]; then
                        # For compression keep track of dirs & files to include and compress/stream in the end - $compress_paths
                       compress_paths="/var/lib/columnstore/data$i/* "  

                    elif $parrallel_rsync ; then 
                        printf " - Parallel Rsync CS Data$i...       \n"
                        initiate_rsyncs $i
                        printf " Done\n"      
                    else
                        printf " - Syncing Columnstore Data$i...      "
                        eval "rsync $additional_rysnc_flags /var/lib/columnstore/data$i/* $backup_location$today/data$i/ $xtra_cmd_args";
                        printf " Done\n"  
                    fi;
                fi
                ((i++))
            done

            # Backup MariaDB data
            if [ $ASSIGNED_DBROOT == "1" ]; then
                
                # logic to increment mysql and keep count if we want to backup incremental mysql data
                # i=1
                # latestMysqlIncrement=0
                # while [ $i -le 365 ]; do
                #     if [ -d $backup_location/mysql$i ]; then  ; else latestMysqlIncrement=i; break; fi;
                # done

                # MariaDB backup wont rerun if folder exists - so clear it before running mariadb-backup
                if [ -d $backup_location$today/mysql ]; then 
                    rm -rf $backup_location$today/mysql
                fi

                # Run mariadb-backup
                if ! $skip_mdb; then
                    if [[ -n "$compress_format" ]]; then
                        printf " - Compressing MariaDB Data...       "
                        mbd_prefix="$backup_location$today/$split_file_mdb_prefix.$compress_format"
                        case $compress_format in
                            pigz)
                                # Handle Cloud 
                                if !  mariabackup --user=root --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $mbd_prefix 2>> $logfile; then
                                    handle_early_exit_on_backup "\nFailed  mariabackup --user=root --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $mbd_prefix 2>> $logfile \n"
                                fi
                                printf " Done @ $mbd_prefix\n"
                                ;;
                            *)  # unknown option
                                handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
                        esac
                    else
                        printf " - Copying MariaDB Data...           "
                        if eval "mariadb-backup --backup --target-dir=$backup_location$today/mysql --user=root $xtra_cmd_args" ; then printf " Done \n"; else printf "\n Failed: mariadb-backup --backup --target-dir=$backup_location$today/mysql --user=root\n"; handle_early_exit_on_backup; fi
                    fi
                else 
                    echo "[!] Skipping mariadb-backup"
                fi

                # Backup CS configurations
                if [[ ! -n "$compress_format" ]]; then
                    printf " - Copying Columnstore Configs...    "
                    eval "rsync $additional_rysnc_flags $CS_CONFIGS_PATH/Columnstore.xml $backup_location$today/configs/ $xtra_cmd_args"
                    eval "rsync $additional_rysnc_flags $STORAGEMANGER_CNF $backup_location$today/configs/ $xtra_cmd_args"
                    if [ -f $CS_CONFIGS_PATH/cmapi_server.conf ]; then  eval "rsync $additional_rysnc_flags $CS_CONFIGS_PATH/cmapi_server.conf $backup_location$today/configs/ $xtra_cmd_args"; fi
                    printf " Done\n"
                else
                    # When restoring - dont want to overwrite the new systems entire Columnstore.xml
                    rm -rf $CS_CONFIGS_PATH/$col_xml_backup
                    rm -rf $CS_CONFIGS_PATH/$cmapi_backup
                    cp $CS_CONFIGS_PATH/Columnstore.xml $CS_CONFIGS_PATH/$col_xml_backup
                    cp $CS_CONFIGS_PATH/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup
                    compress_paths+=" $CS_CONFIGS_PATH/$col_xml_backup $STORAGEMANGER_CNF $CS_CONFIGS_PATH/$cmapi_backup"
                fi;
            fi

            # Backup MariaDB configurations
            if ! $skip_mdb; then
                if [[ ! -n "$compress_format" ]]; then
                    printf " - Copying MariaDB Configs...        "
                    mkdir -p $backup_location$today/configs/mysql/$pm/
                    eval "rsync $additional_rysnc_flags $MARIADB_SERVER_CONFIGS_PATH/* $backup_location$today/configs/mysql/$pm/ $xtra_cmd_args"
                    printf " Done\n\n"
                else
                    compress_paths+=" $MARIADB_SERVER_CONFIGS_PATH/*"
                fi
            fi
            
            # Handle compression for Columnstore Data & Configs
            if [[ -n "$compress_format" ]]; then
                cs_prefix="$backup_location$today/$split_file_cs_prefix.$compress_format"
                compressed_split_size="250M";
                case $compress_format in
                    pigz) 
                        printf " - Compressing CS Data & Configs...  "
                        if ! eval "tar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $cs_prefix 2>> $logfile"; then
                            handle_early_exit_on_backup "[!] - Compression Failed \ntar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $cs_prefix 2>> $logfile \n"
                        fi
                        printf " Done @ $cs_prefix\n"
                        ;;
                    *)  # unknown option
                        handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
                esac
            fi

            if $incremental ; then 
                # Log each incremental run
                now=$(date "+%m-%d-%Y %H:%M:%S"); echo "$pm updated on $now" >> $backup_location$today/incrementallyUpdated.txt
                final_message="Incremental Backup Complete"
            else 
                # Create restore job file
                extra_flags=""
                if [[ -n "$compress_format" ]]; then extra_flags+=" -c $compress_format"; fi; 
                if $skip_mdb; then extra_flags+=" --skip-mariadb-backup"; fi; 
                echo "./$0 restore -l $today -bl $backup_location -bd $backup_destination -s $storage --dbroots $DBROOT_COUNT -m $mode $extra_flags --quiet" > $backup_location$today/restore.job
            fi

            final_message+=" @ $backup_location$today"
            
        elif [ $backup_destination == "Remote" ]; then
        
            # Check/Create Directories on remote server
            printf "[+] Checking Remote Directories... "
            i=1
            makeDataDirectories=""
            while [ $i -le $DBROOT_COUNT ]; do
                makeDataDirectories+="mkdir -p $backup_location$today/data$i ;"
                ((i++))
            done
            echo $makeDirectories
            ssh $scp " mkdir -p $backup_location$today/mysql ;
            $makeDataDirectories
            mkdir -p $backup_location$today/configs ; 
            mkdir -p $backup_location$today/configs/mysql; 
            mkdir -p $backup_location$today/configs/mysql/$pm "
            printf " Done\n"
            
            printf "[~] Rsync Remote Columnstore Data... \n"
            i=1
            while [ $i -le $DBROOT_COUNT ]; do
                #if [ $pm == "pm$i" ]; then rsync -a $additional_rysnc_flags /var/lib/columnstore/data$i/* $scp:$backup_location$today/data$i/ ; fi
                if [ $pm == "pm$i" ]; then 
                    find /var/lib/columnstore/data$i/*  -mindepth 1 -maxdepth 2 | xargs -P $PARALLEL_THREADS -I {} rsync $additional_rysnc_flags /var/lib/columnstore/data$i/* $scp:$backup_location$today/data$i/
                fi
                ((i++))
            done
            printf "[+] Done - rsync\n"

            # Backup MariaDB configurations
            if ! $skip_mdb; then
                printf "[~] Backing up MariaDB configurations... \n"
                rsync $additional_rysnc_flags $MARIADB_SERVER_CONFIGS_PATH/* $scp:$backup_location$today/configs/mysql/$pm/
                printf "[+] Done - configurations\n"
            fi

            # Backup MariaDB data
            if [ $pm == "pm1" ]; then
                if ! $skip_mdb; then
                    printf "[~] Backing up mysql... \n"
                    mkdir -p $backup_location$today
                    if mariadb-backup --backup --target-dir=$backup_location$today --user=root ; then 
                        rsync -a $backup_location$today/* $scp:$backup_location$today/mysql
                        rm -rf $backup_location$today
                    else 
                        printf "\n Failed: mariadb-backup --backup --target-dir=$backup_location$today --user=root\n\n"; 
                        handle_early_exit_on_backup 
                    fi
                else 
                    echo "[!] Skipping mariadb-backup"
                fi;
                
                # Backup CS configurations
                rsync $additional_rysnc_flags $CS_CONFIGS_PATH/Columnstore.xml $scp:$backup_location$today/configs/ 
                rsync $additional_rysnc_flags $STORAGEMANGER_CNF $scp:$backup_location$today/configs/
                if [ -f $CS_CONFIGS_PATH/cmapi_server.conf ]; then  rsync $additional_rysnc_flags $CS_CONFIGS_PATH/cmapi_server.conf $scp:$backup_location$today/configs/; fi
            fi
        
        
            if  $incremental ; then 
                now=$(date "+%m-%d-%Y +%H:%M:%S")
                ssh $scp "echo \"$pm updated on $now\" >> $backup_location$today/incrementallyUpdated.txt"
                final_message="Incremental Backup Complete"
            else
                # Create restore job file
                ssh $scp "echo './$0 restore -l $today -bl $backup_location -bd $backup_destination -s $storage -scp $scp --dbroots $DBROOT_COUNT' > $backup_location$today/restore.job"
            fi
            final_message+=" @ $scp:$backup_location$today"
        fi

    elif [ $storage == "S3" ]; then

        printf "S3 Backup\n"
        # Conconsistency check - wait for assigned journal dir to be empty
        trap handle_ctrl_c_backup SIGINT
        i=1
        j_counts=$(find $cs_journal/data$ASSIGNED_DBROOT/* -type f 2>/dev/null | wc -l)
        max_wait=120
        printf " - Checking storagemanager/journal/data$ASSIGNED_DBROOT/*           "
        while [[ $j_counts -gt 0 ]]; do
            if (( $i%10 == 0 )); then printf "\n[!] Not empty yet - found $j_counts files @ $cs_journal/data$ASSIGNED_DBROOT/*\n"; fi;
            sleep 1
            i=$(($i+1))
            j_counts=$(find $cs_journal/data$ASSIGNED_DBROOT/* -type f 2>/dev/null | wc -l)
            printf "."
        done
        printf " Done\n"

        # Backup storagemanager - columnstore s3 metadata, journal, cache
        if [[ ! -n "$compress_format" ]]; then

            printf " - Syncing Columnstore Metadata \n"
            if $HA; then 
                s3sync $STORAGEMANAGER_PATH $backup_bucket/$today/storagemanager " - Done storagemanager/*\n" "\n\n[!!!] sync failed - storagemanager/*\n\n";
            else 
                s3sync $cs_cache/data$ASSIGNED_DBROOT $backup_bucket/$today/storagemanager/cache/data$ASSIGNED_DBROOT "   + cache/data$ASSIGNED_DBROOT\n" "\n\n[!!!] sync failed - cache/data$ASSIGNED_DBROOT\n\n";
                s3sync $cs_metadata/data$ASSIGNED_DBROOT $backup_bucket/$today/storagemanager/metadata/data$ASSIGNED_DBROOT "   + metadata/data$ASSIGNED_DBROOT\n" "\n\n[!!!] sync failed - metadata/data$ASSIGNED_DBROOT\n\n"
                s3sync $cs_journal/data$ASSIGNED_DBROOT $backup_bucket/$today/storagemanager/journal/data$ASSIGNED_DBROOT "   + journal/data$ASSIGNED_DBROOT\n" "\n\n[!!!] sync failed - journal/data$ASSIGNED_DBROOT\n\n"
            fi;
            
        else
            # For compression keep track of dirs & files to include and compress/stream in the end - $compress_paths
            compress_paths="$cs_cache/data$ASSIGNED_DBROOT $cs_metadata/data$ASSIGNED_DBROOT $cs_journal/data$ASSIGNED_DBROOT "
        fi

        # PM1 mostly backups everything else
        if [ $ASSIGNED_DBROOT == "1" ]; then
            
            # Backup CS configurations
            if [[ ! -n "$compress_format" ]]; then
                s3sync $CS_CONFIGS_PATH/ $backup_bucket/$today/configs/ "   + $CS_CONFIGS_PATH/\n" "\n\n[!!!] sync failed - $CS_CONFIGS_PATH/\n\n";
            else
                compress_paths+="$CS_CONFIGS_PATH "
            fi

            # Backup CS bucket data
            if ! $skip_bucket_data; then
                printf " - Saving Columnstore data ...                       "
                s3sync $protocol://$bucket $backup_bucket/$today/columnstoreData " Done \n"
            else 
                printf " - [!] Skipping columnstore bucket data \n"
            fi

            # Backup MariaDB Server Data
            if ! $skip_mdb; then
                # Handle compressed mariadb backup
                if [[ -n "$compress_format" ]]; then
                    printf " - Compressing MariaDB data local -> bucket...       "
                    mbd_prefix="$backup_bucket/$today/$split_file_mdb_prefix.$compress_format"
                    case $compress_format in
                        pigz)
                            # Handle Cloud 
                            if [ $cloud == "gcp" ]; then
                                if ! mariabackup --user=root --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter="gsutil cp - ${mbd_prefix}_\$FILE 2>$logfile" - chunk 2>$logfile; then
                                    handle_early_exit_on_backup "\nFailed mariadb-backup --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter=\"gsutil cp - ${mbd_prefix}_\$FILE 2>$logfile\" - chunk 2>$logfile \n"
                                fi
                                
                            elif [ $cloud == "aws" ]; then
                                if ! mariabackup --user=root --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter="aws s3 cp - ${mbd_prefix}_\$FILE 2>$logfile 1>&2" - chunk 2>$logfile; then
                                    handle_early_exit_on_backup "\nFailed mariadb-backup --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter=\"aws s3 cp - ${mbd_prefix}_\$FILE 2>$logfile 1>&2\" - chunk 2>$logfile\n"
                                fi
                            fi
                            printf " Done @ $mbd_prefix\n"
                            ;;
                        *)  # unknown option
                            handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
                    esac

                else
                    # Handle uncompressed mariadb backup
                    printf " - Syncing MariaDB data ... \n"
                    rm -rf $backup_location$today/mysql
                    if mkdir -p $backup_location$today/mysql ; then
                        if eval "mariabackup --user=root --backup --target-dir=$backup_location$today/mysql/ $xtra_cmd_args"; then
                            printf "   + mariadb-backup @ $backup_location$today/mysql/ \n"
                        else
                            handle_early_exit_on_backup "\nFailed mariadb-backup --backup --target-dir=$backup_location$today/mysql --user=root\n" true
                        fi

                        s3rm $backup_bucket/$today/mysql/
                        s3sync $backup_location$today/mysql $backup_bucket/$today/mysql/ "   + mariadb-backup to bucket @ $backup_bucket/$today/mysql/ \n"
                        rm -rf $backup_location$today/mysql
                    else 
                        handle_early_exit_on_backup "\nFailed making directory for MariaDB backup\ncommand: mkdir -p $backup_location$today/mysql\n"
                    fi
                fi
            else 
                echo "[!] Skipping mariadb-backup"
            fi
        fi

        # Backup mariadb server configs
        if ! $skip_mdb; then
            if [[ ! -n "$compress_format" ]]; then
                printf " - Syncing MariaDB configurations \n"
                s3sync $MARIADB_SERVER_CONFIGS_PATH/ $backup_bucket/$today/configs/mysql/$pm/ "   + $MARIADB_SERVER_CONFIGS_PATH/\n" "\n\n[!!!] sync failed - try alone and debug\n\n"
            else
                compress_paths+="$MARIADB_SERVER_CONFIGS_PATH"
            fi
        fi

        # Handles streaming the compressed columnstore local files
        if [[ -n "$compress_format" ]]; then
            cs_prefix="$backup_bucket/$today/$split_file_cs_prefix.$compress_format"
            compressed_split_size="250M";
            case $compress_format in
                pigz) 
                    printf " - Compressing CS local files -> bucket ...          "
                    if [ $cloud == "gcp" ]; then
                        if ! eval "tar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b $compressed_split_size --filter=\"gsutil cp - ${cs_prefix}_\\\$FILE 2>$logfile 1>&2\" - chunk 2>$logfile"; then
                            handle_early_exit_on_backup "[!] - Compression/Split/Upload Failed \n"
                        fi
                        
                    elif [ $cloud == "aws" ]; then
                        if ! eval "tar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b $compressed_split_size --filter=\"aws s3 cp - ${cs_prefix}_\\\$FILE 2>$logfile 1>&2\" - chunk 2>$logfile"; then
                            handle_early_exit_on_backup "[!] - Compression/Split/Upload Failed \n"
                        fi
                    fi
                    printf " Done @ $cs_prefix\n"
                    ;;
                *)  # unknown option
                    handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
            esac
        fi

        # Create restore job file & update incremental log
        if  $incremental ; then 
            now=$(date "+%m-%d-%Y +%H:%M:%S")
            increment_file="incremental_history.txt"
            
            # download S3 file, append to it and reupload
            if [[ $(s3ls "$backup_bucket/$today/$increment_file")  ]]; then
                s3cp $backup_bucket/$today/$increment_file $increment_file
            fi
            echo "$pm updated on $now" >> $increment_file
            s3cp $increment_file $backup_bucket/$today/$increment_file
            echo "[+] Updated $backup_bucket/$today/$increment_file"
            rm -rf $increment_file
            final_message="Incremental Backup Complete"
        else
            # Create restore job file
            extra_flags=""
            if [[ -n "$compress_format" ]]; then extra_flags+=" -c $compress_format"; fi; 
            if $skip_mdb; then extra_flags+=" --skip-mariadb-backup"; fi; 
            if $skip_bucket_data; then extra_flags+=" --skip-bucket-data"; fi; 
            if [ ! -z "$s3_url" ];  then extra_flags+=" -url $s3_url"; fi;
            echo "./$0 restore -l $today -s $storage -bb $backup_bucket -dbs $DBROOT_COUNT -m $mode -nb $protocol://$bucket $extra_flags --quiet --continue"  > restoreS3.job
            s3cp restoreS3.job $backup_bucket/$today/restoreS3.job
            rm -rf restoreS3.job
        fi
        final_message+=" @ $backup_bucket/$today"
    
    fi
    
    clear_read_lock
    end=`date +%s`
    runtime=$((end-start))
    printf "\n[+] Runtime: $runtime\n"
    printf "[+] $final_message\n\n"
}

load_default_restore_variables()
{
    # What date folder to load from the backup_location
    load_date=''

    # Where the backup to load is found
    # Example: /mnt/backups/
    backup_location="/tmp/backups/"

    # Is this backup on the same or remote server compared to where this script is running
    # Options: "Local" or "Remote"
    backup_destination="Local"

    # Used only if backup_destination=Remote
    # The user/credentials that will be used to scp the backup files
    # Example: "centos@10.14.51.62"
    scp=""

    # What storage topogoly was used by Columnstore in the backup - found in storagemanager.cnf
    # Options: "LocalStorage" or "S3" 
    storage="LocalStorage"

    # Flag for high available systems (meaning shared storage exists supporting the topology so that each node sees all data)
    HA=false

    # When set to true skips the enforcement that new_bucket should be empty prior to starting a restore
    continue=false 

    # modes ['direct','indirect'] - direct backups run on the columnstore nodes themselves. indirect run on another machine that has read-only mounts associated with columnstore/mariadb
    mode="direct"

    # Name of the Configuration file to load variables from 
    config_file=".cs-backup-config"

    # Only used if storage=S3
    # Name of the bucket to copy into to store the backup S3 data
    # Example: "backup-mcs-bucket"
    backup_bucket=''

    # Fixed Paths
    MARIADB_PATH="/var/lib/mysql"
    CS_CONFIGS_PATH="/etc/columnstore"
    DBRM_PATH="/var/lib/columnstore/data1/systemFiles/dbrm"
    STORAGEMANAGER_PATH="/var/lib/columnstore/storagemanager"
    STORAGEMANGER_CNF="$CS_CONFIGS_PATH/storagemanager.cnf"

    # Configurable Paths
    cs_metadata=$(grep ^metadata_path $STORAGEMANGER_CNF | cut -d "=" -f 2 | tr -d " ")
    cs_journal=$(grep ^journal_path $STORAGEMANGER_CNF  | cut -d "=" -f 2 | tr -d " ")
    cs_cache=$(grep -A25 "\[Cache\]" $STORAGEMANGER_CNF | grep ^path | cut -d "=" -f 2 | tr -d " ")

    if [ ! -f /var/lib/columnstore/local/module ]; then  pm="pm1"; else pm=$(cat /var/lib/columnstore/local/module);  fi;
    pm_number=$(echo "$pm" | tr -dc '0-9')

    # Defines the set of new bucket information to copy the backup to when restoring
    new_bucket=''
    new_region=''
    new_key=''
    new_secret=''

    # Used by on premise S3 vendors
    # Example: "http://127.0.0.1:8000"
    s3_url=""
    no_verify_ssl=false


    # Number of DBroots
    # Integer usually 1 or 3
    DBROOT_COUNT=1

    # Google Cloud
    # PARALLEL_UPLOAD=50
    RETRY_LIMIT=3

    # Compression Variables
    compress_format=""
    split_file_mdb_prefix="mariadb-backup.tar"
    split_file_cs_prefix="cs-localfiles.tar"
    timeout=999999
    logfile="backup.log"
    col_xml_backup="Columnstore.xml.source_backup"
    cmapi_backup="cmapi_server.conf.source_backup"
    PARALLEL_THREADS=4

    # Other
    final_message="Restore Complete"
    mariadb_user="mysql"
    columnstore_user="mysql"
    skip_mdb=false
    skip_bucket_data=false
    quiet=false
    xtra_s3_args=""
    xtra_cmd_args=""
    rsync_flags=" -av";
}

parse_restore_variables()
{
    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"

        case $key in
            restore)
                shift # past argument
                ;;
            -l|--load)
                load_date="$2"
                shift # past argument
                shift # past value
                ;;
            -bl|--backup_location)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -bd|--backup_destination)
                backup_destination="$2"
                shift # past argument
                shift # past value
                ;;
            -scp)
                scp="$2"
                shift # past argument
                shift # past value
                ;;
            -bb|--backup_bucket)
                backup_bucket="$2"
                shift # past argument
                shift # past value
                ;;
            -url| --endpoint-url)
                s3_url="$2"
                shift # past argument
                shift # past value
                ;;
            -s|--storage)
                storage="$2"
                shift # past argument
                shift # past value
                ;;
            -dbs|--dbroots)
                DBROOT_COUNT="$2"
                shift # past argument
                shift # past value
                ;;
            -pm | --nodeid)
                pm="$2"
                pm_number=$(echo "$pm" | tr -dc '0-9')
                shift # past argument
                shift # past value
                ;;
            -nb | --new_bucket)
                new_bucket="$2"
                shift # past argument
                shift # past value
                ;;
            -nr | --new_region)
                new_region="$2"
                shift # past argument
                shift # past value
                ;;
            -nk | --new_key)
                new_key="$2"
                shift # past argument
                shift # past value
                ;;
            -ns | --new_secret)
                new_secret="$2"
                shift # past argument
                shift # past value
                ;;
            -P|--parallel)
                PARALLEL_THREADS="$2"
                shift # past argument
                shift # past value
                ;;
            -ha | --highavilability)
                HA=true
                shift # past argument
                ;;
            --continue)
                continue=true
                shift # past argument
                ;;
            -f|--config-file)
                config_file="$2"
                shift # past argument
                shift # past value
                ;;
            -smdb | --skip-mariadb-backup)
                skip_mdb=true
                shift # past argument
                ;;
            -sb   | --skip-bucket-data)
                skip_bucket_data=true
                shift # past argument
                ;;
            -m  | --mode)
                mode="$2"
                shift # past argument
                shift # past value
                ;;
            -c    | --compress)
                compress_format="$2"
                shift # past argument
                shift # past value
                ;;
            -q    | --quiet)
                quiet=true
                shift # past argument
                ;;
            --no-verify-ssl)
                no_verify_ssl=true 
                shift # past argument
                ;;
            -h|--help|-help|help)
                print_restore_help_text;
                exit 1;
                ;;
            *)  # unknown option
                handle_early_exit_on_restore "\nunknown flag: $1\n"
        esac
    done

    # Load config file
    if [ -f $config_file ]; then
        source $config_file
    fi
}

print_restore_help_text()
{
    echo "
    Columnstore Restore

        -l   | --load                   What backup to load
        -bl  | --backup_location        Directory where the backup was saved
        -bd  | --backup_destination     If the directory is 'Local' or 'Remote' to this script
        -dbs | --dbroots                Number of database roots in the backup
        -scp                            scp connection to remote server if -bd 'Remote'
        -bb  | --backup_bucket          bucket name for where to find the S3 backups
        -url | --endpoint-url           Onprem url to s3 storage api example: http://127.0.0.1:8000
        --no-verify-ssl                 skips verifying ssl certs, useful for onpremise s3 storage
        -s   | --storage                The storage used by columnstore data 'LocalStorage' or 'S3'
        -pm  | --nodeid                 Forces the handling of the restore as this node as opposed to whats detected on disk
        -nb  | --new_bucket             Defines the new bucket to copy the s3 data to from the backup bucket. 
                                        Use -nb if the new restored cluster should use a different bucket than the backup bucket itself
        -nr  | --new_region             Defines the region of the new bucket to copy the s3 data to from the backup bucket   
        -nk  | --new_key                Defines the aws key to connect to the new_bucket  
        -ns  | --new_secret             Defines the aws secret of the aws key to connect to the new_bucket  
        -f   | --config-file            path to backup configuration file to load variables from
        --continue                      this acknowledges data in your --new_bucket is ok to delete when restoring S3
        -smdb| --skip-mariadb-backup    skip restoring mariadb server via mariadb-backup - ideal for only restoring columnstore
        -sb  | --skip-bucket-data       skip restoring columnstore data in the bucket - ideal if looking to only restore mariadb server
        -q   | --quiet                  silence verbose copy command outputs
        -c   | --compress               hint that the backup is compressed in X format - Options: [ pigz ]
        -ha  | --highavilability        Hint for if shared storage is attached @ below on all nodes to see all data
                                            HA LocalStorage ( /var/lib/columnstore/dataX/ )
                                            HA S3           ( /var/lib/columnstore/storagemanager/ )  

        Local Storage Examples:
            ./$0 restore -s LocalStorage -bl /tmp/backups/ -bd Local -l 12-29-2021
            ./$0 restore -s LocalStorage -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -l 12-29-2021
        
        S3 Storage Examples:
            ./$0 restore -s S3 -bb s3://my-cs-backups  -l 12-29-2021
            ./$0 restore -s S3 -bb gs://on-premise-bucket -l 12-29-2021 -url http://127.0.0.1:8000
            ./$0 restore -s S3 -bb s3://my-cs-backups  -l 08-16-2022 -nb s3://new-data-bucket -nr us-east-1 -nk AKIAxxxxxxx3FHCADF -ns GGGuxxxxxxxxxxnqa72csk5 -ha
    ";
}

print_restore_variables()
{

    printf "\nRestore Variables\n"
    if [ -f $config_file ]; then
        printf "%-${s1}s  %-${s2}s\n" "Configuration File:" "$config_file";
        source $config_file
    fi
    if [ $mode == "indirect" ] || $skip_mdb || $skip_bucket_data; then 
        echo "------------------------------------------------"
        echo "Skips: MariaDB($skip_mdb) Bucket($skip_bucket_data)";
    fi;
    echo "------------------------------------------------"
    if [[ -n "$compress_format" ]]; then
        echo "Compression:        true"
        echo "Compression Format: $compress_format";
    else 
        echo "Compression:        false"
    fi
    if [ $storage == "LocalStorage" ]; then 
        echo "Backup Location:    $backup_location"
        echo "Backup Destination: $backup_destination"
        echo "Scp:                $scp"
        echo "Storage:            $storage"
        echo "Load Date:          $load_date"
        echo "timestamp:          $(date +%m-%d-%Y-%H%M%S)"
        echo "DB Root Count:      $DBROOT_COUNT"
        echo "PM:                 $pm"
        echo "PM Number:          $pm_number"

    elif [ $storage == "S3" ]; then
        echo "Backup Location:    $backup_location"  
        echo "Storage:            $storage"
        echo "Load Date:          $load_date"
        echo "timestamp:          $(date +%m-%d-%Y-%H%M%S)"
        echo "PM:                 $pm"
        echo "PM Number:          $pm_number"
        echo "Active bucket:      $( grep -m 1 "^bucket =" $STORAGEMANGER_CNF)"
        echo "Active endpoint:    $( grep -m 1 "^endpoint =" $STORAGEMANGER_CNF)"
        echo "Backup Bucket:      $backup_bucket"
        echo "------------------------------------------------"
        echo "New Bucket:         $new_bucket"
        echo "New Region:         $new_region"
        echo "New Key:            $new_key"
        echo "New Secret:         $new_secret"
        echo "High Availabilty:   $HA"
    fi
    echo "------------------------------------------------"

}

check_package_managers() {
    
    package_manager='';
    if command -v apt &> /dev/null ; then
        if ! command -v dpkg-query &> /dev/null ; then
            printf "[!!] Cant access dpkg-query\n"
            exit 1;
        fi
        package_manager="apt";
    fi

    if command -v yum &> /dev/null ; then
        package_manager="yum";
    fi    

    if [ $package_manager == '' ]; then 
        echo "[!!] No package manager found: yum or apt must be installed"
        exit 1;
    fi;
}

validation_prechecks_for_restore() {

    echo "Prechecks ..."
    if [ $storage != "LocalStorage" ] && [ $storage != "S3" ]; then handle_early_exit_on_restore "Invalid script variable storage: $storage\n"; fi   
    if [ -z "$load_date" ]; then handle_early_exit_on_restore "\n[!!!] Required field --load: $load_date - is empty\n" ; fi
    if [ -z "$mode" ]; then handle_early_exit_on_restore "\n[!!!] Required field --mode: $mode - is empty\n" ; fi
    if [ "$mode" != "direct" ] && [ "$mode" != "indirect" ] ; then handle_early_exit_on_restore "\n[!!!] Invalid field --mode: $mode\n"; fi
    mariadb_user=$(stat -c "%U" /var/lib/mysql/)
    columnstore_user=$(stat -c "%U" /var/lib/columnstore/)

    # Adjust s3api flags for onpremise/custom endpoints
    add_s3_api_flags=""
    if [ ! -z "$s3_url" ];  then add_s3_api_flags+=" --endpoint-url $s3_url"; fi
    if $no_verify_ssl; then add_s3_api_flags+=" --no-verify-ssl"; fi;

    # If remote backup - Validate that scp works 
    if [ $backup_destination == "Remote" ]; then
        if ssh $scp echo ok 2>&1 ;then
            printf 'SSH Works\n\n'
        else
            handle_early_exit_on_restore "Failed Command: ssh $scp"
        fi
    fi

    # Make sure the database is offline
    if [ "$mode" == "direct" ]; then 
        if [ -z $(pidof PrimProc) ]; then printf " - Columnstore Status ...               Offline\n"; else handle_early_exit_on_restore "\n[X] Columnstore is ONLINE - please turn off \n\n";  fi
        if [ -z $(pidof mariadbd) ]; then printf " - MariaDB Server Status ...            Offline\n"; else handle_early_exit_on_restore "\n[X] MariaDB is ONLINE - please turn off \n\n"; fi
    fi

    # If cmapi installed - make cmapi turned off
    check_package_managers
    cmapi_installed_command=""
    case $package_manager in
        yum ) 
            cmapi_installed_command="yum list installed MariaDB-columnstore-cmapi &> /dev/null;";
            ;;
        apt )
            cmapi_installed_command="dpkg-query -s mariadb-columnstore-cmapi &> /dev/null;";
            ;;
        *)  # unknown option
            echo "\npackage manager not implemented: $package_manager\n"
            exit 2;
    esac
    if eval $cmapi_installed_command ; then
        if [ -z $(pidof /usr/share/columnstore/cmapi/python/bin/python3) ]; then printf " - Columnstore Management API Status .. Offline\n"; else handle_early_exit_on_restore "\n[X] Cmapi is ONLINE - please turn off \n\n";  fi
    else
        printf " - Columnstore Management API Status .. Not Installed - Skipping\n";  
    fi

    # Validate addtional relevant arguments per storage option
    if [ $storage == "LocalStorage" ]; then 
        
        if [ -z "$backup_location" ]; then handle_early_exit_on_restore "Invalid --backup_location: $backup_location - is empty";  fi
        if [ -z "$backup_destination" ]; then handle_early_exit_on_restore "Invalid --backup_destination: $backup_destination - is empty"; fi
        if [ $backup_destination == "Remote" ] && [ -d $backup_location$load_date ]; then echo "Switching to '-bd Local'"; backup_destination="Local"; fi
        if [ $backup_destination == "Local" ]; then
            
            if [ ! -d $backup_location ]; then handle_early_exit_on_restore "Invalid directory --backup_location: $backup_location - doesnt exist"; fi
            if [ -z "$(ls -A "$backup_location")" ]; then echo "Invalid --backup_location: $backup_location - directory is empty."; fi;
            if [ ! -d $backup_location$load_date ]; then handle_early_exit_on_restore "Invalid directory --load: $backup_location$load_date - doesnt exist"; else printf " - Backup directory exists\n"; fi
        fi;

        local files=$(ls $backup_location$load_date)
        if [[ -n "$compress_format" ]]; then   
            case "$compress_format" in
                pigz) 
                    flag_cs_local=false 
                    flag_mysql=$skip_mdb

                    while read -r line; do
                        folder_name=$(echo "$line" | awk '{ print $NF }')
                        case "$folder_name" in
                            *mariadb-backup.tar.pigz*) flag_mysql=true ;;
                            *cs-localfiles.tar.pigz*) flag_cs_local=true ;;
                        esac
                    done <<< "$files"

                    if $flag_mysql && $flag_cs_local ; then
                        echo " - Backup subdirectories exist"
                    else
                        echo "[X] Backup is missing subdirectories: "
                        if ! $flag_cs_local; then echo "cs-localfiles.tar.pigz"; fi;
                        if ! $flag_mysql; then echo "mariadb-backup.tar.pigz"; fi;
                        handle_early_exit_on_restore "[!] Backup is missing subdirectories... \nls $backup_location$load_date\n\n"
                    fi
                ;;
                *)  # unknown option
                    handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
            esac
        else 
            
            flag_configs=false
            flag_data=false
            flag_mysql=$skip_mdb
            files=$(ls $backup_location$load_date )
            while read -r line; do
                folder_name=$(echo "$line" | awk '{ print $NF }')
           
                case "$folder_name" in
                    *configs*) flag_configs=true ;;
                    *data*) flag_data=true ;;
                    *mysql*) flag_mysql=true ;;
                esac
            done <<< "$files"

            if $flag_configs && $flag_data && $flag_mysql; then
                echo " - Backup subdirectories exist"
            else
                echo "[X] Backup is missing subdirectories: "
                if ! $flag_configs; then echo "configs"; fi;
                if ! $flag_data; then echo "data"; fi;
                if ! $flag_mysql; then echo "mysql"; fi;
                handle_early_exit_on_restore " Backup is missing subdirectories... \nls $backup_location$load_date\n"
            fi
        fi
    
    elif [ $storage == "S3" ]; then 
        
        # Check for concurrency settings - https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
        if [ $cloud == "aws" ]; then cli_concurrency=$(sudo grep max_concurrent_requests ~/.aws/config 2>/dev/null ); if [ -z "$cli_concurrency" ]; then echo "[!!!] check: '~/.aws/config' - We recommend increasing s3 concurrency for better throughput to/from S3. This value should scale with avaialble CPU and networking capacity"; printf "example: aws configure set default.s3.max_concurrent_requests 200\n";  fi; fi;

        # Validate addtional relevant arguments
        if [ -z "$backup_bucket" ]; then handle_early_exit_on_restore "Invalid --backup_bucket: $backup_bucket - is empty";  fi
        
        # Prepare commands for each cloud
        if [ $cloud == "gcp" ]; then
            check1="$gsutil ls $backup_bucket";
            check2="$gsutil ls -b $new_bucket";
            check3="$gsutil ls $new_bucket";
            check4="$gsutil ls $backup_bucket/$load_date";
            check5="$gsutil ls $backup_bucket/$load_date/";
        else 
            check1="$awscli $add_s3_api_flags s3 ls $backup_bucket"
            check2="$awscli $add_s3_api_flags s3 ls | grep $new_bucket"
            check3="$awscli $add_s3_api_flags s3 ls $new_bucket"
            check4="$awscli $add_s3_api_flags s3 ls $backup_bucket/$load_date"
            check5="$awscli $add_s3_api_flags s3 ls $backup_bucket/$load_date/"
        fi;

        # Check aws cli access to bucket
        if $check1 > /dev/null ; then
            echo -e " - Success listing backup bucket"
        else 
            handle_early_exit_on_restore "[X] Failed to list bucket contents...\n$check1\n"
        fi

        # New bucket exists and empty check
        if [ "$new_bucket" ] && [ $pm == "pm1" ]; then 
            # Removing as storage.buckets.get permission likely not needed
            # new_bucket_exists=$($check2); if [ -z "$new_bucket_exists" ]; then handle_early_exit_on_restore "[!!!] Didnt find new bucket - Check:  $check2\n"; fi; echo "[+] New Bucket exists"; 
            # Throw warning if new bucket is NOT empty
            if ! $continue; then nb_contents=$($check3 | wc -l); if [ $nb_contents -lt 1 ]; then echo " - New Bucket is empty"; else echo "[!!!] New bucket is NOT empty... $nb_contents files exist... exiting"; echo "add "--continue" to skip this exit"; echo -e "\nExample empty bucket command:\n     aws s3 rm $new_bucket --recursive"; echo -e "     gsutil -m rm -r $new_bucket/* \n"; handle_early_exit_on_restore "Please empty bucket or add --continue \n"; fi; else echo " - [!] Skipping empty new_bucket check"; fi; 
        fi

        # Check if s3 bucket load date exists
        if $check4 > /dev/null ; then
            echo -e " - Backup directory exists"
        else 
            handle_early_exit_on_restore "\n[X] Backup directory load date ($backup_bucket/$load_date) DOES NOT exist in S3 \nCheck - $check4 \n\n";
        fi

        # Check if s3 bucket load date subdirectories exist
       if folders=$($check5) > /dev/null ; then
            if [[ -n "$compress_format" ]]; then
                # when compressed, storagemanager/configs wont exist but rather pigz_chunkXXX does. and mysql could be split up
                case "$compress_format" in
                    pigz) 
                        flag_cs_local=false 
                        flag_mysql=$skip_mdb
                        flag_columnstoreData=$skip_bucket_data

                        while read -r line; do
                            folder_name=$(echo "$line" | awk '{ print $NF }')
                            case "$folder_name" in
                                *columnstoreData*) flag_columnstoreData=true ;;
                                *mariadb-backup.tar.pigz*) flag_mysql=true ;;
                                *cs-localfiles.tar.pigz*) flag_cs_local=true ;;
                            esac
                        done <<< "$folders"

                        if $flag_pigz_chunk && $flag_mysql && $flag_columnstoreData ; then
                            echo " - Backup subdirectories exist"
                        else
                            echo "[X] Backup is missing subdirectories: "
                            if ! $flag_cs_local; then echo "cs-localfiles.tar.pigzXXX"; fi;
                            if ! $flag_mysql; then echo "mariadb-backup.tar.pigzXXXX"; fi;
                            if ! $flag_columnstoreData; then echo "columnstoreData"; fi;
                            handle_early_exit_on_restore " Backup is missing subdirectories... \n$check5\n"
                        fi
                    ;;
                    *)  # unknown option
                        handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
                esac
            else
                flag_configs=false
                flag_storagemanager=false
                flag_mysql=$skip_mdb
                flag_columnstoreData=$skip_bucket_data

                while read -r line; do
                    folder_name=$(echo "$line" | awk '{ print $NF }')
                    #echo "DEBUG: $folder_name"
                    case "$folder_name" in
                        *storagemanager*) flag_storagemanager=true ;;
                        *mysql*) flag_mysql=true ;;
                        *columnstoreData*) flag_columnstoreData=true ;;
                        *configs*) flag_configs=true ;;
                        #*) echo "unknown: $folder_name" ;;
                    esac
                done <<< "$folders" 

                if $flag_storagemanager && $flag_mysql && $flag_columnstoreData && $flag_configs; then
                    echo " - Backup subdirectories exist"
                else
                    echo "[X] Backup is missing subdirectories: "
                    if ! $flag_storagemanager; then echo "storagemanager"; fi;
                    if ! $flag_mysql; then echo "mysql"; fi;
                    if ! $flag_columnstoreData; then echo "columnstoreData"; fi;
                    if ! $flag_configs; then echo "configs"; fi;
                    handle_early_exit_on_restore " Backup is missing subdirectories... \n$check5\n"
                fi
            fi;
        else 
             handle_early_exit_on_restore " Failed to list backup contents...\n$check5\n"
        fi;
    fi;

    if $quiet; then xtra_cmd_args+="  2> /dev/null"; rsync_flags=" -a"; fi
}

# Restore Columnstore.xml by updating critical parameters 
# Depends on $CS_CONFIGS_PATH/$col_xml_backup
restore_columnstore_values() {
    
    printf "\nRestore Columnstore.xml Values"
    if ! $quiet; then printf "\n"; else printf "...      "; fi;
    columnstore_config_pairs_to_transfer=(
        "HashJoin TotalUmMemory"
        "DBBC NumBlocksPct"
        "CrossEngineSupport User"
        "CrossEngineSupport Password"
        "HashJoin AllowDiskBasedJoin"
        "RowAggregation AllowDiskBasedAggregation"
        "SystemConfig MemoryCheckPercent"
        "JobList RequestSize"
        "JobList ProcessorThreadsPerScan"
        "DBBC NumThreads"
        "PrimitiveServers ConnectionsPerPrimProc"
    )

    # mcsGetConfig -c Columnstore.xml.source_backup  HashJoin TotalUmMemory
    for pair in "${columnstore_config_pairs_to_transfer[@]}"; do
        level_one=$(echo "$pair" | cut -d ' ' -f 1)
        level_two=$(echo "$pair" | cut -d ' ' -f 2)
        
        # Get the source value using mcsGetConfig -c
        source_value=$(mcsGetConfig -c "$CS_CONFIGS_PATH/$col_xml_backup" "$level_one" "$level_two")
        
        if [ -n "$source_value" ]; then
            # Set the value in the active Columnstore.xml using mcsSetConfig
            if mcsSetConfig "$level_one" "$level_two" "$source_value"; then
                # echo instead of printf to avoid escaping % from HashJoin TotalUmMemory
                if ! $quiet; then echo " - Set $level_one $level_two $source_value"; fi; 
            else
                printf "\n[!] Failed to Set $level_one $level_two $source_value \n";
            fi
        else
            if ! $quiet; then printf " - N/A: $level_one $level_two \n"; fi; 
        fi
    done
    if $quiet; then printf " Done\n"; fi;
}

run_restore()
{
    # Branch logic based on topology
    if [ $storage == "LocalStorage" ]; then 

        # Branch logic based on where the backup resides
        if [ $backup_destination == "Local" ]; then
            
            # MariaDB Columnstore Restore
            printf "\nRestore MariaDB Columnstore LocalStorage\n"

            # Handle Compressed Backup Restore
            if [[ -n "$compress_format" ]]; then

                # Clear appropriate dbroot
                i=1;
                while [ $i -le $DBROOT_COUNT ]; do
                    if [[ $pm == "pm$i" || $HA == true ]]; then
                        printf " - Deleting Columnstore Data$i ...      "; 
                        rm -rf /var/lib/columnstore/data$i/*; 
                        printf " Done \n"; 
                    fi
                    ((i++))
                done    

                tar_flags="xf"
                if ! $quiet; then tar_flags+="v"; fi;
                cs_prefix="$backup_location$load_date/$split_file_cs_prefix.$compress_format"
                case $compress_format in
                    pigz) 
                        printf " - Decompressing CS Files -> local ... "
                        cd /
                        if ! eval "pigz -dc -p $PARALLEL_THREADS $cs_prefix | tar $tar_flags - "; then
                            handle_early_exit_on_restore "Failed to decompress and untar columnstore localfiles\ncommand:\npigz -dc -p $PARALLEL_THREADS $cs_prefix | tar xf - \n\n"
                        fi
                        printf " Done \n"
                        ;;
                    *)  # unknown option
                        handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
                esac

                # Set permissions after restoring files
                i=1;
                while [ $i -le $DBROOT_COUNT ]; do
                    if [[ $pm == "pm$i" || $HA == true ]]; then
                        printf " - Chowning Columnstore Data$i ...      "; 
                        chown $columnstore_user:$columnstore_user -R /var/lib/columnstore/data$i/; 
                        printf " Done \n"; 
                    fi
                    ((i++))
                done

            else
                # Handle Non-Compressed Backup Restore
                # Loop through per dbroot
                i=1;
                while [ $i -le $DBROOT_COUNT ]; do
                    if [[ $pm == "pm$i" || $HA == true ]]; then
                        printf " - Restoring Columnstore Data$i ...     ";
                        rm -rf /var/lib/columnstore/data$i/*; 
                        rsync $rsync_flags $backup_location$load_date/data$i/ /var/lib/columnstore/data$i/;
                        chown $columnstore_user:$columnstore_user -R /var/lib/columnstore/data$i/; 
                        printf " Done Data$i \n"; 
                    fi
                    ((i++))
                done    
                
                # Put configs in place  
                printf " - Columnstore Configs ...             "
                rsync $rsync_flags $backup_location$load_date/configs/storagemanager.cnf $STORAGEMANGER_CNF
                rsync $rsync_flags $backup_location$load_date/configs/Columnstore.xml $CS_CONFIGS_PATH/$col_xml_backup
                rsync $rsync_flags $backup_location$load_date/configs/mysql/$pm/ $MARIADB_SERVER_CONFIGS_PATH/
                if [ -f "$backup_location$load_date/configs/cmapi_server.conf" ]; then rsync $rsync_flags $backup_location$load_date/configs/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup ; fi;
                printf " Done\n" 
            fi
        
        elif [ $backup_destination == "Remote" ]; then
            printf "[~] Copy MySQL Data..."
            tmp="localscpcopy-$load_date"
            rm -rf $backup_location$tmp/mysql/
            mkdir -p $backup_location$tmp/mysql/
            rsync $rsync_flags $scp:$backup_location/$load_date/mysql/ $backup_location$tmp/mysql/
            printf "[+] Copy MySQL Data... Done\n"

            # Loop through per dbroot
            printf "[~] Columnstore Data..."; i=1;
            while [ $i -le $DBROOT_COUNT ]; do
                if [[ $pm == "pm$i" || $HA == true ]]; then rm -rf /var/lib/columnstore/data$i/; rsync -av $scp:$backup_location$load_date/data$i/ /var/lib/columnstore/data$i/ ; chown $columnstore_user:$columnstore_user -R /var/lib/columnstore/data$i/;fi
                ((i++))
            done
            printf "[+] Columnstore Data... Done\n" 

            # Put configs in place
            printf " - Columnstore Configs ...             "
            rsync $rsync_flags $backup_location$load_date/configs/storagemanager.cnf $STORAGEMANGER_CNF
            rsync $rsync_flags $backup_location$load_date/configs/Columnstore.xml $CS_CONFIGS_PATH/$col_xml_backup  
            rsync $rsync_flags $backup_location$load_date/configs/mysql/$pm/ $MARIADB_SERVER_CONFIGS_PATH/ 
            if [ -f "$backup_location$load_date/configs/cmapi_server.conf" ]; then rsync $rsync_flags $backup_location$load_date/configs/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup ; fi;
            printf " Done\n"
            load_date=$tmp
        else 
            handle_early_exit_on_restore "Invalid Script Variable --backup_destination: $backup_destination"
        fi

        # MariaDB Server Restore 
        printf "\nRestore MariaDB Server\n"
        if [[ -n "$compress_format" ]]; then
            # Handle compressed mariadb-backup restore
            mbd_prefix="$backup_location$load_date/$split_file_mdb_prefix.$compress_format"
            case $compress_format in
                pigz) 
                    printf " - Decompressing MariaDB Files -> $MARIADB_PATH ... "
                    rm -rf $MARIADB_PATH/*
                    cd /
                    if ! eval "pigz -dc -p $PARALLEL_THREADS $mbd_prefix |  mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH"; then
                        handle_early_exit_on_restore "Failed to decompress mariadb backup\ncommand:\npigz -dc -p $PARALLEL_THREADS $mbd_prefix |  mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH \n"
                    fi;
                    printf " Done \n"
                    printf " - Running mariabackup --prepare ...   "
                    if eval "mariabackup --prepare --target-dir=$MARIADB_PATH $xtra_cmd_args"; then printf " Done\n"; else handle_early_exit_on_restore "Failed to --prepare\nmariabackup --prepare --target-dir=$MARIADB_PATH"; fi; 
                    ;;
                *)  # unknown option
                    handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
            esac
        else
            eval "mariabackup --prepare --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"
            rm -rf /var/lib/mysql/*
            eval "mariabackup --copy-back --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"
        fi
        printf " - Chowning MariaDB Files ...          ";
        chown -R $mariadb_user:$mariadb_user /var/lib/mysql/
        chown -R $columnstore_user:$mariadb_user /var/log/mariadb/
        printf " Done \n"

    elif [ $storage == "S3" ]; then

        printf "\nRestore MariaDB Columnstore S3\n"

        # Sync the columnstore data from the backup bucket to the new bucket
        if ! $skip_bucket_data && [ "$new_bucket" ] && [ $pm == "pm1" ]; then 
            printf "[+] Starting Bucket Sync:\n - $backup_bucket/$load_date/columnstoreData to $new_bucket\n"
            s3sync $backup_bucket/$load_date/columnstoreData/ $new_bucket/ " - Done with S3 Bucket sync\n"
        else 
            printf "[!] Skipping Columnstore Bucket\n"
        fi;
    
        printf "[+] Starting Columnstore Configurations & Metadata: \n"
        if [[ -n "$compress_format" ]]; then
            cs_prefix="$backup_bucket/$load_date/$split_file_cs_prefix.$compress_format"
            case $compress_format in
                pigz) 
                    printf " - Decompressing CS Files bucket -> local ... "
                    if [ $cloud == "gcp" ]; then
                        # gsutil supports simple "cat prefix*" to standard out unlike awscli
                        cd /
                        if ! eval "$gsutil  cat $cs_prefix* | gzip -dc | tar xf - "; then
                            handle_early_exit_on_restore "Failed to decompress and untar columnstore localfiles\ncommand:\n$gsutil cat $cs_prefix* | gzip -dc | tar xf - \n\n"
                        fi
                    elif [ $cloud == "aws" ]; then
                        
                        # List all the pigz compressed chunks in the S3 prefix - should I clear prior local files?
                        chunk_list=$($awscli s3 ls "$cs_prefix" | awk '{print $NF}')
                        cd /
                    
                        # Use process substitution to concatenate the compressed chunks and pipe to pigz and extract using tar
                        if ! eval "pigz -dc -p $PARALLEL_THREADS <(for chunk in \$chunk_list; do $awscli s3 cp \"$backup_bucket/$load_date/\$chunk\" - ; done) | tar xf - "; then
                            handle_early_exit_on_restore "Failed to decompress and untar columnstore localfiles\ncommand:\npigz -dc -p $PARALLEL_THREADS <(for chunk in \$chunk_list; do $awscli s3 cp \"$backup_bucket/$load_date/\$chunk\" - ; done) | tar xf - \n\n"
                        fi
                    fi
                    printf " Done \n"
                    ;;
                *)  # unknown option
                    handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
            esac
         
        else
            # Columnstore.xml and cmapi are renamed so that only specifc values are restored 
            s3cp $backup_bucket/$load_date/configs/Columnstore.xml $CS_CONFIGS_PATH/$col_xml_backup
            s3cp $backup_bucket/$load_date/configs/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup
            s3cp $backup_bucket/$load_date/configs/storagemanager.cnf $STORAGEMANGER_CNF 
            s3sync $backup_bucket/$load_date/storagemanager/cache/data$pm_number $cs_cache/data$pm_number/ " - Done cache/data$pm_number/\n"
            s3sync $backup_bucket/$load_date/storagemanager/metadata/data$pm_number $cs_metadata/data$pm_number/ " - Done metadata/data$pm_number/\n"
            if ! $skip_mdb; then s3sync $backup_bucket/$load_date/configs/mysql/$pm/ $MARIADB_SERVER_CONFIGS_PATH/ " - Done $MARIADB_SERVER_CONFIGS_PATH/\n"; fi

            if s3ls "$backup_bucket/$today/storagemanager/journal/data$ASSIGNED_DBROOT" > /dev/null 2>&1; then
                s3sync $backup_bucket/$load_date/storagemanager/journal/data$pm_number $cs_journal/data$pm_number/ " - Done journal/data$pm_number/\n"
            else 
                echo " - Done journal/data$pm_number was empty"
            fi
        fi;

        # Set appropraite bucket, prefix, region, key , secret
        printf "[+] Adjusting storagemanager.cnf ... \n"
        target_bucket=$backup_bucket
        target_prefix="prefix = $load_date\/columnstoreData\/"
        if [ ! -z "$new_bucket" ]; then target_bucket=$new_bucket;  target_prefix="# prefix \= cs\/";  fi
        if [ ! -z "$new_region" ];  then 
            if [ $cloud == "gcp" ]; then endpoint="storage.googleapis.com"; else endpoint="s3.$new_region.amazonaws.com"; fi;
            sed -i "s|^endpoint =.*|endpoint = ${endpoint}|g" $STORAGEMANGER_CNF; 
            sed -i "s|^region =.*|region = ${new_region}|g" $STORAGEMANGER_CNF; 
            echo " - Adjusted endpoint & region";
        fi
        # Removes prefix of the protocol and escapes / for sed
        target_bucket_name=$(echo "${target_bucket#$protocol://}" | sed "s/\//\\\\\//g")
        if [ ! -z "$new_key" ];  then sed -i "s|aws_access_key_id =.*|aws_access_key_id = ${new_key}|g" $STORAGEMANGER_CNF; echo " - Adjusted aws_access_key_id";  fi
        if [ ! -z "$new_secret" ];  then sed -i "s|aws_secret_access_key =.*|aws_secret_access_key = ${new_secret}|g" $STORAGEMANGER_CNF; echo " - Adjusted aws_secret_access_key"; fi
        bucket=$( grep -m 1 "^bucket =" $STORAGEMANGER_CNF | sed "s/\//\\\\\//g"); sed -i "s/$bucket/bucket = $target_bucket_name/g" $STORAGEMANGER_CNF; echo " - Adjusted bucket";
        prefix=$( grep -m 1 "^prefix =" $STORAGEMANGER_CNF | sed "s/\//\\\\\//g");
        if [ ! -z "$prefix" ]; then sed -i "s/$prefix/$target_prefix/g" $STORAGEMANGER_CNF; echo " - Adjusted prefix"; fi;
        chown -R $columnstore_user:$columnstore_user /var/lib/columnstore/
        chown -R root:root $MARIADB_SERVER_CONFIGS_PATH/
    
        # Confirm S3 connection works
        if [ $mode == "direct" ]; then echo "[+] Checking S3 Connection ..."; if testS3Connection $xtra_cmd_args; then echo " - S3 Connection passes" ; else handle_early_exit_on_restore "\n[X] S3 Connection issues - retest/configure\n"; fi; fi;
        printf "[+] MariaDB Columnstore Done\n\n"

        if ! $skip_mdb; then
            printf "Restore MariaDB Server\n"
            if [[ -n "$compress_format" ]]; then
                # Handle compressed mariadb-backup restore 
                mbd_prefix="$backup_bucket/$load_date/$split_file_mdb_prefix.$compress_format"
                case $compress_format in
                    pigz) 
                        printf "[+] Decompressing mariadb-backup bucket -> local ... "
                        rm -rf $MARIADB_PATH/*
                        if [ $cloud == "gcp" ]; then
                            if ! eval "$gsutil cat $mbd_prefix* | pigz -dc -p $PARALLEL_THREADS | mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH"; then
                                 handle_early_exit_on_restore "Failed to decompress mariadb backup\ncommand:\n$gsutil cat $mbd_prefix* | pigz -dc -p $PARALLEL_THREADS | mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH\n"
                            fi
                        elif [ $cloud == "aws" ]; then

                            chunk_list=$($awscli s3 ls "$mbd_prefix" | awk '{print $NF}')
                            cd /
                            
                            if ! eval "pigz -dc -p $PARALLEL_THREADS <(for chunk in \$chunk_list; do $awscli s3 cp "$backup_bucket/$load_date/\$chunk" -; done) |  mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH"; then
                                handle_early_exit_on_restore "Failed to decompress mariadb backup\ncommand:\npigz -dc -p $PARALLEL_THREADS <(for chunk in \$chunk_list; do $awscli s3 cp "$backup_bucket/$load_date/\$chunk" -; done) |  mbstream -p $PARALLEL_THREADS -x -C $MARIADB_PATH \n"
                            fi;
                        fi;
                        printf " Done \n"
                        printf " - Running mariabackup --prepare ...                 "
                        if eval "mariabackup --prepare --target-dir=$MARIADB_PATH $xtra_cmd_args"; then printf " Done\n"; else handle_early_exit_on_restore "Failed to --prepare\nmariabackup --prepare --target-dir=$MARIADB_PATH"; fi;
                        ;;
                    *)  # unknown option
                        handle_early_exit_on_backup "\nunknown compression flag: $compress_format\n" 
                esac
                
            else
                # Copy the MariaDB data, prepare and put in place
                printf " - Copying down MariaDB data ...                     "
                rm -rf $backup_location$load_date/mysql/   
                mkdir -p $backup_location$load_date/mysql/
                s3sync $backup_bucket/$load_date/mysql/ $backup_location$load_date/mysql " Done\n"

                # Run prepare and copy back
                printf " - Running mariabackup --prepare ...                 "
                if eval "mariabackup --prepare --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"; then printf " Done\n"; else echo "failed"; fi;
                rm -rf /var/lib/mysql/* > /dev/null
                printf " - Running mariabackup --copy-back ...               "
                if eval "mariabackup --copy-back --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"; then printf " Done\n"; else echo "failed"; fi;
            fi;

            # Check permissions
            printf " - Chowning mysql and log dirs ...                   "
            chown -R $mariadb_user:$mariadb_user /var/lib/mysql/
            chown -R $columnstore_user:$mariadb_user /var/log/mariadb/
            printf " Done \n"
            printf "[+] MariaDB Server Done\n"

        else
            echo "[!] Skipping mariadb server restore"
        fi
    fi

    if [[ $pm == "pm1" ]]; then restore_columnstore_values; fi;
    
    end=`date +%s`
    runtime=$((end-start))
    printf "\n[+] Runtime: $runtime\n"
    printf "[+] $final_message\n\n"
    if ! $quiet; then 
        if [ $pm == "pm1" ]; then 
            echo -e  " - Last you need to manually configure mariadb replication between nodes"
            echo -e  " - systemctl start mariadb "
            echo -e  " - systemctl start mariadb-columnstore-cmapi "
            echo -e  " - mcs cluster start \n\n"
        else
            printf " - Last you need to manually configure mariadb replication between nodes\n"
            echo -e  "Example:"
            echo -e  "mariadb -e \"stop slave;CHANGE MASTER TO MASTER_HOST='\$pm1' , MASTER_USER='repuser' , MASTER_PASSWORD='aBcd123%123%aBc' , MASTER_USE_GTID = slave_pos;start slave;show slave status\G\""
            echo -e  "mariadb -e  \"show slave status\G\" | grep  \"Slave_\" \n"
        fi
    fi
}

load_default_dbrm_variables() {
     # Default variables
    backup_base_name="dbrm_backup"
    backup_interval_minutes=90
    retention_days=7
    backup_location=/tmp/dbrm_backups
    STORAGEMANGER_CNF="/etc/columnstore/storagemanager.cnf"
    storage=$(grep -m 1 "^service = " $STORAGEMANGER_CNF | awk '{print $3}')
    mode="once"
}

print_dbrm_backup_help_text() {
    echo "
    Columnstore DBRM Backup

        -m   | --mode              'loop' or 'once' ; Determines if this script runs in a forever loop sleeping -i minutes or just once 
        -i   | --interval          Number of minutes to sleep when --mode loop 
        -r   | --retention_days    Number of days of dbrm backups to retain - script will delete based on last update file time
        -p   | --path              path of where to save the dbrm backups on disk

        Default: ./$0 dbrm_backup -m once --retention_days 7 --path /tmp/dbrm_backups

        Examples:
            ./$0 dbrm_backup --mode loop --interval 90 --retention_days 7 --path /mnt/dbrm_backups
            
        Cron Example:
        */60 */3 * * * root  bash /root/$0 dbrm_backup -m once --retention_days 7 --path /tmp/dbrm_backups  >> /tmp/dbrm_backups/cs_backup.log  2>&1
    ";
}

parse_dbrms_variables() {

    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            dbrm_backup)
                shift # past argument
                ;;
            -i|--interval)
                backup_interval_minutes="$2"
                shift # past argument
                shift # past value
                ;;
            -r|--retention_days)
                retention_days="$2"
                shift # past argument
                shift # past value
                ;;
            -p|--path)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -m|--mode)
                mode="$2"
                shift # past argument
                shift # past value
                ;;
            -h|--help|-help|help)
                print_dbrm_backup_help_text;
                exit 1;
                ;;
            *)  # unknown option
                printf "\nunknown flag: $1\n"
                print_dbrm_backup_help_text
                exit 1;
        esac
    done

    backup_dir="/var/lib/columnstore/data1/systemFiles/dbrm"
    if [ "$storage" == "S3" ]; then 
        backup_dir="/var/lib/columnstore/storagemanager"
    fi
}

is_numerical_or_decimal() {
    local input="$1"
    local variable_name="$2"

    # Regular expression to match numerical or decimal values
    if [[ $input =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        return 0
    else
        printf "[!] $variable_name = '$input' is not numerical or decimal.\n\n"
        exit 2;
    fi
}

validation_prechecks_for_dbrm_backup() {

    # Confirm storage not empty
    if [ -z "$storage" ]; then printf "[!] Empty storage: \ncheck: grep -m 1 \"^service = \" \$STORAGEMANGER_CNF | awk '{print \$3}' \n\n"; fi;

    # Check mode type
    errors=""
    case $mode in
        once)
             errors+="" ;;
        loop)
             errors+="" ;;
        *)  # unknown option
            printf "\nunknown mode: $mode\n"
            printf "Only 'once' & 'loop' allowed\n\n"
            print_dbrm_backup_help_text
            exit 2;
    esac

    # Check numbers
    is_numerical_or_decimal "$backup_interval_minutes" "Interval"
    is_numerical_or_decimal "$retention_days" "Retention"

    # Check backup location exists
    if [ ! -d $backup_location ]; then 
        echo "[+] Created: $backup_location"
        mkdir $backup_location; 
    fi;

    # Confirm bucket connection
    if [ "$storage" == "S3" ]; then
        if ! testS3Connection 1>/dev/null 2>/dev/null; then
            printf "\n[!] Failed testS3Connection\n\n"
            exit 1;
        fi
    fi;
}

process_dbrm_backup() {

    load_default_dbrm_variables
    parse_dbrms_variables "$@";

    # print variables
    printf "\n[+] Inputs
    CS Storage: $storage
    Source:     $backup_dir
    Backups:    $backup_location
    Interval:   $backup_interval_minutes minutes
    Retention:  $retention_days day(s)
    Mode:       $mode\n"

    validation_prechecks_for_dbrm_backup

    sleep_seconds=$((backup_interval_minutes * 60));
    while true; do
        # Create a new backup directory
        timestamp=$(date +%Y%m%d%H%M%S)
        backup_folder="$backup_location/${backup_base_name}_${timestamp}"
        mkdir -p "$backup_folder"
        
        # Copy files to the backup directory
        cp -arp "$backup_dir"/* "$backup_folder"

        if [ "$storage" == "S3" ]; then 
            # smcat em files to disk
            mkdir $backup_folder/dbrms/
            smls /data1/systemFiles/dbrm 2>/dev/null > $backup_folder/dbrms/dbrms.txt
            smcat /data1/systemFiles/dbrm/BRM_saves_current  2>/dev/null > $backup_folder/dbrms/BRM_saves_current
            smcat /data1/systemFiles/dbrm/BRM_saves_journal 2>/dev/null > $backup_folder/dbrms/BRM_saves_journal
            smcat /data1/systemFiles/dbrm/BRM_saves_em 2>/dev/null > $backup_folder/dbrms/BRM_saves_em
            smcat /data1/systemFiles/dbrm/BRM_saves_vbbm 2>/dev/null > $backup_folder/dbrms/BRM_saves_vbbm
            smcat /data1/systemFiles/dbrm/BRM_saves_vss 2>/dev/null > $backup_folder/dbrms/BRM_saves_vss
            smcat /data1/systemFiles/dbrm/BRM_savesA_em 2>/dev/null > $backup_folder/dbrms/BRM_savesA_em
            smcat /data1/systemFiles/dbrm/BRM_savesA_vbbm 2>/dev/null > $backup_folder/dbrms/BRM_savesA_vbbm
            smcat /data1/systemFiles/dbrm/BRM_savesA_vss 2>/dev/null > $backup_folder/dbrms/BRM_savesA_vss
            smcat /data1/systemFiles/dbrm/BRM_savesB_em 2>/dev/null > $backup_folder/dbrms/BRM_savesB_em
            smcat /data1/systemFiles/dbrm/BRM_savesB_vbbm 2>/dev/null > $backup_folder/dbrms/BRM_savesB_vbbm
            smcat /data1/systemFiles/dbrm/BRM_savesB_vss 2>/dev/null > $backup_folder/dbrms/BRM_savesB_vss
        fi
        
        # Clean up old backups
        # example: find /tmp/dbrmBackups/ -maxdepth 1 -type d -name "dbrm_backup_*" -mtime +1 -exec rm -r {} \;
        find "$backup_location" -maxdepth 1 -type d -name "${backup_base_name}_*" -mtime +$retention_days -exec rm -r {} \;

        printf "[+] Created: $backup_folder\n"
        if [ "$mode" == "once" ]; then  break;  fi;

        printf "[+] Sleeping ... $sleep_seconds seconds\n"
        sleep "$sleep_seconds"
    done

    printf "[+] Complete\n\n"
}

process_backup()
{   
    load_default_backup_variables;
    parse_backup_variables "$@";
    print_backup_variables;
    check_for_dependancies "backup";
    validation_prechecks_for_backup;
    issue_write_locks;
    run_save_brm;
    run_backup;
}

process_restore()
{
    load_default_restore_variables;
    parse_restore_variables "$@";
    print_restore_variables;
    check_for_dependancies "restore";
    validation_prechecks_for_restore;
    run_restore;
}

case "$action" in
    'help' | '--help' | '-help' | '-h') 	
        print_action_help_text
        ;;
    'backup') 
        process_backup "$@";
        ;;
    'dbrm_backup') 	
        process_dbrm_backup "$@";
        ;;
    'restore') 	
        process_restore "$@";
        ;;
    *) 
        printf "\nunknown action: $action\n" 
        print_action_help_text
        ;;
esac

exit 0;