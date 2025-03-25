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
# Version: 3.14
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
mcs_bk_manager_version="3.14"
start=$(date +%s)
action=$1

print_action_help_text() {
    echo "
MariaDB Columnstore Backup Manager

Actions:

    backup                  Full & Incremental columnstore backup with additional flags to augment the backup taken
    restore                 Restore a backup taken with this script
    dbrm_backup             Quick hot backup of internal columnstore metadata only - only use under support recommendation
    dbrm_restore            Restore internal columnstore metadata from dbrm_backup - only use under support recommendation

Documentation:
    bash $0 <action> help

Example:
    bash $0 backup help
    "
}

check_operating_system() {

    OPERATING_SYSTEM=$(awk -F= '/^ID=/{gsub(/"/, "", $2); print $2}' /etc/os-release)

    # Supported OS
    case $OPERATING_SYSTEM in
        centos | rhel | rocky )
            return 1;
            ;;
        ubuntu | debian )
            return 1;
            ;;
        *)  # unknown option
            printf "\ncheck_operating_system: unknown os & version: $OPERATING_SYSTEM\n"
            exit 2;
    esac   
}

load_default_backup_variables() {
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
        centos | rocky | rhel )
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

    # Name of the Configuration file to load variables from - relative or full path accepted
    config_file=""

    # Track your write speed with "dstat --top-cpu --top-io"
    # Monitor individual rsyncs with ` watch -n0.25 "ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l; ps aux | grep 'rsync -a' | grep -vE 'color|grep' " `
    # Determines if columnstore data directories will have multiple rsync running at the same time for different subfolders to parallelize writes
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
    cmapi_key=""
    if [[ -f "$CS_CONFIGS_PATH/cmapi_server.conf" ]]; then
        cmapi_key="$(grep "x-api-key" "$CS_CONFIGS_PATH/cmapi_server.conf" | awk '{print $3}' | tr -d "'")";
    fi
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
    list_backups=false
    retention_file_lock_name=".delete-lock"
    apply_retention_only=false

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

    # Deletes backups older than this variable retention_days
    retention_days=0

    # Tracks if flush read lock has been run
    read_lock=false
    incremental=false
    columnstore_online=false

    confirm_xmllint_installed
   
    # Number of DBroots
    # Integer usually 1 or 3
    DBROOT_COUNT=$(xmllint --xpath "string(//DBRootCount)" $CS_CONFIGS_PATH/Columnstore.xml)
    ASSIGNED_DBROOT=$(xmllint --xpath "string(//ModuleDBRootID$PM_NUMBER-1-3)" $CS_CONFIGS_PATH/Columnstore.xml)
}

parse_backup_variables() {   
    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            backup)
                shift # past argument
                ;;
            -bl|--backup-location)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -bd|--backup-destination)
                backup_destination="$2"
                shift # past argument
                shift # past value
                ;;
            -scp|--secure-copy-protocol)
                scp="$2"
                shift # past argument
                shift # past value
                ;;
            -bb|--backup-bucket)
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
                if [ "$config_file" == "" ] || [ ! -f "$config_file" ]; then
                    handle_early_exit_on_backup "\n[!!!] Invalid Configuration File: $config_file\n" true
                fi  
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
            -smdb |  --skip-mariadb-backup)
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
            -nv-ssl| --no-verify-ssl)
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
            -r|--retention-days)
                retention_days="$2"
                shift # past argument
                shift # past value
                ;;
            -aro| --apply-retention-only)
                apply_retention_only=true
                shift # past argument
                ;;
            -li | --list)
                list_backups=true
                shift # past argument
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
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        if ! source "$config_file"; then
            handle_early_exit_on_backup "\n[!!!] Failed to source configuration file: $config_file\n" true
        fi
    fi

    # Adjustment for indirect mode
    if [ $mode == "indirect" ]; then skip_locks=true; skip_save_brm=true; fi;

    # check retention_days isnt empty string and is an integer
    confirm_integer_else_fail "$retention_days" "Retention"
}

print_backup_help_text() {
     echo "
    Columnstore Backup

        -bl    | --backup-location        Directory where the backup will be saved
        -bd    | --backup-destination     If the directory is 'Local' or 'Remote' to this script
        -scp   | --secure-copy-protocol   scp connection to remote server if -bd 'Remote'
        -bb    | --backup-bucket          Bucket name for where to save S3 backups
        -url   | --endpoint-url           Onprem url to s3 storage api example: http://127.0.0.1:8000
        -nv-ssl| --no-verify-ssl          Skips verifying ssl certs, useful for onpremise s3 storage
        -s     | --storage                The storage used by columnstore data 'LocalStorage' or 'S3'
        -i     | --incremental            Adds columnstore deltas to an existing full backup [ <Folder>, auto_most_recent ]
        -P     | --parallel               Number of parallel rsync/compression threads to run
        -f     | --config-file            Path to backup configuration file to load variables from - see load_default_backup_variables() for defaults
        -sbrm  | --skip-save-brm          Skip saving brm prior to running a backup - ideal for dirty backups
        -slock | --skip-locks             Skip issuing write locks - ideal for dirty backups
        -spoll | --skip-polls             Skip sql checks confirming no write/cpimports running
        -smdb  | --skip-mariadb-backup    Skip running a mariadb-backup for innodb data - ideal for incremental dirty backups
        -sb    | --skip-bucket-data       Skip taking a copy of the columnstore data in the bucket
        -pi    | --poll-interval          Number of seconds between poll checks for active writes & cpimports
        -pmw   | --poll-max-wait          Max number of minutes for polling checks for writes to wait before exiting as a failed backup attempt
        -q     | --quiet                  Silence verbose copy command outputs
        -c     | --compress               Compress backup in X format - Options: [ pigz ]
        -nb    | --name-backup            Define the name of the backup - default: date +%m-%d-%Y
        -r     | --retention-days         Retain backups created within the last X days, the rest are deleted, default 0 = keep all backups
        -aro   | --apply-retention-only   Only apply retention policy to existing backups, does not run a backup
        -li    | --list                   List all backups available in the backup location
        -ha    | --highavilability        Hint wether shared storage is attached @ below on all nodes to see all data
                                            HA LocalStorage ( /var/lib/columnstore/dataX/ )
                                            HA S3           ( /var/lib/columnstore/storagemanager/ ) 

        Local Storage Examples:
            ./$0 backup -bl /tmp/backups/ 
            ./$0 backup -bl /tmp/backups/ -P 8
            ./$0 backup -bl /tmp/backups/ --incremental auto_most_recent
            ./$0 backup -bl /tmp/backups/ --list
            ./$0 backup -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage

        S3 Examples:  
            ./$0 backup -bb s3://my-cs-backups -s S3
            ./$0 backup -bb s3://my-cs-backups -c pigz --quiet -sb
            ./$0 backup -bb gs://my-cs-backups -s S3 --incremental 12-18-2023
            ./$0 backup -bb s3://my-onpremise-bucket -s S3 -url http://127.0.0.1:8000
            
        Cron Example:
        */60 */24 * * *  root  bash /root/$0 -bb s3://my-cs-backups -s S3  >> /root/csBackup.log  2>&1
    ";

    # Hidden flags
    # -m | --mode           Options ['direct','indirect'] - direct backups run on the columnstore nodes themselves. indirect run on another machine that has read-only mounts associated with columnstore/mariadb

}

print_backup_variables() {
    # Log Variables
    s1=20
    s2=20
    printf "\nBackup Variables\n"
    echo "--------------------------------------------------------------------------"
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
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
    printf "%-${s1}s  %-${s2}s\n" "Retention:" "$retention_days";

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

check_for_dependancies() {
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
        if [ -z "$backup_bucket" ]; then handle_failed_dependencies "\n\n[!] Undefined --backup-bucket: $backup_bucket \nfor examples see: ./$0 backup --help\n\n"; fi
        if [[ $backup_bucket == gs://* ]]; then
            cloud="gcp"; protocol="gs";
        elif [[ $backup_bucket == s3://* ]]; then
            cloud="aws"; protocol="s3";
        else 
            handle_failed_dependencies "\n\n[!] Invalid --backup-bucket - doesnt lead with gs:// or s3:// - $backup_bucket\n\n";
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

                handle_failed_dependencies "\n\n[!] Please make sure gsutil cli is installed configured and executable\n\n"
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

                handle_failed_dependencies "\n\n[!] Please make sure aws cli is installed configured and executable\nSee existing .cnf aws credentials with:  grep aws_ $STORAGEMANGER_CNF \n\n"
            else
                awscli=$(which aws 2>/dev/null)
            fi
            
            # aws sytax for silent
            if $quiet; then xtra_s3_args+="--quiet"; fi
        fi;
    fi
}

validation_prechecks_for_backup() {
    echo "Prechecks"

    # Adjust rsync for incremental copy
    additional_rysnc_flags=""
    if $quiet; then xtra_cmd_args+="  2> /dev/null"; additional_rysnc_flags+=" -a"; else additional_rysnc_flags+=" -av "; fi
    if  $incremental ; then additional_rysnc_flags+=" --inplace --no-whole-file --delete"; fi;
    if [ -z "$mode" ]; then printf "\n[!!!] Required field --mode: $mode - is empty\n"; exit 1; fi
    if [ "$mode" != "direct" ] && [ "$mode" != "indirect" ] ; then printf "\n[!!!] Invalid field --mode: $mode\n"; exit 1; fi

    # Poll Variable Checks
    confirm_integer_else_fail "$poll_interval" "poll_interval"
    confirm_integer_else_fail "$poll_max_wait" "poll_max_wait"
    max_poll_attempts=$((poll_max_wait * 60 / poll_interval))
    if [ "$max_poll_attempts" -lt 1 ]; then max_poll_attempts=1; fi;

    # Detect if columnstore online
    if [ $mode == "direct" ]; then
        if [ -z $(pidof PrimProc) ] || [ -z $(pidof WriteEngineServer) ]; then 
            printf " - Columnstore is OFFLINE \n"; 
            export columnstore_online=false; 
        else 
            printf " - Columnstore is ONLINE - safer if offline (but not required to be offline) \n"; 
            export columnstore_online=true; 
        fi
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
        if $incremental; then

            # $backup_location must exist to find an existing full back to add to
            if [ ! -d $backup_location ]; then
                handle_early_exit_on_backup "[X] Backup directory ($backup_location) DOES NOT exist ( -bl <directory> ) \n\n" true; 
            fi

            if [ "$today" == "auto_most_recent" ]; then 
                auto_select_most_recent_backup_for_incremental
            fi
            
            # Validate $today is a non-empty value
            if [ -z "$today" ]; then 
                handle_early_exit_on_backup "\nUndefined folder to increment on ($backup_location$today)\nTry --incremental <folder> or --incremental auto_most_recent \n" true
            fi

            # Cant continue if this folder (which represents a full backup) doesnt exists
            if [ $backup_destination == "Local" ]; then 
                if [ -d $backup_location$today ]; then 
                    printf " - Full backup directory exists\n"; 
                else  
                    handle_early_exit_on_backup "[X] Full backup directory ($backup_location$today) DOES NOT exist \n\n" true; 
                fi;
            elif [ $backup_destination == "Remote" ]; then
                if [[ $(ssh $scp test -d $backup_location$today && echo exists) ]]; then  
                    printf " - Full backup directory exists\n"; 
                else 
                    handle_early_exit_on_backup "[X] Full backup directory ($backup_location$today) DOES NOT exist on remote $scp \n\n" true;  
                fi
            fi
        fi
    elif [ $storage == "S3" ]; then
        
        # Adjust s3api flags for onpremise/custom endpoints
        add_s3_api_flags=""
        if [ -n "$s3_url" ];  then add_s3_api_flags+=" --endpoint-url $s3_url"; fi;
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
            if $( $awscli $add_s3_api_flags s3 ls $backup_bucket > /dev/null ) ; then
                printf " - Success listing backup bucket\n"
            else 
                printf "\n[X] Failed to list bucket contents... \nCheck aws cli credentials: aws configure"
                handle_early_exit_on_backup "\naws $add_s3_api_flags s3 ls $backup_bucket \n" true
            fi
        fi;

        # Incremental Job checks
        if $incremental; then
            if [ "$today" == "auto_most_recent" ]; then 
                auto_select_most_recent_backup_for_incremental
            fi
            
            # Validate $today is a non-empty value
            if [ -z "$today" ]; then 
                handle_early_exit_on_backup "\nUndefined folder to increment on ($backup_bucket/$today)\nTry --incremental <folder> or --incremental auto_most_recent \n" true
            fi

            # Cant continue if this folder (which represents a full backup) doesnt exists
            if [ $cloud == "gcp" ]; then
                if [[ $( $gsutil ls $backup_bucket/$today | head ) ]]; then  
                    printf " - Full backup directory exists\n"; 
                else 
                    handle_early_exit_on_backup "[X] Full backup directory ($backup_bucket/$today) DOES NOT exist in GCS \nCheck - $gsutil ls $backup_bucket/$today | head \n\n" true;   
                fi  
            else
                if [[ $( $awscli $add_s3_api_flags s3 ls $backup_bucket/$today/ | head ) ]]; then  
                    printf " - Full backup directory exists\n"; 
                else 
                    handle_early_exit_on_backup "[X] Full backup directory ($backup_bucket/$today) DOES NOT exist in S3 \nCheck - aws $add_s3_api_flags s3 ls $backup_bucket/$today | head \n\n" true; 
                fi  
            fi;
        fi
    else 
        handle_early_exit_on_backup "Invalid Variable storage: $storage" true
    fi
}

# Used when "--incremental auto_most_recent" passed in during incremental backups
# This function identifies which backup directory is the most recent and sets today=x so that the incremental backup applies to said last full backup
# For LocalStorage: based on ls -td <backup_dir> | head -n 1
# For S3: using the awscli/gsutil, compare the dates of the backup folders restoreS3.job file to find the most recent S3 backup to increment ont top off
auto_select_most_recent_backup_for_incremental() {

    printf " - Searching for most recent backup ...."
    if [ $storage == "LocalStorage" ]; then 
         
        # Example: find /mnt/backups/ -mindepth 1 -maxdepth 1 -type d ! -exec test -f "{}/.auto-delete-via-retention" \; -printf "%T@ %p\n" | sort -nr | awk '{print $2}' | head -n 1
        if [ -f "${backup_location}${retention_file_lock_name}-$pm" ] ; then
            handle_early_exit_on_backup "\n[!!!] ${backup_location}${retention_file_lock_name}-$pm exists. are multiple backups running or was there an error? Manually resolve before retrying\n" true
        fi

        # Primary node is in the middle of applying retention policy, distorting the most recent backup
        if [ -f "${backup_location}${retention_file_lock_name}-pm1" ] ; then
            most_recent_backup="$(head -n1 "${backup_location}${retention_file_lock_name}-pm1" | awk '{print $2}')"
        else
            most_recent_backup=$(find "$backup_location" -mindepth 1 -maxdepth 1 -type d -printf "%T@ %p\n" | sort -nr | awk '{print $2}' | head -n 1)
        fi

        # Debug messaging to understand the most recent backup modified times
        if ! $quiet; then
            echo ""
            find "$backup_location" -mindepth 1 -maxdepth 1 -type d -printf "%T@ %p\n" | sort -nr 
        fi

        # If no backup found, exit
        if [[ -z "$most_recent_backup" ]]; then
            handle_early_exit_on_backup "\n[!!!] No backup found to increment in '$backup_location', please run a full backup or define a folder that exists --incremental <folder>\n" true
        else 
            today=$(basename $most_recent_backup 2>/dev/null)
        fi

    elif [ $storage == "S3" ]; then
        current_date=$(date +%s)
        backups=$(s3ls $backup_bucket)
        most_recent_backup=""
        most_recent_backup_time_diff=$((2**63 - 1));

        while IFS= read -r line; do
            
            folder=$(echo "$line" | awk '{print substr($2, 1, length($2)-1)}')
            date_time=$(s3ls "${backup_bucket}/${folder}/restore --recursive" |  awk '{print $1,$2}')

            if [[ -n "$date_time" ]]; then

                # Parse the date
                backup_date=$(date -d "$date_time" +%s)
                
                # Calculate the difference in days
                time_diff=$(( (current_date - backup_date) ))    
                # echo "date_time: $date_time"
                # echo "backup_date: $backup_date"
                # echo "time_diff: $time_diff"
                # echo "days_diff: $((time_diff / (60*60*24) ))"

                if [ $time_diff -lt $most_recent_backup_time_diff ]; then
                    most_recent_backup=$folder
                    most_recent_backup_time_diff=$time_diff
                fi
            fi
            printf "."
        done <<< "$backups"
       
        # printf "\n\nMost Recent: $most_recent_backup \n"
        # printf "Time Diff: $most_recent_backup_time_diff \n"

        if [[ -z "$most_recent_backup" ]]; then
            handle_early_exit_on_backup "\n[!!!] No backup found to increment, please run a full backup or define a folder that exists --incremental <folder>\n" true
            exit 1;
        else 
            today=$most_recent_backup
        fi
    fi
    printf " selected: $today \n"
}

apply_backup_retention_policy() {
    
    if [ "$retention_days" -eq 0 ]; then
        printf " - Skipping Backup Rentention Policy\n"
        return 0;
    fi

    # PM1 is in charge of deleting the backup 
    # Rest of the nodes should wait for the delete to be over if it finds the lock file
    applying_retention_start=$(date +%s)
    printf "\nApplying Backup Rentention Policy\n"
    if [ "$pm" == "pm1" ] || [ -n "${PM_FORCE_DELETE-}" ] ; then 
    
        retention_file_lock_name="${retention_file_lock_name}-$pm"
        
        if [ $storage == "LocalStorage" ]; then
            
            # Create a lock file to prevent other PMs from deleting backups
            touch "${backup_location}${retention_file_lock_name}"
            echo "most_recent_backup: $most_recent_backup" >> "${backup_location}${retention_file_lock_name}"
            printf " - Created: ${backup_location}${retention_file_lock_name}\n"

            # example1: find /mnt/backups/ -mindepth 1 -maxdepth 1 -type d -name "*" -mmin +10 -exec echo "{}" \;
            # example2: find /mnt/backups/ -mindepth 1 -maxdepth 1 -type d -name "*" -mmin +10 -exec sh -c 'printf " - Deleting: {} \n" && echo "{}" >> "/mnt/backups/.delete-lock-pm1" && rm -rf "{}"' \;
            # find "$backup_location" -mindepth 1 -maxdepth 1 -type d -name "*" -mmin +$retention_days -exec sh -c 'printf " - Deleting: {} \n" && echo "{}" >> "'"${backup_location}${retention_file_lock_name}"'" && rm -rf "{}"' \;
            find "$backup_location" -mindepth 1 -maxdepth 1 -type d -name "*" -mtime +$retention_days -exec sh -c 'printf " - Deleting: {} \n" && echo "{}" >> "'"${backup_location}${retention_file_lock_name}"'" && rm -rf "{}"' \;
            
            # Cleanup the lock file
            rm "${backup_location}${retention_file_lock_name}"
            printf " - Deleted: ${backup_location}${retention_file_lock_name}\n"

        elif [ $storage == "S3" ]; then 

            # Create a lock file to prevent other PMs from deleting backups
            echo "most_recent_backup: $most_recent_backup" >> "${retention_file_lock_name}"
            s3cp "${retention_file_lock_name}" "${backup_bucket}/${retention_file_lock_name}"

            current_date=$(date +%s)
            backups=$(s3ls $backup_bucket)
        
            while IFS= read -r line; do

                delete_backup=false
                folder=$(echo "$line" | awk '{print substr($2, 1, length($2)-1)}')
                date_time=$(s3ls "${backup_bucket}/${folder}/restore --recursive" |  awk '{print $1,$2}')
        
                if [[ -n "$date_time" ]]; then

                    # Parse the date
                    backup_date=$(date -d "$date_time" +%s)
                    
                    # Calculate the difference in days
                    days_diff=$(( (current_date - backup_date) / (60*60*24) ))    
                    # echo "line: $line"
                    # echo "date_time: $date_time"
                    # echo "backup_date: $backup_date"
                    # echo "days_diff: $days_diff"

                    if [ $days_diff -gt "$retention_days" ]; then
                        delete_backup=true
                    fi
                else
                    delete_backup=true
                fi
                
                if $delete_backup; then
                    echo "${backup_bucket}/${folder}" >> "${retention_file_lock_name}"
                    s3cp "${retention_file_lock_name}" "${backup_bucket}/${retention_file_lock_name}"
                    s3rm "${backup_bucket}/${folder}"
                    #printf "\n   - Deleting: ${backup_bucket}/${folder}"
                fi
                printf "."
            done <<< "$backups"

            # Cleanup the lock file
            s3rm "${backup_bucket}/${retention_file_lock_name}"
        fi
        
        applying_retention_end=$(date +%s)
        printf "Done $(human_readable_time $((applying_retention_end-applying_retention_start))) \n" 
    else

        local retry_limit=2160
        local retry_counter=0
        local retry_sleep_interval=5
        if [ $storage == "LocalStorage" ]; then
            
            if [ -f "${backup_location}${retention_file_lock_name}-pm1" ]; then
                printf " - Waiting for PM1 to finish deleting backups (max: $retry_limit intervals of $retry_sleep_interval sec ) ... \n"
            fi;
            while [ -f "${backup_location}${retention_file_lock_name}-pm1" ]; do
                
                if [ -f "${backup_location}${retention_file_lock_name}-pm1" ]; then
                    printf " - Waiting on PM1 to finish deleting %s \n" "$(tail -n 1 "${backup_location}${retention_file_lock_name}-pm1" 2>/dev/null )"
                fi;
                
                if [ $retry_counter -ge $retry_limit ]; then
                    handle_early_exit_on_backup "\n [!] PM1 lock file still exists after $retry_limit sec, deleting backups via retention policy might have failed or taking too long, please investigate, exiting\n" true
                fi
                sleep $retry_sleep_interval
                ((retry_counter++))
            done
        elif [ $storage == "S3" ]; then 
            if [[ $(s3ls "${backup_bucket}/${retention_file_lock_name}")  ]]; then
                printf " - Waiting for PM1 to finish deleting backups (max: $retry_limit intervals of $retry_sleep_interval sec ) ... \n"
            fi;
            while [[ $(s3ls "${backup_bucket}/${retention_file_lock_name}")  ]]; do
                if [[ $(s3ls "${backup_bucket}/${retention_file_lock_name}")  ]]; then
                    printf " - Waiting on PM1 to finish deleting %s \n" "$( s3cp "${backup_bucket}/${retention_file_lock_name}" "-" | tail -n 1 2>/dev/null )"
                fi;
                if [ $retry_counter -ge $retry_limit ]; then
                    handle_early_exit_on_backup "\n [!] PM1 lock file still exists after $retry_limit sec, deleting backups via retention policy might have failed or taking too long, please investigate, exiting\n" true
                fi
                sleep $retry_sleep_interval
                ((retry_counter++))
            done
        fi
        applying_retention_end=$(date +%s)
        printf "Done $(human_readable_time $((applying_retention_end-applying_retention_start))) \n" 
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
    printf "Done\n\n"
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
    # Consider removing when 23.10.x is EOL
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
            handle_early_exit_on_backup "\n[X] Failed issuing read-only lock\n" 
        fi 
    fi
    
    if [ $pm == "pm1" ]; then 
        
        # Set Columnstore ReadOnly Mode
        startreadonly_exists=$(dbrmctl -h 2>&1 | grep "startreadonly")
        printf " - Issuing read-only lock to Columnstore Engine ...   ";
        if ! $columnstore_online; then 
            printf "Skip since offline\n";
        elif [ $DBROOT_COUNT == "1" ] && [[ -n "$startreadonly_exists" ]]; then
            if dbrmctl startreadonly ; then
                if ! $skip_polls; then 
                    cs_read_only_wait_loop
                fi
            else
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via dbrmctl startreadonly\n" 
            fi;

        elif [ $DBROOT_COUNT == "1" ]; then 
            if ! dbrmctl readonly ; then
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via dbrmctl readonly \n" 
            fi
        else 
            if command -v mcs &> /dev/null && command -v jq &> /dev/null ; then
                if ! mcs cluster set mode --mode readonly  | jq -r tostring ; then
                    handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via cmapi\n" 
                fi
            else
                # Older CS versions dont have mcs cli
                cmapiResponse=$(curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapi_key" --data '{"timeout":20, "mode": "readonly"}' -k);
                if [[ $cmapiResponse == '{"error":'* ]] ; then 
                    handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via cmapi\n" 
                else
                    printf "$cmapiResponse \n";
                fi
            fi
        fi
    else
        cs_read_only_wait_loop
    fi
}

poll_check_no_active_sql_writes() {
    poll_active_sql_writes_start=$(date +%s)
    printf " - Polling for active Writes ...                      "
    query="SELECT COUNT(*) FROM information_schema.processlist where user != 'root' AND command = 'Query' AND ( info LIKE '% INSERT %' OR info LIKE '% UPDATE %' OR info LIKE '% DELETE %' OR info LIKE '% LOAD DATA %');"
    attempts=0
    no_writes=false
    while [ "$attempts" -lt "$max_poll_attempts" ]; do
        active_writes=$( mariadb -s -N -e "$query" )
        if [ "$?" -ne 0 ]; then
            handle_early_exit_on_backup "\n$active_writes\n[X] Failed to poll for active writes\n" true
        fi

        if [ "$active_writes" -le 0 ]; then
            poll_active_sql_writes_end=$(date +%s)
            printf "Done $(human_readable_time $((poll_active_sql_writes_start-poll_active_sql_writes_end))) \n" 
            no_writes=true
            break
        else
            
            if ! $quiet; then 
                printf "\nActive write operations detected: $active_writes\n"; 
                mariadb -e "select ID,USER,TIME,LEFT(INFO, 50) from information_schema.processlist where user != 'root' AND command = 'Query' AND ( info LIKE '% INSERT %' OR info LIKE '% UPDATE %' OR info LIKE '% DELETE %' OR info LIKE '% LOAD DATA %');"
            else 
                printf "."
            fi;
            sleep "$poll_interval"
            ((attempts++))
        fi
    done

    if ! $no_writes; then
        mariadb -e "select ID,USER,TIME,INFO from information_schema.processlist where user != 'root' AND command = 'Query' AND ( info LIKE '% INSERT %' OR info LIKE '% UPDATE %' OR info LIKE '% DELETE %' OR info LIKE '% LOAD DATA %');"   
        handle_early_exit_on_backup "\n[X] Exceeded poll_max_wait: $poll_max_wait minutes\nActive write operations detected: $active_writes \n" true
    fi;
    
}

poll_check_no_active_cpimports() {
    poll_active_cpimports_start=$(date +%s)
    printf " - Polling for active cpimports ...                   "
    attempts=0
    no_cpimports=false
    if ! $columnstore_online ; then printf "Skip since offline\n"; return ; fi;
    while [ "$attempts" -lt "$max_poll_attempts" ]; do
        active_cpimports=$(viewtablelock 2>/dev/null )
        if [[ "$active_cpimports" == *"No tables are locked"* ]]; then
            poll_active_cpimports_end=$(date +%s)
            printf "Done $(human_readable_time $((poll_active_cpimports_end-poll_active_cpimports_start))) \n" 
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

run_save_brm() {

    if $skip_save_brm || [ $pm != "pm1" ] || ! $columnstore_online || [ $mode == "indirect" ]; then return; fi;

    printf "\nBlock Resolution Manager\n"
    local tmp_dir="/tmp/DBRMbackup-$today"
    if [ ! -d "$tmp_dir" ]; then mkdir -p $tmp_dir; fi;

    # Copy extent map locally just in case save_brm fails
    if [ $storage == "LocalStorage" ]; then
        printf " - Backing up DBRMs @ $tmp_dir ... " 
        cp -R $DBRM_PATH/* $tmp_dir
        printf " Done \n" 
    elif [ $storage == "S3" ]; then 

        printf " - Backing up minimal DBRMs @ $tmp_dir ... "  
        # Base Set
        smcat /data1/systemFiles/dbrm/BRM_saves_current 2>/dev/null > $tmp_dir/BRM_saves_current 
        smcat /data1/systemFiles/dbrm/BRM_saves_journal 2>/dev/null > $tmp_dir/BRM_saves_journal 
        smcat /data1/systemFiles/dbrm/BRM_saves_em   2>/dev/null > $tmp_dir/BRM_saves_em 
        smcat /data1/systemFiles/dbrm/BRM_saves_vbbm 2>/dev/null > $tmp_dir/BRM_saves_vbbm 
        smcat /data1/systemFiles/dbrm/BRM_saves_vss  2>/dev/null > $tmp_dir/BRM_saves_vss 

        # A Set
        smcat /data1/systemFiles/dbrm/BRM_savesA_em   2>/dev/null > $tmp_dir/BRM_savesA_em 
        smcat /data1/systemFiles/dbrm/BRM_savesA_vbbm 2>/dev/null > $tmp_dir/BRM_savesA_vbbm 
        smcat /data1/systemFiles/dbrm/BRM_savesA_vss  2>/dev/null > $tmp_dir/BRM_savesA_vss 
        
        # B Set
        smcat /data1/systemFiles/dbrm/BRM_savesB_em   2>/dev/null > $tmp_dir/BRM_savesB_em 
        smcat /data1/systemFiles/dbrm/BRM_savesB_vbbm 2>/dev/null > $tmp_dir/BRM_savesB_vbbm 
        smcat /data1/systemFiles/dbrm/BRM_savesB_vss  2>/dev/null > $tmp_dir/BRM_savesB_vss 
        printf " Done \n" 
    fi
    
    printf " - Saving BRMs... \n"
    brm_owner=$(stat -c "%U" $DBRM_PATH/)
    if sudo -u $brm_owner /usr/bin/save_brm ; then 
        rm -rf $tmp_dir
    else  
        printf "\n Failed: save_brm error - see /var/log/messages - backup @ $tmpDir \n\n"; 
        handle_early_exit_on_backup 
    fi
    
}

# Example: s3sync <from> <to> <success_message> <failed_message>
# Example: s3sync s3://$bucket $backup_bucket/$today/columnstoreData "Done - Columnstore data sync complete"
s3sync() {

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
        eval "$cmd"
    else 
        # Default AWS
        cmd="$awscli $xtra_s3_args $add_s3_api_flags s3 sync $from $to"
        $cmd
    fi

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        if [ -n "$success_message" ]; then printf "$success_message"; fi;
    else
        if [ $retries -lt $RETRY_LIMIT ]; then 
            echo "$cmd"
            echo "Retrying: $retries"
            sleep 1;
            s3sync "$1" "$2" "$3" "$4" "$(($retries+1))"
        else
            if [ -n "$failed_message" ]; then printf "$failed_message"; fi;
            echo "Was Trying:   $cmd"
            echo "exiting ..."
            handle_early_exit_on_backup
        fi
    fi
}

# Example: s3cp <from> <to>
s3cp() {
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
s3rm() {
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
s3ls() {
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

clear_read_lock() {

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
        elif command -v mcs &> /dev/null && command -v jq &> /dev/null ; then
            if ! mcs cluster set mode --mode readwrite | jq -r tostring ;then
                handle_early_exit_on_backup "\n[X] Failed issuing columnstore BRM lock via cmapi\n"; 
            fi
        elif curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapi_key" --data '{"timeout":20, "mode": "readwrite"}' -k  ; then 
            printf " \n"
        else
            handle_early_exit_on_backup "\n[X] Failed clearing columnstore BRM lock\n" true;
        fi
    fi
    
} 

# handle_ called when certain checks/functionality fails
handle_failed_dependencies() {
    printf "\nDependency Checks Failed: $1"
    alert "$1"
    exit 1;
}

# first argument is the error message
# 2nd argument true = skip clear_read_lock, false= dont skip
handle_early_exit_on_backup() {   
    skip_clear_locks=${2:-false}
    if ! $skip_clear_locks; then clear_read_lock; fi;
    printf "\nBackup Failed: $1\n"
    alert "$1"
    exit 1;
}

handle_early_exit_on_restore() {
    printf "\nRestore Failed: $1\n"
    alert "$1"
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
    local rsyncInProgress="$(pgrep 'rsync -a' | wc -l )";
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
        rsyncInProgress="$(pgrep 'rsync -a' | wc -l )";
        ((w++))
    done
}

initiate_rsyncs() {
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
deepParallelRsync() {
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

human_readable_time() {
    local total_seconds=$1
    local days=$((total_seconds / 86400))
    local hours=$(( (total_seconds % 86400) / 3600 ))
    local minutes=$(( (total_seconds % 3600) / 60 ))
    local seconds=$(( total_seconds % 60 ))

    local output=""

    # Special case for 0 seconds
    if (( total_seconds == 0 )); then
        printf "<1 sec"
        return
    fi

    if (( days > 0 )); then
        output+=" ${days} days"
    fi
    if (( hours > 0 )); then
        output+=" ${hours} hours"
    fi
    if (( minutes > 0 )); then
        output+=" ${minutes} min"
    fi
    if (( seconds > 0 || total_seconds == 0 )); then
        output+=" ${seconds} sec"
    fi

    printf "${output}" | xargs
}

run_backup() {  
    backup_start=$(date +%s) 
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
            columnstore_backup_start=$(date +%s)
            i=1
            while [ $i -le $DBROOT_COUNT ]; do
                if [[ $ASSIGNED_DBROOT == "$i" || $HA == true ]]; then 
                    if [[ -n "$compress_format" ]]; then
                        # For compression keep track of dirs & files to include and compress/stream in the end - $compress_paths
                       compress_paths="/var/lib/columnstore/data$i/* "  

                    elif $parrallel_rsync ; then 
                        printf " - Parallel Rsync CS Data$i...       \n"
                        initiate_rsyncs $i
                        columnstore_backup_end=$(date +%s)
                        printf " Done $(human_readable_time $((columnstore_backup_end-columnstore_backup_start))) \n"       
                    else
                        printf " - Syncing Columnstore Data$i...      "
                        eval "rsync $additional_rysnc_flags /var/lib/columnstore/data$i/* $backup_location$today/data$i/ $xtra_cmd_args";
                        columnstore_backup_end=$(date +%s)
                        printf " Done $(human_readable_time $((columnstore_backup_end-columnstore_backup_start))) \n" 

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
                mariadb_backup_start=$(date +%s)
                if ! $skip_mdb; then
                    if [[ -n "$compress_format" ]]; then
                        printf " - Compressing MariaDB Data...       "
                        mbd_prefix="$backup_location$today/$split_file_mdb_prefix.$compress_format"
                        case $compress_format in
                            pigz)
                                # Handle Cloud 
                                mariadb_backup_start=$(date +%s)
                                if !  mariabackup --user=root --backup --slave-info --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $mbd_prefix 2>> $logfile; then
                                    handle_early_exit_on_backup "\nFailed  mariabackup --user=root --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $mbd_prefix 2>> $logfile \n"
                                fi
                                mariadb_backup_end=$(date +%s)
                                printf " Done @ $mbd_prefix\n"
                                ;;
                            *)  # unknown option
                                handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
                        esac
                    else
                        printf " - Copying MariaDB Data...           "
                        if eval "mariadb-backup --backup --slave-info --target-dir=$backup_location$today/mysql --user=root $xtra_cmd_args" ; then  
                            mariadb_backup_end=$(date +%s)
                            printf " Done $(human_readable_time $((mariadb_backup_end-mariadb_backup_start))) \n" 
                        else 
                            handle_early_exit_on_backup "\n Failed: mariadb-backup --backup --target-dir=$backup_location$today/mysql --user=root\n"; 
                        fi
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
                    if [ -f $CS_CONFIGS_PATH/cmapi_server.conf ]; then cp $CS_CONFIGS_PATH/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup; fi;
                    compress_paths+=" $CS_CONFIGS_PATH/$col_xml_backup $STORAGEMANGER_CNF $CS_CONFIGS_PATH/$cmapi_backup"
                fi;
            fi

            # Backup MariaDB configurations
            if ! $skip_mdb; then
                if [[ -z "$compress_format" ]]; then
                    printf " - Copying MariaDB Configs...        "
                    mkdir -p $backup_location$today/configs/mysql/$pm/
                    eval "rsync $additional_rysnc_flags $MARIADB_SERVER_CONFIGS_PATH/* $backup_location$today/configs/mysql/$pm/ $xtra_cmd_args"
                    printf " Done\n"
                else
                    compress_paths+=" $MARIADB_SERVER_CONFIGS_PATH/*"
                fi

                # Backup mariadb server replication details
                slave_status=$(mariadb -e "show slave status\G" 2>/dev/null)
                if [[ $? -eq 0 ]]; then
                    if [[ -n "$slave_status" ]]; then
                        if [[ -z "$compress_format" ]]; then
                            mariadb -e "show slave status\G" | grep -E "Master_Host|Master_User|Master_Port|Gtid_IO_Pos" > $backup_location$today/configs/mysql/$pm/replication_details.txt
                        else
                            mariadb -e "show slave status\G" | grep -E "Master_Host|Master_User|Master_Port|Gtid_IO_Pos" > replication_details.txt
                            compress_paths+=" replication_details.txt"
                        fi
                    fi
                else
                    handle_early_exit_on_backup "[!] - Failed to execute the MariaDB command:   mariadb -e 'show slave status\G' \n"
                fi
            fi

            # Handle compression for Columnstore Data & Configs
            if [[ -n "$compress_format" ]]; then
                cs_prefix="$backup_location$today/$split_file_cs_prefix.$compress_format"
                compressed_split_size="250M";
                case $compress_format in
                    pigz) 
                        printf " - Compressing CS Data & Configs...  "
                        columnstore_backup_start=$(date +%s)
                        if ! eval "tar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $cs_prefix 2>> $logfile"; then
                            handle_early_exit_on_backup "[!] - Compression Failed \ntar cf - $compress_paths 2>>$logfile | pigz -p $PARALLEL_THREADS -c > $cs_prefix 2>> $logfile \n"
                        fi
                        columnstore_backup_end=$(date +%s)

                        # Create summary info for list backup
                        get_latest_em_from_dbrm_directory "$DBRM_PATH"
                        em_file_name=$(basename "$em_file_full_path")
                        version_prefix=${em_file_name::-3}
                        journal_file=$(ls -la "${subdir_dbrms}/BRM_saves_journal" 2>/dev/null | awk 'NR==1 {print $5}' )
                        vbbm_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vbbm" 2>/dev/null | awk 'NR==1 {print $5}' )
                        vss_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vss" 2>/dev/null | awk 'NR==1 {print $5}' )
                        echo "em_file_created: $em_file_created" >> $backup_location$today/backup_summary_info.txt
                        echo "em_file_name: $em_file_name" >> $backup_location$today/backup_summary_info.txt
                        echo "em_file_size: $em_file_size" >> $backup_location$today/backup_summary_info.txt
                        echo "journal_file: $journal_file" >> $backup_location$today/backup_summary_info.txt
                        echo "vbbm_file: $vbbm_file" >> $backup_location$today/backup_summary_info.txt
                        echo "vss_file: $vss_file" >> $backup_location$today/backup_summary_info.txt                        
                        echo "compression: $compress_format" >> $backup_location$today/backup_summary_info.txt                        

                        if [ -f replication_details.txt ]; then 
                            rm -rf replication_details.txt; 
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
                echo "$0 restore -l $today -bl $backup_location -bd $backup_destination -s $storage --dbroots $DBROOT_COUNT -m $mode $extra_flags --quiet" > $backup_location$today/restore.job
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
                if [ -f $CS_CONFIGS_PATH/cmapi_server.conf ]; then  
                    rsync $additional_rysnc_flags $CS_CONFIGS_PATH/cmapi_server.conf $scp:$backup_location$today/configs/; 
                fi
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

        printf "\nS3 Backup\n"
        # Conconsistency check - wait for assigned journal dir to be empty
        trap handle_ctrl_c_backup SIGINT
        i=1
        j_counts=$(find $cs_journal/data$ASSIGNED_DBROOT/* -type f 2>/dev/null | wc -l)
        max_wait=180
        printf " - Checking storagemanager/journal/data$ASSIGNED_DBROOT/*           "
        while [[ $j_counts -gt 0 ]]; do

            # Handle when max_wait exceeded
            if [ $i -gt $max_wait ]; then 
                printf "[!] Maybe you have orphaned journal files, active writes or an unreachable bucket \n"; 
                handle_early_exit_on_backup "\n[!] max_wait exceeded for $cs_journal/data$ASSIGNED_DBROOT/* to sync with bucket "; 
                ls -la $cs_journal/data$ASSIGNED_DBROOT/*; 
            fi;
        
            # Print every 10 seconds
            if (( $i%10 == 0 )); then 
                printf "\n[!] Not empty yet - found $j_counts files @ $cs_journal/data$ASSIGNED_DBROOT/*\n"; 
                printf " - Checking storagemanager/journal/data$ASSIGNED_DBROOT/*           "; 
            fi;

            sleep 1
            i=$(($i+1))
            j_counts=$(find $cs_journal/data$ASSIGNED_DBROOT/* -type f 2>/dev/null | wc -l)
            printf "."
        done
        printf " Done\n"

        # Backup storagemanager - columnstore s3 metadata, journal, cache
        if [[ -z "$compress_format" ]]; then

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
            if [[ -z "$compress_format" ]]; then
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
                                if ! mariabackup --user=root --backup --slave-info --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter="gsutil cp - ${mbd_prefix}_\$FILE 2>$logfile" - chunk 2>$logfile; then
                                    handle_early_exit_on_backup "\nFailed mariadb-backup --backup --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter=\"gsutil cp - ${mbd_prefix}_\$FILE 2>$logfile\" - chunk 2>$logfile \n"
                                fi
                                
                            elif [ $cloud == "aws" ]; then
                                if ! mariabackup --user=root --backup --slave-info --stream xbstream --parallel $PARALLEL_THREADS --ftwrl-wait-timeout=$timeout --ftwrl-wait-threshold=999999 --extra-lsndir=/tmp/checkpoint_out 2>>$logfile | pigz -p $PARALLEL_THREADS 2>> $logfile | split -d -a 5 -b 250M --filter="aws s3 cp - ${mbd_prefix}_\$FILE 2>$logfile 1>&2" - chunk 2>$logfile; then
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
                        if eval "mariabackup --user=root --backup --slave-info --target-dir=$backup_location$today/mysql/ $xtra_cmd_args"; then
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
            if [[ -z "$compress_format" ]]; then
                printf " - Syncing MariaDB configurations \n"
                s3sync $MARIADB_SERVER_CONFIGS_PATH/ $backup_bucket/$today/configs/mysql/$pm/ "   + $MARIADB_SERVER_CONFIGS_PATH/\n" "\n\n[!!!] sync failed - try alone and debug\n\n"
            else
                compress_paths+="$MARIADB_SERVER_CONFIGS_PATH"
            fi

            # Backup mariadb server replication details
            slave_status=$(mariadb -e "show slave status\G" 2>/dev/null)
            if [[ $? -eq 0 ]]; then
                if [[ -n "$slave_status" ]]; then
                    mariadb -e "show slave status\G" | grep -E "Master_Host|Master_User|Master_Port|Gtid_IO_Pos" > replication_details.txt
                    if [[ -z "$compress_format" ]]; then
                        s3cp replication_details.txt $backup_bucket/$today/configs/mysql/$pm/replication_details.txt
                    else    
                        compress_paths+=" replication_details.txt"
                    fi
                fi
            else
                handle_early_exit_on_backup "[!] - Failed to execute the MariaDB command:   mariadb -e 'show slave status\G' \n"
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
                    
                    # Cleanup temp files
                    if [ -f replication_details.txt ]; then
                        rm -rf replication_details.txt
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
            if [ -n "$s3_url" ];  then extra_flags+=" -url $s3_url"; fi;
            echo "$0 restore -l $today -s $storage -bb $backup_bucket -dbs $DBROOT_COUNT -m $mode -nb $protocol://$bucket $extra_flags --quiet --continue"  > restoreS3.job
            s3cp restoreS3.job $backup_bucket/$today/restoreS3.job
            rm -rf restoreS3.job
        fi
        final_message+=" @ $backup_bucket/$today"
    
    fi
    
    clear_read_lock
    end=$(date +%s)
    total_runtime=$((end - start))
    other=$total_runtime
    
    printf "\nRuntime Summary\n"
    echo "--------------------"
    if [[ -n "$applying_retention_end" && $applying_retention_end -ne 0 ]]; then
        applying_retention_time=$((applying_retention_end - applying_retention_start))
        printf "%-20s %-15s\n" "Applying Retention:" "$(human_readable_time $applying_retention_time)"
        other=$((other - applying_retention_time))
    fi

    if [[ -n "$poll_active_sql_writes_end" && $poll_active_sql_writes_end -ne 0 ]]; then
        poll_sql_writes_time=$((poll_active_sql_writes_end - poll_active_sql_writes_start))
        printf "%-20s %-15s\n" "Polling SQL Writes:" "$(human_readable_time $poll_sql_writes_time)"
        other=$((other - poll_sql_writes_time))
    fi
        
    if [[ -n "$poll_active_cpimports_end" && $poll_active_cpimports_end -ne 0 ]]; then
        poll_cpimports_time=$((poll_active_cpimports_end - poll_active_cpimports_start))
        printf "%-20s %-15s\n" "Polling cpimports:" "$(human_readable_time $poll_cpimports_time)"
        other=$((other - poll_cpimports_time)) 
    fi

    if [[ -n "$columnstore_backup_end" && $columnstore_backup_end -ne 0 ]]; then
        columnstore_backup_time=$((columnstore_backup_end - columnstore_backup_start))
        printf "%-20s %-15s\n" "Columnstore Backup:" "$(human_readable_time $columnstore_backup_time)"
        other=$((other - columnstore_backup_time))  
    fi

    if [[ -n "$mariadb_backup_end" && $mariadb_backup_end -ne 0 ]]; then
        mariadb_backup_time=$((mariadb_backup_end - mariadb_backup_start))
        printf "%-20s %-15s\n" "MariaDB Backup:" "$(human_readable_time $mariadb_backup_time)" 
        other=$((other - mariadb_backup_time))  
    fi

    if (( other < 0 )); then
        printf "%-20s %-15s\n" "Other:" "N/A (invalid data)"
    else
        if [ $other -gt 0 ]; then
            printf "%-20s %-15s\n" "Other:" "<$(human_readable_time $other)"
        else
            printf "%-20s %-15s\n" "Other:" "<1 sec"
        fi
    fi

    printf "\nTotal Runtime: $(human_readable_time $((end-start))) \n"
    printf "$final_message\n\n"
}

load_default_restore_variables() {
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

    # Name of the Configuration file to load variables from - relative or full path accepted
    config_file=""

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
    list_backups=false
}

parse_restore_variables() {
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
            -bl|--backup-location)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -bd|--backup-destination)
                backup_destination="$2"
                shift # past argument
                shift # past value
                ;;
            -scp|--secure-copy-protocol)
                scp="$2"
                shift # past argument
                shift # past value
                ;;
            -bb|--backup-bucket)
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
            -nb | --new-bucket)
                new_bucket="$2"
                shift # past argument
                shift # past value
                ;;
            -nr | --new-region)
                new_region="$2"
                shift # past argument
                shift # past value
                ;;
            -nk | --new-key)
                new_key="$2"
                shift # past argument
                shift # past value
                ;;
            -ns | --new-secret)
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
            -cont| --continue)
                continue=true
                shift # past argument
                ;;
            -f|--config-file)
                config_file="$2"
                if [ "$config_file" == "" ] || [ ! -f "$config_file" ]; then
                    handle_early_exit_on_restore "\n[!!!] Invalid Configuration File: $config_file\n"
                fi  
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
            -nv-ssl| --no-verify-ssl)
                no_verify_ssl=true 
                shift # past argument
                ;;
            -li | --list)
                list_backups=true
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
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        if ! source "$config_file"; then
            handle_early_exit_on_restore "\n[!!!] Failed to source configuration file: $config_file\n" true
        fi
    fi
}

print_restore_help_text()
{
    echo "
    Columnstore Restore

        -l   | --load                   What backup to load
        -bl  | --backup-location        Directory where the backup was saved
        -bd  | --backup-destination     If the directory is 'Local' or 'Remote' to this script
        -dbs | --dbroots                Number of database roots in the backup
        -scp | --secure-copy-protocol   scp connection to remote server if -bd 'Remote'
        -bb  | --backup_bucket          Bucket name for where to find the S3 backups
        -url | --endpoint-url           Onprem url to s3 storage api example: http://127.0.0.1:8000
        -nv-ssl| --no-verify-ssl)       Skips verifying ssl certs, useful for onpremise s3 storage
        -s   | --storage                The storage used by columnstore data 'LocalStorage' or 'S3'
        -pm  | --nodeid                 Forces the handling of the restore as this node as opposed to whats detected on disk
        -nb  | --new-bucket             Defines the new bucket to copy the s3 data to from the backup bucket. 
                                        Use -nb if the new restored cluster should use a different bucket than the backup bucket itself
        -nr  | --new-region             Defines the region of the new bucket to copy the s3 data to from the backup bucket   
        -nk  | --new-key                Defines the aws key to connect to the new_bucket  
        -ns  | --new-secret             Defines the aws secret of the aws key to connect to the new_bucket  
        -f   | --config-file            Path to backup configuration file to load variables from - see load_default_restore_variables() for defaults
        -cont| --continue               This acknowledges data in your --new_bucket is ok to delete when restoring S3
        -smdb| --skip-mariadb-backup    Skip restoring mariadb server via mariadb-backup - ideal for only restoring columnstore
        -sb  | --skip-bucket-data       Skip restoring columnstore data in the bucket - ideal if looking to only restore mariadb server
        -q   | --quiet                  Silence verbose copy command outputs
        -c   | --compress               Hint that the backup is compressed in X format - Options: [ pigz ]
        -li  | --list                   List available backups in the backup location
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

    # Hidden flags
    # -m | --mode           Options ['direct','indirect'] - direct backups run on the columnstore nodes themselves. indirect run on another machine that has read-only mounts associated with columnstore/mariadb

}

print_restore_variables()
{

    printf "\nRestore Variables\n"
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        printf "%-${s1}s  %-${s2}s\n" "Configuration File:" "$config_file";
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
    if [ -n "$s3_url" ];  then add_s3_api_flags+=" --endpoint-url $s3_url"; fi
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
        if [ -z $(pidof PrimProc) ]; then 
            printf " - Columnstore Status ...               Offline\n"; 
        else 
            handle_early_exit_on_restore "\n[X] Columnstore is ONLINE - please turn off \n\n";  
        fi
        
        if [ -z $(pidof mariadbd) ]; then 
            printf " - MariaDB Server Status ...            Offline\n"; 
        else 
            handle_early_exit_on_restore "\n[X] MariaDB is ONLINE - please turn off \n\n"; 
        fi
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
        if ! sudo mcs cmapi is-ready &> /dev/null ; then 
            printf " - Columnstore Management API Status .. Offline\n"; 
        else 
            handle_early_exit_on_restore "\n[X] Cmapi is ONLINE - please turn off \n\n"; 
        fi
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
            if ! $continue; then nb_contents=$($check3 | wc -l); 
                if [ $nb_contents -lt 1 ]; then 
                    echo " - New Bucket is empty"; 
                else 
                    echo "[!!!] New bucket is NOT empty... $nb_contents files exist... exiting"; 
                    echo "add "--continue" to skip this exit"; 
                    echo -e "\nExample empty bucket command:\n     aws s3 rm $new_bucket --recursive\n     gsutil -m rm -r $new_bucket/* \n"; 
                    handle_early_exit_on_restore "Please empty bucket or add --continue \n"; 
                fi; 
            else 
                echo " - [!] Skipping empty new_bucket check"; 
            fi; 
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
                if [ -f "$backup_location$load_date/configs/cmapi_server.conf" ]; then 
                    rsync $rsync_flags $backup_location$load_date/configs/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup ; 
                fi;
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
                if [[ $pm == "pm$i" || $HA == true ]]; then 
                    rm -rf /var/lib/columnstore/data$i/; 
                    rsync -av $scp:$backup_location$load_date/data$i/ /var/lib/columnstore/data$i/ ; 
                    chown $columnstore_user:$columnstore_user -R /var/lib/columnstore/data$i/;
                fi
                ((i++))
            done
            printf "[+] Columnstore Data... Done\n" 

            # Put configs in place
            printf " - Columnstore Configs ...             "
            rsync $rsync_flags $backup_location$load_date/configs/storagemanager.cnf $STORAGEMANGER_CNF
            rsync $rsync_flags $backup_location$load_date/configs/Columnstore.xml $CS_CONFIGS_PATH/$col_xml_backup  
            rsync $rsync_flags $backup_location$load_date/configs/mysql/$pm/ $MARIADB_SERVER_CONFIGS_PATH/ 
            if [ -f "$backup_location$load_date/configs/cmapi_server.conf" ]; then 
                rsync $rsync_flags $backup_location$load_date/configs/cmapi_server.conf $CS_CONFIGS_PATH/$cmapi_backup ; 
            fi;
            printf " Done\n"
            load_date=$tmp
        else 
            handle_early_exit_on_restore "Invalid Script Variable --backup_destination: $backup_destination"
        fi

        if ! $skip_mdb; then
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
                        if eval "mariabackup --prepare --target-dir=$MARIADB_PATH $xtra_cmd_args"; then 
                            printf " Done\n"; 
                        else 
                            handle_early_exit_on_restore "Failed to --prepare\nmariabackup --prepare --target-dir=$MARIADB_PATH"; 
                        fi; 
                        ;;
                    *)  # unknown option
                        handle_early_exit_on_backup "\nUnknown compression flag: $compress_format\n" 
                esac
            else
                eval "mariabackup --prepare --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"
                rm -rf /var/lib/mysql/*
                eval "mariabackup --copy-back --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"
            fi
        else
            echo "[!] Skipping mariadb server restore"
        
        fi;
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
        if [ -n "$new_bucket" ]; then 
            target_bucket=$new_bucket;  
            target_prefix="# prefix \= cs\/";  
        fi

        if [ -n "$new_region" ];  then 
            if [ $cloud == "gcp" ]; then endpoint="storage.googleapis.com"; else endpoint="s3.$new_region.amazonaws.com"; fi;
            sed -i "s|^endpoint =.*|endpoint = ${endpoint}|g" $STORAGEMANGER_CNF; 
            sed -i "s|^region =.*|region = ${new_region}|g" $STORAGEMANGER_CNF; 
            echo " - Adjusted endpoint & region";
        fi

        # Removes prefix of the protocol and escapes / for sed
        target_bucket_name=$(echo "${target_bucket#$protocol://}" | sed "s/\//\\\\\//g")
        if [ -n "$new_key" ];  then 
            sed -i "s|aws_access_key_id =.*|aws_access_key_id = ${new_key}|g" $STORAGEMANGER_CNF; 
            echo " - Adjusted aws_access_key_id";  
        fi

        if [ -n "$new_secret" ];  then 
            sed -i "s|aws_secret_access_key =.*|aws_secret_access_key = ${new_secret}|g" $STORAGEMANGER_CNF; 
            echo " - Adjusted aws_secret_access_key"; 
        fi

        bucket=$( grep -m 1 "^bucket =" $STORAGEMANGER_CNF | sed "s/\//\\\\\//g"); 
        sed -i "s/$bucket/bucket = $target_bucket_name/g" $STORAGEMANGER_CNF;
        echo " - Adjusted bucket";
        
        prefix=$( grep -m 1 "^prefix =" $STORAGEMANGER_CNF | sed "s/\//\\\\\//g");
        if [ ! -z "$prefix" ]; then 
            sed -i "s/$prefix/$target_prefix/g" $STORAGEMANGER_CNF; 
            echo " - Adjusted prefix"; 
        fi;

        # Check permissions
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
                if eval "mariabackup --prepare --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"; then 
                    printf " Done\n"; 
                else 
                    echo "failed"; 
                fi;
                rm -rf /var/lib/mysql/* > /dev/null
                printf " - Running mariabackup --copy-back ...               "
                if eval "mariabackup --copy-back --target-dir=$backup_location$load_date/mysql/ $xtra_cmd_args"; then 
                    printf " Done\n"; 
                else 
                    echo "failed"; 
                fi;
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
    
    end=$(date +%s)
    runtime=$((end-start))
    printf "\nRuntime: $runtime\n"
    printf "$final_message\n\n"
    if ! $quiet; then 
        if [ $pm == "pm1" ]; then 
            echo -e  "Next: "
            if [ $DBROOT_COUNT -gt 1 ]; then  echo -e  "[!!] Check Replicas to manually configure mariadb replication"; fi
            echo -e  "systemctl start mariadb "
            echo -e  "systemctl start mariadb-columnstore-cmapi "
            echo -e  "mcs cluster start"
            echo -e  "mariadb -e \"show variables like '%gtid_current_pos%';\"\n\n"
        else
            echo -e  "[!!] Check this replica to manually configure mariadb replication after primary node is ready"
            echo -e  "Example:"
            echo -e  "mariadb -e \"SET GLOBAL gtid_slave_pos =  '0-1-14'\""
            echo -e  "mariadb -e \"stop slave;CHANGE MASTER TO MASTER_HOST='\$pm1' , MASTER_USER='repuser' , MASTER_PASSWORD='aBcd123%123%aBc' , MASTER_USE_GTID = slave_pos;start slave;show slave status\G\""
            echo -e  "mariadb -e  \"show slave status\G\" | grep  \"Slave_\" \n"
        fi
    fi
}

load_default_dbrm_variables() {
     # Default variables
    backup_base_name="dbrm_backup"
    backup_interval_minutes=90
    retention_days=0
    backup_location=/tmp/dbrm_backups
    STORAGEMANGER_CNF="/etc/columnstore/storagemanager.cnf"
    storage=$(grep -m 1 "^service = " $STORAGEMANGER_CNF | awk '{print $3}')
    skip_storage_manager=false
    mode="once"
    quiet=false
    
    list_dbrm_backups=false
    dbrm_dir="/var/lib/columnstore/data1/systemFiles/dbrm"
    if [ "$storage" == "S3" ]; then 
        dbrm_dir="/var/lib/columnstore/storagemanager"
    fi
}

load_default_dbrm_restore_variables() {
    auto_start=true
    backup_location="/tmp/dbrm_backups"
    STORAGEMANGER_CNF="/etc/columnstore/storagemanager.cnf"
    storage=$(grep -m 1 "^service = " $STORAGEMANGER_CNF | awk '{print $3}')
    backup_folder_to_restore=""
    skip_dbrm_backup=false
    skip_storage_manager=false
    list_dbrm_backups=false

    dbrm_dir="/var/lib/columnstore/data1/systemFiles/dbrm"
    if [ "$storage" == "S3" ]; then 
        dbrm_dir="/var/lib/columnstore/storagemanager"
    fi
}

print_dbrm_backup_help_text() {
    echo "
    Columnstore DBRM Backup

        -m   | --mode                  ['loop','once']; Determines if this script runs in a forever loop sleeping -i minutes or just once 
        -i   | --interval              Number of minutes to sleep when --mode loop 
        -r   | --retention-days        Retain dbrm backups created within the last X days, the rest are deleted
        -bl  | --backup-location       Path of where to save the dbrm backups on disk
        -nb  | --name-backup           Define the prefix of the backup - default: dbrm_backup+date +%Y%m%d_%H%M%S
        -ssm | --skip-storage-manager  skip backing up storagemanager directory
        -li  | --list                  List available dbrm backups in the backup location

        Default: ./$0 dbrm_backup -m once --retention-days 0 --backup-location /tmp/dbrm_backups

        Examples:
            ./$0 dbrm_backup --list
            ./$0 dbrm_backup --backup-location /mnt/columnstore/dbrm_backups 
            ./$0 dbrm_backup --retention-days 7 --backup-location /mnt/dbrm_backups --mode once -nb my-one-off-backup-before-upgrade
            ./$0 dbrm_backup --retention-days 7 --backup-location /mnt/dbrm_backups --mode loop --interval 90
            
        Cron Example:
        */60 */3 * * * root  bash /root/$0 dbrm_backup -m once --retention-days 7 --backup-location /tmp/dbrm_backups  >> /tmp/dbrm_backups/cs_backup.log  2>&1
    ";
}

print_dbrm_restore_help_text() {
    echo "
    Columnstore DBRM Restore

        -bl  | --backup-location       Path of where the dbrm backups exist on disk
        -l   | --load                  Name of the directory to restore from -bl
        -ns  | --no-start              Do not attempt columnstore startup post dbrm_restore
        -sdbk| --skip-dbrm-backup      Skip backing up dbrms brefore restoring
        -ssm | --skip-storage-manager  Skip restoring storagemanager directory
        -li  | --list                  List available dbrm backups in the backup location

        Default: ./$0 dbrm_restore --backup-location /tmp/dbrm_backups 

        Examples:
            ./$0 dbrm_restore --list
            ./$0 dbrm_restore --backup-location /tmp/dbrm_backups --load dbrm_backup_20240318_172842    
            ./$0 dbrm_restore --backup-location /tmp/dbrm_backups --load dbrm_backup_20240318_172842 --no-start
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
            -r|--retention-days)
                retention_days="$2"
                shift # past argument
                shift # past value
                ;;
            -bl|--backup-location)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -m|--mode)
                mode="$2"
                shift # past argument
                shift # past value
                ;;
            -nb|--name-backup)
                backup_base_name="$2"
                shift # past argument
                shift # past value
                ;;
            -ssm|--skip-storage-manager)
                skip_storage_manager=true
                shift # past argument
                ;;
            -q | --quiet)
                quiet=true
                shift # past argument
                ;;
            -li | --list)
                list_dbrm_backups=true
                shift # past argument
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

    confirm_integer_else_fail "$retention_days" "Retention"
}

parse_dbrm_restore_variables() {

    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            dbrm_restore)
                shift # past argument
                ;;
            -bl|--backup-location)
                backup_location="$2"
                shift # past argument
                shift # past value
                ;;
            -l|--load)
                backup_folder_to_restore="$2"
                shift # past argument
                shift # past value
                ;;
            -ns  | --no-start)
                auto_start=false
                shift # past argument
                ;;
            -sdbk| --skip-dbrm-backup)
                skip_dbrm_backup=true
                shift # past argument
                ;;
            -ssm|--skip-storage-manager)
                skip_storage_manager=true
                shift # past argument
                ;;
            -li | --list)
                list_dbrm_backups=true
                shift # past argument
                ;;
            -h|--help|-help|help)
                print_dbrm_restore_help_text;
                exit 1;
                ;;
            *)  # unknown option
                printf "\nunknown flag: $1\n"
                print_dbrm_restore_help_text
                exit 1;
        esac
    done

    if [[ "${backup_location: -1}" == "/" ]]; then
        # Remove the final /
        backup_location="${backup_location%/}"
    fi
}

confirm_integer_else_fail() {
    local input="$1"
    local variable_name="$2"

    # Regular expression to match integer values
    if [[ $input =~ ^[0-9]+$ ]]; then
        return 0
    else
        printf "[!] $variable_name = '$input' is not an integer.\n\n"
        alert "[!] $variable_name = '$input' is not an integer.\n\n"
        exit 2;
    fi
}

confirm_numerical_or_decimal_else_fail() {
    local input="$1"
    local variable_name="$2"

    # Regular expression to match numerical or decimal values
    if [[ $input =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        return 0
    else
        printf "[!] $variable_name = '$input' is not numerical or decimal.\n\n"
        alert "[!] $variable_name = '$input' is not numerical or decimal.\n\n"
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
    confirm_numerical_or_decimal_else_fail "$backup_interval_minutes" "Interval"

    # Check backup location exists
    if [ ! -d $backup_location ]; then 
        echo "Created: $backup_location"
        mkdir "$backup_location"; 
    fi;

    # Confirm bucket connection
    if [ "$storage" == "S3" ]; then
        if ! testS3Connection 1>/dev/null 2>/dev/null; then
            printf "\n[!] Failed testS3Connection\n\n"
            exit 1;
        fi
    fi;
}

validation_prechecks_before_listing_restore_options() {

    # confirm backup directory exists
    if [ ! -d $backup_location ]; then
        printf "[!!] Backups Directory does NOT exist --backup-location $backup_location \n"
        printf "ls -la $backup_location\n\n"
        exit 1;
    fi
    
    # Check if backup directory is empty
    if [ -z "$(find "$backup_location" -mindepth 1 | head )" ]; then
        printf "[!!] Backups Directory is empty --backup-location $backup_location \n"
        printf "ls -la $backup_location\n\n"
        exit 1
    fi
}

validation_prechecks_for_dbrm_restore() {
    
    printf "Prechecks\n"
    echo "--------------------------------------------------------------------------"
    # Confirm storage not empty
    if [ -z "$storage" ]; then printf "[!] Empty storage: \ncheck: grep -m 1 \"^service = \" \$STORAGEMANGER_CNF | awk '{print \$3}' \n\n"; fi;

    # Check backup directory exists
    if [ ! -d $backup_location ]; then 
        printf "[!] \$backup_location: Path of backups does Not exist\n"
        printf "Path: $backup_location\n\n"
        exit 2;
    fi

    # Check specific backup exists 
    backup_folder_to_restore_dbrms=$backup_folder_to_restore
    if [ $storage == "S3" ]; then
        backup_folder_to_restore_dbrms="${backup_folder_to_restore}/dbrms"
    fi

    if [ ! -d "${backup_location}/${backup_folder_to_restore_dbrms}" ]; then 
        printf "[!] --load: Path of backup to restore does NOT exist\n"
        printf "Path: ${backup_location}/${backup_folder_to_restore_dbrms}\n\n"
        exit 2;
    else   
        echo " - Backup directory exists"
        if [ "$(ls -A ${backup_location}/${backup_folder_to_restore_dbrms})" ]; then

            expected_files=(
                "BRM_saves_current"
                "BRM_saves_em"
                "BRM_saves_journal"
                "BRM_saves_vbbm"
                "BRM_saves_vss"
                "BRM_savesA_em"
                "BRM_savesA_vbbm"
                "BRM_savesA_vss"
                "BRM_savesB_em"
                "BRM_savesB_vbbm"
                "BRM_savesB_vss"
                "oidbitmap"
                "SMTxnID"
            )
            
            # Check if all expected files exist
            for file in "${expected_files[@]}"; do
                if [ ! -f "${backup_location}/${backup_folder_to_restore_dbrms}/${file}" ]; then
                    printf "[!] File not found: ${file} in the DBRM backup directory\n"
                    printf "Path: ${backup_location}/${backup_folder_to_restore_dbrms}/${file}\n\n"
                    exit 2;
                fi
            done

            # For S3 check storagemanager dir exists in backup unless skip storagemanager is passed
            if [ "$storage" == "S3" ] && [ $skip_storage_manager == false ]; then
                if [ ! -d "${backup_location}/${backup_folder_to_restore}/metadata" ]; then 
                    printf "\n[!!] Path Not Found: ${backup_location}/${backup_folder_to_restore}/metadata \n"
                    printf "Retry with a different backup to restore or use flag --skip-storage-manager\n\n"
                    exit 2;
                fi;
            fi
            

            printf " - Backup contains all files\n"

        else
            printf "[!] No files found in the DBRM backup directory\n"
            printf "Path: ${backup_location}/${backup_folder_to_restore_dbrms}\n\n"
            exit 2;
        fi
    fi

    # Confirm bucket connection
    if [ "$storage" == "S3" ]; then
        if testS3Connection 1>/dev/null 2>/dev/null; then
            echo " - S3 Connection works"
        else
            printf "\n[!] Failed testS3Connection\n\n"
            exit 1;
        fi
    fi;

    # Download cs_package_manager.sh if not exists
    if [ ! -f "cs_package_manager.sh" ]; then
        wget https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/develop/cmapi/scripts/cs_package_manager.sh; chmod +x cs_package_manager.sh;
    fi; 
    if source cs_package_manager.sh source ;then 
        echo " - Sourced cs_package_manager.sh"
    else
        printf "\n[!!] Failed to source cs_package_manager.sh\n\n"
        exit 1;
        
    fi

    # Confirm the function exists and the source of cs_package_manager.sh worked
    if command -v check_package_managers &> /dev/null; then
        # The function exists, call it
        check_package_managers
    else
        echo "Error: 'check_package_managers' function not found via cs_package_manager.sh";
        exit 1;
    fi
    cs_package_manager_functions=(
        "start_cmapi"
        "start_mariadb"
        "init_cs_up"
    )

    for func in "${cs_package_manager_functions[@]}"; do
        if command -v $func &> /dev/null; then
            continue;
        else
            echo "Error: '$func' function not found via cs_package_manager.sh";
            exit 1;
        fi
    done
}

print_if_not_quiet() {
    if ! $quiet; then
        printf "$1"; 
    fi
}

process_dbrm_backup() {

    load_default_dbrm_variables
    parse_dbrms_variables "$@";

    handle_list_dbrm_backups

    if ! $quiet ; then

        printf "\nDBRM Backup\n"; 
        echo "--------------------------------------------------------------------------"
        if [ "$storage" == "S3" ]; then echo "Skips:  Storagemanager($skip_storage_manager)"; fi;
        echo "--------------------------------------------------------------------------"
        printf "CS Storage:  $storage\n"; 
        printf "Source:      $dbrm_dir\n"; 
        printf "Backups:     $backup_location\n"; 
        if [ "$mode" == "loop" ]; then 
            printf "Interval:    $backup_interval_minutes minutes\n"; 
        fi;
        printf "Retention:   $retention_days day(s)\n"
        printf "Mode:        $mode\n"
        echo "--------------------------------------------------------------------------"
    fi;

    validation_prechecks_for_dbrm_backup

    sleep_seconds=$((backup_interval_minutes * 60));
    while true; do
        # Create a new backup directory
        timestamp=$(date +%Y%m%d_%H%M%S)
        backup_folder="$backup_location/${backup_base_name}_${timestamp}"
        mkdir -p "$backup_folder"
        
        # Copy files to the backup directory
        if [[ $skip_storage_manager == false || $storage == "LocalStorage" ]]; then
            print_if_not_quiet " - copying $dbrm_dir ...";
            cp -arp "$dbrm_dir"/* "$backup_folder"
            print_if_not_quiet " Done\n";
        fi

        if [ "$storage" == "S3" ]; then
            # smcat em files to disk
            print_if_not_quiet " - copying DBRMs from bucket ...";
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
            smcat /data1/systemFiles/dbrm/oidbitmap 2>/dev/null > $backup_folder/dbrms/oidbitmap
            smcat /data1/systemFiles/dbrm/SMTxnID 2>/dev/null > $backup_folder/dbrms/SMTxnID
            smcat /data1/systemFiles/dbrm/tablelocks 2>/dev/null > $backup_folder/dbrms/tablelocks
            print_if_not_quiet " Done\n";
        fi

        if [ $retention_days -gt 0 ] ; then
            # Clean up old backups
            # example: find /tmp/dbrm_backups -maxdepth 1 -type d -name "dbrm_backup_*" -mtime +1 -exec rm -r {} \;
            print_if_not_quiet " - applying retention policy ...";
            find "$backup_location" -maxdepth 1 -type d -name "${backup_base_name}_*" -mtime +$retention_days -exec rm -r {} \;
            print_if_not_quiet " Done\n";
        fi;

        printf "Created: $backup_folder\n"
        
        if [ "$mode" == "once" ]; then  
            end=$(date +%s)
            runtime=$((end-start))
            if ! $quiet; then  printf "\nRuntime: $runtime\n"; fi;
            break;  
        fi;

        printf "Sleeping ... $sleep_seconds seconds\n"
        sleep "$sleep_seconds"
    done
    
    print_if_not_quiet "Complete\n\n"
}

is_cmapi_installed() {

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
        return 0;
    else
        return 1;

    fi;
}

start_mariadb_cmapi_columnstore() {

    start_mariadb
    start_cmapi
    init_cs_up

    # For verbose debugging
    #grep -i rollbackAll /var/log/mariadb/columnstore/debug.log | tail -n 3 | awk '{ print $1, $2, $3, $(NF-2), $(NF-1), $NF }'
   
}

# Currently assumes systemd installed
shutdown_columnstore_mariadb_cmapi() {

    local print_formatting=35
    init_cs_down 
    wait_cs_down 0 

    printf "%-${print_formatting}s ... " " - Stopping MariaDB Server"
    if ! systemctl stop mariadb; then
        echo "[!!] Failed to stop mariadb"
        exit 1;
    else 
        printf "Done\n"
    fi

    if is_cmapi_installed ; then
        printf "%-${print_formatting}s ... " " - Stopping CMAPI"
        if ! systemctl stop mariadb-columnstore-cmapi; then
            echo "[!!] Failed to stop CMAPI"
            exit 1;
        else 
            printf "Done\n"
        fi
    fi
}

# Input
# $1 - directory to search
# Output
#   subdir_dbrms
#   latest_em_file
#   em_file_size
#   em_file_created
#   em_file_full_path
#   storagemanager_dir_exists
get_latest_em_from_dbrm_directory() {
    
    subdir_dbrms=""
    latest_em_file=""
    em_file_size=""
    em_file_created=""
    em_file_full_path=""
    storagemanager_dir_exists=true

    # Find the most recently modified file in the current subdirectory
    if [ $storage == "S3" ]; then
        subdir_dbrms="${1}/dbrms/"
        subdir_metadata="${1}/metadata/data1/systemFiles/dbrm/"

        # Handle missing metadata directory
        if [ ! -d $subdir_dbrms ]; then
            printf "%-45s Missing dbrms sub directory\n" "$(basename $1)"
            return 1;
        fi

        
        if [ -d "${1}/metadata" ]; then
            latest_em_meta_file=$(find "${subdir_metadata}" -maxdepth 1 -type f -name "BRM_saves*_em.meta" -exec ls -lat {} + | awk 'NR==1 {printf "%-12s %-4s %-2s %-5s %s\n", $5, $6, $7, $8, $9}'| head -n 1 )
        else
            # Handle missing metadata directory & guess the latest em file based on the largest size
            
            # Example:     find /tmp/dbrm_backups/dbrm_backup_20240605_180906/dbrms -maxdepth 1 -type f -name "BRM_saves*_em" -exec ls -lhS {} +
            latest_em_meta_file=$(find "${subdir_dbrms}" -maxdepth 1 -type f -name "BRM_saves*_em" -exec ls -lhS  {} + | awk 'NR==1 {printf "%-12s %-4s %-2s %-5s %s\n", $5, $6, $7, $8, $9}'| head -n 1)
            storagemanager_dir_exists=false
        fi

        em_meta_file_name=$(basename "$latest_em_meta_file")
        latest_em_file="$subdir_dbrms$(echo $em_meta_file_name | sed 's/\.meta$//' )"
        em_file_size=$(ls -la "$latest_em_file" | awk '{print $5}' )
        em_file_created=$(echo "$latest_em_meta_file" | awk '{print $2,$3,$4}' )
        em_file_full_path=$latest_em_file

        if [ ! -f $latest_em_file ]; then
            echo "S3 List Option: Failed to find $latest_em_file"
            exit;
        fi        
    else
        subdir_dbrms="$1"
        latest_em_file=$(find "${subdir_dbrms}" -maxdepth 1 -type f -name "BRM_saves*_em" -exec ls -lat {} + 2>/dev/null | awk 'NR==1 {printf "%-12s %-4s %-2s %-5s %s\n", $5, $6, $7, $8, $9}'| head -n 1)
        em_file_size=$(echo "$latest_em_file" | awk '{print $1}' )
        em_file_created=$(echo "$latest_em_file" | awk '{print $2,$3,$4}' )
        em_file_full_path=$(echo $latest_em_file | awk '{print $NF}' )
    fi
}

list_restore_options_from_backups() {

    if [ $storage == "S3" ]; then 
        echo " Not implemented" ; exit 1;
    else 
        
        if [ -n "$retention_days" ] && [ $retention_days -ne 0 ]; then
            echo "--------------------------------------------------------------------------"
            echo "Retention Policy: $retention_days days"
        fi;
        echo "--------------------------------------------------------------------------"
        printf "%-45s %-13s %-15s %-12s %-12s %-10s %-10s %-10s\n" "Options" "EM-Updated" "Extent Map" "EM-Size" "Journal-Size" "VBBM-Size" "VSS-Size" "Backup Days Old"

        # Iterate over subdirectories
        for subdir in "${backup_location}"/*; do
            
            if [ -f "${subdir}/cs-localfiles.tar.pigz" ]; then
                em_file_created=$( date -r "$subdir" +"%b %d %H:%M" )

                # parse from ${subdir}/backup_summary_info.txt
                if [ -f "${subdir}/backup_summary_info.txt" ]; then
                    em_file_created="$(grep -m 1 "em_file_created" "${subdir}/backup_summary_info.txt" | awk -F': ' '{print $2}')"
                    em_file_name="$(grep -m 1 "em_file_name" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                    em_file_size="$(grep -m 1 "em_file_size" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                    journal_file="$(grep -m 1 "journal_file" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                    vbbm_file="$(grep -m 1 "vbbm_file" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                    vss_file="$(grep -m 1 "vss_file" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                    compression="$(grep -m 1 "compression" "${subdir}/backup_summary_info.txt" | awk '{print $2}')"
                else
                    em_file_created="N/A"
                    em_file_size="N/A"
                    em_file_name="N/A"
                    journal_file="N/A"
                    vbbm_file="N/A"
                    vss_file="N/A"
                    compression="N/A"
                fi   

                file_age=$(($(date +%s) - $(date -r "$subdir" +%s)))
                file_age_days=$((file_age / 86400))
                will_delete="$(find "$subdir" -mindepth 1 -maxdepth 1 -type d -name "*" -mtime +$retention_days -exec echo {} \;)"
                if [ -n "$will_delete" ]; then
                    file_age_days="$file_age_days (Will Delete)"
                fi

                printf "%-35s %-9s %-13s %-15s %-12s %-12s %-10s %-10s %-10s\n" "$(basename "$subdir")" "$compression" "$em_file_created" "$em_file_name" "$em_file_size" "$journal_file" "$vbbm_file" "$vss_file" "$file_age_days"
                continue
            fi
            
            get_latest_em_from_dbrm_directory "$subdir/data1/systemFiles/dbrm/"
               
            if [ -f "${subdir_dbrms}/BRM_saves_journal" ]; then
                em_file_name=$(basename "$em_file_full_path")
                version_prefix=${em_file_name::-3}
                journal_file=$(ls -la "${subdir_dbrms}/BRM_saves_journal" 2>/dev/null | awk 'NR==1 {print $5}' )
                vbbm_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vbbm" 2>/dev/null | awk 'NR==1 {print $5}' )
                vss_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vss" 2>/dev/null | awk 'NR==1 {print $5}' )

                file_age=$(($(date +%s) - $(date -r "$subdir" +%s)))
                file_age_days=$((file_age / 86400))

                # Check if the backup will be deleted given the retention policy defined
                if [ -n "$retention_days" ] && [ $retention_days -ne 0 ]; then 
                    # file_age_days=$((file_age / 60));  will_delete="$(find "$subdir" -mindepth 1 -maxdepth 1 -type d -name "*" -mmin +$retention_days -exec echo {} \;)"
                    will_delete="$(find "$subdir" -mindepth 1 -maxdepth 1 -type d -name "*" -mtime +$retention_days -exec echo {} \;)"

                    if [ -n "$will_delete" ]; then
                        file_age_days="$file_age_days (Will Delete)"
                    fi
                fi;

                printf "%-45s %-13s %-15s %-12s %-12s %-10s %-10s %-10s\n" "$(basename "$subdir")" "$em_file_created" "$em_file_name" "$em_file_size" "$journal_file" "$vbbm_file" "$vss_file" "$file_age_days"
            fi
        done
    fi


}

list_dbrm_restore_options_from_backups() {

    if [ -n "$retention_days" ] && [ $retention_days -ne 0 ]; then
        echo "--------------------------------------------------------------------------"
        echo "Retention Policy: $retention_days days"
    fi;
    echo "--------------------------------------------------------------------------"
    printf "%-45s %-13s %-15s %-12s %-12s %-10s %-10s\n" "Options" "EM-Updated" "Extent Map" "EM-Size" "Journal-Size" "VBBM-Size" "VSS-Size"

    # Iterate over subdirectories
    for subdir in "${backup_location}"/*; do

        get_latest_em_from_dbrm_directory "$subdir"

        if [ -f "${subdir_dbrms}/BRM_saves_journal" ]; then
            em_file_name=$(basename "$em_file_full_path")
            version_prefix=${em_file_name::-3}
            journal_file=$(ls -la "${subdir_dbrms}/BRM_saves_journal" 2>/dev/null | awk 'NR==1 {print $5}' )
            vbbm_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vbbm" 2>/dev/null | awk 'NR==1 {print $5}' )
            vss_file=$(ls -la "${subdir_dbrms}/${version_prefix}_vss" 2>/dev/null | awk 'NR==1 {print $5}' )
            if [ $storagemanager_dir_exists == false ]; then
                vss_file+=" (No Storagemanager Dir)"
            fi; 
            
            printf "%-45s %-13s %-15s %-12s %-12s %-10s %-10s\n" "$(basename "$subdir")" "$em_file_created" "$em_file_name" "$em_file_size" "$journal_file" "$vbbm_file" "$vss_file"
        fi
    done


}

# $1 message
dynamic_list_dbrm_backups() {

    validation_prechecks_before_listing_restore_options
    printf "$1";
    list_dbrm_restore_options_from_backups "$@"
    echo "--------------------------------------------------------------------------"
    printf "Restore with ./$0 dbrm_restore --backup-location $backup_location --load <backup_folder_from_above>\n\n"
}

handle_list_dbrm_backups () {
    
    if $list_dbrm_backups; then
        dynamic_list_dbrm_backups "\nExisting DBRM Backups\n"
        exit 0
    fi;

} 

handle_empty_dbrm_restore_folder() { 

    if [ -z "$backup_folder_to_restore" ]; then
        dynamic_list_dbrm_backups "[!] Pick Option [Required]\n"
        exit 1
    fi;
}

process_dbrm_restore() {
    
    load_default_dbrm_restore_variables
    parse_dbrm_restore_variables "$@"

    # print current job variables
    printf "\nDBRM Restore Variables\n"
    echo "--------------------------------------------------------------------------"
    echo "Skips:    DBRM Backup($skip_dbrm_backup)    Storagemanager($skip_storage_manager)"
    echo "--------------------------------------------------------------------------"
    printf "CS Storage:           $storage \n" 
    printf "Backups Directory:    $backup_location \n"
    printf "Backup to Restore:    $backup_folder_to_restore \n\n"

    validation_prechecks_before_listing_restore_options
    handle_list_dbrm_backups
    handle_empty_dbrm_restore_folder
    validation_prechecks_for_dbrm_restore
    shutdown_columnstore_mariadb_cmapi

    # Take an automated backup
    if [[ $skip_dbrm_backup == false ]]; then
        printf " - Saving a DBRM backup before restoring ... \n"
        if ! process_dbrm_backup -bl $backup_location -r 0 -nb dbrms_before_restore_backup --quiet ; then 
            echo "[!!] Failed to take a DBRM backup before restoring"
            echo "exiting ..."
            exit 1;
        fi;
    fi;

    # Detect newest date _em from the set, if smaller than the current one throw a warning
    get_latest_em_from_dbrm_directory "${backup_location}/${backup_folder_to_restore}"
    if [ ! -f $em_file_full_path ]; then 
        echo "[!] Failed to parse _em file: $em_file_full_path doesnt exist"
        exit 1;
    fi;
    echo "em_file_full_path: $em_file_full_path"
    
    echo "latest_em_file: $latest_em_file"
    echo "em_file_size: $em_file_size"
    echo "em_file_created: $em_file_created"
    echo "storagemanager_dir_exists: $storagemanager_dir_exists"
    echo "subdir_dbrms: $subdir_dbrms"
    
    em_file_name=$(basename $em_file_full_path)
    prefix="${em_file_name%%_em}"
    echo "em_file_name: $em_file_name"
    echo "prefix: $prefix"

    if [ -z "$em_file_name" ]; then
        printf "[!] Undefined EM file name\n"
        printf "find "${backup_location}/${backup_folder_to_restore_dbrms}" -maxdepth 1 -type f -name "BRM_saves*_em" -exec ls -lat {} + \n\n"
        exit 1;
    fi

    # split logic between S3 & LocalStorage
    if [ $storage == "S3" ]; then 
        process_s3_dbrm_restore
    else
        process_localstorage_dbrm_restore
    fi;
}

# $1 - File to cat/ upload into S3
# $2 - Location in storagemanager to overwrite
# example:  smput_or_error "${backup_location}/${backup_folder_to_restore_dbrms}/${prefix}_em" "/data1/systemFiles/dbrm/BRM_saves_em"
smput_or_error() {
    if ! cat "$1" | smput "$2" 2>/dev/null; then
        printf "[!] Failed to smput: $1\n"
    else
        printf "."
    fi  
}

# Depends on 
# em_file - most recent EM
# em_file_full_path - full path to most recent EM
# em_file_name - Just the file name of the EM
# prefix - Prefix of the EM file
process_s3_dbrm_restore() {
    
    printf_offset=45
    printf "\nBefore DBRMs Restore\n"
    echo "--------------------------------------------------------------------------"
    if ! command -v smls  > /dev/null; then
        printf "[!] smls not installed ... Exiting\n\n"
        exit 1
    else 
        current_status=$(smls /data1/systemFiles/dbrm/ 2>/dev/null);
        if [ $? -ne 0 ]; then
            printf "\n[!] Failed to get smls status\n\n" 
            exit 1
        fi
        echo "$current_status" | grep -E "BRM_saves_em|BRM_saves_vbbm|BRM_saves_vss|BRM_saves_journal|BRM_saves_current|oidbitmap"
    fi

    printf "\nRestoring DBRMs\n"
    echo "--------------------------------------------------------------------------"
    printf " - Desired EM:    $em_file_full_path\n"
    printf " - Copying DBRMs: ${backup_location}/${backup_folder_to_restore_dbrms} -> S3 Bucket \n"

    printf "\nPreparing\n"
    printf "%-${printf_offset}s ..." " - Clearing storagemanager caches"
    if [ ! -d "$dbrm_dir/cache" ]; then
        echo "Directory $dbrm_dir/cache does not exist."
        exit 1
    fi
    for cache_dir in "${dbrm_dir}/cache"/*; do
        if [ -d "${dbrm_dir}/cache/${cache_dir}" ]; then
            echo "   - Removing Cache: $cache_dir"
        else
            printf "."
        fi
    done
    printf " Success\n"

    printf "%-${printf_offset}s ... " " - Starting mcs-storagemanager"
    if ! systemctl start mcs-storagemanager ; then
        echo "[!!] Failed to start mcs-storagemanager "
        exit 1;
    else
        printf "Done\n"
    fi

    printf "\nRestoring\n"
    printf "%-${printf_offset}s " " - Restoring Prefix: $prefix "
    smput_or_error "${backup_location}/${backup_folder_to_restore_dbrms}/${prefix}_em" "/data1/systemFiles/dbrm/BRM_saves_em"
    smput_or_error "${backup_location}/${backup_folder_to_restore_dbrms}/${prefix}_vbbm" "/data1/systemFiles/dbrm/BRM_saves_vbbm"
    smput_or_error "${backup_location}/${backup_folder_to_restore_dbrms}/${prefix}_vss" "/data1/systemFiles/dbrm/BRM_saves_vss"
    if ! echo "BRM_saves" | smput /data1/systemFiles/dbrm/BRM_saves_current 2>/dev/null; then
        printf "[!] Failed to smput: BRM_saves_current\n"
    else
        printf "."
    fi 

    em_files=(
        "BRM_saves_journal"
        "oidbitmap"
        "SMTxnID"
        "tablelocks"
    )
    for file in "${em_files[@]}"; do
        if [ ! -f "${backup_location}/${backup_folder_to_restore_dbrms}/${file}" ]; then
            printf "[!] File not found: ${file} in the S3 DBRM backup directory\n"
            printf "Path: ${backup_location}/${backup_folder_to_restore_dbrms}/${file}\n\n"
            continue
        fi
        smput_or_error "${backup_location}/${backup_folder_to_restore_dbrms}/${file}" "/data1/systemFiles/dbrm/${file}"
    done
    printf " Success\n"

    printf "%-${printf_offset}s ... " " - Stopping mcs-storagemanager"
    if ! systemctl stop mcs-storagemanager ; then
        echo "[!!] Failed to stop mcs-storagemanager "
        exit 1;
    else
        printf "Done\n"
    fi
    printf "%-${printf_offset}s ... " " - clearShm"
    clearShm 
    printf "Done\n"
    sleep 2

    printf "%-${printf_offset}s ... " " - Starting mcs-storagemanager"
    if ! systemctl start mcs-storagemanager ; then
        echo "[!!] Failed to start mcs-storagemanager "
        exit 1;
    else
        printf "Done\n"
    fi

    manually_run_loadbrm_and_savebrm

    printf "\nAfter DBRM Restore\n"
    echo "--------------------------------------------------------------------------"
    current_status=$(smls /data1/systemFiles/dbrm/ 2>/dev/null);
    if [ $? -ne 0 ]; then
        printf "\n[!] Failed to get smls status\n\n" 
        exit 1
    fi
    echo "$current_status" | grep -E "BRM_saves_em|BRM_saves_vbbm|BRM_saves_vss|BRM_saves_journal|BRM_saves_current|oidbitmap"

    if $auto_start; then
        printf "\nStartup\n"
        echo "--------------------------------------------------------------------------"
        start_mariadb_cmapi_columnstore
    fi

    # printf "\n[+] Health Check ...\n"
    # sleep 2
    # # run a basic health check
    # mariadb -e "create database if not exists $backup_folder_to_restore"
    # mariadb $backup_folder_to_restore -e "drop table if exists t1"
    # mariadb $backup_folder_to_restore -e "create table t1 (a int) engine=columnstore"
    # mariadb $backup_folder_to_restore -e "insert into t1 values (1)"
    # mariadb $backup_folder_to_restore -e "update t1 set a=1"
    # mariadb $backup_folder_to_restore -e "delete from t1 where a=1"
    # mariadb -e "drop database if exists $backup_folder_to_restore"

    printf "\nDBRM Restore Complete\n\n"
}

# Depends on 
# em_file - most recent EM
# em_file_full_path - full path to most recent EM
# em_file_name - Just the file name of the EM
# prefix - Prefix of the EM
process_localstorage_dbrm_restore() {

    printf "\nBefore DBRMs Restore\n"
    echo "--------------------------------------------------------------------------"
    ls -la "${dbrm_dir}" | grep -E "BRM_saves_em|BRM_saves_vbbm|BRM_saves_vss|BRM_saves_journal|BRM_saves_current"
    printf " - Clearing active DBRMs ... "
    if rm -rf $dbrm_dir ; then
        printf "Done\n"
    else 
        echo "Failed to delete files in $dbrm_dir "
        exit 1; 
    fi

    printf "\nRestoring DBRMs\n"
    echo "--------------------------------------------------------------------------"
    printf " - Desired EM: $em_file_full_path\n"
    printf " - Copying DBRMs: \"${backup_location}/${backup_folder_to_restore_dbrms}\" -> \"$dbrm_dir\" \n"
    cp -arp "${backup_location}/${backup_folder_to_restore_dbrms}" $dbrm_dir
    

    if [ "$prefix" != "BRM_saves" ]; then
        printf " - Restoring Prefix: $prefix \n"
        vbbm_name="${prefix}_vbbm"
        vss_name="${prefix}_vss"
        cp -arpf "${dbrm_dir}/$em_file_name" "${dbrm_dir}/BRM_saves_em"
        cp -arpf "${dbrm_dir}/$vbbm_name"    "${dbrm_dir}/BRM_saves_vbbm"
        cp -arpf "${dbrm_dir}/$vss_name"     "${dbrm_dir}/BRM_saves_vss"
    fi
    echo "BRM_saves" > "${dbrm_dir}/BRM_saves_current"
    chown -R mysql:mysql "${dbrm_dir}"
    clearShm
    sleep 2

    manually_run_loadbrm_and_savebrm
    
    printf "\nAfter DBRM Restore\n"
    echo "--------------------------------------------------------------------------"
    ls -la "${dbrm_dir}" | grep -E "BRM_saves_em|BRM_saves_vbbm|BRM_saves_vss|BRM_saves_journal|BRM_saves_current"

    if $auto_start; then
        printf "\nStartup\n"
        echo "--------------------------------------------------------------------------"
        start_mariadb_cmapi_columnstore
    fi

    # printf "\n[+] Health Check ...\n"
    # sleep 2
    # # run a basic health check
    # mariadb -e "create database if not exists $backup_folder_to_restore"
    # mariadb $backup_folder_to_restore -e "drop table if exists t1"
    # mariadb $backup_folder_to_restore -e "create table t1 (a int) engine=columnstore"
    # mariadb $backup_folder_to_restore -e "insert into t1 values (1)"
    # mariadb $backup_folder_to_restore -e "update t1 set a=1"
    # mariadb $backup_folder_to_restore -e "delete from t1 where a=1"
    # mariadb -e "drop database if exists $backup_folder_to_restore"

    printf "\nDBRM Restore Complete\n\n"
}

manually_run_loadbrm_and_savebrm() {

    local print_formatting=45
    printf "%-${print_formatting}s ... " " - Running load_brm"
    if ! sudo -su mysql /usr/bin/load_brm /var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves ; then
        printf "\n[!!] Failed to complete load_brm successfully\n\n"
        exit 1;
    fi;

    printf "%-${print_formatting}s ... " " - Starting mcs-controllernode"
    if ! systemctl start mcs-controllernode ; then
        echo "[!!] Failed to start mcs-controllernode "
        exit 1;
    else
        printf "Done\n"
    fi;    

    printf "%-${print_formatting}s ... " " - Confirming extent map readable"
    if ! editem -i >/dev/null ; then
        echo "[!!] Failed to run editem -i (read the EM)"
        exit 1;
    else 
        printf "Done\n"
    fi;

    printf "%-${print_formatting}s ... \n" " - Running save_brm"
    if ! sudo -u mysql /usr/bin/save_brm ; then
        echo "[!!] Failed to run save_brm"
        exit 1;
    fi

    printf "%-${print_formatting}s ... " " - Stopping mcs-controllernode"
    if ! systemctl stop mcs-controllernode; then
        echo "[!!] Failed to stop mcs-controllernode"
        exit 1;
    else
        printf "Done\n"
    fi
    
    if [ $storage == "S3" ]; then 
        printf "%-${print_formatting}s ... " " - Stopping mcs-storagemanager"
        if ! systemctl stop mcs-storagemanager ; then
            echo "[!!] Failed to stop mcs-storagemanager "
            exit 1;
        else
            printf "Done\n"
        fi
    fi;

    printf "%-${print_formatting}s ... " " - clearShm"
    clearShm 
    printf "Done\n"
    sleep 2
}

# $1 - message
dynamic_list_backups() {

    validation_prechecks_before_listing_restore_options
    printf "$1";
    list_restore_options_from_backups "$@"
    echo "--------------------------------------------------------------------------"
    printf "Restore with ./$0 restore --backup-location $backup_location --load <backup_folder_from_above>\n\n"
}



handle_list_backups () {

    if $list_backups ; then
        dynamic_list_backups "\nExisting Backups\n"
        exit 0
    fi;
}

handle_empty_restore_folder() {   

    if [ -z "$load_date" ]; then
        dynamic_list_backups "\n[!] Pick Option [Required]\n"
        exit 1
    fi;
}

handle_apply_retention_only() {

    if $apply_retention_only; then
        s1=20
        s2=20
        printf "\nApplying Retention Only Variables\n"
        echo "--------------------------------------------------------------------------"
        if [ -n "$config_file" ] && [ -f "$config_file" ]; then
            printf "%-${s1}s %-${s2}s\n" "Configuration File:" "$config_file";
        fi
        echo "--------------------------------------------------------------------------"
        printf "%-${s1}s  %-${s2}s\n" "Backup Location:" "$backup_location";
        printf "%-${s1}s  %-${s2}s\n" "Timestamp:" "$(date +%m-%d-%Y-%H:%M:%S)";
        printf "%-${s1}s  %-${s2}s\n" "Retention:" "$retention_days";
        echo "--------------------------------------------------------------------------"
        
        if [ $retention_days -eq 0 ]; then
            printf "\n[!] Are you sure retention should be set to 0?\n"
        fi;

        apply_backup_retention_policy
        exit 0
    fi;
}


process_backup()
{   
    load_default_backup_variables;
    parse_backup_variables "$@";
    handle_list_backups "$@"
    handle_apply_retention_only "$@"
    print_backup_variables;
    check_for_dependancies "backup";
    validation_prechecks_for_backup;
    apply_backup_retention_policy;
    issue_write_locks;
    run_save_brm;
    run_backup;
}

process_restore()
{
    load_default_restore_variables;
    parse_restore_variables "$@";
    handle_list_backups "$@"
    handle_empty_restore_folder "$@"
    print_restore_variables;
    check_for_dependancies "restore";
    validation_prechecks_for_restore;
    run_restore;
}

global_dependencies() {
    if ! command -v curl &> /dev/null; then
        printf "\n[!] curl not found. Please install curl\n\n"
        exit 1; 
    fi   
}

print_mcs_bk_mgr_version_info() {
    echo "MariaDB Columnstore Backup Manager"
    echo "Version: $mcs_bk_manager_version"
}

global_dependencies

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
    'dbrm_restore') 	
        process_dbrm_restore "$@";
        ;;
    'restore') 	
        process_restore "$@";
        ;;
    '-v' | 'version' | '--version')
        print_mcs_bk_mgr_version_info
        ;;
    'source' )
        return 0;
        ;;
    *) 
        printf "\nunknown action: $action\n" 
        print_action_help_text
        ;;
esac

exit 0;