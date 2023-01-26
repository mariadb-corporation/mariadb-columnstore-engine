#!/bin/bash
########################################################################
#
# Purpose: Simple Backup of columnstore
# Example: sudo ./simpleBackup.bash 
#
# If doing a Local Storage backup:
#    - backupLocation
#    - backupDestination
#    - scp (only required when saving backup to remote location)
#   
#   Examples:
#        ./simpleBackup.bash -bl /tmp/backups/ -bd Local -s LocalStorage
#        ./simpleBackup.bash -bl /tmp/backups/ -bd Local -s LocalStorage --incremental 02-04-2022
#        ./simpleBackup.bash -bl /tmp/backups/ -bd Local -s LocalStorage --incremental $(ls -t /tmp/backups/ | grep ".*-.*-202.*" | head -n 1)
#        ./simpleBackup.bash -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage
#   
# If doing an S3 based backup  
#    - backupBucket
#
#   Examples:
#        ./simpleBackup.bash -bb backup-clone-mcs-bucket-test-allen -s S3
#        ./simpleBackup.bash -bb backup-clone-mcs-bucket-test-allen -s S3  --incremental 02-04-2022
#        ./simpleBackup.bash -bb on-premise-bucket -s S3 -url 127.0.0.1:9001
#   
#   Cron:
#      * * * * *  root  bash /root/simpleBackup.bash -bb rapiddelete -s S3  >> /root/simpleBackup.log  2>&1
#     
########################################################################
# TODO: add auto cycling of columnstore - maybe just run save_brm?
start=`date +%s`

# What directory to store the backups on this machine or the target machine.
# Consider write permissions of the scp user and the user running this script.
# Mysql Backup will use tmp dir for remote backups temporarily
# Example: /mnt/backups/
backupLocation="/tmp/backups/"

# Are the backups going to be stored on the same machine this script is running on or another server - if Remote you need to setup scp=
# Options: "Local" or "Remote"
backupDestination="Local"

# Used only if backupDestination="Remote"
# The user/credentials that will be used to scp the backup files
# Example: "centos@10.14.51.62"
scp=""

# Only used if storage=S3
# Name of the bucket to copy into to store the backup
# Example: "backup-mcs-bucket"
backupBucket=""

# What storage topogoly is being used by Columnstore - found in /etc/columnstore/storagemanager.cnf
# Options: "LocalStorage" or "S3" 
storage=$(grep -m 1 "service = " /etc/columnstore/storagemanager.cnf | awk '{print $3}')

# Name of the existing bucket used in the cluster - found in /etc/columnstore/storagemanager.cnf
# Example: "mcs-20211101172656965400000001"
bucket=$( grep -m 1 "bucket =" /etc/columnstore/storagemanager.cnf | awk '{print $3}')

# Other Variables
parrallelRsync=false
parallelThreads=8
#today=$(date +%m-%d-%Y-%H%M%S)
today=$(date +%m-%d-%Y)
pm=$(cat /var/lib/columnstore/local/module)
pmNumber=$(echo "$pm" | tr -dc '0-9')
assignedDBroot=$(mcsGetConfig SystemModuleConfig ModuleDBRootID$pmNumber-1-3)
sourceIPs=$(grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}" /etc/columnstore/Columnstore.xml)
sourceHostNames=$(grep "<Node>" /etc/columnstore/Columnstore.xml)
cmapiKey=""
if [ -f /etc/columnstore/cmapi_server.conf ]; then cmapiKey="$(grep "x-api-key" /etc/columnstore/cmapi_server.conf | awk  '{print $3}' | tr -d "'" )"; fi;

# Used by on premise S3 vendors
# Example: "127.0.0.1:9001"
s3url=""

# Tracks if flush read lock has been run
readLock=false;
incremental=false

# Number of DBroots
# Integer usually 1 or 3
DBRootCount=$(mcsGetConfig -a | grep SystemConfig.DBRootCount | awk '{print $3}')

# Dynamic Arguments
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -bl|--backuplocation)
        backupLocation="$2"
        shift # past argument
        shift # past value
        ;;
    -bd|--backupdestination)
        backupDestination="$2"
        shift # past argument
        shift # past value
        ;;
    -scp)
        scp="$2"
        shift # past argument
        shift # past value
        ;;
    -bb|--backupbucket)
        backupBucket="$2"
        shift # past argument
        shift # past value
        ;;
    -url| --s3-endpoint)
        s3url="$2"
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
        parrallelRsync=true
        parallelThreads="$2"
        shift # past argument
        shift # past value
        ;;
    -h|--help)
        echo "
Simple Backup for Columnstore

    -bl | --backuplocation         directory where the backup will be saved
    -bd | --backupdestination      if the directory is 'Local' or 'Remote' to this script
    -scp                           scp connection to remote server if -bd 'Remote'
    -bb | --backupbucket           bucket name to save S3 backups
    -url| --s3-endpoint            onprem url to s3 storage api example: 127.0.0.1:9001
    -s  | --storage                the storage used by columnstore data 'LocalStorage' or 'S3'
    -i  | --incremental            adds columnstore deltas to an existing full backup

    Examples:
        ./simpleBackup.bash -bl /tmp/backups/ -bd Local -s LocalStorage
        ./simpleBackup.bash -bl /tmp/backups/ -bd Local -s LocalStorage --incremental 02-18-2022
        ./simpleBackup.bash -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage
        ./simpleBackup.bash -bb backup-clone-mcs-bucket-test-allen -s S3
        ./simpleBackup.bash -bb backup-clone-mcs-bucket-test-allen -s S3 --incremental 02-18-2022
        ./simpleBackup.bash -bb on-premise-bucket -s S3 -url 127.0.0.1:9001
        
    Cron Example:
    */30 * * * *  root  bash /root/simpleBackup.bash -bb rapiddelete -s S3  >> /root/csBackup.log  2>&1
        "
        exit 1;
        ;;
    *)    # unknown option
        echo "unknown flag: $1"
        exit 1
  esac
done

printf "\nBackup Variables\n"
echo "------------------------------------------------"
echo "Backup Location:    $backupLocation"
echo "Backup Destination: $backupDestination"
echo "Scp:                $scp"
echo "Storage:            $storage"
echo "Folder:             $today"
echo "timestamp:          $(date +%m-%d-%Y-%H%M%S)"
echo "Parallel:           $parrallelRsync"
echo "Threads:            $parallelThreads"
echo "DB Root Count:      $DBRootCount"
echo "PM:                 $pm"
echo "Active bucket:      $bucket"
echo "Backup Bucket:      $backupBucket"
echo "Incremental:        $incremental"
echo "------------------------------------------------"

function clearReadLock {

    if [ $pm == "pm1" ]; then
        # Clear MDB Lock
        if [ -n $(pidof mariadbd) ] && [ $readLock ]; then
            printf "\n[+] Clearing read-only lock ..."; 
            if mariadb -e "UNLOCK TABLES;"; then
                readLock=false;
                printf " Done\n"
            else 
                printf "[X] Failed clearing readLock";
                exit 1; 
            fi
            
        fi

        # Clear CS Lock
        if [ $DBRootCount == "1" ]; then 
            if dbrmctl readwrite ; then
                printf " Cleared Columnstore BRM Lock\n"
            else 
                printf "[X] Failed clearing columnstore BRM lock via dbrmctl";
                exit 1; 
            fi
        elif curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapiKey" --data '{"timeout":20, "mode": "readwrite"}' -k  ; then 
            printf " Cleared Columnstore BRM Lock\n"
        else
            printf "[X] Failed clearing columnstore BRM lock";
            exit 1; 
        fi
    fi
    
} 

function handleEarlyShutdown {
    clearReadLock 
    echo "Backup Failed"
    exit 1;
}

function deepParallelRsync {
    path=$1
    depthCurrent=$2
    depthTarget=$3
    relativePath=$4
    # echo "       path=$1
    # depthCurrent=$2
    # depthTarget=$3
    # relativePath=$4"

    for file in $path/*; do
        
        # If a directory, keep going deeper until depthTarget reached
        if [ -d $file ]; then
            #echo "Directory $file"
            mkdir -p $backupLocation$today/$relativePath/$(basename $file)

            if [ $depthCurrent -ge $depthTarget ]; then
                #echo "copy to relative: $backupLocation$today/$relativePath/"
                if ls $file | xargs -P $parallelThreads -I {} rsync -a $additionalRsyncFlags $file/{} $backupLocation$today/$relativePath/$(basename $file) ; then
                    echo "Completed: $backupLocation$today/$relativePath/$(basename $file)"
                else 
                    echo "Failed: $backupLocation$today/$relativePath/$(basename $file)"
                    exit 1;
                fi
                
            else
                #echo "GOING DEEPER "    

                # Since target depth not reached, recursively call for each directory 
                # TODO - limit by parallelThreads
                deepParallelRsync $file "$((depthCurrent+1))" $depthTarget "$relativePath/$(basename $file)" &
            fi

        # If a file, rsync right away
        elif [ -f $file ]; then
            #echo "File: $file"
            rsync -a $additionalRsyncFlags $file $backupLocation$today/$relativePath/ 
        else
            #echo "Empty Dir? $file"
            #echo "rsync -a $additionalRsyncFlags $file $backupLocation$today/$relativePath/ "
            rsync -a $additionalRsyncFlags $file $backupLocation$today/$relativePath/ 
            
        fi
    done

}

# Check pidof works
if ! command -v pidof  > /dev/null; then
    printf "\n\n Please make sure pidof is installed and executable\n\n"
    exit 1;
fi

# Make sure the database is offline - best for non-volatile backups
# or MDB needs to be online to issue a flush tables with read lock
if [ -z $(pidof mariadbd) ]; then 
    printf "[+] MariaDB is offline ... Needs to be online to issue read only lock \n\n"; 
    exit 1;
elif [ $pm == "pm1" ]; then 
   
    printf "[+] Issuing Read-only Lock ... "; 
    if mariadb -e "FLUSH TABLES WITH READ LOCK;"; then
        readLock=true
        printf " Done\n"; 
    else    
        printf "[X] Failed issuing read-only lock\n\n";
        exit 1; 
    fi 

    # Set columnstore ReadOnly Mode
    if [ $DBRootCount == "1" ]; then 
        if dbrmctl readonly ; then
            printf "\nIssued Columnstore BRM Lock Success\n"
        else 
            printf "[X] Failed issuing columnstore BRM lock via dbrmctl\n";
            exit 1;
        fi
    else 
        cmapiResponse=$(curl -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/mode-set --header 'Content-Type:application/json' --header "x-api-key:$cmapiKey" --data '{"timeout":20, "mode": "readonly"}' -k);
        if [[ $cmapiResponse == '{"error":'* ]] ; then 
            printf "[X] Failed issuing columnstore BRM lock\n";
            exit 1; 
        else
        printf "\nIssued Columnstore BRM Lock Success\n"
        fi
    fi
fi


#if [ -z $(pidof PrimProc) ]; then printf "[+] Columnstore is offline ... done\n"; else printf "[X] Columnstore is ONLINE - please turn off \n\n"; exit 1; fi

# Check MariaDB-backup installed
if ! command -v mariadb-backup  > /dev/null; then
    printf "\n\n Please make sure mariadb-backup is installed and executable\n\n"
    yum install MariaDB-backup
    printf "\n\n Please rerun backup \n\n"
    handleEarlyShutdown
fi

# Check for validate Storage variable
if [ $storage != "LocalStorage" ] && [ $storage != "S3" ]; then
    echo "Invalid Variable storage: $storage"
    handleEarlyShutdown
fi

# Check ssh works in remote mode is used
if [ $backupDestination == "Remote" ]; then
    if ssh -o ConnectTimeout=10 $scp echo ok 2>&1 ;then
        printf 'SSH Works\n'
    else
        echo "Failed Command: ssh $scp"
        handleEarlyShutdown
    fi
fi

# Adjust rsync & s3api flags based on inputs
finalMessage="Backup Complete"
additionalRsyncFlags=""
if  $incremental ; then
    additionalRsyncFlags="--inplace --no-whole-file"
fi
additionalS3apiFlags=""
if [ ! -z "$s3url" ];  then
    additionalS3apiFlags=" --s3-endpoint $s3url";
fi

# Part 1 - Handle the backup of columnstore data
if [ $storage == "LocalStorage" ]; then 
    if [ $backupDestination == "Local" ]; then

        # Incremental Job checks
        if $incremental ; then 
            # Since this is incremental - cant continue if this folder (which represents a full backup) doesnt exists
            if [ -d $backupLocation$today ]; then printf "[+] Full backup directory exists\n"; else  printf "[X] Full backup directory ($backupLocation$today) DOES NOT exist \n\n"; handleEarlyShutdown; fi;
        fi
        
        # Check/Create Directories
        printf "[+] Checking Directories... "
        mkdir -p $backupLocation$today/mysql
        mkdir -p $backupLocation$today/configs 
        mkdir -p $backupLocation$today/configs/mysql
        i=1
        while [ $i -le $DBRootCount ]; do
            if [ $assignedDBroot == "$i" ]; then mkdir -p $backupLocation$today/data$i ; fi
            ((i++))
        done
        printf " Done\n" 

        # Backup Columnstore data
        printf "[~] Rsync Columnstore Data... \n"
        i=1
        while [ $i -le $DBRootCount ]; do
            if [ $assignedDBroot == "$i" ]; then 
                
                if $parrallelRsync ; then 
                    #find /var/lib/columnstore/data$i -type d | xargs -n1 -P10 -I% echo "dir: %"
                    #find /var/lib/columnstore/data$i -type f | xargs -n1 -P10 -I% rsync -ar % $backupLocation$today/data$i
                    #find /var/lib/columnstore/data$i -type d | xargs -n1 -P10 -I% rsync -ar % $backupLocation$today/data$i


                    DEPTH=3
                    deepParallelRsync /var/lib/columnstore/data$i 1 $DEPTH data$i &
                    sleep 2;
                    rsyncInProgress="$(ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l )";
                
                    w=0;
                    while [ $rsyncInProgress -gt "0" ]; do 
                        if [ $(($w % 10)) -eq 0 ]; then 
                            echo -E "$rsyncInProgress rsync processes running ... seconds: $w"
                            du -sh $backupLocation$today/data$i/ /var/lib/columnstore/data$i/
                            echo "Monitor rsync: ps aux | grep 'rsync -a' "
                        fi
                        #ps aux | grep 'rsync -a' | grep -vE 'color|grep'
                        sleep 1 
                        rsyncInProgress="$(ps aux | grep 'rsync -a' | grep -vE 'color|grep' | wc -l )";
                        ((w++))
                    done
                else
                    rsync -a $additionalRsyncFlags /var/lib/columnstore/data$i/* $backupLocation$today/data$i/ ; 
                fi; 
            fi
    
            ((i++))
        done
        printf "[+] Done - rsync\n"


        # Backup MariaDB configurations
        printf "[~] Backing up mariadb configurations... \n"
        mkdir -p $backupLocation$today/configs/mysql/$pm/
        rsync -av $additionalRsyncFlags /etc/my.cnf.d/* $backupLocation$today/configs/mysql/$pm/
        printf "[+] Done - configurations\n"

        # Backup MariaDB data
        if [ $assignedDBroot == "1" ]; then
            # TODO: incremental mysql backup
            # logic to increment mysql and keep count if we want to backup incremental mysql data
            # i=1
            # latestMysqlIncrement=0
            # while [ $i -le 365 ]; do
            #     if [ -d $backupLocation/mysql$i ]; then  ; else latestMysqlIncrement=i; break; fi;
            # done

            # MariaDB backup wont rerun if folder exists - so clear it before running mariadb-backup
            if [ -d $backupLocation$today/mysql ]; then 
                printf "[+] Deleting $backupLocation$today/mysql dir! ... "
                rm -rf $backupLocation$today/mysql
                printf " Done\n" 
            fi

            printf "[~] Backing up mysql... \n"
            if mariadb-backup --backup --target-dir=$backupLocation$today/mysql --user=root ; then printf "[+] Done backing up mysql\n\n"; else printf "\n Failed: mariadb-backup --backup --target-dir=$backupLocation$today/mysql --user=root\n\n"; exit 1; fi
            
            # Backup CS configurations
            rsync -av $additionalRsyncFlags /etc/columnstore/Columnstore.xml $backupLocation$today/configs/ 
            rsync -av $additionalRsyncFlags /etc/columnstore/storagemanager.cnf $backupLocation$today/configs/
            if [ -f /etc/columnstore/cmapi_server.conf ]; then  rsync -av $additionalRsyncFlags /etc/columnstore/cmapi_server.conf $backupLocation$today/configs/; fi
        fi
        
       
        if $incremental ; then 
            now=$(date "+%m-%d-%Y %H:%M:%S")
            echo "$pm updated on $now" >> $backupLocation$today/incrementallyUpdated.txt
            finalMessage="Incremental Backup Complete"
        else 
            # Create restore job file
            echo "./simpleRestore.bash -l $today -bl $backupLocation -bd $backupDestination -s $storage --dbroots $DBRootCount" > $backupLocation$today/restore.job
        fi

        finalMessage+=" @ $backupLocation$today"
        
    elif [ $backupDestination == "Remote" ]; then

        # Incremental Job checks
        if  $incremental ; then 
            # Since this is incremental - cant continue if this folder doesnt exists
            if [[ `ssh $scp test -d $backupLocation$today && echo exists` ]] ; then  printf "[+] Full backup directory exists\n"; else printf "[X] Full backup directory ($backupLocation$today) DOES NOT exist on remote $scp \n\n"; handleEarlyShutdown;  fi
        fi
       
        # Check/Create Directories on remote server
        printf "[+] Checking Remote Directories... "
        i=1
        makeDataDirectories=""
        while [ $i -le $DBRootCount ]; do
            makeDataDirectories+="mkdir -p $backupLocation$today/data$i ;"
            ((i++))
        done
        echo $makeDirectories
        ssh $scp " mkdir -p $backupLocation$today/mysql ;
        $makeDataDirectories
        mkdir -p $backupLocation$today/configs ; 
        mkdir -p $backupLocation$today/configs/mysql; 
        mkdir -p $backupLocation$today/configs/mysql/$pm "
        printf " Done\n"
        
        printf "[~] Rsync Remote Columnstore Data... \n"
        i=1
        while [ $i -le $DBRootCount ]; do
            #if [ $pm == "pm$i" ]; then rsync -a $additionalRsyncFlags /var/lib/columnstore/data$i/* $scp:$backupLocation$today/data$i/ ; fi
            if [ $pm == "pm$i" ]; then 
                find /var/lib/columnstore/data$i/*  -mindepth 1 -maxdepth 2 | xargs -P $parallelThreads -I {} rsync -a $additionalRsyncFlags /var/lib/columnstore/data$i/* $scp:$backupLocation$today/data$i/
            fi
            ((i++))
        done
        printf "[+] Done - rsync\n"

        # Backup MariaDB configurations
        printf "[~] Backing up MariaDB configurations... \n"
        rsync -av $additionalRsyncFlags /etc/my.cnf.d/* $scp:$backupLocation$today/configs/mysql/$pm/
        printf "[+] Done - configurations\n"

        # Backup MariaDB data
        if [ $pm == "pm1" ]; then
            printf "[~] Backing up mysql... \n"
            mkdir -p /tmp/$backupLocation$today
            if mariadb-backup --backup --target-dir=/tmp/$backupLocation$today --user=root ; then 
                rsync -a /tmp/$backupLocation$today/* $scp:$backupLocation$today/mysql
                rm -rf /tmp/$backupLocation$today
            else 
                printf "\n Failed: mariadb-backup --backup --target-dir=/tmp/$backupLocation$today --user=root\n\n"; 
                handleEarlyShutdown 
            fi
            
            # Backup CS configurations
            rsync -av $additionalRsyncFlags /etc/columnstore/Columnstore.xml $scp:$backupLocation$today/configs/ 
            rsync -av $additionalRsyncFlags /etc/columnstore/storagemanager.cnf $scp:$backupLocation$today/configs/
            if [ -f /etc/columnstore/cmapi_server.conf ]; then  rsync -av $additionalRsyncFlags /etc/columnstore/cmapi_server.conf $scp:$backupLocation$today/configs/; fi
        fi
    
       
        if  $incremental ; then 
            now=$(date "+%m-%d-%Y +%H:%M:%S")
            ssh $scp "echo \"$pm updated on $now\" >> $backupLocation$today/incrementallyUpdated.txt"
            finalMessage="Incremental Backup Complete"
        else
             # Create restore job file
            ssh $scp "echo './simpleRestore.bash -l $today -bl $backupLocation -bd $backupDestination -s $storage -scp $scp --dbroots $DBRootCount' > $backupLocation$today/restore.job"
        fi
        finalMessage+=" @ $scp:$backupLocation$today"
    fi

elif [ $storage == "S3" ]; then
    
    # Check aws-cli installed
    if ! command -v aws > /dev/null; then
        which aws
        echo "Hints:  
        curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"
        unzip awscliv2.zip
        sudo ./aws/install
        aws configure"

        printf "\n\n Please make sure aws cli is installed configured and executable"
        printf "\nSee existing .cnf aws credentials with:  grep aws_ /etc/columnstore/storagemanager.cnf \n\n"
        handleEarlyShutdown
    fi

    # Validate addtional relevant arguments
    if [ -z "$backupBucket" ]; then echo "Invalid --backupbucket: $backupBucket - is empty"; exit 1; fi

    # Check aws cli access to bucket
    if aws $additionalS3apiFlags s3 ls $backupBucket > /dev/null ; then
        printf "\n[+] Success listing bucket"
    else 
        printf "\n[X] Failed to list bucket contents... Check aws cli credentials: aws configure"
        printf "\naws $additionalS3apiFlags s3 ls $backupBucket \n\n"
        exit 1;
    fi

    # Incremental Job checks
    if  $incremental ; then 
        # Since this is incremental - cant continue if this folder doesnt exists
        if [[ $( aws $additionalS3apiFlags s3 ls s3://$backupBucket/$today | head )  ]] ; then  printf "[+] Full backup directory exists\n"; else printf "[X] Full backup directory (s3://$backupBucket/$today) DOES NOT exist in S3 \nCheck - aws $additionalS3apiFlags s3 ls s3://$backupBucket/$today | head \n\n"; handleEarlyShutdown;  fi
    fi

    # PM1 mostly backups everything
    if [ $assignedDBroot == "1" ]; then

        printf "\n[+] Syncing S3 Columnstore data to backup bucket... \n"
        if aws $additionalS3apiFlags s3 sync s3://$bucket s3://$backupBucket/$today/columnstoreData ; then
            printf " Done - Columnstore data sync complete\n"
        else
            printf "\n\naws sync failed - try alone and debug\n\n"
            echo "aws $additionalS3apiFlags s3 sync s3://$bucket s3://$backupBucket/$today/columnstoreData"
            echo "exiting ..."
            handleEarlyShutdown
        fi

        # Assume Connection to aws s3 is fine if above passes
        # Backup CS configurations and metadata
        printf "\n[+] Syncing Columnstore configurations and metadata \n"
        aws $additionalS3apiFlags s3 cp /etc/columnstore/Columnstore.xml s3://$backupBucket/$today/configs/ 
        aws $additionalS3apiFlags s3 cp /etc/columnstore/storagemanager.cnf s3://$backupBucket/$today/configs/ 
        aws $additionalS3apiFlags s3 cp /etc/columnstore/cmapi_server.conf s3://$backupBucket/$today/configs/ 
        aws $additionalS3apiFlags s3 sync /var/lib/columnstore/storagemanager/storagemanager-lock s3://$backupBucket/$today/storagemanager/storagemanager-lock
        aws $additionalS3apiFlags s3 sync /var/lib/columnstore/data1/ s3://$backupBucket/$today/data1/  

        # Backup Mysql
        printf "\n[+] Syncing MariaDB data \n"
        rm -rf /tmp/$backupLocation$today/mysql
        if mkdir -p /tmp/$backupLocation$today/mysql ; then
            mariadb-backup --backup --target-dir=/tmp/$backupLocation$today/mysql --user=root 
            aws $additionalS3apiFlags s3 rm s3://$backupBucket/$today/mysql/ --recursive
            aws $additionalS3apiFlags s3 cp /tmp/$backupLocation$today/mysql s3://$backupBucket/$today/mysql/ --recursive 
            rm -rf /tmp/$backupLocation$today/mysql
        else 
            echo "Failed making directory for MariaDB backup"
            echo "command: mkdir -p /tmp/$backupLocation$today/mysql "
            handleEarlyShutdown
        fi
    fi

    # Each node needs to share its metadata
    if aws $additionalS3apiFlags s3 sync /var/lib/columnstore/storagemanager/cache/data$assignedDBroot s3://$backupBucket/$today/storagemanager/cache/data$assignedDBroot ; then printf " Done - cache/data$assignedDBroot\n"; else  printf "\n\naws sync failed - cache/data$assignedDBroot\n\n"; fi
    if aws $additionalS3apiFlags s3 sync /var/lib/columnstore/storagemanager/journal/data$assignedDBroot s3://$backupBucket/$today/storagemanager/journal/data$assignedDBroot ; then printf " Done - journal/data$assignedDBroot\n"; else  printf "\n\naws sync failed - journal/data$assignedDBroot\n\n"; fi
    if aws $additionalS3apiFlags s3 sync /var/lib/columnstore/storagemanager/metadata/data$assignedDBroot s3://$backupBucket/$today/storagemanager/metadata/data$assignedDBroot ; then printf " Done - metadata/data$assignedDBroot\n"; else  printf "\n\naws sync failed - metadata/data$assignedDBroot\n\n"; fi

    # Run by all PMs - mariadb server configs
    printf "\n[+] Syncing MariaDB configurations \n"
    if aws $additionalS3apiFlags s3 sync /etc/my.cnf.d/ s3://$backupBucket/$today/configs/mysql/$pm/ ; then
        printf " Done - MariaDB configurations complete\n"
    else
        printf "\n\naws sync failed - try alone and debug\n\n"
        echo "aws s3 sync /etc/my.cnf.d/ s3://$backupBucket/$today/configs/mysql/$pm/ "
        echo "exiting ..."
        handleEarlyShutdown
    fi

    if  $incremental ; then 
        now=$(date "+%m-%d-%Y +%H:%M:%S")
        # download S3 file, append to it and reupload

        if [[ $( aws $additionalS3apiFlags s3 ls s3://$backupBucket/$today/incrementallyUpdated.txt)  ]]; then
            aws $additionalS3apiFlags s3 cp s3://$backupBucket/$today/incrementallyUpdated.txt incrementallyUpdated.txt
        fi
        echo "$pm updated on $now" >> incrementallyUpdated.txt
        aws $additionalS3apiFlags s3 cp incrementallyUpdated.txt s3://$backupBucket/$today/incrementallyUpdated.txt
        rm -rf incrementallyUpdated.txt
        finalMessage="Incremental Backup Complete"
    else
        # Create restore job file
        echo "./simplerestore.bash -l $today -s $storage"  > restoreS3.job
        aws $additionalS3apiFlags s3 cp restoreS3.job s3://$backupBucket/$today/restoreS3.job
        rm -rf restoreS3.job
    fi
    finalMessage+=" @ s3://$backupBucket/$today"
   
fi

clearReadLock
end=`date +%s`
runtime=$((end-start))
printf "[+] Runtime: $runtime\n"
printf "\n\n[+] $finalMessage\n\n"




