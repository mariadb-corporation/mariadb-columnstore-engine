########################################################################
#
# Purpose: Simple Restore of columnstore
# Example: sudo ./simpleRestore.bash
#
# Local Storage backup:
#    -s   | --storage                'LocalStorage'
#    -l   | --load                   what backup to load
#    -bl  | --backuplocation         directory where the backup was saved
#    -dbs | --dbroots               Number of database roots in the backup
#    -bd  | --backupdestination      if the backup directory is 'Local' or 'Remote' to this script
#    -scp                           scp connection to remote server if -bd 'Remote'          ( required if -bd Remote )
# 
#  Examples:
#        ./simpleRestore.bash -bl /tmp/backups/ -bd Local -s LocalStorage -l 12-29-2021 -dbs 1
#        ./simpleRestore.bash -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage -l 12-29-2021 -dbs 3
#   
# S3 based backup  
#    -s   | --storage                'S3'
#    -bb  | --backupbucket           bucket name to load S3 backups
#    -url | --s3-endpoint            onprem url to s3 storage api example: 127.0.0.1:9001      (optional)
#    -pm  | --nodeid                 Forces the handling of the restore as this node as opposed to whats detected on disk
#    -nb  | --newbucket              Defines the new bucket to copy the s3 data to from the backup bucket   
#    -nr  | --newregion              Defines the region of the new bucket to copy the s3 data to from the backup bucket   
#    -nk  | --newkey                 Defines the aws key to connect to the newbucket  
#    -ns  | --newsecret              Defines the aws secret of the aws key to connect to the newbucket  
#    -ha  | --highavilability        Hint wether shared storage is attached allowing all nodes to see all data
#
#
#   Examples:
#        ./simpleRestore.bash -bb backup-clone-mcs-bucket-test-allen -s S3
#        ./simpleRestore.bash -bb on-premise-bucket -s S3 -l 12-29-2021 -url 127.0.0.1:9001
#        ./simpleRestore.bash -bb allens-west-cs-backups -s S3 -nb allens-eu-active-columnstore-bucket -nr eu-central-1 -ha -l 10-19-2022
#        ./simpleRestore.bash -bb allens-west-cs-backups -s S3 -nb allens-eu-active-columnstore-bucket -nr eu-central-1 -nk AKIA5987BNDAT7S44YY -ns 'QTPQFENIasdf9678Hd04g+owAYSsAI/Uv1cwc'
# 
########################################################################

# What date folder to load from the backupLocation
loadDate=''

# Where the backup to load is found
# Example: /mnt/backups/
backupLocation="/tmp/backups/"

# Is this backup on the same or remote server compared to where this script is running
# Options: "Local" or "Remote"
backupDestination="Local"

# Used only if backupDestination=Remote
# The user/credentials that will be used to scp the backup files
# Example: "centos@10.14.51.62"
scp=""

# What storage topogoly was used by Columnstore in the backup - found in storagemanager.cnf
# Options: "LocalStorage" or "S3" 
storage="LocalStorage"

# Flag for high available systems (meaning shared storage exists supporting the topology so that each node sees all data)
HA=false
continue=false

# Only used if storage=S3
# Name of the bucket to copy into to store the backup S3 data
# Example: "backup-mcs-bucket"
backupBucket=''
pm=$(cat /var/lib/columnstore/local/module)
pmNumber=$(echo "$pm" | tr -dc '0-9')

# Defines the set of new bucket information to copy the backup to when restoring
newBucket=''
newRegion=''
newKey=''
newSecret=''

# Used by on premise S3 vendors
# Example: "127.0.0.1:9001"
s3url=""

# Number of DBroots - TODO: detect automatically based on the backup being loaded
# Integer usually 1 or 3
DBRootCount=1

# Dynamic Arguments
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -l|--load)
        loadDate="$2"
        shift # past argument
        shift # past value
        ;;
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
    -dbs|--dbroots)
        DBRootCount="$2"
        shift # past argument
        shift # past value
        ;;
    -pm | --nodeid)
        pm="$2"
        pmNumber=$(echo "$pm" | tr -dc '0-9')
        shift # past argument
        shift # past value
        ;;
    -nb | --newbucket)
        newBucket="$2"
        shift # past argument
        shift # past value
        ;;
    -nr | --newregion)
        newRegion="$2"
        shift # past argument
        shift # past value
        ;;
    -nk | --newkey)
        newKey="$2"
        shift # past argument
        shift # past value
        ;;
    -ns | --newsecret)
        newSecret="$2"
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
    -h|--help)
        echo "
Simple Restore for Columnstore

    -l   | --load                   What backup to load
    -bl  | --backuplocation         Directory where the backup was saved
    -bd  | --backupdestination      If the directory is 'Local' or 'Remote' to this script
    -dbs | --dbroots                Number of database roots in the backup
    -scp                            scp connection to remote server if -bd 'Remote'
    -bb  | --backupbucket           Bucket name to load S3 backups
    -url | --s3-endpoint            Onprem url to s3 storage api example: 127.0.0.1:9001
    -s   | --storage                The storage used by columnstore data 'LocalStorage' or 'S3'
    -pm  | --nodeid                 Forces the handling of the restore as this node as opposed to whats detected on disk
    -nb  | --newbucket              Defines the new bucket to copy the s3 data to from the backup bucket. 
                                    Use -nb if the new restored cluster should use a different bucket than the backup bucket itself
    -nr  | --newregion              Defines the region of the new bucket to copy the s3 data to from the backup bucket   
    -nk  | --newkey                 Defines the aws key to connect to the newbucket  
    -ns  | --newsecret              Defines the aws secret of the aws key to connect to the newbucket  
    -ha  | --highavilability        Hint wether shared storage is attached allowing all nodes to see all data
                                      - LocalStorage ( /var/lib/columnstore/dataX/ )
                                      - S3           ( /vara/lib/columnstore/storagemanager/ )  

    Local Storage Examples:
        ./simpleRestore.bash -bl /tmp/backups/ -bd Local -s LocalStorage -l 12-29-2021
        ./simpleRestore.bash -bl /tmp/backups/ -bd Remote -scp root@172.31.6.163 -s LocalStorage -l 12-29-2021
    
    S3 Storage Examples:
        ./simpleRestore.bash -bb backup-clone-mcs-bucket-test-allen -s S3 -l 12-29-2021
        ./simpleRestore.bash -bb on-premise-bucket -s S3 -l 12-29-2021 -url 127.0.0.1:9001
        ./simpleRestore.bash -bb mdb0001908-cs-db00007535-20220818122420071900000002  -s S3 -l 08-16-2022 -nb mdb0001908-cs-db00007555-20220822154055110900000002 -nr us-east-1 -nk AKIATSJNTZWKC3FHCADF -ns GGGu5C5Zo0P303rR34MQGmz8exAWZhcnqa72csk5 -ha true
        "
        exit 1;
        ;;
    *)    # unknown option
        echo "unknown flag: $1"
        exit 1
  esac
done

printf "\nRestore Variables\n"
echo "------------------------------------------------"
if [ $storage == "LocalStorage" ]; then 
    echo "Backup Location:    $backupLocation"
    echo "Backup Destination: $backupDestination"
    echo "Scp:                $scp"
    echo "Storage:            $storage"
    echo "Load Date:          $loadDate"
    echo "timestamp:          $(date +%m-%d-%Y-%H%M%S)"
    echo "DB Root Count:      $DBRootCount"
    echo "PM:                 $pm"
    echo "PM Number:          $pmNumber"

elif [ $storage == "S3" ]; then
    echo "Backup Location:    $backupLocation"  
    echo "Storage:            $storage"
    echo "Load Date:          $loadDate"
    echo "timestamp:          $(date +%m-%d-%Y-%H%M%S)"
    echo "PM:                 $pm"
    echo "PM Number:          $pmNumber"
    echo "Active bucket:      $( grep -m 1 "bucket =" /etc/columnstore/storagemanager.cnf)"
    echo "Backup Bucket:      $backupBucket"
    echo "------------------------------------------------"
    echo "New Bucket:         $newBucket"
    echo "New Region:         $newRegion"
    echo "New Key:            $newKey"
    echo "New Secret:         $newSecret"
    echo "High Availabilty:   $HA"
fi
echo "------------------------------------------------"

# Check MairaDB-backup installed
if ! command -v mariadb-backup  > /dev/null; then
    printf "\n\n Please make sure mariadb-backup is installed and executable\n\n"
    yum install MariaDB-backup
    printf "\n\n Please rerun script \n\n"
    exit 1
fi

# Validate some arguments
if [ $storage != "LocalStorage" ] && [ $storage != "S3" ]; then echo "Invalid Script Variable storage: $storage"; exit 1; fi
if [ -z "$loadDate" ]; then echo -e "\n[!!!] Required field --load: $loadDate - is empty\n"; exit 1; fi

# Check if remote - that scp works 
if [ $backupDestination == "Remote" ]; then
    if ssh $scp echo ok 2>&1 ;then
        printf 'SSH Works\n\n'
    else
        echo "Failed Command: ssh $scp"
        exit 1;
    fi
fi

# Adjust s3api flags based on inputs
finalMessage="Restore Complete"
additionalS3apiFlags=""
if [ ! -z "$s3url" ];  then
    additionalS3apiFlags=" --s3-endpoint $s3url";
fi

# Make sure the database is offline
if [ -z $(pidof PrimProc) ]; then printf "[+] Columnstore is offline ... done\n"; else printf "[X] Columnstore is ONLINE - please turn off \n\n"; exit 1; fi
if [ -z $(pidof mariadbd) ]; then printf "[+] MariaDB is offline ... done\n"; else printf "[X] MariaDB is ONLINE - please turn off \n\n"; exit 1; fi

# Branch logic based on topology
if [ $storage == "LocalStorage" ]; then 
    # Validate addtional relevant arguments
    if [ -z "$backupLocation" ]; then echo "Invalid --backuplocation: $backupLocation - is empty"; exit 1; fi
    if [ -z "$backupDestination" ]; then echo "Invalid --backupdestination: $backupDestination - is empty"; exit 1; fi
    if [ $backupDestination == "Remote" ] && [ -d $backupLocation$loadDate ]; then echo "Switching to '-bd Local'"; backupDestination="Local"; fi
    
    # Branch logic based on where the backup resides
    if [ $backupDestination == "Local" ]; then
        
        # Validate addtional relevant arguments
        if [ ! -d $backupLocation ]; then echo "Invalid directory --backuplocation: $backupLocation"; exit 1; fi
        if [ ! -d $backupLocation$loadDate ]; then echo "Invalid directory --load: $backupLocation$loadDate"; exit 1; fi

        # Loop through per dbroot
        printf "[~] Columnstore Data..."; i=1;
        while [ $i -le $DBRootCount ]; do
            if [ $pm == "pm$i" ]; then rm -rf /var/lib/columnstore/data$i/; rsync -av $backupLocation$loadDate/data$i/ /var/lib/columnstore/data$i/; chown mysql:mysql -R /var/lib/columnstore/data$i/; fi
            ((i++))
        done
        printf "[+] Columnstore Data... Done\n" 
        
        # Put configs in place
        printf "[~] Columnstore Configs... "
        rsync -av $backupLocation$loadDate/configs/storagemanager.cnf /etc/columnstore/storagemanager.cnf 
        rsync -av $backupLocation$loadDate/configs/cmapi_server.conf /etc/columnstore/cmapi_server.conf 
        rsync -av $backupLocation$loadDate/configs/mysql/$pm/ /etc/my.cnf.d/ 
        printf "[+] Columnstore Configs... Done\n" 
       
    elif [ $backupDestination == "Remote" ]; then
        printf "[~] Copy MySQL Data..."
        tmp="localscpcopy-$loadDate"
        rm -rf $backupLocation$tmp/mysql/
        mkdir -p $backupLocation$tmp/mysql/
        rsync -av $scp:$backupLocation/$loadDate/mysql/ $backupLocation$tmp/mysql/
        printf "[+] Copy MySQL Data... Done\n"

        # Loop through per dbroot
        printf "[~] Columnstore Data..."; i=1;
        while [ $i -le $DBRootCount ]; do
            if [ $pm == "pm$i" ]; then rm -rf /var/lib/columnstore/data$i/; rsync -av $scp:$backupLocation$loadDate/data$i/ /var/lib/columnstore/data$i/ ; chown mysql:mysql -R /var/lib/columnstore/data$i/;fi
            ((i++))
        done
        printf "[+] Columnstore Data... Done\n" 

        # Put configs in place
        printf "[~] Columnstore Configs... "
        rsync -av $backupLocation$loadDate/configs/storagemanager.cnf /etc/columnstore/storagemanager.cnf 
        rsync -av $backupLocation$loadDate/configs/cmapi_server.conf /etc/columnstore/cmapi_server.conf 
        rsync -av $backupLocation$loadDate/configs/mysql/$pm/ /etc/my.cnf.d/ 
        printf "[+] Columnstore Configs... Done\n"
        loadDate=$tmp
    else 
        echo "Invalid Script Variable --backupdestination: $backupDestination"
        exit 1
    fi

    # Standard mysql restore
    mariabackup --prepare --target-dir=$backupLocation$loadDate/mysql/
    rm -rf /var/lib/mysql
    mariabackup --copy-back --target-dir=$backupLocation$loadDate/mysql/
    chown -R mysql:mysql /var/lib/mysql/
    chown -R mysql:mysql /var/log/mariadb/

elif [ $storage == "S3" ]; then

    # Check aws-cli installed
    if ! command -v aws > /dev/null; then
        which aws
        echo "Hints:  
        curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"
        unzip awscliv2.zip
        sudo ./aws/install
        mv /usr/local/bin/aws /usr/bin/aws
        aws configure"

        printf "\n\n Please make sure aws cli is installed configured and executable\n\n"
        exit 1
    fi

    # Check for concurrency settings - https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
    cliConcurrency=$(sudo grep max_concurrent_requests ~/.aws/config ); if [ -z "$cliConcurrency" ]; then echo "[!!!] check: `~/.aws/config` - We recommend increasing s3 concurrency for better throughput to/from S3. This value should scale with avaialble CPU and networking capacity"; echo "example: aws configure set default.s3.max_concurrent_requests 900";  fi

    # Validate addtional relevant arguments
    if [ -z "$backupBucket" ]; then echo "Invalid --backupbucket: $backupBucket - is empty"; exit 1; fi
    if $HA ; then echo -e "\n[~] High availability should exist - checking df -h"; MOUNT=$(df -h | grep "columnstore/storagemanager"); if [ -z $MOUNT ]; then echo -e "[!!!] Didnt find storagemanager mount... existing\n";exit 1; fi; echo "[+] HA mount exists"; fi
    
    
    # Check aws cli access to bucket
    if aws $additionalS3apiFlags s3 ls $backupBucket  > /dev/null ; then
        echo -e "[+] Success listing backup bucket"
    else 
        echo -e "[X] Failed to list bucket contents..."
        echo -e "\naws $additionalS3apiFlags s3 ls $backupBucket \n"
    fi

    # New bucket exists and empty check
    if [ "$newBucket" ] && [ $pm == "pm1" ]; then 
        NEWBUCKET=$(aws s3 ls | grep $newBucket); if [ -z "$NEWBUCKET" ]; then echo -e "[!!!] Didnt find new bucket - Check:  aws s3 ls | grep $newBucket\n"; exit 1; fi; echo "[+] New Bucket exists"; 
        # TODO: check aws s3 ls $newBucket | head
        if $continue; then "[!] Continue" else NEWBUCKETCONTENTS=$(aws s3 ls $newBucket | head); if [ -z "$NEWBUCKETCONTENTS" ]; then echo "[+] New Bucket is empty"; else echo "[!!!] New bucket is NOT empty... exiting";echo "add --continue to skip this exit" echo "Example Empty Bucket Command: aws s3 rm s3://$newBucket --recursive"; exit 1; fi; fi
    fi

    # Check if s3 bucket load date exists
    if aws $additionalS3apiFlags s3 ls $backupBucket/$loadDate > /dev/null ; then
        echo -e "[+] Backup directory exists"
    else 
        echo -e "\n[X] Backup directory load date (s3://$backupBucket/$loadDate) DOES NOT exist in S3 \nCheck - aws $additionalS3apiFlags s3 ls s3://$backupBucket/$loadDate | head \n\n";
        exit 1;
    fi

    # Sync the columnstore data from the backup bucket to the new bucket
    if [ "$newBucket" ] && [ $pm == "pm1" ]; then 
        echo -e "[~] Sync'ing s3://$backupBucket/$loadDate/columnstoreData to $newBucket"
        aws $additionalS3apiFlags s3 sync s3://$backupBucket/$loadDate/columnstoreData/ s3://$newBucket/
    fi;
 
    # TODO - have the restore.job file define storagemanager values - newbucket= , newregion=, newkey, newsecret, HA flag or not determine if each node copies  /etc/columnstore/storagemanager/metadata/dataX
    printf "[~] Columnstore Configurations and metadata\n"
    # These two commented out only make sense to restore if on the same machines with same ip addresses
    #aws s3 cp s3://$backupBucket/$loadDate/configs/Columnstore.xml /etc/columnstore/Columnstore.xml
    #aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/configs/cmapi_server.conf /etc/columnstore/cmapi_server.conf 
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/configs/storagemanager.cnf /etc/columnstore/storagemanager.cnf 
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/configs/mysql/$pm/ /etc/my.cnf.d/  --recursive
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/storagemanager/cache/data$pmNumber /var/lib/columnstore/storagemanager/cache/data$pmNumber/ --recursive
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/storagemanager/journal/data$pmNumber /var/lib/columnstore/storagemanager/journal/data$pmNumber/ --recursive
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/storagemanager/metadata/data$pmNumber /var/lib/columnstore/storagemanager/metadata/data$pmNumber/ --recursive
    if [ $pm == "pm1" ]; then 
        aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/data1/ /var/lib/columnstore/data1/  --recursive 
    fi
    printf "[+] Columnstore Configurations and metadata ... done \n\n"

    # Set appropraite bucket, prefix, region, key , secret
    targetBucket=$backupBucket
    targetPrefix="prefix = $loadDate\/columnstoreData\/"
    if [ ! -z "$newBucket" ]; then targetBucket=$newBucket;  targetPrefix="# prefix \= cs\/";  fi
    if [ ! -z "$newRegion" ];  then region=$( grep -m 1 "region =" /etc/columnstore/storagemanager.cnf);                sed -i "s/$region/region = $newRegion/g" /etc/columnstore/storagemanager.cnf; fi
    if [ ! -z "$newKey" ];     then key=$( grep -m 1 "aws_access_key_id =" /etc/columnstore/storagemanager.cnf);        sed -i "s/$key/aws_access_key_id = $newKey/g" /etc/columnstore/storagemanager.cnf;  fi
    if [ ! -z "$newSecret" ];  then secret=$( grep -m 1 "aws_secret_access_key =" /etc/columnstore/storagemanager.cnf); sed -i "s/$secret/aws_secret_access_key = $newSecret/g" /etc/columnstore/storagemanager.cnf;  fi
    bucket=$( grep -m 1 "bucket =" /etc/columnstore/storagemanager.cnf | sed "s/\//\\\\\//g"); sed -i "s/$bucket/bucket = $targetBucket/g" /etc/columnstore/storagemanager.cnf
    prefix=$( grep -m 1 "prefix =" /etc/columnstore/storagemanager.cnf | sed "s/\//\\\\\//g"); sed -i "s/$prefix/$targetPrefix/g" /etc/columnstore/storagemanager.cnf 
    chown -R mysql:mysql /var/lib/columnstore/
    chown -R root:root /etc/my.cnf.d/

    # Check if S3 connection works
    if testS3Connection; then echo "[+] S3 Connection passes" ; else printf "\n[X] S3 Connection issues - retest/configure\n"; exit 1; fi

    # Copy the MariaDB data, prepare and put in place
    printf "\n[~] Preparing and Copying back MariaDB data\n"
    rm -rf $backupLocation$loadDate/mysql/   
    mkdir -p $backupLocation$loadDate/mysql/
    aws $additionalS3apiFlags s3 cp s3://$backupBucket/$loadDate/mysql/ $backupLocation$loadDate/mysql  --recursive  
    mariabackup --prepare --target-dir=$backupLocation$loadDate/mysql/
    rm -rf /var/lib/mysql
    mariabackup --copy-back --target-dir=$backupLocation$loadDate/mysql/
    chown -R mysql:mysql /var/lib/mysql/
    chown -R mysql:mysql /var/log/mariadb/
    printf "[+] Preparing and Copying back MariaDB data ... done\n"

fi

echo -e "\n\n[+] $finalMessage\n"
if [ $pm == "pm1" ]; then 
    echo -e  " - Last you need to manually configure mariadb replication between nodes"
    echo -e  " - Check /etc/columnstore/Columnstore.xml customizations via mcsGetConfig -a"
    echo -e  " - systemctl start mariadb "
    echo -e  " - mcsStart \n\n"
else
    printf " - Last you need to manually configure mariadb replication between nodes\n"
    echo -e  "Example:"
    echo -e  "mariadb -e \"stop slave;CHANGE MASTER TO MASTER_HOST='\$pm1' , MASTER_USER='repuser' , MASTER_PASSWORD='aBcd123%123%aBc' , MASTER_USE_GTID = slave_pos;start slave;show slave status\G\""
    echo -e  "mariadb -e  \"show slave status\G\" | grep  \"Slave_\" \n"
fi




