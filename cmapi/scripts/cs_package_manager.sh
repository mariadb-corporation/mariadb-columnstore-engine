#!/bin/bash
# Documentation:   bash cs_package_manager.sh help

# Variables
enterprise_token=""
dev_drone_key="" 
jenkins_user="" 
jenkins_pwd=""   
cs_pkg_manager_version="3.5"
if [ ! -f /var/lib/columnstore/local/module ]; then  pm="pm1"; else pm=$(cat /var/lib/columnstore/local/module);  fi;
pm_number=$(echo "$pm" | tr -dc '0-9')
action=$1

print_help_text() {
    echo "Version $cs_pkg_manager_version

Example Check:
    bash $0 check community
    bash $0 check enterprise

Example Remove: 
    bash $0 remove
    bash $0 remove all --force

Example Single Node Install: 
    bash $0 install [enterprise|community] [version] --token [token]
    bash $0 install enterprise 10.6.12-8 --token [token]
    bash $0 install community 11.1

Example Cluster Install: 
    bash $0 install enterprise help
    bash $0 install enterprise [version] --token [token] --nodes [Ip1,Ip2,Ip3] [flags]

Example Single Node Upgrade: 
    bash $0 upgrade [enterprise|community] [version] --token [token]
    bash $0 upgrade enterprise 10.6.18-14 --token [token]
    bash $0 upgrade community 11.1.4
"   

    if [  -n "$dev_drone_key" ]; then
        echo "
Example Install Dev: 
bash $0 install [enterprise|community|dev] [version|cron|latest|pull_request] [build num] --token xxxxxxx
bash $0 install dev develop cron/8629
bash $0 install dev develop-23.02 pull_request/7256
bash $0 install dev stable-23.10 pull_request/10820

Example Install Jenkins: 
bash $0 install [jenkins] [project]/[branch]/[git commit] --user xxxxxxx --password xxxxxxx
bash $0 install jenkins ENTERPRISE/bb-10.6.14-9-cs-23.02.11-1/2d367136b3d3511ffeb53451de539d44db98ed91/
    "
    fi
}

wait_cs_down() {
    retries=0
    max_number_of_retries=20

    if ! command -v pgrep &> /dev/null; then
        printf "\n[!] pgrep not found. Please install pgrep\n\n"
        exit 1; 
    fi   

    # Loop until the maximum number of retries is reached
    # printf " - Checking Columnstore Offline ...";
    while [ $retries -lt $max_number_of_retries ]; do
        # If columnstore is offline, return
        cs_processlist=$(pgrep -f "PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|save_brm|mcs-loadbrm.py")
        if [ -z "$cs_processlist" ]; then 
            # printf " Done \n";
            mcs_offine=true
            return 0
        else 
            printf "\n[!] Columnstore is ONLINE - waiting 5s to retry, attempt: $retries...\n"; 
            if (( retries % 5 == 0 )); then
                echo "PID List: $cs_processlist"
                echo "$(ps aux | grep -E "PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|save_brm|mcs-loadbrm.py" | grep -v grep) "
            fi
            sleep 5
            ((retries++))
        fi
    done

    # If maximum retries reached, exit with an error
    echo "PID List: $cs_processlist"
    printf "\n[!!!] Columnstore is still online after $max_number_of_retries retries ... exiting \n\n"
    exit 2
}

print_and_copy() {
    printf " - %-35s ..." "$1"
    cp -p $1 $2
    printf " Done\n"
}

print_and_delete() {
    printf " - %-25s ..." "$1"
    rm -rf $1
    printf " Done\n"
}

init_cs_down() {
    mcs_offine=false
    if [ "$pm_number" == "1" ];  then
        if [ -z $(pidof "PrimProc") ]; then 
            # printf "\n[+] Columnstore offline already";
            mcs_offine=true
        else
            
            if is_cmapi_installed ; then
                confirm_cmapi_online_and_configured

                # Stop columnstore
                printf "%-35s ... " " - Stopping Columnstore Engine"
                if command -v mcs &> /dev/null; then
                    if ! mcs_output=$(mcs cluster stop); then
                        echo "[!] Failed stopping via mcs ... trying cmapi curl"
                        stop_cs_cmapi_via_curl
                    fi
                    printf "Done - $(date)\n" 

                    # Handle Errors with exit 0 code
                    if [ ! -z "$(echo $mcs_output | grep "Internal Server Error")" ];then
                        stop_cs_via_systemctl_override
                    fi
                else 
                    stop_cs_cmapi_via_curl
                fi
            else
                stop_cs_via_systemctl
            fi
        fi
    fi
}

init_cs_up(){ 

    if [ "$pm_number" == "1" ];  then
        if [ -n "$(pidof PrimProc)" ]; then 

            num_cs_processlist=$(pgrep -f "PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|save_brm|mcs-loadbrm.py" | wc -l)
            if [ $num_cs_processlist -gt "0" ]; then 
                printf "%-35s ... $num_cs_processlist processes \n" " - Columnstore Engine Online"
            fi

        else 

            # Check cmapi installed
            if is_cmapi_installed ; then
                confirm_cmapi_online_and_configured

                # Start columnstore
                printf "%-35s ..." " - Starting Columnstore Engine"
                if command -v mcs &> /dev/null; then
                    if ! mcs_output=$(mcs cluster start); then
                        echo "[!] Failed starting via mcs ... trying cmapi curl"
                        start_cs_cmapi_via_curl
                    fi
                    printf " Done - $(date)\n" 

                else 
                    start_cs_cmapi_via_curl
                fi
            else
                start_cs_via_systemctl
            fi
        fi
    fi
}

stop_columnstore() {
    init_cs_down
    wait_cs_down
}

is_cmapi_installed() {

    if [ -z "$package_manager" ]; then
        echo "package_manager: $package_manager"
        echo "package_manager is not defined"
        exit 1;
    fi

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
        return 0
    else 
        return 1
    fi
}

start_cmapi() {
    
    if ! command -v systemctl &> /dev/null ; then
        printf "[!!] shutdown_mariadb_and_cmapi: Cant access systemctl\n\n"
        exit 1;
    fi

    if is_cmapi_installed ; then
        printf "%-35s ..." " - Starting CMAPI"
        if systemctl start mariadb-columnstore-cmapi ; then
            printf " Done\n"
        else
            echo "[!!] Failed to start CMAPI"
            exit 1;
        fi;
    fi
}

stop_cmapi() {
    
    if ! command -v systemctl &> /dev/null ; then
        printf "[!!] shutdown_mariadb_and_cmapi: Cant access systemctl\n\n"
        exit 1;
    fi

    if is_cmapi_installed ; then
        printf "%-35s ..." " - Stopping CMAPI"
        if systemctl stop mariadb-columnstore-cmapi ; then
            printf " Done\n"
        else
            echo "[!!] Failed to stop CMAPI"
            exit 1;
        fi;
    fi
}

# Inputs:
# $1 = version already installed
# $2 = version desired to install
# Returns 0 if $2 is greater, else exit
compare_versions() {
    local version1="$1" 
    local version2="$2"
    local exit_message="\n[!] The desired upgrade version: $2 \nis NOT greater than the current installed version of $1\n\n"

    # Split version strings into arrays & remove leading 0
    IFS='._-' read -ra v1_nums <<< "${version1//.0/.}"
    IFS='._-' read -ra v2_nums <<< "${version2//.0/.}"

    # Compare each segment of the version numbers
    for (( i = 0; i < ${#v1_nums[@]}; i++ )); do
        
        v1=${v1_nums[i]}  
        v2=${v2_nums[i]}
        
        if (( v1 > v2 )); then
            # The existing version is newer than the desired version: Exit
            echo -e $exit_message
            exit 1
        elif (( v1 < v2 )); then
            # The desired version is newer than the existing version: Continue
            return 0
        fi
    done

    # Both versions are the same: Exit
    echo -e $exit_message
    exit 1
}

is_mariadb_installed() {

    if [ -z "$package_manager" ]; then
        echo "package_manager: $package_manager"
        echo "package_manager is not defined"
        exit 1;
    fi

    mariadb_installed_command=""
    case $package_manager in
        yum ) 
            mariadb_installed_command="yum list installed MariaDB-server &>/dev/null"
            ;;
        apt )
            mariadb_installed_command="dpkg-query -s mariadb-server &> /dev/null;"
            ;;
        *)  # unknown option
            echo "\nshutdown_mariadb_and_cmapi - package manager not implemented: $package_manager\n"
            exit 2;
    esac
    

    if eval $mariadb_installed_command ; then
        # Check if the systemd service file exists
        if systemctl status mariadb &> /dev/null; then
            # MariaDB systemd service file exists & package is installed
            return 0
        else
            echo "Error: MariaDB systemd service file does not exist."
            return 1
        fi
    else 
        return 1
    fi

}

start_mariadb() {
    if ! command -v systemctl &> /dev/null ; then
        printf "[!!] start_mariadb: Cant access systemctl\n\n"
        exit 1;
    fi
    
    # Start MariaDB
    if is_mariadb_installed ; then
        printf "%-35s ..." " - Starting MariaDB Server"
        if systemctl start mariadb ; then
            printf " Done\n"
        else
            echo "[!!] Failed to start MariaDB Server"
            exit 1;
        fi;
    fi
}

stop_mariadb() {
    if ! command -v systemctl &> /dev/null ; then
        printf "[!!] stop_mariadb: Cant access systemctl\n\n"
        exit 1;
    fi

    # Stop MariaDB
    if is_mariadb_installed ; then
        printf "%-35s ..." " - Stopping MariaDB Server"
        if systemctl stop mariadb ; then
            printf " Done\n"
        else
            echo "[!!] Failed to stop MariaDB Server"
            exit 1;
        fi;
    fi
}

parse_remove_additional_args() {
    # Skip the first parameter
    shift 

    # Defaults
    FORCE=false

    while [[ $# -gt 0 ]]; do
        key="$1"

        case $key in
            all)
                shift # past argument
                ;;
            -f | --force)
                FORCE=true
                shift # past argument
                ;;
            -h|--help|help)
                print_help_text
                exit 1;
                ;;
            *)    # unknown option
                print_help_text
                echo "unknown flag: $1"
                exit 1
                ;;
        esac
    done
}

do_yum_remove() {

    if ! command -v yum &> /dev/null ; then
        printf "[!!] Cant access yum\n"
        exit 1;
    fi

    parse_remove_additional_args "$@"

    if [ "$FORCE" == "true" ]; then
        printf "Forcing Stop\n"
        kill_cs_processes 2>/dev/null
        stop_cs_via_systemctl_override
        kill_cs_processes 2>/dev/null
        kill_mariadbd_processes 2>/dev/null
    fi

    printf "Prechecks\n"
    init_cs_down
    wait_cs_down
    stop_mariadb
    stop_cmapi

    if command -v clearShm &> /dev/null ; then
        clearShm
    fi

    printf "\nRemoving packages \n"

    # remove any mdb rpms on disk
    if ls MariaDB-*.rpm  &>/dev/null; then
        print_and_delete "MariaDB-*.rpm"
    fi

    # remove all current MDB packages
    if yum list installed MariaDB-* &>/dev/null; then
        yum remove MariaDB-* -y
    fi

    if yum list installed galera-* &>/dev/null; then
        yum remove galera-* -y
    fi

    # Check for rpms installed outside packagemanager
    mariadb_packages_still_installed=$(yum list installed | grep MariaDB | awk '{print $1}')
    if [ -n "$mariadb_packages_still_installed" ] ; then
        yum remove $mariadb_packages_still_installed -y
    fi

    # remove offical & custom yum repos
    printf "\nRemoving\n"
    print_and_delete "/etc/yum.repos.d/mariadb.repo"
    print_and_delete "/etc/yum.repos.d/drone.repo"

    if [ "$2" == "all" ]; then
        print_and_delete "/var/lib/mysql/"
        print_and_delete "/var/lib/columnstore/"
        print_and_delete "/etc/my.cnf.d/*"
        print_and_delete "/etc/columnstore/*"
    fi;
}

do_apt_remove() {

    if ! command -v apt &> /dev/null ; then
        printf "[!!] Cant access apt\n"
        exit 1;
    fi

    parse_remove_additional_args "$@"

    if [ "$FORCE" == "true" ]; then
        printf "Forcing Stop\n"
        kill_cs_processes 2>/dev/null
        stop_cs_via_systemctl_override
        kill_cs_processes 2>/dev/null
    fi

    if ! command -v dpkg-query &> /dev/null ; then
        printf "[!!] Cant access dpkg-query\n"
        exit 1;
    fi

    printf "\n[+] Prechecks \n"
    init_cs_down
    wait_cs_down
    stop_mariadb
    stop_cmapi

    if command -v clearShm &> /dev/null ; then
        clearShm
    fi

    printf "\n[+] Removing packages - $(date) ...  \n"
    # remove any mdb rpms on disk
    if ls mariadb*.deb  &>/dev/null; then
        print_and_delete "mariadb*.deb"
    fi

    # remove all current MDB packages
    if [ "$(apt list --installed mariadb-* 2>/dev/null | wc -l)" -gt 1 ];  then
        if [ "$2" == "all" ]; then
            DEBIAN_FRONTEND=noninteractive apt remove --purge -y mariadb-plugin-columnstore mariadb-columnstore-cmapi 
            DEBIAN_FRONTEND=noninteractive apt remove --purge -y mariadb-*
        else
            if ! apt remove mariadb-columnstore-cmapi --purge -y; then
                printf "[!!] Failed to remove columnstore \n"
            fi

            if ! apt remove mariadb-* -y; then
                printf "[!!] Failed to remove the rest of mariadb \n\n"
            fi
        fi
    fi

    if [ "$(apt list --installed mysql-common 2>/dev/null | wc -l)" -gt 1 ];  then
        if ! apt remove mysql-common -y; then
            printf "[!!] Failed to remove mysql-common \n"
        fi
    fi

    printf "\n[+] Removing all columnstore files & dirs\n"
    if [ "$2" == "all" ]; then
            print_and_delete "/var/lib/mysql"
            print_and_delete "/var/lib/columnstore"
            print_and_delete "/etc/columnstore"
            print_and_delete "/etc/mysql/mariadb.conf.d/columnstore.cnf.rpmsave"
    fi
    # remove offical & custom yum repos
    print_and_delete "/lib/systemd/system/mariadb.service"
    print_and_delete "/lib/systemd/system/mariadb.service.d"
    print_and_delete "/etc/apt/sources.list.d/mariadb.list*"
    print_and_delete "/etc/apt/sources.list.d/drone.list"
    systemctl daemon-reload

}

do_remove() {

    check_operating_system
    check_package_managers
    
    case $distro_info in
        centos | rhel | rocky )
            do_yum_remove "$@"
            ;;

        ubuntu | debian )
            do_apt_remove "$@"
            ;;
        *)  # unknown option
            echo "\ndo_remove: os & version not implemented: $distro_info\n"
            exit 2;
    esac

    printf "\nUninstall Complete\n\n"
}

print_enterprise_install_help_text() {
    echo "
    MariaDB Columnstore Package Manager - Enterprise Quick Cluster Install

    Usage: bash $0 install enterprise [version] --token [token] [flags]

    Flags:
        -n  | --nodes               IP address (hostname -I) of nodes to be configured into a cluster, first value will be primary Example: 72.255.12.1,72.255.12.2,72.255.12.3
        -ru | --replication-user    Replication user        Default: repl
        -rp | --replication-pwd     Replication password    Default: Mariadb123%
        -mu | --maxscale-user       Maxscale user           Default: mxs
        -mp | --maxscale-pwd        Maxscale password       Default: Mariadb123%
        -cu | --cross-engine-user   Cross-engine user       Default: cross_engine 
        -cp | --cross-engine-pwd    Cross-engine password   Default: Mariadb123%
        -t  | --token               Enterprise token        Required
        -h  | --help                Help

    Example:
        bash cs_package_manager.sh install enterprise 10.6.14-9 --token xxxxxx-xxxx-xxx-xxxx-xxxxxx --nodes 172.31.45.105,172.31.42.49
        bash cs_package_manager.sh install enterprise 10.6.14-9 --token xxxxxx-xxxx-xxx-xxxx-xxxxxx --nodes 172.31.45.105,172.31.42.49 --replication-pwd \"my_custom_password123%\"
    
    Note: When deploying a cluster, run the same command on all nodes at the same time.
            "
}

print_community_install_help_text() {
    echo "
    MariaDB Columnstore Package Manager - Community Quick Cluster Install

    Usage: bash $0 install community [version] [flags]

    Flags:
        -n  | --nodes               IP address (hostname -I) of nodes to be configured into a cluster, first value will be primary Example: 72.255.12.1,72.255.12.2,72.255.12.3
        -ru | --replication-user    Replication user        Default: repl
        -rp | --replication-pwd     Replication password    Default: Mariadb123%
        -mu | --maxscale-user       Maxscale user           Default: mxs
        -mp | --maxscale-pwd        Maxscale password       Default: Mariadb123%
        -cu | --cross-engine-user   Cross-engine user       Default: cross_engine 
        -cp | --cross-engine-pwd    Cross-engine password   Default: Mariadb123%
        -h  | --help                Help

    Example:
        bash cs_package_manager.sh install community 11.1.5 --token xxxxxx-xxxx-xxx-xxxx-xxxxxx --nodes 172.31.45.105,172.31.42.49
    
    Note: When deploying a cluster, run the same command on all nodes at the same time.
            "
}

print_dev_install_help_text() {
    echo "
    MariaDB Columnstore Package Manager - Dev Quick Cluster Install

    Usage: bash $0 install dev

    Flags:
        -n  | --nodes               IP address (hostname -I) of nodes to be configured into a cluster, first value will be primary Example: 72.255.12.1,72.255.12.2,72.255.12.3
        -ru | --replication-user    Replication user        Default: repl
        -rp | --replication-pwd     Replication password    Default: Mariadb123%
        -mu | --maxscale-user       Maxscale user           Default: mxs
        -mp | --maxscale-pwd        Maxscale password       Default: Mariadb123%
        -cu | --cross-engine-user   Cross-engine user       Default: cross_engine 
        -cp | --cross-engine-pwd    Cross-engine password   Default: Mariadb123%
        -h  | --help                Help Text

    Example:
        bash $0 install dev develop cron/8629
        bash $0 install dev develop-23.02 pull_request/7256
        bash $0 install dev stable-23.10 pull_request/10820 --nodes 172.31.45.105,172.31.42.49
    
    Note: When deploying a cluster, run the same command on all nodes at the same time.
            "
}

print_install_top_level_help_text() {
    echo "
    MariaDB Columnstore Package Manager - Install

    Usage: bash $0 install [enterprise|community] [version] [flags]

    Flags:
        -h  | --help | help               Help Text

    Examples:
        bash $0 install community help
        bash $0 install enterprise help
        "

    if [  -n "$dev_drone_key" ]; then
        echo "Example Install Dev: 
    bash $0 install [enterprise|community|dev] [version|cron|latest|pull_request] [build num] --token xxxxxxx
    bash $0 install dev develop cron/8629
    bash $0 install dev develop-23.02 pull_request/7256
    bash $0 install dev stable-23.10 pull_request/10820 --nodes 172.31.45.105,172.31.42.49
    "
    fi
}

print_install_help_text() {
    
    case $repo in
        enterprise | enterprise_staging )
            print_enterprise_install_help_text
            ;;
        community )
            print_community_install_help_text
            ;;
        dev )
            print_dev_install_help_text
            ;;
        -h|--help|help)
            print_install_top_level_help_text
            exit 1;
            ;;
        *)  # unknown option
            echo "print_install_help_text: Unknown repo: $repo\n"
            exit 2;
    esac

}

set_default_cluster_variables() {
    nodes=""
    replication_user="replication_user"
    replication_pwd="Mariadb123%"
    maxscale_user="maxscale_user"
    maxscale_pwd="Mariadb123%"
    cross_engine_user="cross_engine_user"
    cross_engine_pwd="Mariadb123%"
    CONFIGURE_CMAPI=true
}

parse_install_cluster_additional_args() {
    # Skip the first three parameters
    # $1 = action
    # $2 = community|enterprise
    # $3 = version
    
    if [ $repo == "dev" ]; then
        # $4 = branch/build number
        shift 4
    else
        shift 3
    fi

    # Dynamic Arguments
    while [[ $# -gt 0 ]]; do
        key="$1"

        case $key in
            -t | --token)
                enterprise_token="$2"
                shift # past argument
                shift # past value
                ;;
            -n | --nodes)
                nodes="$2"
                shift # past argument
                shift # past value
                ;;
            -ru | --replication-user)
                replication_user="$2"
                shift # past argument
                shift # past value
                ;;
            -rp | --replication-pwd)
                replication_pwd="$2"
                shift # past argument
                shift # past value
                ;;
            -mu | --maxscale-user)
                maxscale_user="$2"
                shift # past argument
                shift # past value
                ;;
            -mp | --maxscale-pwd)
                maxscale_pwd="$2"
                shift # past argument
                shift # past value
                ;;
            -cu | --cross-engine-user)
                cross_engine_user="$2"
                shift # past argument
                shift # past value
                ;;
            -cp | --cross-engine-pwd)
                cross_engine_pwd="$2"
                shift # past argument
                shift # past value
                ;;
            -scmapi | --skip-cmapi)
                CONFIGURE_CMAPI=false
                shift # past argument
                ;;
            -h|--help|help)
                print_install_help_text
                exit 1;
                ;;
            *)    # unknown option
                print_install_help_text
                echo "parse_install_cluster_additional_args: unknown flag: $1"
                exit 1
                ;;
        esac
    done

    # Enterprise checks
    if [ $repo == "enterprise" ] || [ $repo == "enterprise_staging" ] ; then

        if [ -z "$enterprise_token" ]; then 
            printf "\n[!] Enterprise token empty: $enterprise_token\n"
            printf "1) edit $0 enterprise_token='xxxxxx' \n"
            printf "2) add flag --token xxxxxxxxx \n"
            printf "Find your token @ https://customers.mariadb.com/downloads/token/ \n\n"

            exit 1;
        fi;
    fi
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


# Confirms mac have critical binaries to run this script
# As of 3/2024 supports cs_package_manager.sh check 
check_mac_dependencies() {
    
    # Install ggrep if not exists
    if ! which ggrep >/dev/null 2>&1; then
        echo "Attempting Auto install of ggrep"

        if ! which brew >/dev/null 2>&1; then
            echo "Attempting Auto install of brew" 
            bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        brew install grep
    fi

    # Exit if ggrep still doesnt exist
    if ! which ggrep >/dev/null 2>&1; then
        echo "Failed to install ggrep"
        echo "which ggrep"
        echo ""
        exit 1;
    fi

}

check_operating_system() {

    if [[ $OSTYPE == 'darwin'* ]]; then
        echo "Running on macOS"
        check_mac_dependencies

        # on action=check - these values are used as triggers to prompt the user to select what OS/version they want to check against
        distro_info="mac"
        distro="mac"
        version_id_exact=$(grep -A1 ProductVersion "/System/Library/CoreServices/SystemVersion.plist" | sed -n 's/.*<string>\(.*\)<\/string>.*/\1/p')
        version_id=$( echo "$version_id_exact" | awk -F. '{print $1}')
        distro_short="${distro_info:0:3}${version_id}"
        return
    fi;

    distro_info=$(awk -F= '/^ID=/{gsub(/"/, "", $2); print $2}' /etc/os-release)
    version_id_exact=$( grep 'VERSION_ID=' /etc/os-release | awk -F= '{gsub(/"/, "", $2); print $2}')
    version_id=$( echo "$version_id_exact" | awk -F. '{print $1}')

    echo "Distro: $distro_info"
    echo "Version: $version_id"

    # distros=(centos7 debian11 debian12 rockylinux8 rockylinux9 ubuntu20.04 ubuntu22.04)
    case $distro_info in
        centos | rhel )
            distro="${distro_info}${version_id}"
            ;;
        debian )
            distro="${distro_info}${version_id_exact}"
            ;;
        rocky )
            distro="rockylinux${version_id}"
            ;;
        ubuntu )
            distro="${distro_info}${version_id_exact}"
            ;;
        *)  # unknown option
            printf "\ncheck_operating_system: unknown os & version: $distro_info\n"
            exit 2;
    esac   
    distro_short="${distro_info:0:3}${version_id}"
}

check_cpu_architecture() {

    architecture=$(uname -m)
    echo "CPU: $architecture"

    # arch=(amd64 arm64)
    case $architecture in
        x86_64 )
            arch="amd64"
            ;;
        aarch64 )
            arch="arm64"
            ;;
        *)  # unknown option
            echo "Error: Unsupported architecture ($architecture)"
    esac    
}

check_mdb_installed() {
    packages=""
    current_mariadb_version=""
    case $package_manager in
        yum ) 
            packages=$(yum list installed | grep -i mariadb)
            current_mariadb_version=$(rpm -q --queryformat '%{VERSION}\n' MariaDB-server 2>/dev/null)
            ;;
        apt )
            packages=$(apt list --installed mariadb-* 2>/dev/null | grep -i mariadb);
            current_mariadb_version=$(dpkg-query -f '${Version}\n' -W mariadb-server 2>/dev/null)
            # remove prefix 1:  & +maria~deb1 - example:  current_mariadb_version="1:10.6.16.11+maria~deb1"
            current_mariadb_version="${current_mariadb_version#*:}"
            current_mariadb_version="${current_mariadb_version%%+*}"
            ;;
        *)  # unknown option
            printf "\ncheck_no_mdb_installed: package manager not implemented - $package_manager\n"
            exit 2;
    esac

    if [ -z "$packages" ]; then
        printf "\nMariaDB packages are NOT installed. Please install them before continuing.\n"
        echo $packages;
        printf "Example: bash $0 install [community|enterprise] [version] \n\n"
        exit 2;
    fi;

    
}

check_no_mdb_installed() {

    packages=""
    case $distro_info in
        centos | rhel | rocky )
            packages=$(yum list installed | grep -i mariadb)
            ;;
        ubuntu | debian )
            packages=$(apt list --installed mariadb-* 2>/dev/null | grep -i mariadb);
            ;;
        *)  # unknown option
            printf "\ncheck_no_mdb_installed: os & version not implemented: $distro_info\n"
            exit 2;
    esac

    if [ -n "$packages" ]; then
        printf "\nSome MariaDB packages are installed\nPlease uninstall the following before continuing:\n"
        printf "$packages";
        printf "\n\nExample: bash $0 remove\n\n"
        exit 2;
    fi;
}

check_aws_cli_installed() {

    if ! command -v aws &> /dev/null ; then
        echo "[!] aws cli - binary could not be found"
        echo "[+] Installing aws cli ..."

        cli_url="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
        case $architecture in
            x86_64 )
                cli_url="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
                ;;
            aarch64 )
                cli_url="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"
                ;;
            *)  # unknown option
                echo "Error: Unsupported architecture ($architecture)"
        esac    

        case $distro_info in
            centos | rhel | rocky )
                rm -rf aws awscliv2.zip
                yum install unzip -y;
                curl "$cli_url" -o "awscliv2.zip";
                unzip -q awscliv2.zip;
                sudo ./aws/install;
                mv /usr/local/bin/aws /usr/bin/aws;
                aws configure set default.s3.max_concurrent_requests 70       
                ;;
            ubuntu | debian )
                rm -rf aws awscliv2.zip
                if ! DEBIAN_FRONTEND=noninteractive apt install unzip -y; then
                    echo "[!!] Installing Unzip Failed: Trying update"
                    DEBIAN_FRONTEND=noninteractive  apt update -y;
                    DEBIAN_FRONTEND=noninteractive  apt install unzip -y;
                fi
                curl "$cli_url" -o "awscliv2.zip";
                unzip -q awscliv2.zip;
                sudo ./aws/install;
                mv /usr/local/bin/aws /usr/bin/aws;
                aws configure set default.s3.max_concurrent_requests 70      
                ;;
            *)  # unknown option
                printf "\nos & version not implemented: $distro_info\n"
                exit 2;
        esac
        
       
    fi
}

check_cluster_dependancies() {

    if ! command -v telnet &> /dev/null; then
        printf "\n[!] telnet not found. Attempting Auto-install\n\n"

        case $package_manager in
            yum ) 
                if ! yum install telnet -y; then
                    printf "\n[!!] Failed to install telnet. Please manually install \n\n"
                    exit 1;
                fi
                ;;
            apt )
                if ! apt install telnet -y --quiet; then
                    printf "\n[!!] Failed to install telnet. Please manually install \n\n"
                    exit 1;
                fi
                ;;
            *)  # unknown option
                printf "\ncheck_cluster_dependancies: package manager not implemented - $package_manager\n"
                exit 2;
        esac
    fi  
}

# main goal is to enable binlog with unique server_id for replication
configure_default_mariadb_server_config() {
    
    case $distro_info in
        centos | rhel | rocky )
            server_cnf_dir="/etc/my.cnf.d"
            server_cnf_location="/etc/my.cnf.d/server.cnf"
            ;;
        ubuntu | debian )
            server_cnf_dir="/etc/mysql/mariadb.conf.d"
            server_cnf_location="/etc/mysql/mariadb.conf.d/server.cnf"
            ;;
        *)  # unknown option
            printf "\ncheck_no_mdb_installed: os & version not implemented: $distro_info\n"
            exit 2;
    esac
    
    if [ ! -d $server_cnf_dir ]; then
        echo " - Creating $server_cnf_dir"
        mkdir -p $server_cnf_dir
        chown mysql:mysql $server_cnf_dir
    fi

    echo "[mariadb]
bind_address                           = 0.0.0.0
log_error                              = mariadbd.err
character_set_server                   = utf8
collation_server                       = utf8_general_ci
log_bin                                = mariadb-bin
log_bin_index                          = mariadb-bin.index
relay_log                              = mariadb-relay
relay_log_index                        = mariadb-relay.index
log_slave_updates                      = ON
gtid_strict_mode                       = ON
columnstore_use_import_for_batchinsert = ALWAYS
lower_case_table_names=1
#expire_logs_days= 2

# This must be unique on each MariaDB Enterprise Node
server_id                              = $dbroot" > $server_cnf_location
    
    chown mysql:mysql $server_cnf_location
    stop_mariadb
    start_mariadb

}

create_mariadb_users() {

    if [ -n "$cross_engine_user" ] && [ -n "$cross_engine_pwd" ]; then
        if mariadb -e "CREATE USER '$cross_engine_user'@'127.0.0.1' IDENTIFIED BY '$cross_engine_pwd'; GRANT SELECT,PROCESS ON *.* TO '$cross_engine_user'@'127.0.0.1'" ; then
            echo " - Created User: $cross_engine_user "
        else
            echo "[!] FAILED to create user: $cross_engine_user "
            exit 1
        fi
    fi
    
    if [ -n "$replication_user" ]; then
        if mariadb -e "CREATE USER '$replication_user'@'%' IDENTIFIED BY '$replication_pwd'; GRANT REPLICA MONITOR, REPLICATION REPLICA, REPLICATION REPLICA ADMIN, REPLICATION MASTER ADMIN ON *.* TO '$replication_user'@'%';"; then
            echo " - Created User: $replication_user "
        else
            echo "[!] FAILED to create user: $replication_user "
            exit 1
        fi
    fi

    if [ -n "$maxscale_user" ]; then
        if mariadb -e "CREATE USER $maxscale_user@'%' IDENTIFIED BY '$maxscale_pwd';
        GRANT SUPER, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE, SHOW DATABASES ON *.* TO $maxscale_user@'%';
        GRANT SELECT ON mysql.db TO $maxscale_user@'%';
        GRANT SELECT ON mysql.user TO $maxscale_user@'%';
        GRANT SELECT ON mysql.roles_mapping TO $maxscale_user@'%';
        GRANT SELECT ON mysql.tables_priv TO $maxscale_user@'%';
        GRANT SELECT ON mysql.columns_priv TO $maxscale_user@'%';
        GRANT SELECT ON mysql.proxies_priv TO $maxscale_user@'%';"; then
            echo " - Created User: $maxscale_user "
        else
            echo "[!] FAILED to create user: $maxscale_user "
            exit 1
        fi
    fi
}

configure_columnstore_cross_engine_user() {
    
    if [ -n "$cross_engine_user" ] && [ -n "$cross_engine_pwd" ]; then
        if command -v mcsSetConfig &> /dev/null ; then
            mcsSetConfig CrossEngineSupport User "$cross_engine_user"
            mcsSetConfig CrossEngineSupport Password "$cross_engine_pwd"
        else
            echo "mcsSetConfig doesnt exist: Cant configure cross engine joins"
        fi
    fi
}

poll_for_cmapi_online() {

    local sleep_timer=3

    # Use telnet to check port 8640 on replica nodes
    for node in $(echo $nodes | tr "," "\n"); do
        counter=0
        printf "%-35s ..." " - Checking cmapi port 8640 on $node "
        while true; do
        
            if telnet $node 8640 < /dev/null 2>&1 | grep -q 'Connected'; then
                printf " Success\n"
                break;
            else
                printf "."
            fi

            # Max wait 3000 seconds
            if [ $counter -gt 1000 ]; then
                echo "Failed to find open cmapi port after 1000 checks"
                exit;
            fi
            sleep $sleep_timer
            ((counter++))
        done
    done
}

configure_cluster_via_cmapi() {
    
    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    local dbroot=1
    for node in $(echo $nodes | tr "," "\n"); do
        
        if command -v mcs &> /dev/null ; then
            printf "%-35s ..." " - Adding Node $dbroot: $node "
            if mcs_output=$( timeout 120s mcs cluster node add --node $node ); then 
                printf " Done - $( echo $cmapi_output | jq -r tostring )  \n"
            else 
                echo "[!] Failed ... trying cmapi curl"
                echo "$mcs_output"
                add_node_cmapi_via_curl $node
            fi
        else
            echo "mcs - binary could not be found ..."
            add_node_cmapi_via_curl $node
        fi;
        ((dbroot++))
    done
}

poll_for_primary_mariadb_connectivity() {
    
    if [ -z "$primary_ip" ]; then
        echo "Primary IP not defined"
        exit 1;
    fi

    local port=3306
    local sleep_timer=3
    local counter=0

    # Use telnet to check port 3306 on primary node 
    printf "%-35s ..." " - Checking mariadb port $port on $primary_ip..."
    while true; do
        if telnet $primary_ip $port < /dev/null 2>&1 | grep -q 'Connected'; then
            printf " Success\n"
            break;
        else
            printf "."
        fi

        # Max wait 3000 seconds
        if [ $counter -gt 1000 ]; then
            echo "Failed to find open mariadb port $port after 1000 checks"
            exit;
        fi
        sleep $sleep_timer
        ((counter++))
    done
    
    local counter=0
    printf "%-35s ..." " - Checking replication credentials"
    while true; do
        
        if mariadb -h $primary_ip -u $replication_user -p"$replication_pwd" -e "SELECT 1;" &> /dev/null; then
            printf " Success\n"
            break;
        else
            printf "."
        fi

        # Max wait 3000 seconds
        if [ $counter -gt 1000 ]; then
            echo "Failed to find open mariadb port $port after 1000 checks"
            exit;
        fi
        sleep $sleep_timer
        ((counter++))
    done

}

configure_mariadb_replication() {

    if [ -z "$primary_ip" ]; then
        echo "Primary IP not defined"
        exit 1;
    fi

    mariadb -e "stop slave;CHANGE MASTER TO MASTER_HOST='$primary_ip' , MASTER_USER='$replication_user' , MASTER_PASSWORD='$replication_pwd' , MASTER_USE_GTID = slave_pos;start slave;"
    sleep 2;
    mariadb -e  "show slave status\G" | grep  -E "Slave_IO_Running|Slave_SQL|Last_IO_Error|Gtid_IO_Pos"
}

check_dev_build_exists() {

    if ! aws s3 ls $s3_path --no-sign-request &> /dev/null; then 
        printf "[!] Defined dev build doesnt exist in aws\n\n"
        exit 2;
    fi;
}

check_jenkins_build_exists() {

    # Use curl to check if the URL exists
    http_status=$(curl -s -o /dev/null -w "%{http_code}" --user "$jenkins_user:$jenkins_pwd" "$jenkins_url")

    # Check if the HTTP status code is 200 (OK)
    if [ "$http_status" -eq 200 ]; then
        return 0
    else
        echo "URL does not exist or an error occurred:"
        echo "$jenkins_url"
        exit 2
    fi
}


post_cmapi_install_configuration() {
    
    systemctl daemon-reload
    systemctl enable mariadb-columnstore-cmapi
    systemctl start mariadb-columnstore-cmapi
    mariadb -e "show status like '%Columnstore%';"
    sleep 1;
    
    
    if [ -n "$nodes" ] && [ $dbroot == 1  ] ; then
        
        # Handle Cluster Configuration - Primary Node
        configure_default_mariadb_server_config
        create_mariadb_users
        configure_columnstore_cross_engine_user
        poll_for_cmapi_online
        configure_cluster_via_cmapi
        
    elif [ -n "$nodes" ] && [ $dbroot -gt 1  ]; then
        
        # Handle Cluster Configuration - Replica Nodes
        configure_default_mariadb_server_config
        poll_for_primary_mariadb_connectivity
        configure_mariadb_replication
        
    else
        # Handle Single node
        confirm_cmapi_online_and_configured
        create_mariadb_users
        configure_columnstore_cross_engine_user
        init_cs_up
    fi
}

do_enterprise_apt_install() {

    # Install MariaDB
    apt-get clean
    if ! apt install mariadb-server -y --quiet; then
        printf "\n[!] Failed to install mariadb-server \n\n"
        exit 1;
    fi
    sleep 2
    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! apt install mariadb-plugin-columnstore -y --quiet; then 
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! apt install mariadb-columnstore-cmapi jq -y --quiet; then 
            printf "\n[!] Failed to install cmapi\n\n"
            mariadb -e "show status like '%Columnstore%';"
        else
           post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi

}

do_enterprise_yum_install() {

    # Install MariaDB
    yum clean all
    yum install MariaDB-server -y
    sleep 2
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! yum install MariaDB-columnstore-engine -y; then
        printf "\n[!] Failed to install columnstore\n\n"
        exit 1;
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        
        if ! yum install MariaDB-columnstore-cmapi jq -y; then
            printf "\n[!] Failed to install cmapi\n\n"
            mariadb -e "show status like '%Columnstore%';"
        else 
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi

}

process_cluster_variables() {
    # check what IP addresses in nodes match the current IP
    if [ -n "$nodes" ]; then
        check_cluster_dependancies

        this_ip=$(hostname -I | awk '{print $1}')
        if ! echo "$nodes" | grep -q "$this_ip"; then
            printf "\n[!] Current IP address: $this_ip\nNOT in nodes list: $nodes\n\n"
            exit 1;
        fi
        dbroot=1
        primary_ip=""
        for node in $(echo $nodes | tr "," "\n"); do
            # The first in the list is the primary
            if [ $dbroot -eq 1 ]; then
                primary_ip=$node
            fi
            if [ "$node" == "$this_ip" ]; then
                break;
            fi
            if [ $dbroot -gt 10000 ]; then
               echo "Failed to find $this_ip index in nodes list"
               exit 1;
            fi
            ((dbroot++))
        done

        if [ -z "$primary_ip" ]; then
            echo "Primary IP not found in nodes list"
            exit 1;
        fi

        echo "IP: $this_ip"
        echo "Server ID: $dbroot"
        echo "Nodes: $nodes"
        echo "Primary IP: $primary_ip"
        echo "Replication User: $replication_user"
        echo "Replication Password: $replication_pwd"
        echo "Maxscale User: $maxscale_user"
        echo "Maxscale Password: $maxscale_pwd"
        echo "Cross Engine User: $cross_engine_user"
        echo "Cross Engine Password: $cross_engine_pwd"
    fi
}

quick_version_check() {
    if [ -z $version ]; then 
        printf "\n[!] Version empty: $version\n\n"
        exit 1;
    fi;

    case $version in
        -h | --help | help )
            print_install_help_text
            exit 0;
            ;;
        -* )
            # Confirm doesnt start with "-"", like a flag
            printf "\n[!] Version cannot begin with '-': $version\n\n"
            exit 1
            ;;
        *)
            continue
            ;;
    esac

    # Check if version contains at least one number
    if ! [[ "$version" =~ [0-9] ]]; then
        printf "\n[!] Version must contain at least one numeric character: $version\n\n"
        exit 1
    fi
}

enterprise_install() {
    
    version=$3 
    quick_version_check

    parse_install_cluster_additional_args "$@"

    echo "Token: $enterprise_token"
    echo "MariaDB Version: $version"
    process_cluster_variables
    echo "-----------------------------------------------"

    url="https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup"
    if $enterprise_staging; then 
        url="https://dlm.mariadb.com/$enterprise_token/enterprise-release-helpers-staging/mariadb_es_repo_setup"
    fi

    # Download Repo setup script
    rm -rf mariadb_es_repo_setup
    curl -LO "$url" -o mariadb_es_repo_setup;
    chmod +x mariadb_es_repo_setup;  
    if ! bash mariadb_es_repo_setup --token="$enterprise_token" --apply --mariadb-server-version="$version"; then
        printf "\n[!] Failed to apply mariadb_es_repo_setup...\n\n"
        exit 2;
    fi;

    case $distro_info in
        centos | rhel | rocky )

            if [ ! -f "/etc/yum.repos.d/mariadb.repo" ]; then printf "\n[!] Expected to find mariadb.repo in /etc/yum.repos.d \n\n"; exit 1; fi;

            if $enterprise_staging; then 
                sed -i 's/mariadb-es-main/mariadb-es-staging/g' /etc/yum.repos.d/mariadb.repo
                sed -i 's/mariadb-enterprise-server/mariadb-enterprise-staging/g' /etc/yum.repos.d/mariadb.repo
                printf "\n\n[+] Adjusted mariadb.repo to: mariadb-enterprise-staging\n\n"
            fi;

            do_enterprise_yum_install "$@" 
            ;;
        ubuntu | debian )

            if [ ! -f "/etc/apt/sources.list.d/mariadb.list" ]; then printf "\n[!] Expected to find mariadb.list in /etc/apt/sources.list.d \n\n"; exit 1; fi;

            if $enterprise_staging; then 
                sed -i 's/mariadb-enterprise-server/mariadb-enterprise-staging/g' /etc/apt/sources.list.d/mariadb.list
                apt update
                printf "\n\n[+] Adjusted mariadb.list to: mariadb-enterprise-staging\n\n"
            fi;

            do_enterprise_apt_install "$@" 
            ;;
        *)  # unknown option
            printf "\nenterprise_install: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

community_install() {
    
    version=$3 
    quick_version_check
    
    parse_install_cluster_additional_args "$@"

    echo "MariaDB Community Version: $version"
    process_cluster_variables
    echo "-----------------------------------------------"

    # Download Repo setup
    rm -rf mariadb_repo_setup
   
    if !  curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup |  bash -s -- --mariadb-server-version=mariadb-$version ; then
        echo "version bad or mariadb_repo_setup unavailable. exiting ..."
        exit 2;
    fi;

    case $distro_info in
        centos | rhel | rocky )
            do_community_yum_install "$@" 
            ;;
        ubuntu | debian )
            do_community_apt_install "$@" 
            ;;
        *)  # unknown option
            printf "\ncommunity_install: os & version not implemented: $distro_info\n"
            exit 2;
    esac

}

do_community_yum_install() {

    # Install MariaDB then Columnstore
    yum clean all
    if !  yum install MariaDB-server -y; then
        printf "\n[!] Failed to install MariaDB-server \n\n"
        exit 1;
    fi
    sleep 2;
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! yum install MariaDB-columnstore-engine -y; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        cmapi_installable=$(yum list | grep MariaDB-columnstore-cmapi) 
        if [ -n "$cmapi_installable" ]; then 
            if ! yum install MariaDB-columnstore-cmapi jq -y; then
                printf "\n[!] Failed to install cmapi\n\n"
                exit 1;
            else
                post_cmapi_install_configuration
            fi
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi
}

do_community_apt_install() {

    # Install MariaDB
    apt-get clean
    if ! apt install mariadb-server -y --quiet; then
        printf "\n[!] Failed to install mariadb-server \n\n"
        exit 1;
    fi
    sleep 2
    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! apt install mariadb-plugin-columnstore -y --quiet; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi;

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! apt install mariadb-columnstore-cmapi jq -y --quiet ; then 
            printf "\n[!] Failed to install cmapi \n\n"
            mariadb -e "show status like '%Columnstore%';"
        else 
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi
}

get_set_cmapi_key() {

    CMAPI_CNF="/etc/columnstore/cmapi_server.conf"

    if [ ! -f $CMAPI_CNF ]; then 
        echo "[!!] No cmapi config file found"
        exit  1;
    fi;

    # Add API Key if missing
    if [ -z "$(grep ^x-api-key $CMAPI_CNF)" ]; then
       
        if ! command -v openssl &> /dev/null ; then
            api_key="19bb89d77cb8edfe0864e05228318e3dfa58e8f45435fbd9bd12c462a522a1e9"
        else 
            api_key=$(openssl rand -hex 32)
        fi
        
        printf "%-35s ..." " - Setting API Key:"
        if cmapi_output=$( curl -s https://127.0.0.1:8640/cmapi/0.4.0/cluster/status \
        --header 'Content-Type:application/json' \
        --header "x-api-key:$api_key" -k ) ; then
            printf " Done - $( echo $cmapi_output | jq -r tostring )  \n"
            sleep 2;
        else
            printf  " Failed to set API key\n\n"
            exit 1;
        fi
    else 
        api_key=$(grep ^x-api-key $CMAPI_CNF | cut -d "=" -f 2 | tr -d " ")
    fi
}

add_node_cmapi_via_curl() {

    node_ip=$1
    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    # Add Node
    printf "%-35s ..." " - Adding node ($node_ip) via curl"
    if cmapi_output=$( curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/node \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data "{\"timeout\": 120, \"node\": \"$node_ip\"}" ); then
        printf " Done - $(echo $cmapi_output | jq -r tostring )\n"
    else
        echo "Failed adding node: $node_ip"
        exit 1;
    fi

}

start_cs_via_systemctl() {
    if systemctl start mariadb-columnstore ; then
        echo " - Started Columnstore"
    else
        echo "[!!] Failed to start columnstore via systemctl"
        exit 1;
    fi;
}

start_cs_cmapi_via_curl() {

    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    if curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/start  \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data '{"timeout":20}'; then
        echo " - Started Columnstore"
    else 
        echo " - [!] Failed to start columnstore via cmapi curl"
        echo " - Trying via systemctl ..."
        start_cs_via_systemctl
    fi;
}

stop_service_if_exists() {
    local timeout="30s"
    local service_name="$1"
    if systemctl list-units --full -all | grep -Fq "$service_name"; then
        if ! timeout $timeout systemctl stop "$service_name" 2>/dev/null ; then
            echo "Failed to stop $service_name"
        fi
    fi
}
    
stop_cs_via_systemctl_override() {

    stop_service_if_exists "mcs-ddlproc"
    stop_service_if_exists "mcs-dmlproc"
    stop_service_if_exists "mcs-workernode@1"
    stop_service_if_exists "mcs-workernode@2"
    stop_service_if_exists "mcs-controllernode"
    stop_service_if_exists "mcs-storagemanager"
    stop_service_if_exists "mcs-primproc"
    stop_service_if_exists "mcs-writeengineserver"
    stop_service_if_exists "mariadb-columnstore-cmapi"
}

kill_cs_processes() {
   ps -ef | grep -E '(PrimProc|ExeMgr|DMLProc|DDLProc|WriteEngineServer|StorageManager|controllernode|workernode|load_brm|save_brm)' | grep -v "grep" | awk '{print $2}' | xargs kill -9
}

kill_mariadbd_processes() {
    ps -ef | grep -E '(mariadbd)' | grep -v "grep" | awk '{print $2}' | xargs kill -9
}

stop_cs_via_systemctl() {
    printf " - Trying to stop columnstore via systemctl"
    if systemctl stop mariadb-columnstore ; then
        printf " Done\n"
    else
        printf "\n[!!] Failed to stop columnstore\n"
        exit 1;
    fi;
}

stop_cs_cmapi_via_curl() {

    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    if curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/shutdown  \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data '{"timeout":20}'; then
        echo " - Stopped Columnstore via curl"
    else 
        printf "\n[!] Failed to stop columnstore via cmapi\n"
        stop_cs_via_systemctl
    fi;
}

add_primary_node_cmapi() {

    primary_ip="127.0.0.1"
    if [ -z $api_key  ]; then get_set_cmapi_key; fi;
    
    if command -v mcs &> /dev/null ; then
        # Only add 127.0.0.1 if no nodes are configured in cmapi
        if [ "$(mcs cluster status | jq -r '.num_nodes')" == "0" ]; then
            printf "%-35s ..." " - Adding primary node"
            if mcs_output=$( timeout 30s mcs cluster node add --node $primary_ip ); then 
                echo " Done - $( echo $mcs_output | jq -r tostring )"
            else 
                echo "[!] Failed ... trying cmapi curl"
                echo "$mcs_output"
                add_node_cmapi_via_curl $primary_ip
            fi;
        fi;
        
    else 
        echo "mcs - binary could not be found"
        add_node_cmapi_via_curl $primary_ip
        printf "%-35s ..." " - Starting Columnstore Engine"
        start_cs_cmapi_via_curl
    fi
}


dev_install() {
    
    if [ -z $dev_drone_key ]; then printf "Missing dev_drone_key: \n"; exit; fi;
    check_aws_cli_installed

    echo "Branch: $3"
    echo "Build: $4"
    dronePath="s3://$dev_drone_key"
    branch="$3"
    build="$4"
    product="10.6-enterprise"
    if [ -z "$branch" ]; then printf "Missing branch: $branch\n"; exit 2; fi;
    if [ -z "$build" ]; then printf "Missing build: $branch\n"; exit 2; fi;
    parse_install_cluster_additional_args "$@"

    # Construct URLs
    s3_path="$dronePath/$branch/$build/$product/$arch"
    drone_http=$(echo "$s3_path" | sed "s|s3://$dev_drone_key/|https://${dev_drone_key}.s3.amazonaws.com/|")
    echo "Locations:"
    echo "Bucket: $s3_path"
    echo "Drone: $drone_http"
    process_cluster_variables
    echo "###################################"
    
    check_dev_build_exists

    case $distro_info in
        centos | rhel | rocky )
            s3_path="${s3_path}/$distro"
            drone_http="${drone_http}/$distro"
            do_dev_yum_install "$@" 
            ;;
        ubuntu | debian )
            do_dev_apt_install "$@" 
            ;;
        *)  # unknown option
            printf "\ndev_install: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

do_dev_yum_install() {

    echo "[drone]
name=Drone Repository
baseurl="$drone_http"
gpgcheck=0
enabled=1
    " > /etc/yum.repos.d/drone.repo
    yum clean all
    # yum makecache
    # yum list --disablerepo="*" --enablerepo="drone"

    # ALL RPMS:  aws s3 cp $s3_path/ . --recursive --exclude "debuginfo" --include "*.rpm"
    aws s3 cp $s3_path/ . --recursive --exclude "*" --include "MariaDB-server*" --exclude "*debug*" --no-sign-request

    # Confirm Downloaded server rpm
    if ! ls MariaDB-server-*.rpm 1> /dev/null 2>&1; then
        echo "Error: No MariaDB-server RPMs were found."
        exit 1
    fi

    # Install MariaDB Server
    if ! yum install MariaDB-server-*.rpm -y; then
        printf "\n[!] Failed to install MariaDB-server \n\n"
        exit 1;
    fi

    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! yum install MariaDB-columnstore-engine -y; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! yum install MariaDB-columnstore-cmapi jq -y; then
            printf "\n[!] Failed to install cmapi\n\n"
            exit 1;
        else
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi
}

do_dev_apt_install() {

    echo "deb [trusted=yes] ${drone_http} ${distro}/" >  /etc/apt/sources.list.d/repo.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin cspkg.s3.amazonaws.com
Pin-Priority: 1700
EOF

    # Install MariaDB Server
    apt-get clean
    apt-get update 
    if ! apt install mariadb-server -y --quiet; then
        printf "\n[!] Failed to install mariadb-server \n\n"
        exit 1;
    fi
    sleep 2
    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! apt install mariadb-plugin-columnstore -y --quiet; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi;

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! apt install mariadb-columnstore-cmapi jq -y --quiet ; then 
            printf "\n[!] Failed to install cmapi \n\n"
            mariadb -e "show status like '%Columnstore%';"
        else 
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi

}

do_jenkins_yum_install() {

    # list packages
    grep_list="MariaDB-backup-*|MariaDB-client-*|MariaDB-columnstore-engine-*|MariaDB-common-*|MariaDB-server-*|MariaDB-shared-*|galera-enterprise-*|cmapi"
    rpm_list=$(curl -s -u $jenkins_user:$jenkins_pwd $jenkins_url/$os_package/ | grep -oP '(?<=href=").+?\.rpm' | sed 's/.*\///' | grep -v debug | grep -E "$grep_list")
    
    if [ -z "$rpm_list" ]; then
        echo "No RPMs found"
        echo "command:  curl -s -u $jenkins_user:$jenkins_pwd $jenkins_url/$os_package/"
        exit 2
    fi

    for rpm in $rpm_list; do
        echo "Downloading $rpm..."
        if ! curl -s --user "$jenkins_user:$jenkins_pwd" -o "$rpm" "$jenkins_url/$os_package/$rpm"; then
            echo "Failed to download: $rpm"
            exit 2
        fi
    done

    # Confirm Downloaded server rpm
    if ! ls MariaDB-server-*.rpm 1> /dev/null 2>&1; then
        echo "Error: No MariaDB-server RPMs were found."
        exit 1
    fi

    # Install MariaDB Server
    if ! yum install MariaDB-server-* galera-enterprise-* MariaDB-client-* MariaDB-common-* MariaDB-shared-* -y; then
        printf "\n[!] Failed to install MariaDB-server \n\n"
        exit 1;
    fi

    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! yum install MariaDB-columnstore-engine-* -y; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi

    if ! ls MariaDB-columnstore-cmapi* 1> /dev/null 2>&1; then
 
        # Construct URLs
        dronePath="s3://$dev_drone_key"
        branch="stable-23.10"
        build="latest"
        product="10.6-enterprise"
        s3_path="$dronePath/$branch/$build/$product/$arch/rockylinux${version_id}"
        check_aws_cli_installed
        echo "Attempting to download cmapi from drone: $s3_path"
        
        aws s3 cp $s3_path/ . --recursive --exclude "*" --include "MariaDB-columnstore-cmapi-*" --exclude "*debug*" --no-sign-request

        if ! ls MariaDB-columnstore-cmapi* 1> /dev/null 2>&1; then
            echo "Error: No MariaDB-columnstore-cmapi RPMs were found."
            exit 1
        fi
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! yum install MariaDB-columnstore-cmapi-* jq -y; then
            printf "\n[!] Failed to install cmapi\n\n"
            exit 1;
        else
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi
    
    return
}

do_jenkins_apt_install() {
    # list packages
    echo "++++++++++++++++++++++++++++++++++++++++++++"
    grep_list="mariadb-backup-*|mariadb-client-*|mariadb-plugin-columnstore-*|mariadb-common*|mariadb-server-*|mariadb-shared-*|galera-enterprise-*|cmapi|libmariadb3|mysql-common"
    rpm_list=$(curl -s -u $jenkins_user:$jenkins_pwd $jenkins_url/$os_package/ | grep -oP '(?<=href=").+?\.deb' | sed 's/.*\///' | grep -v debug | grep -E "$grep_list")

    if [ -z "$rpm_list" ]; then
        echo "No RPMs found"
        echo "command:  curl -s -u $jenkins_user:$jenkins_pwd $jenkins_url/$os_package/"
        exit 2
    fi
    
    for rpm in $rpm_list; do
        echo "Downloading $rpm..."
        if ! curl -s --user "$jenkins_user:$jenkins_pwd" -o "$rpm" "$jenkins_url/$os_package/$rpm"; then
            echo "Failed to download: $rpm"
            exit 2
        fi
    done

    # Install MariaDB
    DEBIAN_FRONTEND=noninteractive apt update 
    DEBIAN_FRONTEND=noninteractive apt upgrade -y --quiet
    apt-get clean
    DEBIAN_FRONTEND=noninteractive sudo apt install gawk libdbi-perl lsof perl rsync -y --quiet
    DEBIAN_FRONTEND=noninteractive sudo apt install libdbi-perl socat libhtml-template-perl -y --quiet
    if ! DEBIAN_FRONTEND=noninteractive apt install $(pwd)/mysql-common*.deb $(pwd)/mariadb-server*.deb $(pwd)/galera-enterprise-* $(pwd)/mariadb-common*.deb  $(pwd)/mariadb-client-*.deb $(pwd)/libmariadb3*.deb -y --quiet; then
        printf "\n[!] Failed to install mariadb-server \n\n"
        exit 1;
    fi
    sleep 2
    systemctl daemon-reload
    systemctl enable mariadb
    systemctl start mariadb

    # Install Columnstore
    if ! DEBIAN_FRONTEND=noninteractive apt install $(pwd)/mariadb-plugin-columnstore*.deb  -y --quiet --allow-downgrades; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi;

    if ! ls mariadb-columnstore-cmapi*.deb 1> /dev/null 2>&1; then
 
        # Construct URLs
        dronePath="s3://$dev_drone_key"
        branch="stable-23.10"
        build="latest"
        product="10.6-enterprise"
        s3_path="$dronePath/$branch/$build/$product/$arch/$distro"
        check_aws_cli_installed
        echo "Attempting to download cmapi from drone: $s3_path"
        
        aws s3 cp $s3_path/ . --recursive --exclude "*" --include "mariadb-columnstore-cmapi*" --exclude "*debug*" --no-sign-request

        if ! ls mariadb-columnstore-cmapi*.deb 1> /dev/null 2>&1; then
            echo "Error: No MariaDB-columnstore-cmapi RPMs were found."
            echo "do_jenkins_apt_install:   aws s3 ls $s3_path/ "
            exit 1
        fi
    fi

    # Install CMAPI
    if $CONFIGURE_CMAPI ; then
        if ! DEBIAN_FRONTEND=noninteractive apt install $(pwd)/mariadb-columnstore-cmapi*.deb jq -y --quiet ; then 
            printf "\n[!] Failed to install cmapi \n\n"
            mariadb -e "show status like '%Columnstore%';"
        else 
            post_cmapi_install_configuration
        fi
    else
        create_mariadb_users
        configure_columnstore_cross_engine_user
    fi




    return
}


jenkins_install() {

    if [ -z $jenkins_user ]; then printf "Missing jenkins_user: \n"; exit; fi;
    if [ -z $jenkins_pwd ]; then printf "Missing jenkins_pwd: \n"; exit; fi;

    product_branch_commit="$3"
    echo "Product/Branch/Commit: $product_branch_commit"
    if [ -z "$product_branch_commit" ]; then printf "Missing Product/Branch/Commit: $product_branch_commit\n"; exit 2; fi;
    parse_install_cluster_additional_args "$@"

    # Construct URLs
    jenkins_url="https://es-repo.mariadb.net/jenkins/${product_branch_commit}/"
    echo "Jenkins URL: $jenkins_url"
    process_cluster_variables
    echo "###################################"
    
    check_jenkins_build_exists
    
    if $CONFIGURE_CMAPI && [ -z $dev_drone_key ]; then printf "Missing dev_drone_key: \n"; exit; fi;


    case $distro_info in
        centos | rhel | rocky )
            jenkins_url="${jenkins_url}RPMS"
            os_package="rhel-$version_id"
            if [ "$architecture" == "arm64" ]; then
                os_package+="rhel-$version_id-arm"
            fi
            do_jenkins_yum_install "$@" 
            ;;
            
        ubuntu | debian )
            jenkins_url="${jenkins_url}DEB"

            if [ "$distro_info" == "ubuntu" ]; then
                version_formatted="${version_id_exact//.}"
                os_package="ubuntu-$version_formatted"
            else
                os_package="debian-$version_id"
            fi
      
            
            if [ "$architecture" == "arm64" ]; then
                os_package="${os_package}-arm"
            fi
            do_jenkins_apt_install "$@" 
            ;;
        *)  # unknown option
            printf "\njenkins_install: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

do_install() {

    check_operating_system
    check_cpu_architecture
    check_no_mdb_installed
    check_package_managers
    set_default_cluster_variables

    repo=$2
    enterprise_staging=false
    echo "Repository: $repo"
    case $repo in
        enterprise )
            # pull from enterprise repo
            enterprise_install "$@" ;
            ;;
        enterprise_staging )
            enterprise_staging=true
            enterprise_install "$@" ;
            ;;
        community )
            # pull from public community repo
            community_install "$@" ;
            ;;
        dev )
            # pull from dev repo - requires dev_drone_key
            dev_install "$@" ;
            ;;
        jenkins ) 
            # pull from jenkins repo - requires jenkins_user_name and jenkins_password
            jenkins_install "$@" ;
            ;;
        -h | --help | help )
            print_install_help_text
            exit 0;
            ;;
        *)  # unknown option
            print_install_help_text
            echo "Unknown repo: $repo\n"
            exit 2;
    esac

    printf "\nInstall Complete\n\n"
}

# Small augmentation of https://github.com/mariadb-corporation/mariadb-columnstore-engine/blob/develop/cmapi/check_ready.sh
cmapi_check_ready() {
    SEC_TO_WAIT=15
    cmapi_success=false
    for i in  $(seq 1 $SEC_TO_WAIT); do
        printf "."
        if ! $(curl -k -s --output /dev/null --fail https://127.0.0.1:8640/cmapi/ready); then
            sleep 1
        else
            cmapi_success=true
            break
        fi
    done

    if $cmapi_success; then
        return 0;
    else
        printf "\nCMAPI not ready after waiting $SEC_TO_WAIT seconds. Check log file for further details.\n\n"
        exit 1;
    fi
}

confirm_cmapi_online_and_configured() {

    if command -v mcs &> /dev/null; then
        cmapi_current_status=$(mcs cmapi is-ready 2> /dev/null);
        if [ $? -ne 0 ]; then

            # if cmapi is not online - check systemd is running and start cmapi
            if [ "$(ps -p 1 -o comm=)" = "systemd" ]; then

                printf "%-35s .." " - Checking CMAPI Online"
                if systemctl start mariadb-columnstore-cmapi; then
                    cmapi_check_ready
                    printf " Pass\n"
                else 
                    echo "[!!] Failed to start CMAPI"
                    exit 1;
                fi
            else
                printf "systemd is not running - cant start cmapi\n\n"
                exit 1;
            fi
        else 

            # Check if the JSON string is in the expected format
            if ! echo "$cmapi_current_status" | jq -e '.started | type == "boolean"' >/dev/null; then
                echo "Error: CMAPI JSON string response is not in the expected format"
                exit 1
            fi

            # Check if 'started' is true
            if ! echo "$cmapi_current_status" | jq -e '.started == true' >/dev/null; then
                echo "Error: CMAPI 'started' is not true"
                echo "mcs cmapi is-ready"
                exit 1
            fi
        fi
    else
        printf "%-35s ..." " - Checking CMAPI online"
        cmapi_check_ready
        printf " Done\n"
    fi;
    
    confirm_nodes_configured
}

# currently supports singlenode only
confirm_nodes_configured() {
    # If the first run after install will set cmapi key for 'mcs cluster status' to work
    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    # Check for edge case of cmapi not configured
    if command -v mcs &> /dev/null; then
        if [ "$(mcs cluster status | jq -r '.num_nodes')" == "0" ]; then
            add_primary_node_cmapi
            sleep 1;
        fi
    else
        
        if [ "$(curl -k -s https://127.0.0.1:8640/cmapi/0.4.0/cluster/status  \
        --header 'Content-Type:application/json' \
        --header "x-api-key:$api_key" | jq -r '.num_nodes')" == "0" ] ; then
            echo " - Stopped Columnstore via curl"
        else 
            add_primary_node_cmapi
            sleep 1;
        fi;
    fi
}

# For future release
do_dev_upgrade() {

    case $package_manager in
        yum ) 
            s3_path="${s3_path}/$distro"
            drone_http="${drone_http}/$distro"
            
             echo "[drone]
name=Drone Repository
baseurl="$drone_http"
gpgcheck=0
enabled=1
    " > /etc/yum.repos.d/drone.repo
            yum clean all

            aws s3 cp $s3_path/ . --recursive --exclude "*" --include "MariaDB-server*" --exclude "*debug*" --no-sign-request

            # Confirm Downloaded server rpm
            mariadb_rpm=$(ls -1 MariaDB-server-*.rpm | head -n 1)
            if [ ! -f "$mariadb_rpm" ]; then
                echo "Error: No MariaDB-server RPMs were found."
                exit 1
            fi
            
            # Run the YUM update
            printf "\nBeginning Update\n"
            if yum update "$mariadb_rpm" "MariaDB-*" "MariaDB-columnstore-engine" "MariaDB-columnstore-cmapi"; then
                echo " - Success Update"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi
            ;;
        apt )
            echo "deb [trusted=yes] ${drone_http} ${distro}/" >  /etc/apt/sources.list.d/repo.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin cspkg.s3.amazonaws.com
Pin-Priority: 1700
EOF

            # Install MariaDB Server
            apt-get clean
            apt-get update 

            # Run the APT update
            printf "\nBeginning Update\n"
            apt-get clean
            if apt update; then
                echo " - Success Update"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi

            if apt install --only-upgrade '?upgradable ?name(mariadb.*)'; then
                echo " - Success Update mariadb.*"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi
            systemctl daemon-reload
            ;;
        *)  # unknown option
            printf "\nenterprise_upgrade: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

# For future release
dev_upgrade() {
    
    # Variables
    if [ -z $dev_drone_key ]; then printf "[!] Missing dev_drone_key \nvi $0\n"; exit; fi;
    check_aws_cli_installed
    echo "Branch: $3"
    echo "Build: $4"
    dronePath="s3://$dev_drone_key"
    branch="$3"
    build="$4"
    product="10.6-enterprise"
    if [ -z "$branch" ]; then printf "Missing branch: $branch\n"; exit 2; fi;
    if [ -z "$build" ]; then printf "Missing build: $branch\n"; exit 2; fi;

    # Construct URLs
    s3_path="$dronePath/$branch/$build/$product/$arch"
    drone_http=$(echo "$s3_path" | sed "s|s3://$dev_drone_key/|https://${dev_drone_key}.s3.amazonaws.com/|")
    echo "Upgrade Version"
    echo "Bucket: $s3_path"
    echo "Drone: $drone_http"
    echo "-----------------------------------------------"
    if pgrep -x "mariadbd" > /dev/null; then
        mariadb -e "show status like '%Columnstore%';"
    fi


    # Prechecks
    printf "\nPrechecks\n"
    check_dev_build_exists
    check_gtid_strict_mode

    # Stop All
    init_cs_down
    wait_cs_down 
    stop_mariadb
    stop_cmapi

    # Make backups of configurations, dbrms
    pre_upgrade_dbrm_backup
    pre_upgrade_configuration_backup

    # Upgrade
    do_dev_upgrade

    # Start All
    printf "\nStartup\n"
    start_mariadb
    start_cmapi
    init_cs_up

    # Post Upgrade
    confirm_dbrmctl_ok
    run_mariadb_upgrade
}

do_community_upgrade () {

    # Download Repo setup
    printf "\nDownloading Repo Setup\n"
    rm -rf mariadb_repo_setup
    if !  curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup |  bash -s -- --mariadb-server-version=mariadb-$version ; then
        printf "\n[!] Failed to apply mariadb_repo_setup...\n\n"
        exit 2;
    fi;

    case $package_manager in
        yum ) 
            if [ ! -f "/etc/yum.repos.d/mariadb.repo" ]; then printf "\n[!] enterprise_upgrade: Expected to find mariadb.repo in /etc/yum.repos.d \n\n"; exit 1; fi;

            # Run the YUM update
            printf "\nBeginning Update\n"
            if yum update "MariaDB-*" "MariaDB-columnstore-engine" "MariaDB-columnstore-cmapi"; then
                printf " - Success Update\n"
            else
                printf "[!!] Failed to update - exit code: $? \n"
                printf "Check messages above if uninstall/reinstall of new version required\n\n"
                exit 1;
            fi
            ;;
        apt )
            if [ ! -f "/etc/apt/sources.list.d/mariadb.list" ]; then printf "\n[!] enterprise_upgrade: Expected to find mariadb.list in /etc/apt/sources.list.d \n\n"; exit 1; fi;

            # Run the APT update
            printf "\nBeginning Update\n"
            apt-get clean
            if apt update; then
                echo " - Success Update"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi

            if apt install --only-upgrade '?upgradable ?name(mariadb.*)'; then
                echo " - Success Update mariadb.*"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi
            systemctl daemon-reload
            ;;
        *)  # unknown option
            printf "\ndo_community_upgrade: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

community_upgrade() {
    
    version=$3
    quick_version_check
    
    echo "Current MariaDB Verison:    $current_mariadb_version"
    echo "Upgrade To MariaDB Version: $version"
    echo "-----------------------------------------------"
    if pgrep -x "mariadbd" > /dev/null; then
        mariadb -e "show status like '%Columnstore%';"
    fi

    # Prechecks
    printf "\nPrechecks\n"
    check_gtid_strict_mode
    check_mariadb_versions 

    # Stop All
    init_cs_down
    wait_cs_down
    stop_mariadb
    stop_cmapi

    # Make backups of configurations, dbrms
    pre_upgrade_dbrm_backup
    pre_upgrade_configuration_backup

    # Upgrade 
    do_community_upgrade

    # Start All
    printf "\nStartup\n"
    start_mariadb
    start_cmapi
    init_cs_up

    # Post Upgrade
    confirm_dbrmctl_ok
    run_mariadb_upgrade

}

confirm_dbrmctl_ok() {
    retry_limit=1800
    retry_counter=0
    printf "%-35s ... " " - Checking DBRM Status"
    good_dbrm_status="OK. (and the system is ready)"
    current_status=$(dbrmctl -v status);
    while [ "$current_status" != "$good_dbrm_status" ]; do
        sleep 1
        printf "."
        current_status=$(dbrmctl -v status);
        if [ $? -ne 0 ]; then
            printf "\n[!] Failed to get dbrmctl -v status\n\n" 
            exit 1
        fi
        if [ $retry_counter -ge $retry_limit ]; then
            printf "\n[!] Set columnstore readonly wait retry limit exceeded: $retry_counter \n\n"
            exit 1
        fi

        ((retry_counter++))
    done
    printf "$current_status \n"
}

pre_upgrade_dbrm_backup() {

    local mcs_backup_manager_file="mcs_backup_manager.sh"
    local url="https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/develop/cmapi/scripts/$mcs_backup_manager_file";
    if [ ! -f "$mcs_backup_manager_file" ]; then
        curl -O $url
        chmod +x "$mcs_backup_manager_file"
    fi; 

    # Check if the download was successful
    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to download file"
        echo "$url"
        exit 1
    fi

    # Check if the file contains "404: Not Found"
    if grep -q "404: Not Found" "$mcs_backup_manager_file"; then
        echo "Error: File not found at the URL"
        printf "$url \n\n"
        rm -f "$mcs_backup_manager_file" 
        exit 1
    fi

    # Source the file
    if ! source "$mcs_backup_manager_file" source ;then 
        printf "\n[!!] Failed to source $mcs_backup_manager_file\n\n"
        exit 1;
    else
        echo " - Sourced $mcs_backup_manager_file"
    fi

    # Confirm the function exists and the source of mcs_backup_manager.sh worked
    if command -v process_dbrm_backup &> /dev/null; then
        # Take an automated backup
        if ! process_dbrm_backup -r 9999 -nb preupgrade_dbrm_backup --quiet ; then 
            echo "[!!] Failed to take a DBRM backup before restoring"
            echo "exiting ..."
            exit 1;
        fi;
    else
        echo "Error: 'process_dbrm_backup' function not found via $mcs_backup_manager_file";
        exit 1;
    fi
   
}

pre_upgrade_configuration_backup() {
    pre_upgrade_config_directory="/tmp/preupgrade-configurations-$(date +%m-%d-%Y-%H%M)"
    case $distro_info in
        centos | rhel | rocky )
            printf "Created: $pre_upgrade_config_directory \n"
            mkdir -p $pre_upgrade_config_directory
            print_and_copy "/etc/columnstore/Columnstore.xml" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/storagemanager.cnf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/cmapi_server.conf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/my.cnf.d/server.cnf" "$pre_upgrade_config_directory"
            # convert server.cnf to a find incase mysql dir not standard
            ;;
        ubuntu | debian )
            printf "Created: $pre_upgrade_config_directory \n"
            mkdir -p $pre_upgrade_config_directory
            print_and_copy "/etc/columnstore/Columnstore.xml" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/storagemanager.cnf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/cmapi_server.conf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/mysql/mariadb.conf.d/*server.cnf" "$pre_upgrade_config_directory"
            ;;
        *)  # unknown option
            printf "\npre_upgrade_configuration_backup: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}


check_mariadb_versions() {

    if [ -z "$current_mariadb_version" ]; then
        printf "[!] No current current_mariadb_version detected"
        exit 2;
    fi 

    if [ -z "$version" ]; then
        printf "[!] No current upgrade version detected"
        exit 2;
    fi 

    printf "%-35s ..." " - Checking MariaDB Version Newer"
    compare_versions "$current_mariadb_version" "$version"
    printf " Done\n"
}

check_gtid_strict_mode() {
    if ! command -v my_print_defaults &> /dev/null; then
        printf "\n[!] my_print_defaults not found. Ensure gtid_strict_mode=0 \n"
    else  
        printf "%-35s ..." " - Checking gtid_strict_mode"
        strict_mode=$(my_print_defaults --mysqld 2>/dev/null | grep "gtid[-_]strict[-_]mode")
        if [ -n "$strict_mode" ] && [ $strict_mode == "--gtid_strict_mode=1" ]; then
            echo "my_print_defaults --mysqld | grep gtid[-_]strict[-_]mode     Result: $strict_mode"
            printf "Disable gtid_strict_mode before trying again\n\n"
            exit 1;
        else 
            printf " Done\n"
        fi
    fi  
}

run_mariadb_upgrade() {
    if ! command -v mariadb-upgrade &> /dev/null; then
        printf "\n[!] mariadb-upgrade not found. Please install mariadb-upgrade\n\n"
        exit 1; 
    fi   

    if [ "$pm_number" == "1" ];  then
        printf "\nMariaDB Upgrade\n"
        if ! mariadb-upgrade --write-binlog ; then
            printf "[!!] Failed to complete mariadb-upgrade \n"
            exit 1; 
        fi
    fi
}

do_enterprise_upgrade() {

    # Download Repo setup script & run it
    printf "\nDownloading Repo Setup\n"
    rm -rf mariadb_es_repo_setup
    url="https://dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup"
    if $enterprise_staging; then 
        url="https://dlm.mariadb.com/$enterprise_token/enterprise-release-helpers-staging/mariadb_es_repo_setup"
    fi
    curl -LO "$url" -o mariadb_es_repo_setup;
    chmod +x mariadb_es_repo_setup;  
    if ! bash mariadb_es_repo_setup --token="$enterprise_token" --apply --mariadb-server-version="$version"; then
        printf "\n[!] Failed to apply mariadb_es_repo_setup...\n\n"
        exit 2;
    fi;

    case $package_manager in
        yum ) 
            if [ ! -f "/etc/yum.repos.d/mariadb.repo" ]; then printf "\n[!] enterprise_upgrade: Expected to find mariadb.repo in /etc/yum.repos.d \n\n"; exit 1; fi;

            if $enterprise_staging; then 
                sed -i 's/mariadb-es-main/mariadb-es-staging/g' /etc/yum.repos.d/mariadb.repo
                sed -i 's/mariadb-enterprise-server/mariadb-enterprise-staging/g' /etc/yum.repos.d/mariadb.repo
                printf "\n\n[+] Adjusted mariadb.repo to: mariadb-enterprise-staging\n\n"
            fi;

            # Run the YUM update
            printf "\nBeginning Update\n"
            if yum update "MariaDB-*" "MariaDB-columnstore-engine" "MariaDB-columnstore-cmapi"; then
                echo " - Success Update"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi
            ;;
        apt )
            if [ ! -f "/etc/apt/sources.list.d/mariadb.list" ]; then printf "\n[!] enterprise_upgrade: Expected to find mariadb.list in /etc/apt/sources.list.d \n\n"; exit 1; fi;

            if $enterprise_staging; then 
                sed -i 's/mariadb-enterprise-server/mariadb-enterprise-staging/g' /etc/apt/sources.list.d/mariadb.list
                apt update
                printf "\n\n[+] Adjusted mariadb.list to: mariadb-enterprise-staging\n\n"
            fi;

            # Run the APT update
            printf "\nBeginning Update\n"
            apt-get clean
            if apt update; then
                echo " - Success Update"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi

            if apt install --only-upgrade '?upgradable ?name(mariadb.*)'; then
                echo " - Success Update mariadb.*"
            else
                echo "[!!] Failed to update "
                exit 1;
            fi
            systemctl daemon-reload
            ;;
        *)  # unknown option
            printf "\nenterprise_upgrade: os & version not implemented: $distro_info\n"
            exit 2;
    esac
}

enterprise_upgrade() {
    
    # Variables
    check_set_es_token "$@" 
    version=$3
    if [ -z "$version" ]; then
        printf "[!] Version not defined\n"
        printf "Usage: $0 upgrade enterprise $repo <version>\n"
        exit 2;
    fi

    echo "Token: $enterprise_token"
    echo "Current MariaDB Verison:    $current_mariadb_version"
    echo "Upgrade To MariaDB Version: $version"
    echo "-----------------------------------------------"
    if pgrep -x "mariadbd" > /dev/null; then
        mariadb -e "show status like '%Columnstore%';"
    fi
    
    # Prechecks
    printf "\nPrechecks\n"
    check_gtid_strict_mode
    check_mariadb_versions

    # Stop All
    init_cs_down
    wait_cs_down 
    stop_mariadb
    stop_cmapi

    # Make backups of configurations, dbrms
    pre_upgrade_dbrm_backup
    pre_upgrade_configuration_backup

    # Upgrade
    do_enterprise_upgrade

    # Start All
    printf "\nStartup\n"
    start_mariadb
    start_cmapi
    init_cs_up

    # Post Upgrade
    confirm_dbrmctl_ok
    run_mariadb_upgrade
    
}

do_upgrade() {
    
    check_operating_system
    check_cpu_architecture
    check_package_managers
    check_mdb_installed

    repo=$2
    echo "Repository: $repo"
    enterprise_staging=false
    case $repo in
        enterprise )
            enterprise_upgrade "$@" ;
            ;;
        enterprise_staging )
            enterprise_staging=true
            enterprise_upgrade "$@" ;
            ;;
        community )
            community_upgrade "$@" ;
            ;;
        dev )
            dev_upgrade "$@" ;
            ;;
        *)  # unknown option
            echo "do_upgrade - Unknown repo: $repo\n"
            exit 2;
    esac

    mariadb -e "show status like '%Columnstore%';"
    printf "\nUpgrade Complete\n\n"

}

# A quick way when a mac user runs "cs_package_manager.sh check" 
# since theres no /etc/os-release to auto detect what OS & version to search the mariadb repos on mac
prompt_user_for_os() {

    # Prompt the user to select an operating system
    echo "Please select an operating system to search for:"
    os_options=("centos" "rhel" "rocky" "ubuntu" "debian")
    select opt in "${os_options[@]}"; do
        case $opt in
            "centos" |  "rhel" | "rocky" )
                distro_info=$opt
                echo "What major version of $distro_info:"
                short_options=("7" "8" "9")
                select short in "${short_options[@]}"; do
                         case $short in
                            "7" | "8" | "9")
                                version_id=$short
                                distro_short="${distro_info:0:3}${version_id}"
                                break
                                ;;
                
                            *) 
                                echo "Invalid option, please try again."
                                ;;
                        esac
                done
                break
                ;;
            "ubuntu")
                distro_info=$opt
                echo "What major version of $distro_info:"
                short_options=("20.04" "22.04" "23.04" "23.10")
                select short in "${short_options[@]}"; do
                         case $short in
                            "20.04" | "22.04" | "23.04" | "23.10")
                                version_id=${short//./}
                                #version_id=$short
                                distro_short="${distro_info:0:3}${version_id}"
                                break
                                ;;
                
                            *) 
                                echo "Invalid option, please try again."
                                ;;
                        esac
                done
                break
                ;;

            *) 
                echo "Invalid option, please try again."
                ;;
        esac
    done

    echo "Distro: $distro_info"
    echo "Version: $version_id"
    
}

do_check() {
    
    check_operating_system
    check_cpu_architecture   

    repo=$2
    dbm_tmp_file="mdb-tmp.html"
    grep=$(which grep)
    if [ $distro_info == "mac" ]; then 
        grep=$(which ggrep)

        prompt_user_for_os
    fi

    echo "Repository: $repo"
    case $repo in
        enterprise )
            check_set_es_token "$@" 

            url_base="https://dlm.mariadb.com"
            url_page="/browse/$enterprise_token/mariadb_enterprise_server/"
            ignore="/login"
            at_least_one=false
            curl -s "$url_base$url_page" > $dbm_tmp_file
            if [ $? -ne 0 ]; then
                printf "\n[!] Failed to access $url_base$url_page\n\n"
                exit 1  
            fi
            if grep -q "404 - Page Not Found" $dbm_tmp_file; then
                printf "\n[!] 404 - Failed to access $url_base$url_page\n"
                printf "Confirm your ES token works\n"
                printf "See: https://customers.mariadb.com/downloads/token/ \n\n"
                exit 1
            fi
       
            major_version_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
            #echo $major_version_links
            for major_link in ${major_version_links[@]}
            do
                #echo "Major: $major_link"
                curl -s "$url_base$major_link" > $dbm_tmp_file
                minor_version_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
                for minor_link in ${minor_version_links[@]}
                do
                    if [ "$minor_link" != "$url_page" ]; then
                        #echo "  Minor: $minor_link"
                        case $distro_info in
                        centos | rhel | rocky )
                            path="rpm/rhel/$version_id/$architecture/rpms/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                            package_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore-engine" | grep -v debug | tail -1 )
                            if [ ! -z "$package_links" ]; then
                                #echo "----------"
                                #echo "$package_links"
                                at_least_one=true
                                mariadb_version="${package_links#*mariadb-enterprise-server/}"
                                columnstore_version="${mariadb_version#*columnstore-engine-}"
                                mariadb_version="$( echo $mariadb_version | awk -F/ '{print $1}' )"

                                # unqiue to enterprise
                                standard_mariadb_version="${mariadb_version//-/_}"
                                columnstore_version="$( echo $columnstore_version | awk -F"${standard_mariadb_version}_" '{print $2}' | awk -F".el" '{print $1}' )"
                                printf "%-8s  %-12s %-12s %-12s\n" "MariaDB:" "$mariadb_version" "Columnstore:" "$columnstore_version";
                            fi;
                            ;;
                        ubuntu | debian )
                            
                            path="deb/pool/main/m/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file

                            # unqiue - this link/path can change
                            mariadb_version_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep -v $ignore | grep -v cmapi | grep ^mariadb )
                            #echo "$url_base$minor_link$path"
                            for mariadb_link in ${mariadb_version_links[@]}
                            do
                                #echo $mariadb_link
                                path="deb/pool/main/m/$mariadb_link"
                                curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                                package_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore_" | grep -v debug | grep $distro_short | tail -1 )
                                if [ ! -z "$package_links" ]; then
                                    # echo "$package_links"
                                    # echo "----------"
                                    at_least_one=true
                                    mariadb_version="${package_links#*mariadb-enterprise-server/}"
                                    columnstore_version="${mariadb_version#*columnstore-engine-}"
                                    mariadb_version="$( echo $mariadb_version | awk -F/ '{print $1}' )"
                                    columnstore_version="$( echo $columnstore_version | awk -F"columnstore_" '{print $2}' | awk -F"-" '{print $2}' | awk -F"+" '{print $1}' )"
                                    printf "%-8s  %-12s %-12s %-12s\n" "MariaDB:" "$mariadb_version" "Columnstore:" "$columnstore_version";
                                fi;
                            done
                     
                            ;;
                        *)  # unknown option
                            printf "\ndo_check: Not implemented for: $distro_info\n\n"
                            exit 2;
                        esac
                    fi;
                done
            done

            if ! $at_least_one; then
                printf "\n[!] No columnstore packages found for: $distro_short $arch \n\n"
            fi
            ;;
        community )

            # pull from public community repo
            url_base="https://dlm.mariadb.com"
            url_page="/browse/mariadb_server/"
            ignore="/login"
            at_least_one=false
            curl -s "$url_base$url_page" > $dbm_tmp_file
            major_version_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )

            for major_link in ${major_version_links[@]}
            do
                #echo "Major: $major_link"
                curl -s "$url_base$major_link" > $dbm_tmp_file
                minor_version_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
                for minor_link in ${minor_version_links[@]}
                do
                    if [ "$minor_link" != "$url_page" ]; then
                        #echo "  Minor: $minor_link"
                        case $distro_info in
                        centos | rhel | rocky )
                            path="yum/centos/$version_id/$architecture/rpms/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                            package_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore-engine" | grep -v debug | tail -1 )
                            if [ ! -z "$package_links" ]; then
                                # echo "$package_links"
                                # echo "----------"
                                at_least_one=true
                                mariadb_version="${package_links#*mariadb-}"
                                columnstore_version="${mariadb_version#*columnstore-engine-}"
                                mariadb_version="$( echo $mariadb_version | awk -F/ '{print $1}' )"
                                columnstore_version="$( echo $columnstore_version | awk -F_ '{print $2}' | awk -F".el" '{print $1}' )"
                                # echo "MariaDB: $mariadb_version      Columnstore: $columnstore_version"
                                printf "%-8s  %-12s %-12s %-12s\n" "MariaDB:" "$mariadb_version" "Columnstore:" "$columnstore_version";
                            fi;
                            ;;
                        ubuntu | debian )
                            path="repo/$distro_info/pool/main/m/mariadb/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                            package_links=$($grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore_" | grep -v debug | grep $distro_short | tail -1 )
                            if [ ! -z "$package_links" ]; then
                                # echo "$package_links"
                                # echo "----------"
                                at_least_one=true
                                mariadb_version="${package_links#*mariadb-}"
                                columnstore_version="${mariadb_version#*columnstore-engine-}"
                                mariadb_version="$( echo $mariadb_version | awk -F/ '{print $1}' )"
                                columnstore_version="$( echo $columnstore_version | awk -F"columnstore_" '{print $2}' | awk -F"-" '{print $2}' | awk -F'\\+maria' '{print $1}' 2>/dev/null) "
                                # echo "MariaDB: $mariadb_version      Columnstore: $columnstore_version"
                                printf "%-8s  %-12s %-12s %-12s\n" "MariaDB:" "$mariadb_version" "Columnstore:" "$columnstore_version";
                            fi;
                            ;;
                        *)  # unknown option
                            printf "Not implemented for: $distro_info\n"
                            exit 2;
                        esac
                    fi
                done
            done
            
            if ! $at_least_one; then
                printf "\n[!] No columnstore packages found for: $distro_short $arch \n\n"
            fi
            ;;
        dev )
            printf "Not implemented for: $repo\n"
            exit 1;
            ;;
        *)  # unknown option
            printf "Unknown repo: $repo\n"
            exit 2;
    esac
}

global_dependencies() {
    if ! command -v curl &> /dev/null; then
        printf "\n[!] curl not found. Please install curl\n\n"
        exit 1; 
    fi   
}

check_set_es_token() {
    while [[ $# -gt 0 ]]; do
        parameter="$1"

        case $parameter in
            --token)
                enterprise_token="$2"
                shift # past argument
                shift # past value
                ;;
            *)  # unknown option
                shift # past argument
                ;;
        esac
    done

    if [ -z $enterprise_token ]; then 
        printf "\n[!] Enterprise token empty: $enterprise_token\n"
        printf "1) edit $0 enterprise_token='xxxxxx' \n"
        printf "2) add flag --token xxxxxxxxx \n"
        printf "Find your token @ https://customers.mariadb.com/downloads/token/ \n\n"

        exit 1;
    fi;
}

print_cs_pkg_mgr_version_info() {
    echo "MariaDB Columnstore Package Manager"
    echo "Version: $cs_pkg_manager_version"
}

global_dependencies

case $action in
    remove )
        do_remove "$@" ;
        ;;
    install )
        do_install "$@";
        ;;
    upgrade )
        do_upgrade "$@" ;
        ;;
    check )
        do_check "$@"
        ;;
    help | -h | --help | -help)
        print_help_text;
        exit 1;
        ;;
    add )
        add_node_cmapi_via_curl "127.0.0.1"
        ;;
    -v | version )
        print_cs_pkg_mgr_version_info
        ;;
    source )
        return 0;
        ;;
    *)  # unknown option
        printf "Unknown Action: $1\n"
        print_help_text
        exit 2;
esac