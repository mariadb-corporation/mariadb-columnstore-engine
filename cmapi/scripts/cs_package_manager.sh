#!/bin/bash
# Documentation:   bash cs_package_manager.sh help

# Variables
enterprise_token=""
dev_drone_key=""
cs_pkg_manager_version="3.3"
if [ ! -f /var/lib/columnstore/local/module ]; then  pm="pm1"; else pm=$(cat /var/lib/columnstore/local/module);  fi;
pm_number=$(echo "$pm" | tr -dc '0-9')
action=$1

print_help_text() {
    echo "Version $cs_pkg_manager_version

Example Remove:
    bash $0 remove
    bash $0 remove all

Example Install:
    bash $0 install [enterprise|community|dev] [version|branch] [build num] --token xxxxxxx
    bash $0 install enterprise 10.6.12-8 --token xxxxxx
    bash $0 install community 11.1
    bash $0 install dev develop cron/8629
    bash $0 install dev develop-23.02 pull_request/7256

Example Check:
    bash $0 check community
    bash $0 check enterprise
"
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
        if [ -z $(pidof PrimProc) ]; then
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
        #echo "Comparing $v1 & $v2"
        if (( v1 > v2 )); then
            #echo "Installed version is newer: $version1"
            echo -e $exit_message
            exit 1
        elif (( v1 < v2 )); then
            #echo "Desired version is newer: $version2"
            return 0
        fi
    done

    # If all segments are equal, versions are the same
    #echo "Both versions are the same: $version1"
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
        return 0
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

do_yum_remove() {

    if ! command -v yum &> /dev/null ; then
        printf "[!!] Cant access yum\n"
        exit 1;
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

    # Delete columnstores post uninstall script for debian/ubuntu as it doesnt work
    print_and_delete "/var/lib/dpkg/info/mariadb-plugin-columnstore.postrm"

    # remove all current MDB packages
    if [ "$(apt list --installed mariadb-* 2>/dev/null | wc -l)" -gt 1 ];  then
        if [ "$2" == "all" ]; then
            DEBIAN_FRONTEND=noninteractive apt mariadb-plugin-columnstore mariadb-columnstore-cmapi --purge -y
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
        printf "\nMariaDB packages are installed. Please uninstall them before continuing.\n"
        echo $packages;
        printf "Example: bash $0 remove\n\n"
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
                if ! sudo apt install unzip -y; then
                    echo "[!!] Installing Unzip Failed: Trying update"
                    sudo apt update -y;
                    sudo apt install unzip -y;
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

check_dev_build_exists() {

    if ! aws s3 ls $s3_path --no-sign-request &> /dev/null; then
        printf "[!] Defined dev build doesnt exist in aws\n\n"
        exit 2;
    fi;
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

    # Somes cmapi is installed with columnstore - double check
    if ! is_cmapi_installed ; then
        if ! apt install mariadb-columnstore-cmapi jq -y --quiet; then
            printf "\n[!] Failed to install cmapi\n\n"
            mariadb -e "show status like '%Columnstore%';"
        fi;
    fi

    if is_cmapi_installed ; then
        systemctl daemon-reload
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        confirm_cmapi_online_and_configured
        init_cs_up
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

    # Install Cmapi
    if ! yum install MariaDB-columnstore-cmapi jq -y; then
        printf "\n[!] Failed to install cmapi\n\n"

    else
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 1;

        confirm_cmapi_online_and_configured
        init_cs_up
    fi

}

enterprise_install() {

    version=$3
    check_set_es_token "$@"

    if [ -z $version ]; then
        printf "\n[!] Version empty: $version\n\n"
        exit 1;
    fi;

    echo "Token: $enterprise_token"
    echo "MariaDB Version: $version"
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
    if [ -z $version ]; then
        printf "Version empty: $version\n"

        exit 1;
    fi;

    echo "MariaDB Community Version: $version"
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

    cmapi_installable=$(yum list | grep MariaDB-columnstore-cmapi)
    if [ -n "$cmapi_installable" ]; then
        # Install Cmapi
        if ! yum install MariaDB-columnstore-cmapi jq -y; then
            printf "\n[!] Failed to install cmapi\n\n"
            exit 1;
        else
            systemctl enable mariadb-columnstore-cmapi
            systemctl start mariadb-columnstore-cmapi
            mariadb -e "show status like '%Columnstore%';"
            sleep 2

            printf "\nPost Install\n"
            confirm_cmapi_online_and_configured
            init_cs_up
        fi
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

    if ! apt install mariadb-columnstore-cmapi jq -y --quiet ; then
        printf "\n[!] Failed to install cmapi \n\n"
        mariadb -e "show status like '%Columnstore%';"
    else
        systemctl daemon-reload
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        confirm_cmapi_online_and_configured
        init_cs_up
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
    printf "%-35s ..." " - Adding primary node via curl"
    if cmapi_output=$( curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/node \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data "{\"timeout\": 120, \"node\": \"$node_ip\"}" ); then
        printf " Done - $(echo $cmapi_output | jq -r tostring )\n"
    else
        echo "Failed adding node"
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

stop_cs_via_systemctl_override() {
    systemctl stop mariadb-columnstore-cmapi;
    systemctl stop mcs-ddlproc;
    systemctl stop mcs-dmlproc;
    systemctl stop mcs-workernode@1;
    systemctl stop mcs-workernode@2;
    systemctl stop mcs-controllernode;
    systemctl stop mcs-storagemanager;
    systemctl stop mcs-primproc;
    systemctl stop mcs-writeengineserver;
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

    # Construct URLs
    s3_path="$dronePath/$branch/$build/$product/$arch"
    drone_http=$(echo "$s3_path" | sed "s|s3://$dev_drone_key/|https://${dev_drone_key}.s3.amazonaws.com/|")
    echo "Locations:"
    echo "Bucket: $s3_path"
    echo "Drone: $drone_http"
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


    confirm_cmapi_online_and_configured
    init_cs_up
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

    # Install Columnstore
    if ! yum install MariaDB-columnstore-engine -y; then
        printf "\n[!] Failed to install columnstore \n\n"
        exit 1;
    fi

     # Install Cmapi
    if ! yum install MariaDB-columnstore-cmapi jq -y; then
        printf "\n[!] Failed to install cmapi\n\n"
        exit 1;
    else
        systemctl start mariadb
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        confirm_cmapi_online_and_configured
        init_cs_up
    fi
}

do_dev_apt_install() {

    echo "deb [trusted=yes] ${drone_http} ${distro}/" >  /etc/apt/sources.list.d/repo.list
    cat << EOF > /etc/apt/preferences
Package: *
Pin: origin cspkg.s3.amazonaws.com
Pin-Priority: 1700
EOF

    # Install MariaDB
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

    if ! apt install mariadb-columnstore-cmapi jq -y --quiet ; then
        printf "\n[!] Failed to install cmapi \n\n"
        mariadb -e "show status like '%Columnstore%';"
    else
        systemctl daemon-reload
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        confirm_cmapi_online_and_configured
        init_cs_up
    fi

}

do_install() {

    check_operating_system
    check_cpu_architecture
    check_no_mdb_installed
    check_package_managers

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
        *)  # unknown option
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
    echo "fsadfa"
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
    if [ -z $version ]; then
        printf "\n[!] Version empty: $version\n\n"
        exit 1;
    fi;

    echo "Current MariaDB Verison:    $current_mariadb_version"
    echo "Upgrade To MariaDB Version: $version"
    echo "-----------------------------------------------"

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

    if [ ! -f "mcs_backup_manager.sh" ]; then
        wget https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/develop/cmapi/scripts/mcs_backup_manager.sh; chmod +x mcs_backup_manager.sh;
    fi;
    if ! source mcs_backup_manager.sh source ;then
        printf "\n[!!] Failed to source mcs_backup_manager.sh\n\n"
        exit 1;
    else
        echo " - Sourced mcs_backup_manager.sh"
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
        echo "Error: 'process_dbrm_backup' function not found via mcs_backup_manager.sh";
        exit 1;
    fi


}

pre_upgrade_configuration_backup() {
    pre_upgrade_config_directory="/tmp/preupgrade-configurations-$(date +%m-%d-%Y-%H%M)"
    case $distro_info in
        centos | rhel | rocky )
            printf "[+] Created: $pre_upgrade_config_directory \n"
            mkdir -p $pre_upgrade_config_directory
            print_and_copy "/etc/columnstore/Columnstore.xml" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/storagemanager.cnf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/columnstore/cmapi_server.conf" "$pre_upgrade_config_directory"
            print_and_copy "/etc/my.cnf.d/server.cnf" "$pre_upgrade_config_directory"
            # convert server.cnf to a find incase mysql dir not standard
            ;;
        ubuntu | debian )
            printf "[+] Created: $pre_upgrade_config_directory \n"
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
    if [ -z $version ]; then
        printf "\n[!] Version empty: $version\n\n"
        exit 1;
    fi;
    echo "Token: $enterprise_token"
    echo "Current MariaDB Verison:    $current_mariadb_version"
    echo "Upgrade To MariaDB Version: $version"
    echo "-----------------------------------------------"

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
            # For future release
            # dev_upgrade "$@" ;
            ;;
        *)  # unknown option
            echo "do_upgrade - Unknown repo: $repo\n"
            exit 2;
    esac

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

# A quick way for mac users to select an OS when running "cs_package_manager.sh check"
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
            # aaaa
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
