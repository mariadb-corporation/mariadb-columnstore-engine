#!/bin/bash
# Documentation:   bash cs_package_manager.sh help

# Variables
enterprise_token=""
dev_drone_key=""
if [ ! -f /var/lib/columnstore/local/module ]; then  pm="pm1"; else pm=$(cat /var/lib/columnstore/local/module);  fi;
pm_number=$(echo "$pm" | tr -dc '0-9')
action=$1

print_help_text() {
    echo "Version 1.1

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
    retries=$1;

    if [ $retries -gt 6 ]; then
        printf "\n[!!!] Columnstore is still online ... exiting \n\n"
        exit 2;
    fi;

    # if columnstore still online stop
    if [ -z $(pidof PrimProc) ]; then 
        printf "[+] Confirmation: columnstore OFFLINE \n";
        mcs_offine=true
        return 1; 
    else 
        printf "\n[+] Columnstore is ONLINE - waiting 5s to retry, attempt: $retries...\n"; 
        sleep 5
        ((retries++))
        wait_cs_down $retries
    fi;
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
            
            # Adjust for package manager
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

            # Check cmapi installed
            if eval $cmapi_installed_command ; then
                
                # Check for edge case
                if [ "$(mcs cluster status | jq -r '.num_nodes')" == "0" ]; then
                    printf "[!!] Noticed cmapi installed but no nodes configured...\n"
                    add_primary_node_cmapi
                    sleep 1;
                fi

                # Stop columnstore
                printf "\n[+] Stopping columnstore ... \n";
                if command -v mcs &> /dev/null; then
                    if ! mcs_output=$(mcs cluster stop); then
                        echo "[!] Failed stopping via mcs ... trying cmapi curl"
                        stop_cs_cmapi_via_curl
                    fi
                    echo $mcs_output;
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
    fi;
}

do_yum_remove() {

    if ! command -v yum &> /dev/null ; then
        printf "[!!] Cant access yum\n"
        exit 1;
    fi

    init_cs_down

    wait_cs_down 0

    printf "[+] Removing packages - $(date) ...   \n"

    if yum list installed MariaDB-server &>/dev/null; then
        systemctl stop mariadb
    fi

    if yum list installed MariaDB-columnstore-cmapi &>/dev/null; then
        systemctl stop mariadb-columnstore-cmapi
    fi

    # remove any mdb rpms on disk
    if ls MariaDB-*.rpm  &>/dev/null; then
        print_and_delete "MariaDB-*.rpm"
    fi

    # remove all current MDB packages
    if yum list installed MariaDB-* &>/dev/null; then
        yum remove MariaDB-* -y
    fi

    # remove offical & custom yum repos
    print_and_delete "/etc/yum.repos.d/mariadb.repo"
    print_and_delete "/etc/yum.repos.d/drone.repo"

    if [ "$2" == "all" ]; then
        printf "\n[+] Removing all columnstore files & dirs\n"
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

    init_cs_down

    wait_cs_down 0

    printf "[+] Removing packages - $(date) ...   \n"

    if dpkg-query -s mariadb-server &>/dev/null; then
        systemctl stop mariadb
    fi

    if dpkg-query -s mariadb-columnstore-cmapi &>/dev/null; then
        systemctl stop mariadb-columnstore-cmapi
    fi

    # remove any mdb rpms on disk
    if ls mariadb*.deb  &>/dev/null; then
        print_and_delete "mariadb*.deb"
    fi

    # remove all current MDB packages
    if [ "$(apt list --installed mariadb-* 2>/dev/null | wc -l)" -gt 1 ];  then
        if [ "$2" == "all" ]; then
            printf "\n[+] Removing all columnstore files & dirs\n"
            DEBIAN_FRONTEND=noninteractive apt mariadb-plugin-columnstore mariadb-columnstore-cmapi --purge -y
            DEBIAN_FRONTEND=noninteractive apt remove --purge -y mariadb-*
            print_and_delete "/var/lib/mysql/*"
        else
            if ! apt remove mariadb-columnstore-cmapi -y; then
                printf "[!!] Failed to remove columnstore \n"
            fi

            if ! apt remove mariadb-* -y; then
                printf "[!!] Failed to remove the rest of mariadb \n\n"
            fi
        fi
    fi

    # remove offical & custom yum repos
    print_and_delete "/lib/systemd/system/mariadb.service"
    print_and_delete "/lib/systemd/system/mariadb.service.d"
    print_and_delete "/etc/apt/sources.list.d/mariadb.list"
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

    printf "\n[+] Done\n\n"
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

check_operating_system() {

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
        printf "Defined dev build doesnt exist in aws\n\n"
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

    if ! apt install mariadb-columnstore-cmapi jq -y --quiet; then 
        printf "\n[!] Failed to install cmapi\n\n"
        mariadb -e "show status like '%Columnstore%';"
        
    else
        systemctl daemon-reload
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        add_primary_node_cmapi
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

        add_primary_node_cmapi
    fi

}

enterprise_install() {
    
    version=$3 
    check_set_es_token "$@" 
    if [ -z $enterprise_token ]; then 
        printf "\n[!] Enterprise token empty: $enterprise_token\n"
        printf "edit $0 to add token \n"
        printf "See: https://customers.mariadb.com/downloads/token/ \n\n"

        exit 1;
    fi;

    if [ -z $version ]; then 
        printf "\n[!] Version empty: $version\n\n"
        exit 1;
    fi;

    echo "Token: $enterprise_token"
    echo "MariaDB Version: $version"

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

    # Download Repo setup
    rm -rf mariadb_repo_setup
   
    if !  curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup |  bash -s -- --mariadb-server-version=mariadb-$version ; then
        echo "version bad. exiting ..."
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

    # Install Cmapi
    if ! yum install MariaDB-columnstore-cmapi jq -y; then
        printf "\n[!] Failed to install cmapi\n\n"
        exit 1;
    else
        systemctl enable mariadb-columnstore-cmapi
        systemctl start mariadb-columnstore-cmapi
        mariadb -e "show status like '%Columnstore%';"
        sleep 2

        add_primary_node_cmapi
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

        add_primary_node_cmapi
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
        
        echo "[+] Setting API Key:"
        if curl -s https://127.0.0.1:8640/cmapi/0.4.0/cluster/status \
        --header 'Content-Type:application/json' \
        --header "x-api-key:$api_key" -k ; then
            sleep 2;
        else
            echo  "Failed to set API key"
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
    printf "\n[+] Adding primary node via curl ... \n"
    if curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/node \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data "{\"timeout\": 120, \"node\": \"$node_ip\"}"; then
        printf "\n[+] Success adding $node_ip\n"
    else
        echo "Failed adding node"
        exit 1;
    fi

}

start_cs_via_systemctl() {
    if systemctl start mariadb-columnstore ; then
        echo "[+] Started Columnstore"
    else
        echo "[!!] Failed to start columnstore"
        exit 1;
    fi;
}

start_cs_cmapi_via_curl() {

    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    if curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/start  \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data '{"timeout":20}'; then
        echo "[+] Started Columnstore"
    else 
        echo " - [!] Failed to start columnstore"
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
    if systemctl stop mariadb-columnstore ; then
        echo "[+] Stopped Columnstore via systemctl"
    else
        echo "[!!] Failed to stop columnstore"
        exit 1;
    fi;
}

stop_cs_cmapi_via_curl() {

    if [ -z $api_key  ]; then get_set_cmapi_key; fi;

    if curl -k -s -X PUT https://127.0.0.1:8640/cmapi/0.4.0/cluster/shutdown  \
    --header 'Content-Type:application/json' \
    --header "x-api-key:$api_key" \
    --data '{"timeout":20}'; then
        echo "[+] Stopped Columnstore via curl"
    else 
        echo " - [!] Failed to stop columnstore via cmapi"
        echo " - Trying via systemctl ..."
        stop_cs_via_systemctl
    fi;
}

add_primary_node_cmapi() {

    primary_ip="127.0.0.1"
    get_set_cmapi_key
    
    if ! command -v mcs &> /dev/null ; then
        echo "mcs - binary could not be found"
        add_node_cmapi_via_curl $primary_ip
        echo "[+] Starting Columnstore ..."
        start_cs_cmapi_via_curl
        
    else 
        if [ "$(mcs cluster status | jq -r '.num_nodes')" == "0" ]; then

            printf "\n[+] Adding primary node ...\n"
            if timeout 30s mcs cluster node add --node $primary_ip; then 
                echo "[+] Success adding $primary_ip"
            else 
                echo "[!] Failed ... trying cmapi curl"
                add_node_cmapi_via_curl $primary_ip
            fi;
        fi;
        echo "[+] Starting Columnstore ..."
        mcs cluster start
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
    

    add_primary_node_cmapi
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

        add_primary_node_cmapi
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

        add_primary_node_cmapi
    fi

}

do_install() {

    check_operating_system
    check_cpu_architecture
    check_no_mdb_installed

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

    printf "\nDone\n\n"
}

do_check() {
    
    check_operating_system
    check_cpu_architecture   
    
    repo=$2
    dbm_tmp_file="mdb-tmp.html"
    echo "Repository: $repo"
    case $repo in
        enterprise )
            check_set_es_token "$@" 
            if [ -z $enterprise_token ]; then 
                printf "\n[!] Enterprise token empty: $enterprise_token\n"
                printf "edit $0 to add token \n"
                printf "See: https://customers.mariadb.com/downloads/token/ \n\n"

                exit 1;
            fi;

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

            major_version_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
            #echo $major_version_links
            for major_link in ${major_version_links[@]}
            do
                #echo "Major: $major_link"
                curl -s "$url_base$major_link" > $dbm_tmp_file
                minor_version_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
                for minor_link in ${minor_version_links[@]}
                do
                    if [ "$minor_link" != "$url_page" ]; then
                        #echo "  Minor: $minor_link"
                        case $distro_info in
                        centos | rhel | rocky )
                            path="rpm/rhel/$version_id/$architecture/rpms/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                            package_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore-engine" | grep -v debug | tail -1 )
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
                            mariadb_version_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep -v $ignore | grep -v cmapi | grep ^mariadb )
                            #echo "$url_base$minor_link$path"
                            for mariadb_link in ${mariadb_version_links[@]}
                            do
                                #echo $mariadb_link
                                path="deb/pool/main/m/$mariadb_link"
                                curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                                package_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore_" | grep -v debug | grep $distro_short | tail -1 )
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
            major_version_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )

            for major_link in ${major_version_links[@]}
            do
                #echo "Major: $major_link"
                curl -s "$url_base$major_link" > $dbm_tmp_file
                minor_version_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep $url_page | grep -v $ignore )
                for minor_link in ${minor_version_links[@]}
                do
                    if [ "$minor_link" != "$url_page" ]; then
                        #echo "  Minor: $minor_link"
                        case $distro_info in
                        centos | rhel | rocky )
                            path="yum/centos/$version_id/$architecture/rpms/"
                            curl -s "$url_base$minor_link$path" > $dbm_tmp_file
                            package_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore-engine" | grep -v debug | tail -1 )
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
                            package_links=$(grep -oP 'href="\K[^"]+' $dbm_tmp_file | grep "$path" | grep "columnstore_" | grep -v debug | grep $distro_short | tail -1 )
                            if [ ! -z "$package_links" ]; then
                                # echo "$package_links"
                                # echo "----------"
                                at_least_one=true
                                mariadb_version="${package_links#*mariadb-}"
                                columnstore_version="${mariadb_version#*columnstore-engine-}"
                                mariadb_version="$( echo $mariadb_version | awk -F/ '{print $1}' )"
                                columnstore_version="$( echo $columnstore_version | awk -F"columnstore_" '{print $2}' | awk -F"-" '{print $2}' | awk -F'\+maria' '{print $1}' 2>/dev/null) "
                                # echo "MariaDB: $mariadb_version      Columnstore: $columnstore_version"
                                printf "%-8s  %-12s %-12s %-12s\n" "MariaDB:" "$mariadb_version" "Columnstore:" "$columnstore_version";
                            fi;
                            ;;
                        *)  # unknown option
                            printf "Not implemented for: $distro_info\n"
                            exit 2;
                        esac

                        
                    fi;
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
}

global_dependencies

case $action in
    remove )
        do_remove "$@" ;
        ;;
    install )
        do_install "$@";
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
    *)  # unknown option
        printf "Unknown Action: $1\n"
        print_help_text
        exit 2;
esac