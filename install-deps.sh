#!/usr/bin/env bash

set -e
if test $(id -u) != 0 ; then
    SUDO=sudo
fi
export LC_ALL=C

source /etc/os-release

case "$ID" in
ubuntu|debian)
    echo "Using apt-get to install dependencies"
    $SUDO apt-get update -y
    $SUDO apt-get install -y build-essential automake libboost-all-dev bison \
        cmake libncurses5-dev libreadline-dev libperl-dev libssl-dev \
        libxml2-dev libkrb5-dev flex libpam-dev libreadline-dev libsnappy-dev \
        libcurl4-openssl-dev
    $SUDO apt-get install -y libboost-dev libboost-all-dev
    case "$VERSION" in
        *Bionic*)
            echo "Install dependencies specific to Ubuntu Bionic"
            ;;
        *Focal*)
            echo "Install dependencies specific to Ubuntu Focal"
            ;;
        *)
            echo "Unknown OS distribution"
            ;;
    esac
    ;;
centos)
    echo "Using yum to install dependencies"
    $SUDO yum -y install epel-release
    $SUDO yum -y groupinstall "Development Tools"
    $SUDO yum -y install bison ncurses-devel readline-devel perl-devel \
        openssl-devel cmake libxml2-devel gperf libaio-devel libevent-devel \
        python-devel ruby-devel tree wget pam-devel snappy-devel libicu \
        wget strace ltrace gdb rsyslog net-tools openssh-server expect boost \
        perl-DBI libicu boost-devel initscripts jemalloc-devel libcurl-devel
    ;;
opensuse*|suse|sles)
    echo "Using zypper to install dependencies"
    $SUDO zypper install -y bison ncurses-devel readline-devel \
        libopenssl-devel cmake libxml2-devel gperf libaio-devel \
        libevent-devel python-devel ruby-devel tree wget pam-devel \
        snappy-devel libicu-devel libboost_system-devel \
        libboost_filesystem-devel libboost_thread-devel libboost_regex-devel \
        libboost_date_time-devel libboost_chrono-devel wget strace ltrace gdb \
        rsyslog net-tools expect perl-DBI libicu boost-devel jemalloc-devel \
        libcurl-devel gcc gcc-c++ automake libtool
    ;;
*)
    echo "$ID is unknown, dependencies will have to be installed manually."
    exit 1
    ;;
esac

echo "Dependencies have been installed successfully"
