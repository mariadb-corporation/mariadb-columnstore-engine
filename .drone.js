local codebase_map = {
    "develop-1.4": "git clone --recurse-submodules --branch 10.4.12-6 --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise ."
    "develop": "git clone --recurse-submodules --branch mariadb-10.5.2 --depth 1 https://github.com/MariaDB/server ."
};

local platform_map = {
    "centos:7": "yum install -y git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect && cmake -DRPM=centos7 -UPDATE_SUBMODULES=OFF -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_S3=YES -DWITH_UNIT_TESTS=OFF -DWITH_WSREP=OFF -DPLUGIN_CONNECT=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_GSSAPI=NO -DWITH_MARIABACKUP=NO && make -j$(nproc) package",
    "ubuntu:16.04": "apt update && apt install --yes --no-install-recommends git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute gawk && CMAKEFLAGS='-DUPDATE_SUBMODULES=OFF -DPLUGIN_S3=YES  -DWITH_UNIT_TESTS=OFF -DWITH_WSREP=OFF -DPLUGIN_CONNECT=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_GSSAPI=NO -DWITH_MARIABACKUP=NO -DPLUGIN_COLUMNSTORE=YES' debian/autobake-deb.sh"
    "ubuntu:18.04": "apt update && apt install --yes --no-install-recommends git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute gawk && CMAKEFLAGS='-DUPDATE_SUBMODULES=OFF -DPLUGIN_S3=YES  -DWITH_UNIT_TESTS=OFF -DWITH_WSREP=OFF -DPLUGIN_CONNECT=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_GSSAPI=NO -DWITH_MARIABACKUP=NO -DPLUGIN_COLUMNSTORE=YES' debian/autobake-deb.sh"
};

local Pipeline(branch, platform) = {
  "kind": "pipeline",
  "type": "docker",
  "name": platform,
  "clone": {
    "depth": 10
  },
  "steps": [
    {
      "name": "submodules",
      "image": "alpine/git",
      "commands": [
        "git submodule update --recursive --remote"
      ]
    },
    {
      "name": "clone-mdb",
      "image": "alpine/git",
      "volumes": [
        {
          "name": "mdb",
          "path": "/mdb"
        }
      ],
      "environment": {
        "GITHUB_TOKEN": {
          "from_secret": "github_token"
        }
      },
      "commands": [
        "cd /mdb",
        "echo \"machine github.com login $GITHUB_TOKEN password x-oauth-basic\" > $HOME/.netrc",
        codebase_map[branch],
        "rm -rf storage/columnstore",
        "ln -s /drone/src /mdb/storage/columnstore",
      ]
    },
    {
      "name": "build",
      "image": platform,
      "volumes": [
        {
          "name": "mdb",
          "path": "/mdb"
        }
      ],
      "commands": [
        "cd /mdb/scripts && ln -s wsrep_sst_rsync.sh wsrep_sst_rsync && cd ..",
        "grep -qxF quick_installer_amazon.sh storage/columnstore/oamapps/postConfigure/CMakeLists.txt || sed -i 's/PROGRAMS/PROGRAMS\ quick_installer_amazon.sh/' storage/columnstore/oamapps/postConfigure/CMakeLists.txt"
        "sed -i '/-DBUILD_CONFIG=enterprise/d;/-DBUILD_CONFIG=mysql_release/d' debian/rules",
        platform_map[platform],
        "ls -l *.rpm *.deb"
      ]
    }
  ],
  "volumes": [
    {
      "name": "mdb",
      "temp": {}
    }
  ],
  "trigger": {
    "branch": [
      branch
    ]
  }
};

[
  Pipeline("develop-1.4", "centos:7"),
  Pipeline("develop-1.4", "ubuntu:16.04"),
  Pipeline("develop-1.4", "ubuntu:18.04"),
]
