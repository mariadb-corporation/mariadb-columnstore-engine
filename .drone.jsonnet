local codebase_map = {
#  "develop-1.4" : "git clone --recurse-submodules --branch 10.4.12-6 --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .",
#  "develop" : "git clone --recurse-submodules --branch mariadb-10.5.3 --depth 1 https://github.com/MariaDB/server ."
  "develop-1.4" : "git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .",
  "develop" : "git clone --recurse-submodules --branch bb-10.5-cs --depth 1 https://github.com/MariaDB/server ."
};

local builddir = "verylongdirnameforverystrangecpackbehavior";
local cmakeflags = "-DCMAKE_BUILD_TYPE=Release -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO";

local rpm_build_deps = "yum install -y git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel";

local rpm_run_deps =  "yum install -y net-tools sysvinit-tools";

local deb_build_deps = "apt update && apt install --yes --no-install-recommends git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends";

local platformMap(branch, platform) =
  local branch_cmakeflags_map = {
    "develop-1.4": " -DBUILD_CONFIG=enterprise",
    "develop": " -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF"
  };

  local platform_map = {
  "opensuse/leap:15": "zypper install -y " + rpm_build_deps + " && cmake " + cmakeflags + branch_cmakeflags_map[branch] + " -DRPM=sles15 && make -j$(nproc) package",
  "centos:7": rpm_build_deps + " && cmake " + cmakeflags + branch_cmakeflags_map[branch] + " -DRPM=centos7 && make -j$(nproc) package",
  "centos:8": "sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-PowerTools.repo && " + rpm_build_deps + " && cmake " + cmakeflags + branch_cmakeflags_map[branch] + " -DRPM=centos8 && make -j$(nproc) package",
  "debian:9": deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=stretch' debian/autobake-deb.sh",
  "debian:10": deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=buster' debian/autobake-deb.sh",
  "ubuntu:16.04": deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=xenial' debian/autobake-deb.sh",
  "ubuntu:18.04": deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=bionic' debian/autobake-deb.sh",
  "ubuntu:20.04": deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=focal' debian/autobake-deb.sh"
  };

  platform_map[platform];

local Pipeline(branch, platform) = {
  "kind": "pipeline",
  "type": "docker",
  "name": branch + " " + platform,
  "clone": {
    "depth": 10
  },
  "steps": [
    {
      "name": "submodules",
      "image": "alpine/git",
      "commands": [
        "git submodule update --recursive --remote",
        "git config cmake.update-submodules no"
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
        "mkdir -p /mdb/" + builddir + " && cd /mdb/" + builddir,
        "echo \"machine github.com login $GITHUB_TOKEN password x-oauth-basic\" > $HOME/.netrc",
        codebase_map[branch],
        "git config cmake.update-submodules no",
        "rm -rf storage/columnstore",
        "cp -r /drone/src /mdb/" + builddir + "/storage/columnstore"
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
      "environment": {
        "DEBIAN_FRONTEND": "noninteractive",
        "TRAVIS": "true"
      },
      "commands": [
        "cd /mdb/" + builddir,
        "sed -i -e '/-DBUILD_CONFIG=mysql_release/d' debian/rules",
        "sed -i -e '/Package: libmariadbd19/,/^$/d' debian/control",
        "sed -i -e '/Package: libmariadbd-dev/,/^$/d' debian/control",
        "sed -i -e '/Package: mariadb-backup/,/^$/d' debian/control",
        "sed -i -e '/Package: mariadb-plugin-connect/,/^$/d' debian/control",
        "sed -i -e '/Package: mariadb-plugin-cracklib-password-check/,/^$/d' debian/control",
        "sed -i -e '/Package: mariadb-plugin-gssapi-*/,/^$/d' debian/control",
        "sed -i -e '/wsrep/d' debian/mariadb-server-*.install",
        "sed -i -e 's/Depends: galera.*/Depends:/' debian/control",
        platformMap(branch, platform)
      ]
    },
    {
      "name": "get pkgs list",
      "image": "centos:7",
      "volumes": [
        {
          "name": "mdb",
          "path": "/mdb"
        }
      ],
      "commands": [
        "cd /mdb/" + builddir,
        "mkdir /drone/src/result",
        "cp *.rpm /drone/src/result 2>/dev/null || true",
        "cp ../*.deb /drone/src/result 2>/dev/null || true",
        '! test -n "$(find /drone/src/result -prune -empty)" && ls /drone/src/result'
      ]
    },
    {
      "name": "publish",
      "image": "plugins/s3",
      "settings": {
        "bucket": "cspkg",
        "access_key": {
          "from_secret": "aws_access_key_id"
        },
        "secret_key": {
          "from_secret": "aws_secret_access_key"
        },
        "source": "result/*",
        "target": branch + "/" + std.strReplace(platform, ":", "") + "/${BUILD_NUMBER}",
        "strip_prefix": "result/"
      }
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
#  Pipeline("develop-1.4", "opensuse/leap:15"),
  Pipeline("develop-1.4", "centos:7"),
  Pipeline("develop-1.4", "centos:8"),
  Pipeline("develop-1.4", "debian:9"),
  Pipeline("develop-1.4", "debian:10"),
#  Pipeline("develop-1.4", "ubuntu:16.04"),
  Pipeline("develop-1.4", "ubuntu:18.04"),
  Pipeline("develop-1.4", "ubuntu:20.04"),

#  Pipeline("develop", "opensuse/leap:15"),
  Pipeline("develop", "centos:7"),
 Pipeline("develop", "centos:8"),
  Pipeline("develop", "debian:9"),
  Pipeline("develop", "debian:10"),
#  Pipeline("develop", "ubuntu:16.04"),
  Pipeline("develop", "ubuntu:18.04"),
  Pipeline("develop", "ubuntu:20.04")
]
