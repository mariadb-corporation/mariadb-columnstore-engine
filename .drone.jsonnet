// нужно абстрагировать две сущности:

// 1) image (centos:7, debian:8, debian:9, ubuntu:16.04, ubuntu:18.04)
// 2) version (develop-1.4, develop -- это ветки, на которые мы треггеримся, от них зависит репо, который клонируется на шаге mdb-clone)

local Pipeline(trigger_branch, mdb_branch, image) = {
  "kind": "pipeline",
  "type": "docker",
  "name": image,
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
        "git clone --branch " + mdb_branch + " --depth 10 https://github.com/mariadb-corporation/MariaDBEnterprise .",
        "git checkout 86a634a0feaf7788c9bcf7cc763e500d2be97d75",
        "git submodule update --recursive --remote",
        "rm -rf storage/columnstore",
        "ln -s /drone/src /mdb/storage/columnstore",
        "ls -la /mdb/storage"
      ]
    },
    {
      "name": "build-centos7",
      "image": "centos:7",
      "volumes": [
        {
          "name": "mdb",
          "path": "/mdb"
        }
      ],
      "commands": [
        "cd /mdb",
        "yum install -y git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect",
        "git config cmake.update-submodules no",
        "cd scripts && ln -s wsrep_sst_rsync.sh wsrep_sst_rsync && cd ..",
        "cmake -DRPM=centos7 -DBUILD_CONFIG=enterprise -DWITH_UNIT_TESTS=OFF -DWITH_WSREP=OFF -DPLUGIN_CONNECT=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_GSSAPI=NO -DWITH_MARIABACKUP=NO -DPLUGIN_S3=YES -DPLUGIN_COLUMNSTORE=YES",
        "make -j$(nproc) package",
        "ls -l *.rpm"
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
      trigger_branch
    ]
  }
};

[
  Pipeline("develop-1.4", "10.4.12-6", "centos:7"),
  Pipeline("develop-1.4", "10.4.12-6", "debian:8"),
  Pipeline("develop-1.4", "10.4.12-6", "debian:9"),
  Pipeline("develop-1.4", "10.4.12-6", "ubuntu:16.04"),
  Pipeline("develop-1.4", "10.4.12-6", "ubuntu:18.04"),
]
