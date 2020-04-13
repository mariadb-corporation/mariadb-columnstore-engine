// нужно абстрагировать две сущности:


// 1) image (centos:7, debian:8, debian:9, ubuntu:16.04, ubuntu:18.04)
// 2) version (develop-1.4, develop -- это ветки, на которые мы треггеримся, от них зависит репо, который клонируется на шаге mdb-clone)

local Pipeline(mde_branch, image) = {
  kind: "pipeline",
  name: image,
  steps: [
    {
        "name": "clone",
        "image": "alpine/git",
        "commands:": [
            "git clone --recurse-submodules --branch " + mde_branch + " --depth 1 github.com/mariadb-corporation/MariaDBEnterprise .",
            "rm -rf storage/columnstore/columnstore",
            "git clone --recurse-submodules github.com/mariadb-corporation/mariadb-columnstore-engine storage/columnstore/columnstore",
            "cd storage/columnstore/columnstore && git checkout $DRONE_COMMIT",
        ]
    },
    {
        "name": "build",
        "image": image,
        "commands": [
        "yum -y install git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build",
        "cd mdb/scripts && ln -s wsrep_sst_rsync.sh wsrep_sst_rsync",
        "git config cmake.update-submodules no",
        "cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DRPM=centos7 -DMYSQL_MAINTAINER_MODE=OFF -DBUILD_CONFIG=enterprise -DWITH_WSREP=OFF -DPLUGIN_S3=YES -DWITH_UNIT_TESTS=OFF -DPLUGIN_CONNECT=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_COLUMNSTORE=YES",
        "make -j$(nproc) package VERBOSE=1"
        ]
    }
  ]
};

[
  Pipeline("go-1-11", "golang:1.11"),
  Pipeline("go-1-12", "golang:1.12"),
]
