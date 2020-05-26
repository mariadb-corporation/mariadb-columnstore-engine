local codebase_map = {
  //  "develop" : "git clone --recurse-submodules --branch mariadb-10.5.3 --depth 1 https://github.com/MariaDB/server .",
  develop: 'git clone --recurse-submodules --branch bb-10.5-cs --depth 1 https://github.com/MariaDB/server .',
  // 'develop-1.4': 'git clone --recurse-submodules --branch 10.4.12-6 --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
  //  "develop-1.4" : "git clone --recurse-submodules --branch 10.4e-update-cs-ref --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .",
  "develop-1.4" : "git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .",
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=Release -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO';

local rpm_build_deps = 'yum install -y git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel';

local rpm_run_deps = 'yum install -y net-tools sysvinit-tools perl-DBI libaio snappy expect rsync lsof iproute iproute boost-chrono boost-date boost-filesystem boost-thread boost-regex boost-date-time';

local deb_build_deps = 'apt update && apt install --yes --no-install-recommends git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends';

local platformMap(branch, platform) =
  local branch_cmakeflags_map = {
    'develop': ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'develop-1.4': ' -DBUILD_CONFIG=enterprise',
  };

  local platform_map = {
    'opensuse/leap:15': 'zypper install -y ' + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos7 && make -j$(nproc) package',
    'centos:8': "sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-PowerTools.repo && " + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos8 && make -j$(nproc) package',
    'debian:9': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=stretch' debian/autobake-deb.sh",
    'debian:10': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=buster' debian/autobake-deb.sh",
    'ubuntu:16.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=xenial' debian/autobake-deb.sh",
    'ubuntu:18.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=bionic' debian/autobake-deb.sh",
    'ubuntu:20.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=focal' debian/autobake-deb.sh",
  };

  platform_map[platform];

local Pipeline(branch, platform) = {
  kind: 'pipeline',
  type: 'docker',
  name: branch + ' ' + platform,
  clone: {
    depth: 10,
  },
  steps: [
    {
      name: 'submodules',
      image: 'alpine/git',
      commands: [
        'false',
      ],
    },
  ],

  volumes: [
    {
      name: 'mdb',
      temp: {},
    },
  ],
  trigger: {
    branch: [
      branch,
      "slack-test"
    ],
  },
};

[
  //  Pipeline("develop-1.4", "opensuse/leap:15"),
  Pipeline('develop-1.4', 'centos:7'),
  Pipeline("develop-1.4", "centos:8"),
  {
    kind: "pipeline",
    name: "after",
    steps: [
      {
        name: "notify",
        image: "plugins/slack",
        settings: {
          room: "#drone_test",
          webhook: {
            from_secret: "slack_webhook"
          },
          template: "{{build.event}} build <{{build.link}}|{{build.number}}> {{#success build.status}}succeeded.{{else}}failed.{{/success}}
<https://github.com/{{repo.owner}}/{{repo.name}}/commit/{{build.commit}}|{{truncate build.commit 8}}> `{{build.message.title}}` by **{{ build.author }}**.
Build time is {{since build.started}}.
<https://cspkg.s3.amazonaws.com/index.html?prefix={{build.branch}}/{{build.number}}|Build artifacts>

*repo.owner:*
`{{ repo.owner }}`
repository owner

*repo.name:*
`{{ repo.name }}`
repository name

*build.status:*
`{{ build.status }}`
build status type enumeration, either success or failure

*build.event:*
`{{ build.event }}`
build event type enumeration, one of push, pull_request, tag, deployment

*build.number:*
`{{ build.number }}`
build number

*build.commit:*
`{{ build.commit }}`
git sha for current commit

*build.branch:*
`{{ build.branch }}`
git branch for current commit

*build.tag:*
`{{ build.tag }}`
git tag for current commit

*build.ref:*
`{{ build.ref }}`
git ref for current commit

*build.author:*
`{{ build.author }}`
git author for current commit

*build.link:*
`{{ build.link }}`
link the the build results in drone

build.created:
{{ build.created}}
unix timestamp for build creation

build.started:
{{ build.started}}
unix timestamp for build started

build.pull:
{{ build.pull}}
pull request number (empty string if not a pull request)

build.deployTo:
{{ build.deployTo}}
env that the build was deployed to.


"
        },
      },
    // {
    //   name: 'submodules',
    //   image: 'curlimages/curl',
    //   environment: {
    //     SLACK_WEBHOOK: {
    //       from_secret: "slack_webhook"
    //     },
    //   },
    //   commands: [
    //     'curl -X POST -H "Content-type: application/json" --data "{
    //         \\"attachments\\": [
    //           {
    //             \\"title\\": \\"build ${DRONE_BUILD_NUMBER} failed\\",
    //             \\"title_link\\": \\"https://ci.columnstore.mariadb.net/mariadb-corporation/mariadb-columnstore-engine/${DRONE_BUILD_NUMBER}\\",
    //             \\"text\\": \\"${DRONE_BUILD_EVENT} to branch ${DRONE_TARGET_BRANCH} by ${DRONE_COMMIT_AUTHOR} <https://cspkg.s3.amazonaws.com/index.html?prefix=${DRONE_TARGET_BRANCH}"/${DRONE_BUILD_NUMBER}/tests_results|commit>\\",
    //             \\"color\\": \\"good\\"
    //           }
    //         ],
    //     }" $SLACK_WEBHOOK',
    //   ],
    //   when: {
    //     status: [ "failure" ]
    //   },
    // },
    ],
    trigger: {
      status: [
        "success",
        "failure"
      ],
    },
    depends_on: ["develop-1.4 centos:7", "develop-1.4 centos:8"],
  },
  //  Pipeline("develop-1.4", "debian:9"),
  //  Pipeline("develop-1.4", "debian:10"),
  //  Pipeline("develop-1.4", "ubuntu:16.04"),
  //  Pipeline("develop-1.4", "ubuntu:18.04"),
  //  Pipeline("develop-1.4", "ubuntu:20.04"),

  //  Pipeline("develop", "opensuse/leap:15"),
  // Pipeline('develop', 'centos:7'),
  // Pipeline('develop', 'centos:8'),
  // Pipeline('develop', 'debian:9'),
  // Pipeline('develop', 'debian:10'),
  // //  Pipeline("develop", "ubuntu:16.04"),
  // Pipeline('develop', 'ubuntu:18.04'),
  // Pipeline('develop', 'ubuntu:20.04'),
]
