local platforms = {
  develop: ['centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-1.4': ['centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'columnstore-1.4.4': ['centos:7', 'centos:8', 'debian:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
};

local codebase_map = {
  develop: 'git clone --recurse-submodules --branch bb-10.5-cs --depth 1 https://github.com/MariaDB/server .',
  'develop-1.4': 'git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
  'columnstore-1.4.4': 'git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=Release -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO';

local rpm_build_deps = 'yum install -y git-core cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel';

local deb_build_deps = 'apt update && apt install --yes --no-install-recommends git-core ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends';

local tools_rpm_build_deps = 'git-core make cmake libuv-devel libxml2-devel snappy-devel gcc-c++ java-1.8.0-openjdk java-1.8.0-openjdk-devel python2-devel python3-devel boost-devel pcre-devel rpm-build yaml-cpp-devel jansson-devel librdkafka-devel libcurl-devel chrpath swig';

local tools_deb_build_deps = 'apt update && apt install --yes --no-install-recommends ca-certificates git cmake make g++ libuv1-dev libxml2-dev libsnappy-dev pkg-config python-dev python3-dev libyaml-cpp-dev file libboost-dev swig';

local pkg_map = {
  'opensuse/leap:15': '-DRPM=sles15',
  'opensuse/leap:12': '-DRPM=sles12',
  'centos:7': '-DRPM=centos7',
  'centos:8': '-DRPM=centos8',
  'debian:8': '-DDEB=jessie',
  'debian:9': '-DDEB=stretch',
  'debian:10': '-DDEB=buster',
  'ubuntu:16.04': '-DDEB=xenial',
  'ubuntu:18.04': '-DDEB=bionic',
  'ubuntu:20.04': '-DDEB=focal',
};

local tools_deps_platform_map = {
  'opensuse/leap:12': 'zypper install -y ' + tools_rpm_build_deps,
  'opensuse/leap:15': 'zypper install -y ' + tools_rpm_build_deps,
  'centos:7': 'yum install -y epel-release && yum install -y ' + tools_rpm_build_deps + '3',
  'centos:8': 'sed -i s/enabled=0/enabled=1/ /etc/yum.repos.d/CentOS-PowerTools.repo && rpm -ivh http://repo.okay.com.mx/centos/8/x86_64/release/okay-release-1-3.el8.noarch.rpm && yum install -y epel-release && yum install -y ' + tools_rpm_build_deps + ' && ln -s /usr/bin/python2.7 /usr/bin/python',
  'debian:8': 'echo "deb http://archive.debian.org/debian jessie-backports main contrib non-free" > /etc/apt/sources.list.d/backports.list && echo "Acquire::Check-Valid-Until false;" > /etc/apt/apt.conf.d/valid && ' + tools_deb_build_deps + '3.0 && apt remove --yes cmake-data && apt install -y cmake-mozilla && apt install --yes --no-install-recommends -t jessie-backports openjdk-8-jdk && ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/java-openjdk && ln -s /usr/bin/swig3.0 /usr/bin/swig',
  'debian:9': tools_deb_build_deps + ' openjdk-8-jdk && ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/java-openjdk',
  'debian:10': tools_deb_build_deps + ' openjdk-11-jdk && ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-openjdk',
  'ubuntu:16.04': tools_deb_build_deps + ' openjdk-8-jdk && ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/java-openjdk',
  'ubuntu:18.04': tools_deb_build_deps + ' openjdk-11-jdk && ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-openjdk',
  'ubuntu:20.04': tools_deb_build_deps + ' openjdk-11-jdk && apt remove libboost1.71-dev && apt install --yes --no-install-recommends libboost1.67-dev && ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-openjdk',
};

local platformMap(branch, platform) =
  local branch_cmakeflags_map = {
    develop: ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'develop-1.4': ' -DBUILD_CONFIG=enterprise',
    'columnstore-1.4.4': ' -DBUILD_CONFIG=enterprise',
  };

  local platform_map = {
    'opensuse/leap:12': 'zypper install -y ' + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=sles12 && make -j$(nproc) package',
    'opensuse/leap:15': 'zypper install -y ' + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + ' && make -j$(nproc) package',
    'centos:8': "sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-PowerTools.repo && " + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + ' && make -j$(nproc) package',
    'debian:8': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
    'debian:9': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
    'debian:10': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
    'ubuntu:16.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
    'ubuntu:18.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
    'ubuntu:20.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + ' ' + pkg_map[platform] + "' debian/autobake-deb.sh",
  };

  platform_map[platform];

local Pipeline(branch, platform, event) = {
  local pipeline = self,

  _volumes:: {
    mdb: {
      name: 'mdb',
      path: '/mdb',
    },
  },
  tools:: {
    name: 'tools',
    image: platform,
    environment: {
      DEBIAN_FRONTEND: 'noninteractive',
      JAVA_HOME: '/usr/lib/jvm/java-openjdk',
    },
    commands: [
        tools_deps_platform_map[platform]
    ] + std.flattenArrays([
        [
          'git clone --recurse-submodules --depth 1 --branch ' + branch + ' https://github.com/mariadb-corporation/mariadb-columnstore-' + repo,
          'cd mariadb-columnstore-' + repo,
          'cmake -DCMAKE_BUILD_TYPE=Release ' + pkg_map[platform],
          'make -j$(nproc) package',
          'make install',
          'mkdir -p /drone/src/result',
          'cp ' + (if std.split(platform, ':')[0] == 'centos' then '*.rpm' else '*.deb') + ' /drone/src/result/',
          'cd ..',
        ]
        for repo in ['api', 'tools']
    ]),
  },
  tests:: {
    name: 'tests',
    image: platform,
    commands: [
      (if platform == 'centos:7' then 'yum install -y sysvinit-tools' else ''),
      (if platform == 'centos:8' then 'yum install -y diffutils' else ''),
      'yum install -y lz4 wget git-core rsyslog',
      "sed -i '/OmitLocalLogging/d' /etc/rsyslog.conf",
      "sed -i 's/off/on/' /etc/rsyslog.conf",
      'rm -f /etc/rsyslog.d/listen.conf',
      'rsyslogd',
      'yum install -y result/*.rpm',
      'kill $(pidof rsyslogd) && while pidof rsyslogd; do sleep 2; done',
      'rsyslogd',
      'bash -o pipefail ./build/columnstore_startup.sh',
      'git clone --recurse-submodules --branch develop-1.4 --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      'wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/',
      'cd mariadb-columnstore-regression-test/mysql/queries/nightly/alltest',
      './go.sh --sm_unit_test_dir=/mdb/' + builddir + '/storage/columnstore/storage-manager' + (if event == 'pull_request' then ' --tests=test000.sh' else ''),
    ],
  },
  tests_report:: {
    name: 'tests report',
    image: platform,
    when: {
      status: ['success', 'failure'],
    },
    volumes: [pipeline._volumes.mdb],
    commands: [
      'cd mariadb-columnstore-regression-test/mysql/queries/nightly/alltest',
      'test -f go.log && cat go.log || echo missing-go-log-file',
      'test -f testErrorLogs.tgz && mv testErrorLogs.tgz /drone/src/result/ || echo missing-test-result-archive',
    ],
  },
  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event]),
  clone: {
    depth: 10,
  },
  steps: [
           //           {
           //             name: 'submodules',
           //             image: 'alpine/git',
           //             commands: [
           //               'git submodule update --init --recursive --remote',
           //               'git config cmake.update-submodules no',
           //             ],
           //           },
           //           {
           //             name: 'clone-mdb',
           //             image: 'alpine/git',
           //             volumes: [pipeline._volumes.mdb],
           //             commands: [
           //               'mkdir -p /mdb/' + builddir + ' && cd /mdb/' + builddir,
           //               codebase_map[branch],
           //               'git config cmake.update-submodules no',
           //               'rm -rf storage/columnstore',
           //               'cp -r /drone/src /mdb/' + builddir + '/storage/columnstore',
           //             ],
           //           },
           //           {
           //             name: 'build',
           //             image: platform,
           //             volumes: [pipeline._volumes.mdb],
           //             environment: {
           //               DEBIAN_FRONTEND: 'noninteractive',
           //               TRAVIS: 'true',
           //             },
           //             commands: [
           //               'cd /mdb/' + builddir,
           //               "sed -i -e '/-DBUILD_CONFIG=mysql_release/d' debian/rules",
           //               "sed -i -e '/Package: libmariadbd19/,/^$/d' debian/control",
           //               "sed -i -e '/Package: libmariadbd-dev/,/^$/d' debian/control",
           //               "sed -i -e '/Package: mariadb-backup/,/^$/d' debian/control",
           //               "sed -i -e '/Package: mariadb-plugin-connect/,/^$/d' debian/control",
           //               "sed -i -e '/Package: mariadb-plugin-cracklib-password-check/,/^$/d' debian/control",
           //               "sed -i -e '/Package: mariadb-plugin-gssapi-*/,/^$/d' debian/control",
           //               "sed -i -e '/wsrep/d' debian/mariadb-server-*.install",
           //               "sed -i -e 's/Depends: galera.*/Depends:/' debian/control",
           //               "sed -i -e 's/\"galera-enterprise-4\"//' cmake/cpack_rpm.cmake",
           //               platformMap(branch, platform),
           //               'ls -l /mdb/' + builddir + '/storage/columnstore/storage-manager',
           //             ],
           //           },
           //           {
           //             name: 'pack artifacts',
           //             image: 'alpine',
           //             volumes: [pipeline._volumes.mdb],
           //             commands: [
           //               'cd /mdb/' + builddir,
           //               'mkdir -p /drone/src/result',
           //               'apk add --no-cache git',
           //               'echo "engine: $DRONE_COMMIT" > gitcommit.txt',
           //               'echo "server: $$(git rev-parse HEAD)" >> gitcommit.txt',
           //               'echo "buildNo: $DRONE_BUILD_NUMBER" >> gitcommit.txt',
           //               'cp ' + (if std.split(platform, ':')[0] == 'centos' then '*.rpm' else '../*.deb') + ' gitcommit.txt /drone/src/result/',
           //             ],
           //           },
         ] +
         (if std.count(['columnstore-1.4.4'], branch) > 0 then [pipeline.tools] else []) +
         // (if std.count(['develop-1.4', 'columnstore-1.4.4'], branch) > 0 && std.split(platform, ':')[0] == 'centos' then [pipeline.tests] else []) +
         // (if std.count(['develop-1.4', 'columnstore-1.4.4'], branch) > 0 && std.split(platform, ':')[0] == 'centos' then [pipeline.tests_report] else []) +
         [
           {
             name: 'publish',
             image: 'plugins/s3',
             when: {
               status: ['success', 'failure'],
             },
             settings: {
               bucket: 'cspkg',
               access_key: {
                 from_secret: 'aws_access_key_id',
               },
               secret_key: {
                 from_secret: 'aws_secret_access_key',
               },
               source: 'result/*',
               target: branch + '/${DRONE_BUILD_NUMBER}/' + std.strReplace(platform, ':', ''),
               strip_prefix: 'result/',
             },
           },
         ],

  volumes: [pipeline._volumes.mdb { temp: {} }],
  trigger: {
    event: [event],
    branch: [branch],
  } + (if event == 'cron' then {
         cron: ['nightly-' + std.strReplace(branch, '.', '-')],
       } else {}),
};

local FinalPipeline(branch, event) = {
  kind: 'pipeline',
  name: std.join(' ', ['after', branch, event]),
  steps: [
    {
      name: 'notify',
      image: 'plugins/slack',
      settings: {
        room: '#drone_test',
        webhook: {
          from_secret: 'slack_webhook',
        },
        template: (if event != 'pull_request' then '*' + event else '*pull request <https://github.com/{{repo.owner}}/{{repo.name}}/pull/{{build.pull}}|#{{build.pull}}>') +
                  ' build <{{build.link}}|{{build.number}}> {{#success build.status}}succeeded{{else}}failed{{/success}}*.\n\n*Branch*: <https://github.com/{{repo.owner}}/{{repo.name}}/tree/{{build.branch}}|{{build.branch}}>\n*Commit*: <https://github.com/{{repo.owner}}/{{repo.name}}/commit/{{build.commit}}|{{truncate build.commit 8}}> {{truncate build.message.title 100 }}\n*Author*: {{ build.author }}\n*Duration*: {{since build.started}}\n*Artifacts*: https://cspkg.s3.amazonaws.com/index.html?prefix={{build.branch}}/{{build.number}}',
      },
    },
  ],
  trigger: {
    event: [event],
    branch: [branch],
    status: [
      'success',
      'failure',
    ],
  } + (if event == 'cron' then {
         cron: ['nightly-' + std.strReplace(branch, '.', '-')],
       } else {}),
  depends_on: std.map(function(p) std.join(' ', [branch, p, event]), platforms[branch]),
};

[
  Pipeline(b, p, e)
  for b in ['develop', 'develop-1.4', 'columnstore-1.4.4']
  for p in platforms[b]
  for e in ['pull_request', 'cron', 'custom']
] + [
  FinalPipeline(b, e)
  for b in ['develop', 'develop-1.4', 'columnstore-1.4.4']
  for e in ['pull_request', 'cron', 'custom']
]
