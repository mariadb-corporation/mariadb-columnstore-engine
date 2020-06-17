local platforms = {
  develop: ['opensuse/leap:15', 'centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-1.4': ['centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
};

local codebase_map = {
  //  "develop": "git clone --recurse-submodules --branch mariadb-10.5.3 --depth 1 https://github.com/MariaDB/server .",
  develop: 'git clone --recurse-submodules --branch bb-10.5-cs --depth 1 https://github.com/MariaDB/server .',
  'develop-1.4': 'git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=Release -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO';

local rpm_build_deps = 'install -y systemd-devel git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel';

local deb_build_deps = 'apt update && apt install --yes --no-install-recommends systemd libsystemd-dev git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends';

local platformMap(branch, platform) =
  local branch_cmakeflags_map = {
    develop: ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'develop-1.4': ' -DBUILD_CONFIG=enterprise',
  };

  local platform_map = {
    'opensuse/leap:15': 'zypper ' + rpm_build_deps + ' libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel libboost_atomic-devel && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': 'yum ' + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos7 && make -j$(nproc) package',
    'centos:8': "yum install -y libgcc && sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-PowerTools.repo && yum " + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos8 && make -j$(nproc) package',
    'debian:9': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=stretch' debian/autobake-deb.sh",
    'debian:10': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=buster' debian/autobake-deb.sh",
    'ubuntu:16.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=xenial' debian/autobake-deb.sh",
    'ubuntu:18.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=bionic' debian/autobake-deb.sh",
    'ubuntu:20.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + branch_cmakeflags_map[branch] + " -DDEB=focal' debian/autobake-deb.sh",
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
  tests:: {
    name: 'tests',
    image: platform,
    commands: [
      (if platform == 'centos:7' then 'yum install -y sysvinit-tools' else ''),
      (if platform == 'centos:8' then 'yum install -y diffutils' else ''),
      'yum install -y lz4 wget git rsyslog',
      "sed -i '/OmitLocalLogging/d' /etc/rsyslog.conf",
      "sed -i 's/off/on/' /etc/rsyslog.conf",
      'rm -f /etc/rsyslog.d/listen.conf',
      'rsyslogd',
      'yum install -y result/*.rpm',
      'kill $(pidof rsyslogd) && while pidof rsyslogd; do sleep 2; done',
      'rsyslogd',
      'bash -o pipefail ./build/columnstore_startup.sh',
      'git clone --recurse-submodules --branch ' + branch + ' --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      'wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/',
      'cd mariadb-columnstore-regression-test/mysql/queries/nightly/alltest',
      './go.sh --sm_unit_test_dir=/drone/src/storage-manager' + (if event == 'pull_request' then ' --tests=test000.sh' else ''),
      'cat go.log',
      'test -f testErrorLogs.tgz && mv testErrorLogs.tgz /drone/src/result/ || echo no-errors-archive',
    ],
  },
  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event]),
  clone: {
    depth: 10,
  },
  steps: [
           {
             name: 'submodules',
             image: 'alpine/git',
             commands: [
               'git submodule update --init --recursive --remote',
               'git config cmake.update-submodules no',
             ],
           },
           {
             name: 'clone-mdb',
             image: 'alpine/git',
             volumes: [pipeline._volumes.mdb],
             commands: [
               'mkdir -p /mdb/' + builddir + ' && cd /mdb/' + builddir,
               codebase_map[branch],
               'git config cmake.update-submodules no',
               'rm -rf storage/columnstore/columnstore',
               'cp -r /drone/src /mdb/' + builddir + '/storage/columnstore/columnstore',
             ],
           },
           {
             name: 'build',
             image: platform,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: 'noninteractive',
               TRAVIS: 'true',
             },
             commands: [
               'cd /mdb/' + builddir,
               "sed -i -e '/-DBUILD_CONFIG=mysql_release/d' debian/rules",
               "sed -i -e '/Package: libmariadbd19/,/^$/d' debian/control",
               "sed -i -e '/Package: libmariadbd-dev/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-backup/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-connect/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-cracklib-password-check/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-gssapi-*/,/^$/d' debian/control",
               "sed -i -e '/wsrep/d' debian/mariadb-server-*.install",
               "sed -i -e 's/Depends: galera.*/Depends:/' debian/control",
               "sed -i -e 's/\"galera-enterprise-4\"//' cmake/cpack_rpm.cmake",
               "sed -i '/columnstore/Id' debian/autobake-deb.sh",
               "sed -i 's/.*flex.*/echo/' debian/autobake-deb.sh",
               "sed -i '/libjemalloc2\\,$$/a \\ \\ \\ \\ \\ \\ \\ \\ \\ python | python2 | python3,' debian/control",
               platformMap(branch, platform),
             ],
           },
           {
             name: 'list pkgs',
             image: 'alpine',
             volumes: [pipeline._volumes.mdb],
             commands: [
               'cd /mdb/' + builddir,
               'mkdir /drone/src/result',
               'apk add --no-cache git',
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'cp ' + (if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'opensuse/leap') then '*.rpm' else '../*.deb') + ' buildinfo.txt /drone/src/result/',
               'ls -l /drone/src/result',
             ],
           },
         ] +
         (if branch == 'develop-1.4' && std.split(platform, ':')[0] == 'centos' then [pipeline.tests] else []) +
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
               target: branch + '/${DRONE_BUILD_NUMBER}/' + std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
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
        webhook: {
          from_secret: 'slack_webhook',
        },
        template: '*' + event + (if event == 'pull_request' then ' <https://github.com/{{repo.owner}}/{{repo.name}}/pull/{{build.pull}}|#{{build.pull}}>' else '') +
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
  for b in ['develop', 'develop-1.4']
  for p in platforms[b]
  for e in ['pull_request', 'cron', 'custom']
] + [
  FinalPipeline(b, e)
  for b in ['develop', 'develop-1.4']
  for e in ['pull_request', 'cron', 'custom']
]
