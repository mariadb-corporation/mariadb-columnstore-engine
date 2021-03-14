local events = ['pull_request', 'cron', 'custom'];

local platforms = {
 'develop-1.4': ["centos:7", "centos:8", "debian:9", "debian:10", "ubuntu:16.04", "ubuntu:18.04", "ubuntu:20.04"],
};

local codebase_map = {
  'develop-1.4': "git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .",
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=Release -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO -DBUILD_CONFIG=enterprise';

local rpm_build_deps = 'yum install -y git cmake make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel';

local deb_build_deps = 'apt update && apt install --yes --no-install-recommends git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends';

local platformMap(branch, platform) =
  local platform_map = {
    'opensuse/leap:15': 'zypper install -y ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=centos7 && make -j$(nproc) package',
    'centos:8': "sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/*PowerTools.repo && " + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=centos8 && make -j$(nproc) package',
    'debian:9': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=stretch' debian/autobake-deb.sh",
    'debian:10': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=buster' debian/autobake-deb.sh",
    'ubuntu:16.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=xenial' debian/autobake-deb.sh",
    'ubuntu:18.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=bionic' debian/autobake-deb.sh",
    'ubuntu:20.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=focal' debian/autobake-deb.sh",
  };

  platform_map[platform];

local Pipeline(branch, platform, event) = {
  local regression_ref = if (std.split(branch, '-')[0] == 'develop') then branch else 'develop-1.4',

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
    [if event == 'cron' then 'failure']: 'ignore',
    volumes: [pipeline._volumes.mdb],
    commands: [
      (if platform == 'centos:7' then 'yum install -y sysvinit-tools' else '' ),
      (if platform == 'centos:8' then 'yum install -y diffutils' else '' ),
      'yum install -y lz4 wget git rsyslog',
      "sed -i '/OmitLocalLogging/d' /etc/rsyslog.conf",
      "sed -i 's/off/on/' /etc/rsyslog.conf",
      "rm -f /etc/rsyslog.d/listen.conf",
      'rsyslogd',
      "yum install -y /mdb/" + builddir + "/*.rpm",
      'kill $(pidof rsyslogd) && while pidof rsyslogd; do sleep 2; done',
      'rsyslogd',
      'bash -o pipefail ./build/columnstore_startup.sh',
      'git clone --recurse-submodules --branch ' + regression_ref + ' --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      'wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/',
      'cd mariadb-columnstore-regression-test/mysql/queries/nightly/alltest',
      "./go.sh --sm_unit_test_dir=/mdb/" + builddir + "/storage/columnstore/storage-manager" + (if event != 'cron' then ' --tests=test000.sh' else ''),
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
  name: std.join(" ", [branch, platform, event]),
  clone: {
    depth: 10,
  },
  steps: [
    {
      name: 'submodules',
      image: 'alpine/git',
      commands: [
        'git submodule update --init --recursive',
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
        'rm -rf storage/columnstore',
        'cp -r /drone/src /mdb/' + builddir + '/storage/columnstore',
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
        platformMap(branch, platform),
      ],
    },
    {
      name: 'pack artifacts',
      image: 'alpine',
      volumes: [pipeline._volumes.mdb],
      commands: [
        'cd /mdb/' + builddir,
        'mkdir /drone/src/result',
        'apk add --no-cache git',
        'echo "engine: $DRONE_COMMIT" > gitcommit.txt',
        'echo "server: $$(git rev-parse HEAD)" >> gitcommit.txt',
        'echo "buildNo: $DRONE_BUILD_NUMBER" >> gitcommit.txt',
        'tar cf /drone/src/result/${DRONE_BUILD_NUMBER}-' + branch + '-' + std.strReplace(platform, ':', '') + '.tar ' + (if std.split(platform, ":")[0]=="centos" then '*.rpm' else '../*.deb') + ' gitcommit.txt'
      ],
    },
  ] +
  (if std.split(platform, ":")[0]=="centos" then [pipeline.tests] else []) +
  (if std.split(platform, ":")[0]=="centos" then [pipeline.tests_report] else []) +
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
        target: branch + '/' + event + '/${DRONE_BUILD_NUMBER}/' + std.strReplace(platform, ':', ''),
        strip_prefix: 'result/',
      },
    },
  ],

  volumes: [pipeline._volumes.mdb + {"temp": {}}],
  trigger: {
    event: [event],
    branch: [branch],
  } + (if event == 'cron' then {
    cron: ['nightly-'+ std.strReplace(branch, '.', '-')]
  } else {})
};

local FinalPipeline(branch, event) = {
    kind: "pipeline",
    name: std.join(" ", ["after", branch, event]),
    steps: [
      {
        name: "notify",
        image: "plugins/slack",
        settings: {
          room: "#drone_test",
          webhook: {
            from_secret: "slack_webhook"
          },
          template: (if event == 'cron' then "*Nightly" else "*Pull Request <https://github.com/{{repo.owner}}/{{repo.name}}/pull/{{build.pull}}|#{{build.pull}}>" ) +
          " build <{{build.link}}|{{build.number}}> {{#success build.status}}succeeded{{else}}failed{{/success}}*.

*Branch*: <https://github.com/{{repo.owner}}/{{repo.name}}/tree/{{build.branch}}|{{build.branch}}>
*Commit*: <https://github.com/{{repo.owner}}/{{repo.name}}/commit/{{build.commit}}|{{truncate build.commit 8}}> {{truncate build.message.title 100 }}
*Author*: {{ build.author }}
*Duration*: {{since build.started}}
*Artifacts*: https://cspkg.s3.amazonaws.com/index.html?prefix={{build.branch}}/{{build.number}}"
        },
      },
    ],
    trigger: {
      event: [event],
      branch: [branch],
      status: [
        "success",
        "failure"
      ],
    } + (if event == 'cron' then {
      cron: ['nightly-'+ std.strReplace(branch, '.', '-')]
    } else {}),
    depends_on: std.map(function(p) std.join(" ", [branch, p, event]), platforms[branch])
};

[
Pipeline(b, p, e)
for b in std.objectFields(platforms)
for p in platforms[b]
for e in events
] + [
FinalPipeline(b, e)
for b in std.objectFields(platforms)
for e in events
]
