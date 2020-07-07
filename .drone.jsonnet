local platforms = {
  develop: ['opensuse/leap:15', 'centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-1.4': ['centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
};

local codebase_map = {
  //develop: 'git clone --recurse-submodules --branch 10.5 --depth 1 https://github.com/MariaDB/server .',
  develop: 'git clone --recurse-submodules --branch 10.5-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
  'develop-1.4': 'git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=RelWithDebInfo -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_PERFSCHEMA=NO -DPLUGIN_SPHINX=NO -DWITH_MARIABACKUP=OFF';

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
  local pkg_format = if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'opensuse/leap') then 'rpm' else 'deb',
  local init = if (pkg_format == 'rpm') then '/usr/lib/systemd/systemd' else 'systemd',
  local img = if (std.split(platform, ':')[0] == 'centos') then platform else 'romcheck/' + std.strReplace(platform, '/', '-'),

  local pipeline = self,
  _volumes:: {
    mdb: {
      name: 'mdb',
      path: '/mdb',
    },
    docker: {
      name: 'docker',
      path: '/var/run/docker.sock',
    },
  },
  smoke:: {
    name: 'smoke',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker run -e DEBIAN_FRONTEND=noninteractive -e MCS_USE_S3_STORAGE=0 --name smoke --privileged --detach --volume /sys/fs/cgroup:/sys/fs/cgroup:ro ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp /drone/src/result smoke:/',
      if (std.split(platform, ':')[0] == 'centos') then 'docker exec -t smoke bash -c "yum install -y git which rsyslog hostname && yum install -y /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'debian' || std.split(platform, ':')[0] == 'ubuntu') then 'docker exec -t smoke bash -c "apt update && apt install -y git rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t smoke bash -c "zypper install -y git which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      // "docker exec -t smoke sed -i '/\\[mariadb\\]/a plugin_maturity=stable' /etc/" + (if pkg_format == 'deb' then 'mysql/mariadb.conf.d/50-' else 'my.cnf.d/') + 'server.cnf',
      'docker exec -t smoke systemctl start mariadb',
      'docker exec -t smoke systemctl start mariadb-columnstore',
      'docker exec -t smoke mysql -e "create database if not exists test; create table test.t1 (a int) engine=Columnstore; insert into test.t1 values (1); select * from test.t1"',
      'docker exec -t smoke systemctl restart mariadb',
      'docker exec -t smoke systemctl restart mariadb-columnstore',
      'sleep 5',
      'docker exec -t smoke mysql -e "insert into test.t1 values (2); select * from test.t1"',
    ],
  },
  regression:: {
    name: 'regression',
    image: 'docker:git',
    // failure: 'ignore',
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      'docker run -e DEBIAN_FRONTEND=noninteractive -e MCS_USE_S3_STORAGE=0 --name regression --privileged --detach --volume /sys/fs/cgroup:/sys/fs/cgroup:ro ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp /drone/src/result regression:/',
      'docker cp /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager regression:/',
      if (std.split(platform, ':')[0] == 'centos') then 'docker exec -t regression bash -c "yum install -y diffutils tar lz4 wget git which rsyslog hostname && yum install -y /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'debian' || std.split(platform, ':')[0] == 'ubuntu') then 'docker exec -t regression bash -c "apt update && apt install -y tar liblz4-tool wget git rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t regression bash -c "zypper install -y gzip tar lz4 wget git which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      'docker exec -t regression systemctl start mariadb',
      'docker exec -t regression systemctl start mariadb-columnstore',
      'docker exec -t regression mysql -e "create database if not exists test; create table test.t1 (a int) engine=Columnstore; insert into test.t1 values (1); select * from test.t1"',
      'git clone --recurse-submodules --branch ' + branch + ' --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      'docker cp mariadb-columnstore-regression-test regression:/',
      'docker exec -t regression bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"',
      // 'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression ./go.sh --sm_unit_test_dir=/storage-manager' + (if event == 'pull_request' then ' --tests=test000.sh,test001.sh' else ''),
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression ./go.sh --sm_unit_test_dir=/storage-manager' + (if event == 'pull_request' then ' --tests=test000.sh' else ''),
    ],
  },
  smokelog:: {
    name: 'smokelog',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker exec -t smoke journalctl -ru mariadb --no-pager || true',
      'docker exec -t smoke cat /var/log/mariadb/columnstore/debug.log || true',
      'docker stop smoke && docker rm smoke || true',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  regressionlog: {
    name: 'regressionlog',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression cat go.log',
      'docker cp regression:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs.tgz /drone/src/result/ || true',
      'docker stop regression && docker rm regression || true',
    ],
    when: {
      status: ['success', 'failure'],
    },
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
               'git config --global url."https://github.com/".insteadOf git@github.com:',
               codebase_map[branch],
               'git rev-parse HEAD',
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
               'cp ' + (if pkg_format == 'deb' then '../' else '') + '*.' + pkg_format + ' buildinfo.txt /drone/src/result/',
               'ls -l /drone/src/result',
             ],
           },
         ] +
         (if branch == 'develop' then [pipeline.smoke] else []) +
         (if branch == 'develop' then [pipeline.smokelog] else []) +
         (if branch == 'develop' then [pipeline.regression] else []) +
         (if branch == 'develop' then [pipeline.regressionlog] else []) +
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
  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: '/var/run/docker.sock' } }],
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
