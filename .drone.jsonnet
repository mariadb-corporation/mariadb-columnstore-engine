local platforms = {
  develop: ['opensuse/leap:15', 'centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-1.5': ['opensuse/leap:15', 'centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'columnstore-1.5.4-1': ['opensuse/leap:15', 'centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-1.4': ['centos:7', 'centos:8', 'debian:9', 'debian:10', 'ubuntu:16.04', 'ubuntu:18.04', 'ubuntu:20.04'],
};

local codebase_map = {
  develop: 'git clone --recurse-submodules --branch 10.6 --depth 1 https://github.com/MariaDB/server .',
  'develop-1.5': 'git clone --recurse-submodules --branch 10.5 --depth 1 https://github.com/MariaDB/server .',
  'columnstore-1.5.4-1': 'git clone --recurse-submodules --branch 10.5 --depth 1 https://github.com/MariaDB/server .',
  'develop-1.4': 'git clone --recurse-submodules --branch 10.4-enterprise --depth 1 https://github.com/mariadb-corporation/MariaDBEnterprise .',
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';
local cmakeflags = '-DCMAKE_BUILD_TYPE=RelWithDebInfo -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO';

local rpm_build_deps = 'install -y systemd-devel git make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect readline-devel createrepo';

local deb_build_deps = 'apt update && apt install --yes --no-install-recommends dpkg-dev systemd libsystemd-dev git ca-certificates devscripts equivs build-essential libboost-all-dev libdistro-info-perl flex pkg-config automake libtool lsb-release bison chrpath cmake dh-apparmor dh-systemd gdb libaio-dev libcrack2-dev libjemalloc-dev libjudy-dev libkrb5-dev libncurses5-dev libpam0g-dev libpcre3-dev libreadline-gplv2-dev libsnappy-dev libssl-dev libsystemd-dev libxml2-dev unixodbc-dev uuid-dev zlib1g-dev libcurl4-openssl-dev dh-exec libpcre2-dev libzstd-dev psmisc socat expect net-tools rsync lsof libdbi-perl iproute2 gawk && mk-build-deps debian/control && dpkg -i mariadb-10*.deb || true && apt install -fy --no-install-recommends';

local platformMap(branch, platform) =
  local branch_cmakeflags_map = {
    develop: ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'develop-1.5': ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'columnstore-1.5.4-1': ' -DBUILD_CONFIG=mysql_release -DWITH_WSREP=OFF',
    'develop-1.4': ' -DBUILD_CONFIG=enterprise',
  };

  local platform_map = {
    'opensuse/leap:15': 'zypper ' + rpm_build_deps + ' cmake libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel libboost_atomic-devel && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': 'yum install -y epel-release && yum install -y cmake3 && ln -s /usr/bin/cmake3 /usr/bin/cmake && yum ' + rpm_build_deps + ' && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos7 && make -j$(nproc) package',
    'centos:8': "yum install -y libgcc && sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-PowerTools.repo && yum " + rpm_build_deps + ' cmake && cmake ' + cmakeflags + branch_cmakeflags_map[branch] + ' -DRPM=centos8 && make -j$(nproc) package',
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
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name smoke$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp result smoke$${DRONE_BUILD_NUMBER}:/',
      if (std.split(platform, ':')[0] == 'centos') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release git which rsyslog hostname && yum install -y /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'debian' || std.split(platform, ':')[0] == 'ubuntu') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "apt update && apt install -y git rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "zypper install -y git which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      // set mariadb server option: plugin_maturity level. just to get working plugin when it diverges with server
      // "docker exec -t smoke$${DRONE_BUILD_NUMBER} sed -i '/\\[mariadb\\]/a plugin_maturity=gamma' /etc/" + (if pkg_format == 'deb' then 'mysql/mariadb.conf.d/50-' else 'my.cnf.d/') + 'server.cnf',
      // start mariadb and mariadb-columnstore services and run simple query
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl start mariadb-columnstore',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} mysql -e "create database if not exists test; create table test.t1 (a int) engine=Columnstore; insert into test.t1 values (1); select * from test.t1"',
      // restart mariadb and mariadb-columnstore services and run simple query again. temporary disabled b/c of the issue:
      // ERROR 1815 (HY000) at line 1: Internal error: CAL0001: dmlprocessor.cpp PackageHandler::run() package type 0 IDB-2006: 'test.t1' does not exist in Columnstore 
      // 'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl restart mariadb',
      // 'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl restart mariadb-columnstore',
      // 'sleep 10',
      // 'docker exec -t smoke$${DRONE_BUILD_NUMBER} mysql -e "insert into test.t1 values (2); select * from test.t1"',
    ],
  },
  mtr:: {
    name: 'mtr',
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    commands: [
      // clone mtr repo
      'git clone --depth 1 https://github.com/mariadb-corporation/columnstore-tests',
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name mtr$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp result mtr$${DRONE_BUILD_NUMBER}:/',
      if (std.split(platform, ':')[0] == 'centos') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release which rsyslog hostname && yum install -y /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'debian' || std.split(platform, ':')[0] == 'ubuntu') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "apt update && apt install -y rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "zypper install -y which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      'docker cp columnstore-tests/mysql-test/suite/columnstore mtr$${DRONE_BUILD_NUMBER}:/usr/share/mysql-test/suite/',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "cd /usr/share/mysql-test && ./mtr --force --max-test-fail=0 --suite=columnstore/basic --skip-test-list=suite/columnstore/basic/failed.def --extern socket=/var/lib/mysql/mysql.sock"',
    ],
  },
  mtrlog:: {
    name: 'mtrlog',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start mariadb service logs ----------"',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} journalctl -u mariadb --no-pager || echo "mariadb service failure"',
      'echo "---------- end mariadb service logs ----------"',
      'echo',
      'echo "---------- start columnstore debug log ----------"',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"',
      'echo "---------- end columnstore debug log ----------"',
      'echo "---------- end columnstore debug log ----------"',
      'docker cp mtr$${DRONE_BUILD_NUMBER}:/usr/share/mysql-test/var/log /drone/src/result/mtr-logs || echo "missing /usr/share/mysql-test/var/log"',
      'docker stop mtr$${DRONE_BUILD_NUMBER} && docker rm mtr$${DRONE_BUILD_NUMBER} || echo "cleanup mtr failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  regression:: {
    name: 'regression',
    image: 'docker:git',
    #failure: (if event == 'cron' then 'ignore' else ''),
    failure: 'ignore',
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      // clone regression test repo
      'git clone --recurse-submodules --branch develop-1.5 --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name regression$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      // copy packages, regresssion test suite and storage manager unit test binary to the instance
      'docker cp result regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp mariadb-columnstore-regression-test regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager regression$${DRONE_BUILD_NUMBER}:/',
      // check storage-manager unit test binary file
      'docker exec -t regression$${DRONE_BUILD_NUMBER} ls -l /storage-manager',
      if (std.split(platform, ':')[0] == 'centos') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release diffutils tar lz4 wget git which rsyslog hostname && yum install -y /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'debian' || std.split(platform, ':')[0] == 'ubuntu') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "apt update && apt install -y tar liblz4-tool wget git rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "zypper install -y gzip tar lz4 wget git which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      // copy  test data for regression test suite
      'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"',
      // set mariadb lower_case_table_names=1 config option
      'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/^.mariadb.$/a lower_case_table_names=1" /etc/' + (if pkg_format == 'deb' then 'mysql/mariadb.conf.d/50-' else 'my.cnf.d/') + 'server.cnf',
      // start mariadb and mariadb-columnstore services
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb-columnstore',
      // run regression test000 on pull request and manual build events. on other events run all tests
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} ./go.sh --sm_unit_test_dir=/storage-manager' + (if event == 'pull_request' || event == 'manual' then ' --tests=test000.sh' else ''),
    ],
  },
  smokelog:: {
    name: 'smokelog',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start mariadb service logs ----------"',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} journalctl -u mariadb --no-pager || echo "mariadb service failure"',
      'echo "---------- end mariadb service logs ----------"',
      'echo',
      'echo "---------- start columnstore debug log ----------"',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"',
      'echo "---------- end columnstore debug log ----------"',
      'docker stop smoke$${DRONE_BUILD_NUMBER} && docker rm smoke$${DRONE_BUILD_NUMBER} || echo "cleanup smoke failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  regressionlog:: {
    name: 'regressionlog',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start columnstore regression short report ----------"',
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} cat go.log || echo "missing go.log"',
      'echo "---------- end columnstore regression short report ----------"',
      'echo',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs.tgz /drone/src/result/ || echo "missing testErrorLogs.tgz"',
      'docker stop regression$${DRONE_BUILD_NUMBER} && docker rm regression$${DRONE_BUILD_NUMBER} || echo "cleanup regression failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  dockerfile:: {
    name: 'dockerfile',
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    commands: [
      'git clone --depth 1 https://github.com/mariadb-corporation/mariadb-community-columnstore-docker.git',
      'cd mariadb-community-columnstore-docker',
      'apk add --no-cache patch',
      'patch Dockerfile ../Dockerfile.patch',
      'cp ../result/MariaDB-common-10* ../result/MariaDB-client-10* ../result/MariaDB-server-10* ../result/MariaDB-shared-10* ../result/MariaDB-columnstore-engine-10* ./',
    ],
  },
  ecr:: {
    name: 'ecr',
    image: 'plugins/ecr',
    settings: {
      registry: '866067714787.dkr.ecr.us-east-1.amazonaws.com',
      repo: 'columnstore/engine',
      context: 'mariadb-community-columnstore-docker',
      dockerfile: 'mariadb-community-columnstore-docker/Dockerfile',
      access_key: {
        from_secret: 'aws_access_key_id',
      },
      secret_key: {
        from_secret: 'aws_secret_access_key'
      },
    },
  },
  docker:: {
    name: 'docker',
    image: 'plugins/docker',
    settings: {
      repo: 'romcheck/columnstore',
      context: '/drone/src/mariadb-community-columnstore-docker',
      dockerfile: 'mariadb-community-columnstore-docker/Dockerfile',
      username: 'romcheck',
      password: {
        from_secret: 'dockerhub_token',
      }
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
               "sed -i 's/-DPLUGIN_PERFSCHEMA=NO//' debian/autobake-deb.sh",
               #"sed -i 's/BETA/GAMMA/' storage/columnstore/CMakeLists.txt",
               #"sed -i -e '/mcs-start-storagemanager.py/d' debian/mariadb-plugin-columnstore.install",
               platformMap(branch, platform),
               (if pkg_format == 'rpm' then 'createrepo .' else 'dpkg-scanpackages ../ | gzip > ../Packages.gz '),
             ],
           },
           {
             name: 'list pkgs',
             image: 'docker:git',
             volumes: [pipeline._volumes.mdb],
             commands: [
               'cd /mdb/' + builddir,
               'mkdir /drone/src/result',
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'cp -r ' + (if pkg_format == 'deb' then '../Packages.gz ../' else 'repodata ') + '*.' + pkg_format + ' buildinfo.txt /drone/src/result/',
               'ls -l /drone/src/result',
               'echo "check columnstore package:"',
               'ls -l /drone/src/result | grep columnstore',
               # get rid of the annoying password policy plugin pkg (have no idea how to exclude it from build)
               'rm -f /drone/src/result/MariaDB-cracklib-password*',
             ],
           },
         ] +
         #(if (platform == 'centos:8' && event == 'cron') then [pipeline.dockerfile] else []) +
         #(if (platform == 'centos:8' && event == 'cron') then [pipeline.docker] else []) +
         #(if (platform == 'centos:8' && event == 'cron') then [pipeline.ecr] else []) +
         (if (platform == 'centos:7') then [pipeline.mtr] else []) +
         (if (platform == 'centos:7') then [pipeline.mtrlog] else []) +
         (if platform != 'centos:7' then [pipeline.smoke] else []) +
         (if platform != 'centos:7' then [pipeline.smokelog] else []) +
         [pipeline.regression] +
         [pipeline.regressionlog] +
         [
           {
             name: 'publish',
             image: 'plugins/s3-sync',
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
               source: 'result',
               target: branch + '/' + event + '/${DRONE_BUILD_NUMBER}/' + std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
             },
           },
           {
             name: 'publish latest',
             image: 'plugins/s3-sync',
             when: {
               status: ['success', 'failure'],
               event: ['cron'],
             },
             settings: {
               bucket: 'cspkg',
               access_key: {
                 from_secret: 'aws_access_key_id',
               },
               secret_key: {
                 from_secret: 'aws_secret_access_key',
               },
               source: 'result',
               target: branch + '/latest/' + std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
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
                  ' build <{{build.link}}|{{build.number}}> {{#success build.status}}succeeded{{else}}failed{{/success}}*.\n\n*Branch*: <https://github.com/{{repo.owner}}/{{repo.name}}/tree/{{build.branch}}|{{build.branch}}>\n*Commit*: <https://github.com/{{repo.owner}}/{{repo.name}}/commit/{{build.commit}}|{{truncate build.commit 8}}> {{truncate build.message.title 100 }}\n*Author*: {{ build.author }}\n*Duration*: {{since build.started}}\n*Artifacts*: https://cspkg.s3.amazonaws.com/index.html?prefix={{build.branch}}/{{build.event}}/{{build.number}}',
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
  for b in ['develop', 'develop-1.5', 'develop-1.4', 'columnstore-1.5.4-1']
  for p in platforms[b]
  for e in ['pull_request', 'cron', 'custom']
] + [
  FinalPipeline(b, e)
  for b in ['develop', 'develop-1.5', 'develop-1.4', 'columnstore-1.5.4-1']
  for e in ['pull_request', 'cron', 'custom']
]
