local events = ['pull_request', 'cron'];

local platforms = {
  develop: ['opensuse/leap:15', 'centos:7', 'centos:8', 'rockylinux:8', 'debian:10', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-6': ['opensuse/leap:15', 'centos:7', 'centos:8', 'rockylinux:8', 'debian:10', 'ubuntu:18.04', 'ubuntu:20.04'],
  'develop-5': ['opensuse/leap:15', 'centos:7', 'centos:8', 'rockylinux:8', 'debian:10', 'ubuntu:18.04', 'ubuntu:20.04'],
};

local platforms_arm = {
  develop: ['rockylinux:8'],
  'develop-6': ['rockylinux:8'],
};

local any_branch = '**';
local platforms_custom = ['opensuse/leap:15', 'centos:7', 'centos:8', 'rockylinux:8', 'debian:10', 'ubuntu:18.04', 'ubuntu:20.04'];
local platforms_arm_custom = ['rockylinux:8'];

local platforms_mtr = ['centos:7', 'centos:8', 'ubuntu:20.04'];

local server_ref_map = {
  develop: '10.8',
  'develop-6': '10.6-enterprise',
  'develop-5': '10.5',
  '**': '10.8',
};

local builddir = 'verylongdirnameforverystrangecpackbehavior';

local cmakeflags = '-DCMAKE_BUILD_TYPE=RelWithDebInfo -DPLUGIN_COLUMNSTORE=YES -DPLUGIN_XPAND=NO -DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO ' +
                   '-DPLUGIN_TOKUDB=NO -DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO ' +
                   '-DWITH_EMBEDDED_SERVER=OFF -DWITH_WSREP=OFF ' +
                   '-DBUILD_CONFIG=mysql_release -DWITH_UNITTESTS=YES -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST';

local rpm_build_deps = 'install -y lz4 systemd-devel git make gcc gcc-c++ libaio-devel openssl-devel boost-devel bison snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel expect createrepo';
local deb_build_deps = 'apt update --yes && apt install --yes --no-install-recommends build-essential devscripts ccache equivs eatmydata dh-systemd ' +
                       '&& mk-build-deps debian/control -t "apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends" -r -i';
local centos8_build_deps = 'dnf install -y gcc-toolset-10 libarchive cmake lz4-devel && . /opt/rh/gcc-toolset-10/enable ';
local rockylinux8_powertools = "dnf install -y 'dnf-command(config-manager)' && dnf config-manager --set-enabled powertools ";


local platformMap(platform) =
  local platform_map = {
    'opensuse/leap:15': 'zypper ' + rpm_build_deps + ' pcre2-devel liblz4-devel cmake libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel libboost_atomic-devel gcc-fortran && cmake ' + cmakeflags + ' -DRPM=sles15 && make -j$(nproc) package',
    'centos:7': 'yum install -y epel-release && yum install -y cmake3 && ln -s /usr/bin/cmake3 /usr/bin/cmake && yum ' + rpm_build_deps + ' lz4-devel pcre2-devel && cmake ' + cmakeflags + ' -DRPM=centos7 && make -j$(nproc) package',
    'centos:8': 'yum ' + rpm_build_deps + ' lz4-devel cmake && cmake ' + cmakeflags + ' -DRPM=centos8 && make -j$(nproc) package',
    'rockylinux:8': rockylinux8_powertools + ' && ' + centos8_build_deps + ' && dnf ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=rockylinux8 && make -j$(nproc) package',
    'debian:10': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=buster' debian/autobake-deb.sh",
    'ubuntu:18.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=bionic' debian/autobake-deb.sh",
    'ubuntu:20.04': deb_build_deps + " && CMAKEFLAGS='" + cmakeflags + " -DDEB=focal' debian/autobake-deb.sh",
  };
  platform_map[platform];


local testRun(platform) =
  local platform_map = {
    'opensuse/leap:15': 'ctest -R columnstore: -j $(nproc) --output-on-failure',
    'centos:7': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'centos:8': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'rockylinux:8': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'debian:10': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:18.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:20.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
  };
  platform_map[platform];


local testPreparation(platform) =
  local platform_map = {
    'opensuse/leap:15': 'zypper install -y gtest boost-devel libboost_system-devel libboost_filesystem-devel libboost_thread-devel libboost_regex-devel libboost_date_time-devel libboost_chrono-devel libboost_atomic-devel cppunit-devel snappy-devel cmake',
    'centos:7': 'yum -y install epel-release && yum install -y gtest-devel cppunit-devel cmake3 boost-devel snappy-devel',
    'centos:8': 'yum install -y lz4 gtest-devel cppunit-devel cmake3 boost-devel snappy-devel',
    'rockylinux:8': rockylinux8_powertools + ' && dnf install -y git lz4 cppunit-devel cmake3 boost-devel snappy-devel gtest-devel',
    'debian:10': 'apt update && apt install --yes libboost-all-dev libgtest-dev libcppunit-dev libsnappy-dev googletest cmake',
    'ubuntu:18.04': 'apt update && apt install --yes libboost-all-dev libgtest-dev libcppunit-dev googletest libsnappy-dev cmake g++ && cd /usr/src/googletest; cmake . && cmake --build . --target install; cd -',
    'ubuntu:20.04': 'apt update && apt install --yes libboost-all-dev libgtest-dev libcppunit-dev googletest libsnappy-dev cmake',
  };
  platform_map[platform];


local Pipeline(branch, platform, event, arch='amd64') = {
  local pkg_format = if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'opensuse/leap' || std.split(platform, ':')[0] == 'rockylinux') then 'rpm' else 'deb',
  local init = if (pkg_format == 'rpm') then '/usr/lib/systemd/systemd' else 'systemd',
  local mtr_path = if (pkg_format == 'rpm') then '/usr/share/mysql-test' else '/usr/share/mysql/mysql-test',
  local socket_path = if (pkg_format == 'rpm') then '/var/lib/mysql/mysql.sock' else '/run/mysqld/mysqld.sock',
  local config_path_prefix = if (pkg_format == 'rpm') then '/etc/my.cnf.d/' else '/etc/mysql/mariadb.conf.d/50-',
  local img = if (platform == 'centos:7' || std.split(platform, ':')[0] == 'rockylinux') then platform else 'romcheck/' + std.strReplace(platform, '/', '-'),
  local regression_ref = if (std.split(branch, '-')[0] == 'develop') then branch else 'develop-6',
  local branchp = if (branch == '**') then '' else branch,

  local server_remote = if (std.split(branch, '-')[0] == 'columnstore' || branch == 'develop-6') then 'https://github.com/mariadb-corporation/MariaDBEnterprise' else 'https://github.com/MariaDB/server',

  local pipeline = self,

  publish(step_prefix='pkg', eventp=event + '/${DRONE_BUILD_NUMBER}'):: {
    name: 'publish ' + step_prefix,
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
      target: branchp + '/' + eventp + '/' + arch + '/' + std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
      delete: 'true',
    },
  },
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
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release which rsyslog hostname procps-ng && yum install -y /result/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "zypper install -y which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      // start mariadb and mariadb-columnstore services and run simple query
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl start mariadb-columnstore',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} mariadb -e "create database if not exists test; create table test.t1 (a int) engine=Columnstore; insert into test.t1 values (1); select * from test.t1"',
      // restart mariadb and mariadb-columnstore services and run simple query again
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl restart mariadb',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} systemctl restart mariadb-columnstore',
      'sleep 10',
      'docker exec -t smoke$${DRONE_BUILD_NUMBER} mariadb -e "insert into test.t1 values (2); select * from test.t1"',
    ],
  },
  mtr:: {
    name: 'mtr',
    image: 'docker:git',
    [if arch == 'arm64' then 'failure']: 'ignore',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env MYSQL_TEST_DIR=' + mtr_path + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name mtr$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp result mtr$${DRONE_BUILD_NUMBER}:/',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "zypper install -y which hostname rsyslog patch perl-Data-Dumper-Concise perl-Memoize-ExpireLRU && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release diffutils which rsyslog hostname procps-ng patch perl-Data-Dumper perl-Getopt-Long perl-Memoize perl-Time-HiRes cracklib-dicts && yum install -y /result/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y rsyslog hostname patch && apt install -y -f /result/*.' + pkg_format + '"' else '',
      'docker cp mysql-test/columnstore mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/suite/',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} chown -R mysql:mysql ' + mtr_path,
      // disable systemd 'ProtectSystem' (we need to write to /usr/share/)
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} sed -i "/ProtectSystem/d" /usr/lib/systemd/system/mariadb.service',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl daemon-reload',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} mariadb -e "create database if not exists test;"',
      // delay mtr for manual debugging on live instance
      'sleep $${MTR_DELAY_SECONDS:-1s}',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path + ' --force --max-test-fail=0 --suite=columnstore/basic,columnstore/bugfixes"',
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
      'docker cp mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/var/log /drone/src/result/mtr-logs || echo "missing ' + mtr_path + '/var/log"',
      'docker stop mtr$${DRONE_BUILD_NUMBER} && docker rm mtr$${DRONE_BUILD_NUMBER} || echo "cleanup mtr failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  regression:: {
    name: 'regression',
    image: 'docker:git',
    [if event == 'cron' then 'failure']: 'ignore',
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    environment: {
      REGRESSION_TESTS: if (event == 'cron') then '' else '${REGRESSION_TESTS:-test000.sh}',
      REGRESSION_REF: '${REGRESSION_REF:-' + regression_ref + '}',
      REGRESSION_TIMEOUT: {
        from_secret: 'regression_timeout',
      },
    },
    commands: [
      // clone regression test repo
      'git clone --recurse-submodules --branch $$REGRESSION_REF --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      // where are we now?
      'cd mariadb-columnstore-regression-test',
      'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
      'cd ..',
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name regression$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      // copy packages, regresssion test suite and storage manager unit test binary to the instance
      'docker cp result regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp mariadb-columnstore-regression-test regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager regression$${DRONE_BUILD_NUMBER}:/',
      // check storage-manager unit test binary file
      'docker exec -t regression$${DRONE_BUILD_NUMBER} ls -l /storage-manager',
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release diffutils tar lz4 wget which rsyslog hostname procps-ng && yum install -y /result/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y tar liblz4-tool wget rsyslog hostname && apt install -y -f /result/*.' + pkg_format + '"' else '',
      if (std.split(platform, '/')[0] == 'opensuse') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "zypper install -y gzip tar lz4 wget which hostname rsyslog && zypper install -y --allow-unsigned-rpm /result/*.' + pkg_format + '"' else '',
      // copy test data for regression test suite
      'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"',
      // set mariadb lower_case_table_names=1 config option
      'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/^.mariadb.$/a lower_case_table_names=1" ' + config_path_prefix + 'server.cnf',
      // set default client character set to utf-8
      'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/^.client.$/a default-character-set=utf8" ' + config_path_prefix + 'client.cnf',
      // start mariadb and mariadb-columnstore services
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb-columnstore',
      // delay regression for manual debugging on live instance
      'sleep $${REGRESSION_DELAY_SECONDS:-1s}',
      // run regression test000 on pull request and manual (may be overwritten by env variable parameter) build events. on other events run all tests
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} timeout -k 1m -s SIGKILL --preserve-status $${REGRESSION_TIMEOUT:-10h} ./go.sh --sm_unit_test_dir=/storage-manager --tests=$${REGRESSION_TESTS}',
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
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} bash -c "tar czf testErrorLogs2.tgz *.log /var/log/mariadb/columnstore" || echo "failed to grab regression results"',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs2.tgz /drone/src/result/ || echo "missing testErrorLogs.tgz"',
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
        from_secret: 'aws_secret_access_key',
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
      },
    },
  },

  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event, arch]),
  platform: { arch: arch },
  // [if arch == 'arm64' then 'node']: { arch: 'arm64' },
  clone: { depth: 10 },
  steps: [
           {
             name: 'submodules',
             image: 'alpine/git',
             commands: [
               'git submodule update --init --recursive',
               'git config cmake.update-submodules no',
               'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
             ],
           },
           {
             name: 'clone-mdb',
             image: 'alpine/git',
             volumes: [pipeline._volumes.mdb],
             environment: {
               SERVER_REF: '${SERVER_REF:-' + server_ref_map[branch] + '}',
               SERVER_REMOTE: '${SERVER_REMOTE:-' + server_remote + '}',
               SERVER_SHA: '${SERVER_SHA:-' + server_ref_map[branch] + '}',
             },
             commands: [
               'echo $$SERVER_REF',
               'echo $$SERVER_REMOTE',
               'mkdir -p /mdb/' + builddir + ' && cd /mdb/' + builddir,
               'git config --global url."https://github.com/".insteadOf git@github.com:',
               'git -c submodule."storage/rocksdb/rocksdb".update=none -c submodule."wsrep-lib".update=none -c submodule."storage/columnstore/columnstore".update=none clone --recurse-submodules --depth 200 --branch $$SERVER_REF $$SERVER_REMOTE .',
               'git reset --hard $$SERVER_SHA',
               'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
               'git config cmake.update-submodules no',
               'rm -rf storage/columnstore/columnstore',
               'cp -r /drone/src /mdb/' + builddir + '/storage/columnstore/columnstore',
             ],
           },
           {
             name: 'build',
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: 'noninteractive',
               BUILDPACKAGE_FLAGS: '-b',  // Save time and produce only binary packages, not source
             },
             commands: [
               'cd /mdb/' + builddir,
               // Remove Debian build flags that could prevent ColumnStore from building
               "sed '/-DPLUGIN_COLUMNSTORE=NO/d' -i debian/rules",
               // Tweak debian packaging stuff
               "sed -i -e '/Package: mariadb-backup/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-connect/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-gssapi*/,/^$/d' debian/control",
               "sed -i -e '/Package: mariadb-plugin-xpand*/,/^$/d' debian/control",
               'sed "/Package: mariadb-plugin-mroonga/,/^$/d" -i debian/control',
               'sed "/Package: mariadb-plugin-rocksdb/,/^$/d" -i debian/control',
               'sed "/Package: mariadb-plugin-spider/,/^$/d" -i debian/control',
               'sed "/Package: mariadb-plugin-oqgraph/,/^$/d" -i debian/control',
               'sed "/ha_sphinx.so/d" -i debian/mariadb-server-*.install',
               'sed "/Package: libmariadbd19/,/^$/d" -i debian/control',
               'sed "/Package: libmariadbd-dev/,/^$/d" -i debian/control',
               // Disable Galera
               "sed -i -e 's/Depends: galera.*/Depends:/' debian/control",
               "sed -i -e 's/\"galera-enterprise-4\"//' cmake/cpack_rpm.cmake",
               "sed -i -e '/wsrep/d' debian/mariadb-server-*.install",
               // Leave test package for mtr
               "sed -i '/(mariadb|mysql)-test/d;/-test/d' debian/autobake-deb.sh",
               "sed -i '/test-embedded/d' debian/mariadb-test.install",
               // From Debian Bullseye/Ubuntu Groovy, liburing replaces libaio
               "apt-cache madison liburing-dev | grep 'liburing-dev' || sed 's/liburing-dev/libaio-dev/g' -i debian/control && sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules && sed '/-DWITH_URING=yes/d' -i debian/rules",
               // From Debian Buster/Ubuntu Focal onwards libpmem-dev is available
               "apt-cache madison libpmem-dev | grep 'libpmem-dev' || sed '/libpmem-dev/d' -i debian/control && sed '/-DWITH_PMEM/d' -i debian/rules",
               // Change plugin_maturity level
               // "sed -i 's/BETA/GAMMA/' storage/columnstore/CMakeLists.txt",
               // Workaround till upstream removes 4535 workaround (workaround for workaround!)
               "sed -i '/MCOL-4535/,/^$/d' debian/autobake-deb.sh",
               testPreparation(platform),
               platformMap(platform),
               if (pkg_format == 'rpm') then 'createrepo .' else 'dpkg-scanpackages ../ | gzip > ../Packages.gz',
             ],
           },
           {
             name: 'unittests',
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: 'noninteractive',
             },
             commands: [
               'cd /mdb/' + builddir,
               testPreparation(platform),
               testRun(platform),
             ],
           },
           {
             name: 'list pkgs',
             image: 'docker:git',
             volumes: [pipeline._volumes.mdb],
             commands: [
               'cd /mdb/' + builddir,
               'mkdir -p /drone/src/result',
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'cp -r ' + (if (pkg_format == 'deb') then '../Packages.gz ../' else 'repodata ') + '*.' + pkg_format + ' buildinfo.txt /drone/src/result/',
               'ls -l /drone/src/result',
               'echo "check columnstore package:"',
               'ls -l /drone/src/result | grep columnstore',
             ],
           },
         ] +
         [pipeline.publish()] +
         (if (event == 'cron') || (event == 'push') then [pipeline.publish('pkg latest', 'latest')] else []) +
         // (if (platform == 'centos:8' && event == 'cron') then [pipeline.dockerfile] + [pipeline.docker] + [pipeline.ecr] else []) +
         [pipeline.smoke] +
         [pipeline.smokelog] +
         (if (std.member(platforms_mtr, platform)) then [pipeline.mtr] + [pipeline.mtrlog] + [pipeline.publish('mtr')] else []) +
         (if (event == 'cron' && std.member(platforms_mtr, platform)) || (event == 'push') then [pipeline.publish('mtr latest', 'latest')] else []) +
         [pipeline.regression] +
         [pipeline.regressionlog] +
         [pipeline.publish('regression')] +
         (if (event == 'cron') || (event == 'push') then [pipeline.publish('regression latest', 'latest')] else []),
  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: '/var/run/docker.sock' } }],
  trigger: {
    event: [event],
    branch: [branch],
  } + (if event == 'cron' then {
         cron: ['nightly-' + std.strReplace(branch, '.', '-')],
       } else {}),
};

local FinalPipeline(branch, event, arch='amd64') = {
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
  } + (if event == 'cron' then { cron: ['nightly-' + std.strReplace(branch, '.', '-')] } else {}),
  depends_on: std.map(function(p) std.join(' ', [branch, p, event, arch]), platforms[branch]) +
              (if branch == 'develop' then std.map(function(p) std.join(' ', [branch, p, event, 'arm64']), platforms_arm[branch]) else []),
};

[
  Pipeline(b, p, e)
  for b in std.objectFields(platforms)
  for p in platforms[b]
  for e in events
] +
[
  Pipeline(b, p, e, 'arm64')
  for b in std.objectFields(platforms_arm)
  for p in platforms_arm[b]
  for e in events
] +
[
  FinalPipeline(b, e)
  for b in std.objectFields(platforms)
  for e in events
]

+

[
  Pipeline(any_branch, p, 'custom')
  for p in platforms_custom
] +
[
  Pipeline(any_branch, p, 'custom', 'arm64')
  for p in platforms_arm_custom
]
