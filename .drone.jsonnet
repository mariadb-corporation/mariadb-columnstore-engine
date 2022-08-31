local events = ['pull_request', 'cron'];

local platforms = {
  develop: ['centos:7', 'rockylinux:8', 'rockylinux:9', 'debian:11', 'ubuntu:20.04', 'ubuntu:22.04'],
  'develop-6': ['centos:7', 'rockylinux:8', 'debian:11', 'ubuntu:20.04', 'ubuntu:22.04'],
};

local servers = {
  develop: ['10.9', '10.6-enterprise'],
  'develop-6': ['10.6-enterprise'],
};

local platforms_arm = {
  develop: ['rockylinux:8', 'rockylinux:9', 'debian:11', 'ubuntu:20.04', 'ubuntu:22.04'],
  'develop-6': ['rockylinux:8'],
};

local any_branch = '**';
local platforms_custom = ['centos:7', 'rockylinux:8', 'debian:11', 'ubuntu:20.04', 'ubuntu:22.04'];
local platforms_arm_custom = ['rockylinux:8'];

local platforms_mtr = ['centos:7', 'rockylinux:8', 'ubuntu:20.04'];

local builddir = 'verylongdirnameforverystrangecpackbehavior';

local cmakeflags = '-DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_CONFIG=mysql_release ' +
                   '-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache ' +
                   '-DPLUGIN_COLUMNSTORE=YES -DWITH_UNITTESTS=YES ' +
                   '-DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO ' +
                   '-DPLUGIN_CONNECT=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO ' +
                   '-DPLUGIN_GSSAPI=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO ' +
                   '-DWITH_EMBEDDED_SERVER=NO -DWITH_WSREP=NO';

local clang_version = '13';
local gcc_version = '10';

local gcc_update_alternatives = 'update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-' + gcc_version + ' 100 --slave /usr/bin/g++ g++ /usr/bin/g++-' + gcc_version + '--slave /usr/bin/gcov gcov /usr/bin/gcov-' + gcc_version + ' ';
local clang_update_alternatives = 'update-alternatives --install /usr/bin/clang clang /usr/bin/clang-' + clang_version + ' 100 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-' + clang_version + ' && update-alternatives --install /usr/bin/cc cc /usr/bin/clang 100 && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++ 100 ';


local rpm_build_deps = 'install -y lz4 systemd-devel git make libaio-devel openssl-devel boost-devel bison ' +
                       'snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool ' +
                       'policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel ' +
                       'expect createrepo ';

local centos7_build_deps = 'yum install -y epel-release centos-release-scl ' +
                           '&& yum install -y pcre2-devel devtoolset-' + gcc_version + ' devtoolset-' + gcc_version + '-gcc cmake3 lz4-devel ' +
                           '&& ln -s /usr/bin/cmake3 /usr/bin/cmake && . /opt/rh/devtoolset-' + gcc_version + '/enable ';

local rockylinux8_build_deps = "dnf install -y 'dnf-command(config-manager)' " +
                               '&& dnf config-manager --set-enabled powertools ' +
                               '&& dnf install -y gcc-toolset-' + gcc_version + ' libarchive cmake lz4-devel ' +
                               '&& . /opt/rh/gcc-toolset-' + gcc_version + '/enable ';


local rockylinux9_build_deps = "dnf install -y 'dnf-command(config-manager)' " +
                               '&& dnf config-manager --set-enabled crb ' +
                               '&& dnf install -y pcre2-devel lz4-devel gcc gcc-c++';

local debian11_deps = 'apt update && apt install -y gnupg wget && echo "deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-' + clang_version + ' main" >>  /etc/apt/sources.list  && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt install -y clang-' + clang_version + ' && ' + clang_update_alternatives;
local ubuntu20_04_deps = 'apt update && apt install -y gnupg wget && echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-' + clang_version + ' main" >>  /etc/apt/sources.list  && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt install -y clang-' + clang_version + ' &&' + clang_update_alternatives;

local deb_build_deps = 'apt update --yes && apt install --yes --no-install-recommends build-essential devscripts git ccache equivs eatmydata libssl-dev && mk-build-deps debian/control -t "apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends" -r -i ';
local turnon_clang = 'export CC=/usr/bin/clang; export CXX=/usr/bin/clang++ ';
local bootstrap_deps = "apt-get -y update && apt-get -y install build-essential automake libboost-all-dev bison cmake libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest libsnappy-dev libjemalloc-dev liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev libdistro-info-perl ";


local platformMap(platform, arch) =
  local clang_force = if (arch == 'arm64') then ' && export CXX=/usr/bin/clang++ && export CC=/usr/bin/clang ';
  local platform_map = {
    'centos:7': centos7_build_deps + ' && yum ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=centos7 && sleep $${BUILD_DELAY_SECONDS:-1s} && make -j$(nproc) package',
    'rockylinux:8': rockylinux8_build_deps + ' && dnf ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=rockylinux8 && sleep $${BUILD_DELAY_SECONDS:-1s} && make -j$(nproc) package',
    'rockylinux:9': rockylinux9_build_deps + ' && dnf ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=rockylinux9 && sleep $${BUILD_DELAY_SECONDS:-1s} && make -j$(nproc) package',
    'debian:11':    bootstrap_deps + ' && ' + deb_build_deps + ' && ' + debian11_deps + ' && ' + turnon_clang + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=bullseye' debian/autobake-deb.sh",
    'ubuntu:20.04': bootstrap_deps + ' && ' + deb_build_deps + ' && ' + ubuntu20_04_deps + ' && ' + turnon_clang + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=focal' debian/autobake-deb.sh",
    'ubuntu:22.04': bootstrap_deps + ' && ' + deb_build_deps + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=jammy' debian/autobake-deb.sh",
  };
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-');
  platform_map[platform] + ' | tee ' + result + '/build.log';


local testRun(platform) =
  local platform_map = {
    'centos:7': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'rockylinux:8': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'rockylinux:9': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'debian:11': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:20.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:22.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
  };
  platform_map[platform];


local testPreparation(platform) =
  local platform_map = {
    'centos:7': 'yum -y install epel-release && yum install -y git cppunit-devel cmake3 boost-devel snappy-devel',
    'rockylinux:8': rockylinux8_build_deps + ' && dnf install -y git lz4 cppunit-devel cmake3 boost-devel snappy-devel',
    'rockylinux:9': rockylinux9_build_deps + ' && dnf install -y git lz4 cppunit-devel cmake3 boost-devel snappy-devel',
    'debian:11': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake',
    'ubuntu:20.04': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake',
    'ubuntu:22.04': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake',
  };
  platform_map[platform];


local Pipeline(branch, platform, event, arch='amd64', server='10.9') = {
  local pkg_format = if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'rpm' else 'deb',
  local init = if (pkg_format == 'rpm') then '/usr/lib/systemd/systemd' else 'systemd',
  local mtr_path = if (pkg_format == 'rpm') then '/usr/share/mysql-test' else '/usr/share/mysql/mysql-test',
  local socket_path = if (pkg_format == 'rpm') then '/var/lib/mysql/mysql.sock' else '/run/mysqld/mysqld.sock',
  local config_path_prefix = if (pkg_format == 'rpm') then '/etc/my.cnf.d/' else '/etc/mysql/mariadb.conf.d/50-',
  local img = if (platform == 'centos:7' || std.split(platform, ':')[0] == 'rockylinux') then platform else 'romcheck/' + std.strReplace(platform, '/', '-'),
  local regression_ref = if (std.split(branch, '-')[0] == 'develop') then branch else 'develop-6',
  // local regression_tests = if (std.startsWith(platform, 'debian') || std.startsWith(platform, 'ubuntu:20')) then 'test000.sh' else 'test000.sh,test001.sh',
  local regression_tests = 'test000.sh,test001.sh',
  local branchp = if (branch == '**') then '' else branch,
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),

  local container_tags = if (event == 'cron') then [branch, branch + '-' + std.strReplace(event, '_', '-') + '-${DRONE_BUILD_NUMBER}'] else [branch + '-' + std.strReplace(event, '_', '-') + '-${DRONE_BUILD_NUMBER}'],
  local container_version = branch + '/' + event + '/${DRONE_BUILD_NUMBER}/' + server + '/' + arch,

  local server_remote = if (std.endsWith(server, 'enterprise')) then 'https://github.com/mariadb-corporation/MariaDBEnterprise' else 'https://github.com/MariaDB/server',

  local sccache_arch = if (arch == 'amd64') then 'x86_64' else 'aarch64',
  local get_sccache = 'curl -L -o sccache.tar.gz https://github.com/mozilla/sccache/releases/download/v0.3.0/sccache-v0.3.0-' + sccache_arch + '-unknown-linux-musl.tar.gz ' +
                      '&& tar xzf sccache.tar.gz ' +
                      '&& install sccache*/sccache /usr/local/bin/',

  local pipeline = self,

  publish(step_prefix='pkg', eventp=event + '/${DRONE_BUILD_NUMBER}'):: {
    name: 'publish ' + step_prefix,
    depends_on: [std.strReplace(step_prefix, ' latest', '')],
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
      source: result,
      target: branchp + '/' + eventp + '/' + server + '/' + arch + '/' + result,
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
    depends_on: ['pkg'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name smoke$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp ' + result + ' smoke$${DRONE_BUILD_NUMBER}:/',
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release which rsyslog hostname procps-ng && yum install -y /' + result + '/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t smoke$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y rsyslog hostname && apt install -y -f /' + result + '/*.' + pkg_format + '"',
      'sleep $${SMOKE_DELAY_SECONDS:-1s}',
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
    depends_on: ['smoke'],
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env MYSQL_TEST_DIR=' + mtr_path + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name mtr$${DRONE_BUILD_NUMBER} --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
      'docker cp ' + result + ' mtr$${DRONE_BUILD_NUMBER}:/',
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "yum install -y epel-release diffutils which rsyslog hostname patch perl-Data-Dumper perl-Getopt-Long perl-Memoize perl-Time-HiRes cracklib-dicts procps-ng && yum install -y /' + result + '/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y rsyslog hostname patch && apt install -y -f /' + result + '/*.' + pkg_format + '"' else '',
      'docker cp mysql-test/columnstore mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/suite/',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} chown -R mysql:mysql ' + mtr_path,
      // disable systemd 'ProtectSystem' (we need to write to /usr/share/)
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} sed -i "/ProtectSystem/d" /usr/lib/systemd/system/mariadb.service',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl daemon-reload',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} mariadb -e "create database if not exists test;"',
      // set memory allowonce for hashjoin operation
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "/usr/bin/mcsSetConfig HashJoin TotalUmMemory 60%"',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} systemctl restart mariadb-columnstore',
      // delay mtr for manual debugging on live instance
      'sleep $${MTR_DELAY_SECONDS:-1s}',
      'docker exec -t mtr$${DRONE_BUILD_NUMBER} bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path + ' --force --max-test-fail=0 --suite=columnstore/basic,columnstore/bugfixes"',
    ],
  },
  mtrlog:: {
    name: 'mtrlog',
    depends_on: ['mtr'],
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
      'docker cp mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/var/log /drone/src/' + result + '/mtr-logs || echo "missing ' + mtr_path + '/var/log"',
      'docker stop mtr$${DRONE_BUILD_NUMBER} && docker rm mtr$${DRONE_BUILD_NUMBER} || echo "cleanup mtr failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  regression:: {
    name: 'regression',
    depends_on: ['smoke'],
    image: 'docker:git',
    [if (event == 'cron' || arch == 'arm64') then 'failure']: 'ignore',

    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    environment: {
      REGRESSION_TESTS: if (event == 'cron') then '' else '${REGRESSION_TESTS:-' + regression_tests + '}',
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
      'docker cp ' + result + ' regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp mariadb-columnstore-regression-test regression$${DRONE_BUILD_NUMBER}:/',
      'docker cp /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager regression$${DRONE_BUILD_NUMBER}:/',
      // check storage-manager unit test binary file
      'docker exec -t regression$${DRONE_BUILD_NUMBER} ls -l /storage-manager',
      if (std.split(platform, ':')[0] == 'centos' || std.split(platform, ':')[0] == 'rockylinux') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "yum install -y gcc-c++ epel-release diffutils tar lz4 wget which rsyslog hostname procps-ng && yum install -y /' + result + '/*.' + pkg_format + '"' else '',
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d',
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "apt update --yes && apt install -y tar liblz4-tool wget rsyslog hostname && apt install -y -f g++ /' + result + '/*.' + pkg_format + '"' else '',
      // copy test data for regression test suite
      'docker exec -t regression$${DRONE_BUILD_NUMBER} bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"',
      // set mariadb lower_case_table_names=1 config option
      'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/^.mariadb.$/a lower_case_table_names=1" ' + config_path_prefix + 'server.cnf',
      // set default client character set to utf-8
      'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/^.client.$/a default-character-set=utf8" ' + config_path_prefix + 'client.cnf',
      // Regression tests hacks to pass on debs
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/character-set-server/d" ' + config_path_prefix + 'server.cnf',
      if (pkg_format == 'deb') then 'docker exec -t regression$${DRONE_BUILD_NUMBER} sed -i "/collation-server/d" ' + config_path_prefix + 'server.cnf',
      // start mariadb and mariadb-columnstore services
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb',
      'docker exec -t regression$${DRONE_BUILD_NUMBER} systemctl start mariadb-columnstore',
      // delay regression for manual debugging on live instance
      'sleep $${REGRESSION_DELAY_SECONDS:-1s}',
      // run regression test000 and test001 on pull request and manual (may be overwritten by env variable parameter) build events. on other events run all tests
      'docker exec -t regression$${DRONE_BUILD_NUMBER} /usr/bin/g++ /mariadb-columnstore-regression-test/mysql/queries/queryTester.cpp -O2 -o  /mariadb-columnstore-regression-test/mysql/queries/queryTester',
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} timeout -k 1m -s SIGKILL --preserve-status $${REGRESSION_TIMEOUT:-10h} ./go.sh --sm_unit_test_dir=/storage-manager --tests=$${REGRESSION_TESTS}',
    ],
  },
  smokelog:: {
    name: 'smokelog',
    depends_on: ['smoke'],
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
    depends_on: ['regression'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start columnstore regression short report ----------"',
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} cat go.log || echo "missing go.log"',
      'echo "---------- end columnstore regression short report ----------"',
      'echo',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs.tgz /drone/src/' + result + '/ || echo "missing testErrorLogs.tgz"',
      'docker exec -t --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest regression$${DRONE_BUILD_NUMBER} bash -c "tar czf testErrorLogs2.tgz *.log /var/log/mariadb/columnstore" || echo "failed to grab regression results"',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs2.tgz /drone/src/' + result + '/ || echo "missing testErrorLogs.tgz"',
      'ls -l /drone/src/' + result,
      'docker stop regression$${DRONE_BUILD_NUMBER} && docker rm regression$${DRONE_BUILD_NUMBER} || echo "cleanup regression failure"',
    ],
    when: {
      status: ['success', 'failure'],
    },
  },
  dockerfile:: {
    name: 'dockerfile',
    depends_on: ['publish pkg'],
    failure: 'ignore',
    image: 'alpine/git',
    commands: [
      'git clone --depth 1  https://github.com/mariadb-corporation/mariadb-skysql-columnstore-docker docker',
      "sed -i 's|dlm.mariadb.com/enterprise-release-helpers/mariadb_es_repo_setup|cspkg.s3.amazonaws.com/cs_repo|' docker/Dockerfile",
    ],
  },
  dockerhub:: {
    name: 'dockerhub',
    depends_on: ['dockerfile'],
    failure: 'ignore',
    image: 'plugins/docker',
    environment: {
      VERSION: container_version,
    },
    settings: {
      repo: 'mariadb/enterprise-columnstore-dev',
      context: 'docker',
      dockerfile: 'docker/Dockerfile',
      build_args_from_env: ['VERSION'],
      tags: container_tags,
      username: {
        from_secret: 'dockerhub_user',
      },
      password: {
        from_secret: 'dockerhub_password',
      },
    },
  },


  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event, arch, server]),
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
               SERVER_REF: '${SERVER_REF:-' + server + '}',
               SERVER_REMOTE: '${SERVER_REMOTE:-' + server_remote + '}',
               SERVER_SHA: '${SERVER_SHA:-' + server + '}',
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
             depends_on: ['clone-mdb'],
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: 'noninteractive',
               DEB_BUILD_OPTIONS: 'parallel=4',
               DH_BUILD_DDEBS: '1',
               BUILDPACKAGE_FLAGS: '-b',  // Save time and produce only binary packages, not source
               AWS_ACCESS_KEY_ID: {
                 from_secret: 'aws_access_key_id',
               },
               AWS_SECRET_ACCESS_KEY: {
                 from_secret: 'aws_secret_access_key',
               },
               SCCACHE_BUCKET: 'cs-sccache',
               SCCACHE_S3_USE_SSL: 'true',
               SCCACHE_S3_KEY_PREFIX: result + branch + server + arch + '${DRONE_PULL_REQUEST}',
               //SCCACHE_ERROR_LOG: '/tmp/sccache_log.txt',
               //SCCACHE_LOG: 'debug',
             },
             commands: [
               'cd /mdb/' + builddir,
               'mkdir ' + result,
               "sed -i 's|.*-d storage/columnstore.*|elif [[ -d storage/columnstore/columnstore/debian ]]|' debian/autobake-deb.sh",
               if (std.startsWith(server, '10.6')) then "sed -i 's/mariadb-server/mariadb-server-10.6/' storage/columnstore/columnstore/debian/control",
               // Remove Debian build flags that could prevent ColumnStore from building
               "sed -i '/-DPLUGIN_COLUMNSTORE=NO/d' debian/rules",
               // Disable dh_missing strict check for missing files
               'sed -i s/--fail-missing/--list-missing/ debian/rules',
               // Tweak debian packaging stuff
               'for i in mariadb-backup mariadb-plugin libmariadbd; do sed -i "/Package: $i.*/,/^$/d" debian/control; done',
               "sed -i 's/Depends: galera.*/Depends:/' debian/control",
               'for i in wsrep ha_sphinx embedded; do sed -i /$i/d debian/*.install; done',
               // Install build dependencies for deb
               if (pkg_format == 'deb') then "apt-cache madison liburing-dev | grep liburing-dev || sed 's/liburing-dev/libaio-dev/g' -i debian/control && sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules && sed '/-DWITH_URING=yes/d' -i debian/rules && apt-cache madison libpmem-dev | grep 'libpmem-dev' || sed '/libpmem-dev/d' -i debian/control && sed '/-DWITH_PMEM/d' -i debian/rules && sed '/libfmt-dev/d' -i debian/control",
               // Change plugin_maturity level
               // "sed -i 's/BETA/GAMMA/' storage/columnstore/CMakeLists.txt",
               if (pkg_format == 'deb') then 'apt update -y && apt install -y curl' else 'yum install -y curl',
               get_sccache,
               testPreparation(platform),
               // disable LTO for 22.04 for now
               if (platform == 'ubuntu:22.04') then 'apt install -y lto-disabled-list && for i in mariadb-plugin-columnstore mariadb-server mariadb-server-core mariadb mariadb-10.6; do echo "$i any" >> /usr/share/lto-disabled-list/lto-disabled-list; done && grep mariadb /usr/share/lto-disabled-list/lto-disabled-list',
               platformMap(platform, arch),
               'sccache --show-stats',
               if (pkg_format == 'rpm') then 'mv *.' + pkg_format + ' ' + result + '/' else 'mv ../*.' + pkg_format + ' ' + result + '/',
               if (pkg_format == 'rpm') then 'createrepo ' + result else 'dpkg-scanpackages ' + result + ' | gzip > ' + result + '/Packages.gz',
             ],
           },
           {
             name: 'unittests',
             depends_on: ['build'],
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
             name: 'pkg',
             depends_on: ['unittests'],
             image: 'docker:git',
             volumes: [pipeline._volumes.mdb],
             commands: [
               'cd /mdb/' + builddir,
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'mv buildinfo.txt ' + result + '/',
               'mv ' + result + ' /drone/src/',
               'ls -l /drone/src/' + result,
               'echo "check columnstore package:"',
               'ls -l /drone/src/' + result + ' | grep columnstore',
             ],
           },
         ] +
         [pipeline.publish()] +
         (if (event == 'cron') then [pipeline.publish('pkg latest', 'latest')] else []) +
         (if (event != 'custom') && (platform == 'rockylinux:8') && (arch == 'amd64') && (server == '10.6-enterprise') then [pipeline.dockerfile] + [pipeline.dockerhub] else []) +
         [pipeline.smoke] +
         [pipeline.smokelog] +
         (if (std.member(platforms_mtr, platform)) then [pipeline.mtr] + [pipeline.mtrlog] + [pipeline.publish('mtr')] else []) +
         (if (event == 'cron' && std.member(platforms_mtr, platform)) then [pipeline.publish('mtr latest', 'latest')] else []) +
         [pipeline.regression] +
         [pipeline.regressionlog] +
         [pipeline.publish('regressionlog')] +
         (if (event == 'cron') then [pipeline.publish('regression latest', 'latest')] else []),
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
  } + (if event == 'cron' then { cron: ['nightly-' + std.strReplace(branch, '.', '-')] } else {}),
  depends_on: std.map(function(p) std.join(' ', ['develop', p, event, 'amd64', '10.9']), platforms.develop) +
              std.map(function(p) std.join(' ', ['develop', p, event, 'amd64', '10.6-enterprise']), platforms.develop) +

              std.map(function(p) std.join(' ', ['develop', p, event, 'arm64', '10.9']), platforms_arm.develop) +
              std.map(function(p) std.join(' ', ['develop', p, event, 'arm64', '10.6-enterprise']), platforms_arm.develop) +

              std.map(function(p) std.join(' ', ['develop-6', p, event, 'amd64', '10.6-enterprise']), platforms['develop-6']) +

              std.map(function(p) std.join(' ', ['develop-6', p, event, 'arm64', '10.6-enterprise']), platforms_arm['develop-6']) +

              ['develop-6 ubuntu:22.04 ' + event + ' amd64 10.9'],
};


[
  Pipeline(b, p, e, 'amd64', s)
  for b in std.objectFields(platforms)
  for p in platforms[b]
  for s in servers[b]
  for e in events
] +
[
  Pipeline('develop-6', 'ubuntu:22.04', e, 'amd64', '10.9')
  for e in events
] +
[
  Pipeline(b, p, e, 'arm64', s)
  for b in std.objectFields(platforms_arm)
  for p in platforms_arm[b]
  for s in servers[b]
  for e in events
] +

[
  FinalPipeline(b, 'cron')
  for b in std.objectFields(platforms)
] +

[
  Pipeline(any_branch, p, 'custom')
  for p in platforms_custom
] +
[
  Pipeline(any_branch, p, 'custom', 'arm64')
  for p in platforms_arm_custom
]
