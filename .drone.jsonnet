local events = ['pull_request', 'cron'];

local servers = {
  develop: ['10.6-enterprise'],
  'stable-23.10': ['10.6-enterprise'],
};

local platforms = {
  develop: ['rockylinux:8', 'rockylinux:9', 'debian:11', 'debian:12', 'ubuntu:20.04', 'ubuntu:22.04', 'ubuntu:24.04'],
  'stable-23.10': ['rockylinux:8', 'rockylinux:9', 'debian:11', 'debian:12', 'ubuntu:20.04', 'ubuntu:22.04','ubuntu:24.04'],
};

local platforms_arm = {
  develop: ['rockylinux:8', 'rockylinux:9', 'debian:11', 'debian:12', 'ubuntu:20.04', 'ubuntu:22.04', 'ubuntu:24.04'],
  'stable-23.10': ['rockylinux:8', 'rockylinux:9', 'debian:11', 'debian:12', 'ubuntu:20.04', 'ubuntu:22.04', 'ubuntu:24.04'],
};

local any_branch = '**';
local platforms_custom = platforms.develop;
local platforms_arm_custom = platforms_arm.develop;

local platforms_mtr = platforms.develop;

local builddir = 'verylongdirnameforverystrangecpackbehavior';

local cmakeflags = '-DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_CONFIG=mysql_release ' +
                   '-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache ' +
                   '-DPLUGIN_COLUMNSTORE=YES -DWITH_UNITTESTS=YES ' +
                   '-DPLUGIN_MROONGA=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_TOKUDB=NO ' +
                   '-DPLUGIN_CONNECT=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO ' +
                   '-DPLUGIN_GSSAPI=NO -DPLUGIN_SPIDER=NO -DPLUGIN_OQGRAPH=NO -DPLUGIN_SPHINX=NO ' +
                   '-DWITH_EMBEDDED_SERVER=NO -DWITH_WSREP=NO -DWITH_COREDUMPS=ON';

local clang_version = '16';
local gcc_version = '11';

local clang_update_alternatives = 'update-alternatives --install /usr/bin/clang clang /usr/bin/clang-' + clang_version + ' 100 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-' + clang_version + ' && update-alternatives --install /usr/bin/cc cc /usr/bin/clang 100 && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++ 100 ';


local rpm_build_deps = 'install -y lz4 systemd-devel git make libaio-devel openssl-devel boost-devel bison ' +
                       'snappy-devel flex libcurl-devel libxml2-devel ncurses-devel automake libtool ' +
                       'policycoreutils-devel rpm-build lsof iproute pam-devel perl-DBI cracklib-devel ' +
                       'expect createrepo python3 ';

local rockylinux8_build_deps = "dnf install -y 'dnf-command(config-manager)' " +
                               '&& dnf config-manager --set-enabled powertools ' +
                               '&& dnf install -y gcc-toolset-' + gcc_version + ' libarchive cmake lz4-devel ' +
                               '&& . /opt/rh/gcc-toolset-' + gcc_version + '/enable ';


local rockylinux9_build_deps = "dnf install -y 'dnf-command(config-manager)' " +
                               '&& dnf config-manager --set-enabled crb ' +
                               '&& dnf install -y pcre2-devel lz4-devel gcc gcc-c++';

local debian11_deps = 'apt update && apt install -y gnupg wget && echo "deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-' + clang_version + ' main" >>  /etc/apt/sources.list  && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt install -y clang-' + clang_version + ' && ' + clang_update_alternatives;
local ubuntu20_04_deps = 'apt update && apt install -y gnupg wget && echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-' + clang_version + ' main" >>  /etc/apt/sources.list  && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt install -y clang-' + clang_version + ' && ' + clang_update_alternatives;

local deb_build_deps = 'apt update --yes && apt install --yes --no-install-recommends build-essential devscripts git ccache equivs eatmydata libssl-dev && mk-build-deps debian/control -t "apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends" -r -i ';
local turnon_clang = 'export CC=/usr/bin/clang; export CXX=/usr/bin/clang++ ';
local bootstrap_deps = 'apt-get -y update && apt-get -y install build-essential automake libboost-all-dev bison cmake libncurses5-dev libaio-dev libsystemd-dev libpcre2-dev libperl-dev libssl-dev libxml2-dev libkrb5-dev flex libpam-dev git libsnappy-dev libcurl4-openssl-dev libgtest-dev libcppunit-dev googletest libsnappy-dev libjemalloc-dev liblz-dev liblzo2-dev liblzma-dev liblz4-dev libbz2-dev libbenchmark-dev libdistro-info-perl ';

local mtr_suite_list = 'basic,bugfixes';
local mtr_full_set = 'basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly';

local upgrade_test_lists = {
  "rockylinux8":  {
                    "arm64": ["10.6.4-1", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.15-10"],
                    "amd64": ["10.6.4-1", "10.6.5-2", "10.6.7-3", "10.6.8-4", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"]
                  },
  "rockylinux9":  {
                    "arm64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
                    "amd64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"]
                  },
  "debian11": {
                "arm64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
                "amd64": ["10.6.5-2", "10.6.7-3", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"]
              },
  "debian12": {
    "arm64": [],
    "amd64": []
  },
  "ubuntu20.04": {
                   "arm64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
                   "amd64": ["10.6.4-1", "10.6.5-2", "10.6.7-3", "10.6.8-4", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"]
                 },
  "ubuntu22.04": {
                   "arm64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
                   "amd64": ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"]
                 },
  "ubuntu24.04":
  {
    "arm64": [],
    "amd64": []
  },
};

local platformMap(platform, arch) =
  local platform_map = {
    'rockylinux:8': rockylinux8_build_deps + ' && dnf ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=rockylinux8 && sleep $${BUILD_DELAY_SECONDS:-1s} && make -j$(nproc) package',
    'rockylinux:9': rockylinux9_build_deps + ' && dnf ' + rpm_build_deps + ' && cmake ' + cmakeflags + ' -DRPM=rockylinux9 && sleep $${BUILD_DELAY_SECONDS:-1s} && make -j$(nproc) package',
    'debian:11': bootstrap_deps + ' && ' + deb_build_deps + ' && ' + debian11_deps + ' && ' + turnon_clang + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=bullseye' debian/autobake-deb.sh",
    'debian:12': bootstrap_deps + ' && ' + deb_build_deps + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=bookworm' debian/autobake-deb.sh",
    'ubuntu:20.04': bootstrap_deps + ' && ' + deb_build_deps + ' && ' + ubuntu20_04_deps + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=focal' debian/autobake-deb.sh",
    'ubuntu:22.04': bootstrap_deps + ' && ' + deb_build_deps + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=jammy' debian/autobake-deb.sh",
    'ubuntu:24.04': bootstrap_deps + ' && ' + deb_build_deps + " && sleep $${BUILD_DELAY_SECONDS:-1s} && CMAKEFLAGS='" + cmakeflags + " -DDEB=jammy' debian/autobake-deb.sh",
  };
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-');
  'export CLICOLOR_FORCE=1; ' + platform_map[platform] + ' | storage/columnstore/columnstore/build/ansi2txt.sh ' + result + '/build.log';


local testRun(platform) =
  local platform_map = {
    'rockylinux:8': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'rockylinux:9': 'ctest3 -R columnstore: -j $(nproc) --output-on-failure',
    'debian:11': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'debian:12': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:20.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:22.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',
    'ubuntu:24.04': 'cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure',

  };
  platform_map[platform];


local testPreparation(platform) =
  local platform_map = {
    'rockylinux:8': rockylinux8_build_deps + ' && dnf install -y git lz4 cppunit-devel cmake3 boost-devel snappy-devel pcre2-devel',
    'rockylinux:9': rockylinux9_build_deps + ' && dnf install -y git lz4 cppunit-devel cmake3 boost-devel snappy-devel pcre2-devel',
    'debian:11': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev',
    'debian:12': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev',
    'ubuntu:20.04': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev',
    'ubuntu:22.04': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev',
    'ubuntu:24.04': 'apt update && apt install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev',

  };
  platform_map[platform];

local Pipeline(branch, platform, event, arch='amd64', server='10.6-enterprise') = {
  local pkg_format = if (std.split(platform, ':')[0] == 'rockylinux') then 'rpm' else 'deb',
  local init = if (pkg_format == 'rpm') then '/usr/lib/systemd/systemd' else 'systemd',
  local mtr_path = if (pkg_format == 'rpm') then '/usr/share/mysql-test' else '/usr/share/mysql/mysql-test',
  local cmapi_path = '/usr/share/columnstore/cmapi',
  local etc_path = '/etc/columnstore',
  local socket_path = if (pkg_format == 'rpm') then '/var/lib/mysql/mysql.sock' else '/run/mysqld/mysqld.sock',
  local config_path_prefix = if (pkg_format == 'rpm') then '/etc/my.cnf.d/' else '/etc/mysql/mariadb.conf.d/50-',
  local img = if (platform == 'rockylinux:8') then platform else 'detravi/' + std.strReplace(platform, '/', '-'),
  local branch_ref = if (branch == any_branch) then 'develop' else branch,
  // local regression_tests = if (std.startsWith(platform, 'debian') || std.startsWith(platform, 'ubuntu:20')) then 'test000.sh' else 'test000.sh,test001.sh',

  local branchp = if (branch == '**') then '' else branch + '/',
  local brancht = if (branch == '**') then '' else branch + '-',
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),

  local packages_url = 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/${DRONE_BUILD_NUMBER}/' + server,
  local publish_pkg_url = "https://cspkg.s3.amazonaws.com/index.html?prefix=" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/" + result + "/",
  local repo_pkg_url_no_res = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/",

  local container_tags = if (event == 'cron') then [brancht + std.strReplace(event, '_', '-') + '${DRONE_BUILD_NUMBER}', brancht] else [brancht + std.strReplace(event, '_', '-') + '${DRONE_BUILD_NUMBER}'],
  local container_version = branchp + event + '/${DRONE_BUILD_NUMBER}/' + server + '/' + arch,

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
      // branchp has slash if not empty
      target: branchp + eventp + '/' + server + '/' + arch + '/' + result,
      delete: 'true',
    },
  },

  local regression_tests = if (event == 'cron') then [
    'test000.sh',
    'test001.sh',
    'test005.sh',
    'test006.sh',
    'test007.sh',
    'test008.sh',
    'test009.sh',
    'test010.sh',
    'test011.sh',
    'test012.sh',
    'test013.sh',
    'test014.sh',
    'test023.sh',
    'test201.sh',
    'test202.sh',
    'test203.sh',
    'test204.sh',
    'test210.sh',
    'test211.sh',
    'test212.sh',
    'test222.sh',
    'test297.sh',
    'test299.sh',
    'test300.sh',
    'test400.sh',
    'test500.sh',
  ] else [
    'test000.sh',
    'test001.sh',
  ],

  local mdb_server_versions = upgrade_test_lists[result][arch],

  local indexes(arr) = std.range(0, std.length(arr) - 1),

  local execInnerDocker(command, dockerImage, flags = '') =
    'docker exec ' + flags + ' -t ' + dockerImage + ' ' + command,

  local execInnerDockerNoTTY(command, dockerImage, flags = '') =
    'docker exec ' + flags + ' ' + dockerImage + ' ' + command,

  local installRpmDeb(pkg_format, rpmpackages, debpackages) =
    if (pkg_format == 'rpm')
      then ' bash -c "yum install -y ' + rpmpackages + '"'
      else ' bash -c "apt update --yes && apt install -y ' + debpackages + '"',


  local dockerImage(stepname) = stepname + "$${DRONE_BUILD_NUMBER}",
  local installEngine(dockerImage, pkg_format) =
    if (pkg_format == 'deb') then execInnerDocker('bash -c "apt install -y mariadb-plugin-columnstore mariadb-test"', dockerImage)
                             else execInnerDocker('bash -c "yum install -y MariaDB-columnstore-engine MariaDB-test"', dockerImage),

  local installCmapi(dockerImage, pkg_format) =
    if (pkg_format == 'deb') then execInnerDocker('bash -c "apt install -y mariadb-columnstore-cmapi"', dockerImage)
                             else execInnerDocker('bash -c "yum install -y MariaDB-columnstore-cmapi"', dockerImage),

  local prepareTestStage(dockerImage, pkg_format, result, do_setup) = [
    'apk add bash && bash core_dumps/docker-awaiter.sh ' + dockerImage,
    if (pkg_format == 'deb')
      then execInnerDocker('sed -i "s/exit 101/exit 0/g" /usr/sbin/policy-rc.d', dockerImage),

    'echo "Docker CGroups opts here"',
    'ls -al /sys/fs/cgroup/cgroup.controllers || true ',
    'ls -al /sys/fs/cgroup/ || true ',
    'ls -al /sys/fs/cgroup/memory || true',
    "docker ps --filter=name=" + dockerImage,

    execInnerDocker('echo "Inner Docker CGroups opts here"', dockerImage),
    execInnerDocker('ls -al /sys/fs/cgroup/cgroup.controllers || true', dockerImage),
    execInnerDocker('ls -al /sys/fs/cgroup/ || true', dockerImage),
    execInnerDocker('ls -al /sys/fs/cgroup/memory || true', dockerImage),


    execInnerDocker('mkdir core', dockerImage),
    execInnerDocker('chmod 777 core', dockerImage),
    'docker cp core_dumps/. ' + dockerImage  +  ':/',
    'docker cp build/utils.sh ' + dockerImage  +  ':/',
    'docker cp setup-repo.sh ' + dockerImage  +  ':/',
    if (do_setup) then execInnerDocker('/setup-repo.sh', dockerImage),
    execInnerDocker(installRpmDeb(pkg_format,
      "cracklib-dicts diffutils elfutils epel-release findutils iproute gawk gcc-c++ gdb hostname lz4 patch perl procps-ng rsyslog sudo tar wget which",
      "elfutils findutils iproute2 g++ gawk gdb hostname liblz4-tool patch procps rsyslog sudo tar wget"), dockerImage),
    execInnerDocker('sysctl -w kernel.core_pattern="/core/%E_' + result + '_core_dump.%p"', dockerImage),
  ],

  local reportTestStage(dockerImage, result, stage) = [
    execInnerDocker('bash -c "/logs.sh ' + stage + '"', dockerImage),
    execInnerDocker('bash -c "/core_dump_check.sh core /core/ ' + stage + '"' , dockerImage),
    'docker cp ' + dockerImage + ':/core/ /drone/src/' + result + '/',
    'docker cp ' + dockerImage + ':/unit_logs/ /drone/src/' + result + '/',
    'ls -l /drone/src/' + result,
    execInnerDocker('bash -c "/core_dump_drop.sh core"', dockerImage),
    'docker stop ' + dockerImage + ' && docker rm ' + dockerImage + ' || echo "cleanup ' + stage + ' failure"',
  ],

  regression(name, depends_on):: {
    name: name,
    depends_on: depends_on,
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    when: {
      status: ['success', 'failure'],
    },
    [if (name != 'test000.sh' && name != 'test001.sh') then 'failure']: 'ignore',
    environment: {
      REGRESSION_TIMEOUT: {
        from_secret: 'regression_timeout',
      },
    },
    commands: [
      execInnerDocker("mkdir -p reg-logs", dockerImage("regression"), "--workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"),
      execInnerDocker("bash -c 'sleep 4800 && bash /save_stack.sh /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/reg-logs/' & ",
                      dockerImage("regresion")),
      execInnerDockerNoTTY('bash -c "timeout -k 1m -s SIGKILL --preserve-status $${REGRESSION_TIMEOUT} ./go.sh --sm_unit_test_dir=/storage-manager --tests=' + name + ' || ./regression_logs.sh ' + name + '"',
                      dockerImage("regression"),
                      "--env PRESERVE_LOGS=true --workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest"),
    ],
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
    depends_on: ['publish pkg'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'docker run --memory 3g --env OS=' + result + ' --env PACKAGES_URL=' + packages_url + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name smoke$${DRONE_BUILD_NUMBER} --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target']
      + prepareTestStage(dockerImage("smoke"), pkg_format, result, true) + [
      installEngine(dockerImage("smoke"), pkg_format),
      'sleep $${SMOKE_DELAY_SECONDS:-1s}',
      // start mariadb and mariadb-columnstore services and run simple query
      execInnerDocker('systemctl start mariadb', dockerImage("smoke")),
      execInnerDocker("/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local", dockerImage("smoke")),
      execInnerDocker('systemctl restart mariadb-columnstore', dockerImage("smoke")),
      execInnerDocker('mariadb -e "create database if not exists test; create table test.t1 (a int) engine=Columnstore; insert into test.t1 values (1); select * from test.t1"',
                      dockerImage("smoke")),

      // restart mariadb and mariadb-columnstore services and run simple query again
      execInnerDocker('systemctl restart mariadb', dockerImage("smoke")),
      execInnerDocker('systemctl restart mariadb-columnstore', dockerImage("smoke")),
      'sleep 10',
      execInnerDocker('mariadb -e "insert into test.t1 values (2); select * from test.t1"', dockerImage("smoke")),
    ],
  },
  upgrade(version):: {
    name: 'upgrade-test from ' + version,
    depends_on: ['smoke'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    environment: {
      UPGRADE_TOKEN: {
        from_secret: 'es_token',
      },
    },
    commands: [
      // why do we mount cgroups here, but miss it on other steps?
      'docker run --volume /sys/fs/cgroup:/sys/fs/cgroup:ro --env OS=' + result + ' --env PACKAGES_URL=' + packages_url + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --env UCF_FORCE_CONFNEW=1 --name upgrade$${DRONE_BUILD_NUMBER}' + version + ' --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target']
      + prepareTestStage(dockerImage('upgrade') + version, pkg_format, result, false) + [
      if (pkg_format == 'deb')
       then execInnerDocker('bash -c "./upgrade_setup_deb.sh '+ version + ' ' + result + ' ' + arch + ' ' + repo_pkg_url_no_res +' $${UPGRADE_TOKEN}"',
                             dockerImage('upgrade') + version),
      if (std.split(platform, ':')[0] == 'rockylinux')
       then execInnerDocker('bash -c "./upgrade_setup_rpm.sh '+ version + ' ' + result + ' ' + arch + ' ' + repo_pkg_url_no_res + ' $${UPGRADE_TOKEN}"',
                             dockerImage('upgrade') + version),
    ],
  },
  upgradelog:: {
    name: 'upgradelog',
    depends_on: std.map(function(p) 'upgrade-test from ' + p, mdb_server_versions),
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo',
    ] + std.flatMap(function(ver) reportTestStage(dockerImage('upgrade') + ver, result, "upgrade_"+ver), mdb_server_versions),
    when: {
      status: ['success', 'failure'],
    },
  },
  mtr:: {
    name: 'mtr',
    depends_on: ['smoke'],
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    environment: {
      MTR_SUITE_LIST: '${MTR_SUITE_LIST:-' + mtr_suite_list + '}',
      MTR_FULL_SUITE: '${MTR_FULL_SUITE:-false}',
    },
    commands: [
      'docker run --shm-size=500m --memory 8g --env MYSQL_TEST_DIR=' + mtr_path + ' --env OS=' + result + ' --env PACKAGES_URL=' + packages_url + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name mtr$${DRONE_BUILD_NUMBER} --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target']
      + prepareTestStage('mtr$${DRONE_BUILD_NUMBER}', pkg_format, result, true) + [
      installEngine(dockerImage("mtr"), pkg_format),
      'docker cp mysql-test/columnstore mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/suite/',
      execInnerDocker('chown -R mysql:mysql ' + mtr_path, dockerImage("mtr")),
      // disable systemd 'ProtectSystem' (we need to write to /usr/share/)
      execInnerDocker("bash -c 'sed -i /ProtectSystem/d $(systemctl show --property FragmentPath mariadb | sed s/FragmentPath=//)'", dockerImage('mtr')),
      execInnerDocker('systemctl daemon-reload', dockerImage("mtr")),
      execInnerDocker('systemctl start mariadb', dockerImage("mtr")),
      // Set RAM consumption limits to avoid RAM contention b/w mtr and regression steps.
      execInnerDocker("/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local", dockerImage("mtr")),
      execInnerDocker('mariadb -e "create database if not exists test;"', dockerImage("mtr")),
      execInnerDocker('systemctl restart mariadb-columnstore', dockerImage("mtr")),

      // delay mtr for manual debugging on live instance
      'sleep $${MTR_DELAY_SECONDS:-1s}',
      'MTR_SUITE_LIST=$([ "$MTR_FULL_SUITE" == true ] && echo "' + mtr_full_set + '" || echo "$MTR_SUITE_LIST")',
      if (event == 'custom' || event == 'cron') then
        execInnerDocker('bash -c "wget -qO- https://cspkg.s3.amazonaws.com/mtr-test-data.tar.lz4 | lz4 -dc - | tar xf - -C /"',
                        dockerImage('mtr')),
      if (event == 'custom' || event == 'cron') then
        execInnerDocker('bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path + ' --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/setup"',
                        dockerImage('mtr')),

      if (event == 'cron') then
        execInnerDocker('bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path +
                        ' --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite='
                         + std.join(',', std.map(function(x) 'columnstore/' + x, std.split(mtr_full_set, ','))),
                         dockerImage('mtr')) + '"'
                         else
        execInnerDocker('bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path +
                        ' --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/$${MTR_SUITE_LIST//,/,columnstore/}"',
                        dockerImage('mtr')),
    ],
  },
  mtrlog:: {
    name: 'mtrlog',
    depends_on: ['mtr'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start mariadb service logs ----------"',
      execInnerDocker('journalctl -u mariadb --no-pager || echo "mariadb service failure"',  dockerImage('mtr')),
      'echo "---------- end mariadb service logs ----------"',
      'echo',
      'echo "---------- start columnstore debug log ----------"',
      execInnerDocker('cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"', dockerImage('mtr')),
      'echo "---------- end columnstore debug log ----------"',
      'echo "---------- end columnstore debug log ----------"',
      'docker cp mtr$${DRONE_BUILD_NUMBER}:' + mtr_path + '/var/log /drone/src/' + result + '/mtr-logs || echo "missing ' + mtr_path + '/var/log"'
    ] + reportTestStage(dockerImage('mtr'), result, "mtr"),
    when: {
      status: ['success', 'failure'],
    },
  },
  prepare_regression:: {
    name: 'prepare regression',
    depends_on: ['mtr', 'publish pkg', 'publish cmapi build'],
    when: {
      status: ['success', 'failure'],
    },
    image: 'docker:git',
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    environment: {
      REGRESSION_BRANCH_REF: '${DRONE_SOURCE_BRANCH}',
      REGRESSION_REF_AUX: branch_ref,
    },
    commands: [
      // compute branch.
      'echo "$$REGRESSION_REF"',
      'echo "$$REGRESSION_BRANCH_REF"',
      // if REGRESSION_REF is empty, try to see whether regression repository has a branch named as one we PR.
      'export REGRESSION_REF=$${REGRESSION_REF:-$$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-regression-test --h --sort origin "refs/heads/$$REGRESSION_BRANCH_REF" | grep -E -o "[^/]+$$")}',
      'echo "$$REGRESSION_REF"',
      // REGRESSION_REF can be empty if there is no appropriate branch in regression repository.
      // assign what is appropriate by default.
      'export REGRESSION_REF=$${REGRESSION_REF:-$$REGRESSION_REF_AUX}',
      'echo "$$REGRESSION_REF"',
      // clone regression test repo
      'git clone --recurse-submodules --branch $$REGRESSION_REF --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-regression-test',
      // where are we now?
      'cd mariadb-columnstore-regression-test',
      'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
      'cd ..',
      'docker run --shm-size=500m --memory 10g --env OS=' + result + ' --env PACKAGES_URL=' + packages_url + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --name regression$${DRONE_BUILD_NUMBER} --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target']
      + prepareTestStage(dockerImage('regression'), pkg_format, result, true) + [

      'docker cp mariadb-columnstore-regression-test regression$${DRONE_BUILD_NUMBER}:/',
      // list storage manager binary
      'ls -la /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager',
      'docker cp /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager regression$${DRONE_BUILD_NUMBER}:/',
      // check storage-manager unit test binary file
      execInnerDocker('ls -l /storage-manager',dockerImage('regression')),
      // copy test data for regression test suite
      execInnerDocker('bash -c "wget -qO- https://cspkg.s3.amazonaws.com/testData.tar.lz4 | lz4 -dc - | tar xf - -C mariadb-columnstore-regression-test/"',dockerImage('regression')),
      installEngine(dockerImage('regression'), pkg_format),

      // set mariadb lower_case_table_names=1 config option
      execInnerDocker('sed -i "/^.mariadb.$/a lower_case_table_names=1" ' + config_path_prefix + 'server.cnf', dockerImage('regression')),
      // set default client character set to utf-8
      execInnerDocker('sed -i "/^.client.$/a default-character-set=utf8" ' + config_path_prefix + 'client.cnf',dockerImage('regression')),

      // Set RAM consumption limits to avoid RAM contention b/w mtr andregression steps.
      execInnerDocker("/usr/bin/mcsSetConfig SystemConfig CGroup just_no_group_use_local", dockerImage("regression")),

      execInnerDocker('systemctl start mariadb',dockerImage('regression')),
      execInnerDocker('systemctl restart mariadb-columnstore',dockerImage('regression')),
      // delay regression for manual debugging on live instance
      'sleep $${REGRESSION_DELAY_SECONDS:-1s}',
      execInnerDocker('/usr/bin/g++ /mariadb-columnstore-regression-test/mysql/queries/queryTester.cpp -O2 -o  /mariadb-columnstore-regression-test/mysql/queries/queryTester',dockerImage('regression')),
    ],
  },
  smokelog:: {
    name: 'smokelog',
    depends_on: ['smoke'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start mariadb service logs ----------"',
      execInnerDocker('journalctl -u mariadb --no-pager || echo "mariadb service failure"', dockerImage('smoke')),
      'echo "---------- end mariadb service logs ----------"',
      'echo',
      'echo "---------- start columnstore debug log ----------"',
      execInnerDocker('cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"', dockerImage('smoke')),
      'echo "---------- end columnstore debug log ----------"'
    ] + reportTestStage(dockerImage('smoke'), result, "smoke"),
    when: {
      status: ['success', 'failure'],
    },
  },
  cmapilog:: {
    name: 'cmapilog',
    depends_on: ['cmapi test'],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start mariadb service logs ----------"',
      execInnerDocker('journalctl -u mariadb --no-pager || echo "mariadb service failure"', dockerImage('cmapi')),
      'echo "---------- end mariadb service logs ----------"',
      'echo',
      'echo "---------- start columnstore debug log ----------"',
      execInnerDocker('cat /var/log/mariadb/columnstore/debug.log || echo "missing columnstore debug.log"', dockerImage('cmapi')),
      'echo "---------- end columnstore debug log ----------"',
      'echo "---------- start cmapi log ----------"',
      execInnerDocker('cat /var/log/mariadb/columnstore/cmapi_server.log || echo "missing cmapi cmapi_server.log"', dockerImage('cmapi')),
      'echo "---------- end cmapi log ----------"']
      + reportTestStage(dockerImage('cmapi'), result, "cmapi"),
    when: {
      status: ['success', 'failure'],
    },
  },
  regressionlog:: {
    name: 'regressionlog',
    depends_on: [regression_tests[std.length(regression_tests) - 1]],
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    commands: [
      'echo "---------- start columnstore regression short report ----------"',
      execInnerDocker('cat go.log || echo "missing go.log"',
                      dockerImage('regression'),
                      '--workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest'),

      'echo "---------- end columnstore regression short report ----------"',
      'echo',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/reg-logs/ /drone/src/' + result + '/',
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs.tgz /drone/src/' + result + '/ || echo "missing testErrorLogs.tgz"',
      execInnerDocker('bash -c "tar czf regressionQueries.tgz /mariadb-columnstore-regression-test/mysql/queries/"',dockerImage('regression')),
      execInnerDocker('bash -c "tar czf testErrorLogs2.tgz *.log /var/log/mariadb/columnstore" || echo "failed to grab regression results"',
                      dockerImage('regression'),
                      '--workdir /mariadb-columnstore-regression-test/mysql/queries/nightly/alltest'),
      'docker cp regression$${DRONE_BUILD_NUMBER}:/mariadb-columnstore-regression-test/mysql/queries/nightly/alltest/testErrorLogs2.tgz /drone/src/' + result + '/ || echo "missing testErrorLogs.tgz"',
      'docker cp regression$${DRONE_BUILD_NUMBER}:regressionQueries.tgz /drone/src/' + result + '/'
    ] + reportTestStage(dockerImage('regression'), result, "regression"),
    when: {
      status: ['success', 'failure'],
    },
  },
  dockerfile:: {
    name: 'dockerfile',
    depends_on: ['publish pkg', 'publish cmapi build'],
    //failure: 'ignore',
    image: 'alpine/git',
    environment: {
      DOCKER_BRANCH_REF: '${DRONE_SOURCE_BRANCH}',
      DOCKER_REF_AUX: branch_ref,
    },
    commands: [
      // compute branch.
      'echo "$$DOCKER_REF"',
      'echo "$$DOCKER_BRANCH_REF"',
      // if DOCKER_REF is empty, try to see whether docker repository has a branch named as one we PR.
      'export DOCKER_REF=$${DOCKER_REF:-$$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-docker --h --sort origin "refs/heads/$$DOCKER_BRANCH_REF" | grep -E -o "[^/]+$$")}',
      'echo "$$DOCKER_REF"',
      // DOCKER_REF can be empty if there is no appropriate branch in docker repository.
      // assign what is appropriate by default.
      'export DOCKER_REF=$${DOCKER_REF:-$$DOCKER_REF_AUX}',
      'echo "$$DOCKER_REF"',
      'git clone --branch $$DOCKER_REF --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-docker docker',
      'touch docker/.secrets',
    ],
  },
  dockerhub:: {
    name: 'dockerhub',
    depends_on: ['dockerfile'],
    //failure: 'ignore',
    image: 'plugins/docker',
    environment: {
      VERSION: container_version,
      MCS_REPO: 'columnstore',
      DEV: 'true',
      // branchp has slash if not empty
      MCS_BASEURL: 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/${DRONE_BUILD_NUMBER}/' + server + '/' + arch + '/' + result + '/',
      CMAPI_REPO: 'cmapi',
      CMAPI_BASEURL: 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/${DRONE_BUILD_NUMBER}/' + server + '/' + arch + '/' + result + '/',
    },
    settings: {
      repo: 'mariadb/enterprise-columnstore-dev',
      context: 'docker',
      dockerfile: 'docker/Dockerfile',
      build_args_from_env: ['VERSION', 'MCS_REPO', 'MCS_BASEURL', 'CMAPI_REPO', 'CMAPI_BASEURL', 'DEV'],
      tags: container_tags,
      username: {
        from_secret: 'dockerhub_user',
      },
      password: {
        from_secret: 'dockerhub_password',
      },
    },
  },
  cmapipython:: {
    name: 'cmapi python',
    depends_on: ['clone-mdb'],
    image: img,
    volumes: [pipeline._volumes.mdb],
    environment: {
      PYTHON_URL_AMD64: 'https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.9.13+20220802-x86_64_v2-unknown-linux-gnu-pgo+lto-full.tar.zst',
      PYTHON_URL_ARM64: 'https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.9.13+20220802-aarch64-unknown-linux-gnu-noopt-full.tar.zst',
    },
    commands: [
      'cd cmapi',
      '%s install -y wget zstd findutils gcc' % if (pkg_format == 'rpm') then 'yum install -y epel-release && yum makecache && yum ' else 'apt update && apt',
      'wget -qO- $${PYTHON_URL_' + std.asciiUpper(arch) + '} | tar --use-compress-program=unzstd -xf - -C ./',
      'mv python pp && mv pp/install python',
      'chown -R root:root python',
      if (platform == 'rockylinux:9') then 'yum install -y libxcrypt-compat',
      if (arch == 'arm64') then 'export CC=gcc',
      'python/bin/pip3 install -t deps --only-binary :all -r requirements.txt',
      './cleanup.sh',
      'cp cmapi_server/cmapi_server.conf cmapi_server/cmapi_server.conf.default',
    ],
  },
  cmapibuild:: {
    name: 'cmapi build',
    depends_on: ['cmapi python'],
    image: img,
    volumes: [pipeline._volumes.mdb],
    environment: {
      DEBIAN_FRONTEND: 'noninteractive',
    },
    commands: [
      'cd cmapi',
      if (platform == 'rockylinux:9') then 'dnf install -y yum-utils && dnf config-manager --set-enabled devel && dnf update -y',
      if (pkg_format == 'rpm') then 'yum install -y cmake make rpm-build libarchive createrepo findutils redhat-lsb-core' else 'apt update && apt install --no-install-recommends -y cmake make dpkg-dev lsb-release',
      './cleanup.sh',
      'cmake -D' + std.asciiUpper(pkg_format) + '=1 -DSERVER_DIR=/mdb/' + builddir + ' . && make package',
      'mkdir ./' + result,
      'mv -v *.%s ./%s/' % [pkg_format, result],
      if (pkg_format == 'rpm') then 'createrepo ./' + result else 'dpkg-scanpackages %s | gzip > ./%s/Packages.gz' % [result, result],
      'mkdir /drone/src/' + result,
      'yes | cp -vr ./%s /drone/src/' % result,
    ],
  },
  cmapitest:: {
    name: 'cmapi test',
    depends_on: ['publish cmapi build', 'smoke'],
    image: 'docker:git',
    volumes: [pipeline._volumes.docker],
    environment: {
      PYTHONPATH: '/usr/share/columnstore/cmapi/deps',
    },
    commands: [
      'docker run --env OS=' + result + ' --env PACKAGES_URL=' + packages_url + ' --env DEBIAN_FRONTEND=noninteractive --env MCS_USE_S3_STORAGE=0 --env PYTHONPATH=$${PYTHONPATH} --name cmapi$${DRONE_BUILD_NUMBER} --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target'] +
      prepareTestStage(dockerImage('cmapi'), pkg_format, result, true) + [
      if (platform == 'rockylinux:9') then execInnerDocker('bash -c "yum install -y libxcrypt-compat"', dockerImage('cmapi')),
      installEngine(dockerImage('cmapi'), pkg_format),
      installCmapi(dockerImage('cmapi'), pkg_format),
      'cd cmapi',
      'for i in mcs_node_control cmapi_server failover; do docker cp $${i}/test cmapi$${DRONE_BUILD_NUMBER}:' + cmapi_path + '/$${i}/; done',
      'docker cp run_tests.py cmapi$${DRONE_BUILD_NUMBER}:' + cmapi_path + '/',
      execInnerDocker('systemctl start mariadb-columnstore-cmapi', dockerImage('cmapi')),
      // set API key to /etc/columnstore/cmapi_server.conf
      execInnerDocker('bash -c "mcs cluster set api-key --key somekey123"', dockerImage('cmapi')),
      // copy cmapi conf file for test purposes (there are api key already set inside)
      execInnerDocker('bash -c "cp %s/cmapi_server.conf %s/cmapi_server/"' % [etc_path, cmapi_path], dockerImage('cmapi')),
      execInnerDocker('systemctl stop mariadb-columnstore-cmapi', dockerImage('cmapi')),
      execInnerDocker('bash -c "cd ' + cmapi_path + ' && python/bin/python3 run_tests.py"', dockerImage('cmapi')),
    ],
  },
  multi_node_mtr:: {
    name: 'mtr',
    depends_on: ['dockerhub'],
    //failure: 'ignore',
    image: 'docker',
    volumes: [pipeline._volumes.docker],
    environment: {
      DOCKER_LOGIN: {
        from_secret: 'dockerhub_user',
      },
      DOCKER_PASSWORD: {
        from_secret: 'dockerhub_password',
      },
      MCS_IMAGE_NAME: 'mariadb/enterprise-columnstore-dev:' + container_tags[0],
    },
    commands: [
      'echo $$DOCKER_PASSWORD | docker login --username $$DOCKER_LOGIN --password-stdin',
      'cd docker',
      'cp .env_example .env',
      'sed -i "/^MCS_IMAGE_NAME=/s/=.*/=${MCS_IMAGE_NAME}/" .env',
      'sed -i "/^MAXSCALE=/s/=.*/=false/" .env',
      'docker-compose up -d',
      'docker exec mcs1 provision mcs1 mcs2 mcs3',
      'docker cp ../mysql-test/columnstore mcs1:' + mtr_path + '/suite/',
      'docker exec -t mcs1 chown mysql:mysql -R ' + mtr_path,
      'docker exec -t mcs1 mariadb -e "create database if not exists test;"',
      // delay for manual debugging on live instance
      'sleep $${COMPOSE_DELAY_SECONDS:-1s}',
      'docker exec -t mcs1 bash -c "cd ' + mtr_path + ' && ./mtr --extern socket=' + socket_path + ' --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/basic,columnstore/bugfixes"',
    ],
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
               'ls -la ../',
               'mkdir ' + result,
               "sed -i 's|.*-d storage/columnstore.*|elif [[ -d storage/columnstore/columnstore/debian ]]|' debian/autobake-deb.sh",
               if (std.startsWith(server, '10.6')) then "sed -i 's/mariadb-server/mariadb-server-10.6/' storage/columnstore/columnstore/debian/control",
               // Remove Debian build flags that could prevent ColumnStore from building
               "sed -i '/-DPLUGIN_COLUMNSTORE=NO/d' debian/rules",
               // Disable dh_missing strict check for missing files
               'sed -i s/--fail-missing/--list-missing/ debian/rules',
               // Tweak debian packaging stuff
               'for i in mariadb-plugin libmariadbd; do sed -i "/Package: $i.*/,/^$/d" debian/control; done',
               "sed -i 's/Depends: galera.*/Depends:/' debian/control",
               'for i in galera wsrep ha_sphinx embedded; do sed -i /$i/d debian/*.install; done',
               // Install build dependencies for deb
               if (pkg_format == 'deb') then "apt-cache madison liburing-dev | grep liburing-dev || sed 's/liburing-dev/libaio-dev/g' -i debian/control && sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules && sed '/-DWITH_URING=yes/d' -i debian/rules && apt-cache madison libpmem-dev | grep 'libpmem-dev' || sed '/libpmem-dev/d' -i debian/control && sed '/-DWITH_PMEM/d' -i debian/rules && sed '/libfmt-dev/d' -i debian/control",
               // Change plugin_maturity level
               // "sed -i 's/BETA/GAMMA/' storage/columnstore/CMakeLists.txt",
               if (pkg_format == 'deb') then 'apt update -y && apt install -y curl' else if (platform == 'rockylinux:9') then 'yum install -y curl-minimal' else 'yum install -y curl',
               get_sccache,
               testPreparation(platform),
               // disable LTO for 22.04 for now
               if (platform == 'ubuntu:22.04' || platform == 'ubuntu:24.04') then 'apt install -y lto-disabled-list && for i in mariadb-plugin-columnstore mariadb-server mariadb-server-core mariadb mariadb-10.6; do echo "$i any" >> /usr/share/lto-disabled-list/lto-disabled-list; done && grep mariadb /usr/share/lto-disabled-list/lto-disabled-list',
               platformMap(platform, arch),
               'sccache --show-stats',
               // move engine and cmapi packages to one dir to make a repo
               'mv -v -t ./%s/ %s/*.%s /drone/src/cmapi/%s/*.%s ' % [result, if (pkg_format == 'rpm') then '.' else '..', pkg_format, result, pkg_format],
               if (pkg_format == 'rpm') then 'createrepo ./' + result else 'dpkg-scanpackages %s | gzip > ./%s/Packages.gz' % [result, result],
               // list storage manager binary
               'ls -la /mdb/' + builddir + '/storage/columnstore/columnstore/storage-manager',
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
             image: 'alpine/git',
             when: {
               status: ['success', 'failure'],
             },
             volumes: [pipeline._volumes.mdb],
             commands: [
               'cd /mdb/' + builddir,
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'mv buildinfo.txt ./%s/' % result,
               'yes | cp -vr ./%s/. /drone/src/%s/' % [result, result],
               'ls -l /drone/src/' + result,
               'echo "check columnstore package:"',
               'ls -l /drone/src/%s | grep columnstore' % result,
             ],
           },
         ] +
         [pipeline.cmapipython] + [pipeline.cmapibuild] +
         [pipeline.publish('cmapi build')] +
         [pipeline.publish()] +
         [
           {
             name: 'publish pkg url',
             depends_on: ['publish pkg'],
             image: 'alpine/git',
             commands: [
               "echo -e '\\e]8;;" + publish_pkg_url + '\\e\\\\' + publish_pkg_url + "\\e]8;;\\e\\\\'",
               "echo 'for installation run:'",
               "echo 'export OS="+result+"'",
               "echo 'export PACKAGES_URL="+packages_url+"'",
             ],
           },
         ] +
         (if (event == 'cron') then [pipeline.publish('pkg latest', 'latest')] else []) +
         [pipeline.smoke] +
         [pipeline.smokelog] +
         [pipeline.publish('smokelog')] +
         [pipeline.cmapitest] +
         [pipeline.cmapilog] +
         [pipeline.publish('cmapilog')] +
         [pipeline.upgrade(mdb_server_versions[i]) for i in indexes(mdb_server_versions)] +
         (if (std.length(mdb_server_versions) == 0) then [] else [pipeline.upgradelog] + [pipeline.publish('upgradelog')]) +
         (if (platform == 'rockylinux:8' && arch == 'amd64') then [pipeline.dockerfile] + [pipeline.dockerhub] + [pipeline.multi_node_mtr] else [pipeline.mtr] + [pipeline.publish('mtr')] + [pipeline.mtrlog] + [pipeline.publish('mtrlog')]) +
         (if (event == 'cron' && platform == 'rockylinux:8' && arch == 'amd64') then [pipeline.publish('mtr latest', 'latest')] else []) +
         [pipeline.prepare_regression] +
         [pipeline.regression(regression_tests[i], [if (i == 0) then 'prepare regression' else regression_tests[i - 1]]) for i in indexes(regression_tests)] +
         [pipeline.regressionlog] +
         [pipeline.publish('regressionlog')] +
         (if (event == 'cron') then [pipeline.publish('regressionlog latest', 'latest')] else []),

  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: '/var/run/docker.sock' } }],
  trigger: {
    event: [event],
    branch: [branch],
  },
  //  + (if event == 'cron' then {
  //        cron: ['nightly-' + std.strReplace(branch, '.', '-')],
  //      } else {}),
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
  depends_on: std.map(function(p) std.join(' ', [branch, p, event, 'amd64', '10.6-enterprise']), platforms.develop) +
              std.map(function(p) std.join(' ', [branch, p, event, 'arm64', '10.6-enterprise']), platforms_arm.develop),
};

[
  Pipeline(b, p, e, 'amd64', s)
  for b in std.objectFields(platforms)
  for p in platforms[b]
  for s in servers[b]
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
  Pipeline(any_branch, p, 'custom', 'amd64', '10.6-enterprise')
  for p in platforms_custom
] +
[
  Pipeline(any_branch, p, 'custom', 'arm64', '10.6-enterprise')
  for p in platforms_arm_custom
]
