local events = ["pull_request", "cron"];


local current_branch = "stable-23.10";

local servers = {
  "stable-23.10": ["10.6-enterprise"],
};

local platforms = {
  "stable-23.10": ["rockylinux:8", "rockylinux:9", "debian:12", "ubuntu:22.04", "ubuntu:24.04"],
};

local platforms_arm = {
  "stable-23.10": ["rockylinux:8", "rockylinux:9", "debian:12", "ubuntu:22.04", "ubuntu:24.04"],
};

local rewrite_ubuntu_mirror = @"sed -i 's|//\\(us\\.\\)\\?archive\\.ubuntu\\.com|//us.archive.ubuntu.com|g' /etc/apt/sources.list || true; " +
                              @"sed -i 's|//\\(us\\.\\)\\?archive\\.ubuntu\\.com|//us.archive.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources || true; " +
                              "cat /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list | grep archive || true; ";

local customEnvCommandsMap = {
  // 'clang-18': ['apt install -y clang-18', 'export CC=/usr/bin/clang-18', 'export CXX=/usr/bin/clang++-18'],
  "clang-20": [
    rewrite_ubuntu_mirror,
    "apt-get clean && apt-get update",
    "apt-get install -y wget curl lsb-release software-properties-common gnupg",
    "wget https://apt.llvm.org/llvm.sh",
    "bash llvm.sh 20",
    "export CC=/usr/bin/clang",
    "export CXX=/usr/bin/clang++",
  ],
};

local customEnvCommands(envkey, builddir) =
  local updateAlternatives = {
    "clang-20": ["bash /mdb/" + builddir +
                 "/storage/columnstore/columnstore/build/update-clang-version.sh 20 100"],
  };
  (if (std.objectHas(customEnvCommandsMap, envkey))
   then customEnvCommandsMap[envkey] + updateAlternatives[envkey] else []);


local customBootstrapParamsForExisitingPipelines(envkey) =
  local customBootstrapMap = {
    "ubuntu:24.04": "--custom-cmake-flags '-DCOLUMNSTORE_ASAN_FOR_UNITTESTS=YES'",
  };
  (if (std.objectHas(customBootstrapMap, envkey))
   then customBootstrapMap[envkey] else "");

local customBootstrapParamsForAdditionalPipelinesMap = {
  ASAN: "--asan",
  TSAN: "--tsan",
  UBSAN: "--ubsan",
};


local any_branch = "**";
local platforms_custom = platforms[current_branch];
local platforms_arm_custom = platforms_arm[current_branch];

local platforms_mtr = platforms[current_branch];

local builddir = "verylongdirnameforverystrangecpackbehavior";

local mtr_suite_list = "basic,bugfixes";
local mtr_full_set = "basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly";

local upgrade_test_lists = {
  rockylinux8: {
    arm64: ["10.6.4-1", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.15-10"],
    amd64: ["10.6.4-1", "10.6.5-2", "10.6.7-3", "10.6.8-4", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
  },
  rockylinux9: {
    arm64: ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
    amd64: ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
  },

  debian12: {
    arm64: [],
    amd64: [],
  },
  "ubuntu20.04": {
    arm64: ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
    amd64: ["10.6.4-1", "10.6.5-2", "10.6.7-3", "10.6.8-4", "10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
  },
  "ubuntu22.04": {
    arm64: ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
    amd64: ["10.6.9-5", "10.6.11-6", "10.6.12-7", "10.6.14-9", "10.6.15-10"],
  },
  "ubuntu24.04":
    {
      arm64: [],
      amd64: [],
    },
};

local testRun(platform) =
  local platform_map = {
    "rockylinux:8": "ctest3 -R columnstore: -j $(nproc) --output-on-failure",
    "rockylinux:9": "ctest3 -R columnstore: -j $(nproc) --output-on-failure",
    "debian:12": "cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure",
    "ubuntu:20.04": "cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure",
    "ubuntu:22.04": "cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure",
    "ubuntu:24.04": "cd builddir; ctest -R columnstore: -j $(nproc) --output-on-failure",

  };
  platform_map[platform];

local gcc_version = "11";

local rockylinux8_deps = "dnf install -y 'dnf-command(config-manager)' " +
                         "&& dnf config-manager --set-enabled powertools " +
                         "&& dnf install -y gcc-toolset-" + gcc_version + " libarchive cmake " +
                         "&& . /opt/rh/gcc-toolset-" + gcc_version + "/enable ";

local rockylinux9_deps = "dnf install -y 'dnf-command(config-manager)' " +
                         "&& dnf config-manager --set-enabled crb " +
                         "&& dnf install -y gcc gcc-c++";

local rockylinux_common_deps = " && dnf install -y git lz4 lz4-devel cppunit-devel cmake3 boost-devel snappy-devel pcre2-devel";

local deb_deps = rewrite_ubuntu_mirror + "apt-get clean && apt-get update && apt-get install --yes git libboost-all-dev libcppunit-dev libsnappy-dev cmake libpcre2-dev";

local testPreparation(platform) =
  local platform_map = {
    "rockylinux:8": rockylinux8_deps + rockylinux_common_deps,
    "rockylinux:9": rockylinux9_deps + rockylinux_common_deps,
    "debian:12": deb_deps,
    "ubuntu:20.04": deb_deps,
    "ubuntu:22.04": deb_deps,
    "ubuntu:24.04": deb_deps,
  };
  platform_map[platform];

local make_clickable_link(link) = "echo -e '\\e]8;;" +  link + "\\e\\\\" +  link + "\\e]8;;\\e\\\\'";
local echo_running_on = ["echo running on ${DRONE_STAGE_MACHINE}",
      make_clickable_link("https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#Instances:search=:${DRONE_STAGE_MACHINE};v=3;$case=tags:true%5C,client:false;$regex=tags:false%5C,client:false;sort=desc:launchTime")];

local Pipeline(branch, platform, event, arch="amd64", server="10.6-enterprise", customBootstrapParams="", customBuildEnvCommandsMapKey="") = {
  local pkg_format = if (std.split(platform, ":")[0] == "rockylinux") then "rpm" else "deb",
  local init = if (pkg_format == "rpm") then "/usr/lib/systemd/systemd" else "systemd",
  local mtr_path = if (pkg_format == "rpm") then "/usr/share/mysql-test" else "/usr/share/mysql/mysql-test",
  local cmapi_path = "/usr/share/columnstore/cmapi",
  local etc_path = "/etc/columnstore",
  local socket_path = if (pkg_format == "rpm") then "/var/lib/mysql/mysql.sock" else "/run/mysqld/mysqld.sock",
  local config_path_prefix = if (pkg_format == "rpm") then "/etc/my.cnf.d/" else "/etc/mysql/mariadb.conf.d/50-",
  local img = if (platform == "rockylinux:8") then platform else "detravi/" + std.strReplace(platform, "/", "-"),
  local branch_ref = if (branch == any_branch) then current_branch else branch,
  // local regression_tests = if (std.startsWith(platform, 'debian') || std.startsWith(platform, 'ubuntu:20')) then 'test000.sh' else 'test000.sh,test001.sh',

  local branchp = if (branch == "**") then "" else branch + "/",
  local brancht = if (branch == "**") then "" else branch + "-",
  local platformKey = std.strReplace(std.strReplace(platform, ":", ""), "/", "-"),
  local result = platformKey + if customBuildEnvCommandsMapKey != "" then "_" + customBuildEnvCommandsMapKey else "",

  local packages_url = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server,
  local publish_pkg_url = "https://cspkg.s3.amazonaws.com/index.html?prefix=" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/" + result + "/",
  local repo_pkg_url_no_res = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/",

  local container_tags = if (event == "cron") then [brancht + std.strReplace(event, "_", "-") + "${DRONE_BUILD_NUMBER}", brancht] else [brancht + std.strReplace(event, "_", "-") + "${DRONE_BUILD_NUMBER}"],
  local container_version = branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch,

  local server_remote = if (std.endsWith(server, "enterprise")) then "https://github.com/mariadb-corporation/MariaDBEnterprise" else "https://github.com/MariaDB/server",

  local sccache_arch = if (arch == "amd64") then "x86_64" else "aarch64",
  local get_sccache = ["echo getting sccache...",
                      rewrite_ubuntu_mirror,
                      "(apt-get clean && apt-get update -y && apt-get install -y curl || yum install -y curl || true)",
                      "curl -L -o sccache.tar.gz https://github.com/mozilla/sccache/releases/download/v0.10.0/sccache-v0.10.0-" + sccache_arch + "-unknown-linux-musl.tar.gz &&",
                      "tar xzf sccache.tar.gz",
                      "install sccache*/sccache /usr/local/bin/ && echo sccache installed"],

  local pipeline = self,

  publish(step_prefix="pkg", eventp=event + "/${DRONE_BUILD_NUMBER}"):: {
    name: "publish " + step_prefix,
    depends_on: [std.strReplace(step_prefix, " latest", ""), "createrepo"],
    image: "amazon/aws-cli:2.22.30",
    when: {
      status: ["success", "failure"],
    },
    environment: {
      AWS_ACCESS_KEY_ID: {
        from_secret: "aws_access_key_id",
      },
      AWS_SECRET_ACCESS_KEY: {
        from_secret: "aws_secret_access_key",
      },
      AWS_REGION: "us-east-1",
      AWS_DEFAULT_REGION: "us-east-1",
    },
    commands: [
      "ls " + result,
      '[ -z "$(ls -A "' + result + '")" ] && echo Nothing to publish! && exit 1',

      "aws s3 sync " + result + " s3://cspkg/" + branchp + eventp + "/" + server + "/" + arch + "/" + result + " --only-show-errors",
      'echo "Data uploaded to: ' + publish_pkg_url + '"',
      make_clickable_link(publish_pkg_url),
      "rm -rf " + result + "/*",
    ],
  },

  local regression_tests = if (event == "cron") then [
    "test000.sh",
    "test001.sh",
    "test005.sh",
    "test006.sh",
    "test007.sh",
    "test008.sh",
    "test009.sh",
    "test010.sh",
    "test011.sh",
    "test012.sh",
    "test013.sh",
    "test014.sh",
    "test023.sh",
    "test201.sh",
    "test202.sh",
    "test203.sh",
    "test204.sh",
    "test210.sh",
    "test211.sh",
    "test212.sh",
    //  "test222.sh", FIXME: restore the test
    "test297.sh",
    "test299.sh",
    "test400.sh",
    "test500.sh",
  ] else [
    "test000.sh",
    "test001.sh",
  ],

  local mdb_server_versions = upgrade_test_lists[platformKey][arch],

  local indexes(arr) = std.range(0, std.length(arr) - 1),

  local execInnerDocker(command, containerName, flags="") =
    "docker exec " + flags + " -t " + containerName + " " + command,

  local execInnerDockerNoTTY(command, containerName, flags="") =
    "docker exec " + flags + " " + containerName + " " + command,

  local getContainerName(stepname) = stepname + "$${DRONE_BUILD_NUMBER}",

  local installCmapi(containerName, pkg_format) =
    if (pkg_format == "deb") then execInnerDocker('bash -c "apt-get clean && apt-get update -y && apt-get install -y mariadb-columnstore-cmapi"', containerName)
    else execInnerDocker('bash -c "yum update -y && yum install -y MariaDB-columnstore-cmapi"', containerName),

  local prepareTestContainer(containerName, result, do_setup) =
    'sh -c "apk add bash && bash /mdb/' + builddir + "/storage/columnstore/columnstore/build/prepare_test_container.sh" +
    " --container-name " + containerName +
    " --docker-image " + img +
    " --result-path " + result +
    " --packages-url " + packages_url +
    " --do-setup " + std.toString(do_setup) + '"',

  local reportTestStage(containerName, result, stage) = 'sh -c "apk add bash && bash /mdb/' + builddir + "/storage/columnstore/columnstore/build/report_test_stage.sh " + containerName + " " + result + " " + stage + '"',

  _volumes:: {
    mdb: {
      name: "mdb",
      path: "/mdb",
    },
    docker: {
      name: "docker",
      path: "/var/run/docker.sock",
    },
  },
  smoke:: {
    name: "smoke",
    depends_on: ["publish pkg"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.mdb, pipeline._volumes.docker],
    commands: [
      prepareTestContainer(getContainerName("smoke"), result, true),
      "bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/run_smoke.sh " + getContainerName("smoke"),
    ],
  },
  smokelog:: {
    name: "smokelog",
    depends_on: ["smoke"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      reportTestStage(getContainerName("smoke"), result, "smoke"),
    ],
    when: {
      status: ["success", "failure"],
    },
  },
  upgrade(version):: {
    name: "upgrade-test from " + version,
    depends_on: ["regressionlog"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker],
    environment: {
      UPGRADE_TOKEN: {
        from_secret: "es_token",
      },
    },
    commands: [
      // why do we mount cgroups here, but miss it on other steps?
      prepareTestContainer(getContainerName("upgrade") + version, result, false),
      if (pkg_format == "deb")
      then execInnerDocker('bash -c "./upgrade_setup_deb.sh ' + version + " " + result + " " + arch + " " + repo_pkg_url_no_res + ' $${UPGRADE_TOKEN}"',
                           getContainerName("upgrade") + version),
      if (pkg_format == "rpm")
      then execInnerDocker('bash -c "./upgrade_setup_rpm.sh ' + version + " " + result + " " + arch + " " + repo_pkg_url_no_res + ' $${UPGRADE_TOKEN}"',
                           getContainerName("upgrade") + version),
    ],
  },
  upgradelog:: {
    name: "upgradelog",
    depends_on: std.map(function(p) "upgrade-test from " + p, mdb_server_versions),
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands:
       ["echo"] +
         std.map(
           function(ver)
             reportTestStage(
               getContainerName("upgrade") + ver,
               result,
               "upgrade_" + ver
             ),
           mdb_server_versions
         ),
    when: {
      status: ["success", "failure"],
    },
  },
  mtr:: {
    name: "mtr",
    depends_on: ["smoke"],
    image: "docker:git",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    environment: {
      MTR_SUITE_LIST: "${MTR_SUITE_LIST:-" + mtr_suite_list + "}",
      MTR_FULL_SUITE: "${MTR_FULL_SUITE:-false}",
    },
    commands: [
      prepareTestContainer(getContainerName("mtr"), result, true),
      'MTR_SUITE_LIST=$([ "$MTR_FULL_SUITE" == true ] && echo "' + mtr_full_set + '" || echo "$MTR_SUITE_LIST")',

      'apk add bash && bash /mdb/' + builddir + '/storage/columnstore/columnstore/build/run_mtr.sh' +
      ' --container-name ' + getContainerName("mtr") +
      ' --distro ' + platform +
      ' --suite-list $${MTR_SUITE_LIST}' +
      ' --triggering-event ' + event,
    ],
  },
  mtrlog:: {
    name: "mtrlog",
    depends_on: ["mtr"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      reportTestStage(getContainerName("mtr"), result, "mtr"),
    ],
    when: {
      status: ["success", "failure"],
    },
  },
  regression(name, depends_on):: {
    name: name,
    depends_on: depends_on,
    image: "docker:git",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    when: {
      status: ["success", "failure"],
    },
    [if (name != "test000.sh" && name != "test001.sh") then "failure"]: "ignore",
    environment: {
      REGRESSION_TIMEOUT: {
        from_secret: "regression_timeout",
      },
      REGRESSION_BRANCH_REF: "${DRONE_SOURCE_BRANCH}",
      REGRESSION_REF_AUX: branch_ref,
    },
    commands: [
      prepareTestContainer(getContainerName("regression"), result, true),

      // REGRESSION_REF can be empty if there is no appropriate branch in regression repository.
      // if REGRESSION_REF is empty, try to see whether regression repository has a branch named as one we PR.
      'export REGRESSION_REF=$${REGRESSION_REF:-$$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-regression-test --h --sort origin "refs/heads/$$REGRESSION_BRANCH_REF" | grep -E -o "[^/]+$$")}',
      "export REGRESSION_REF=$${REGRESSION_REF:-$$REGRESSION_REF_AUX}",
      'echo "$$REGRESSION_REF"',

      "apk add bash && bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/run_regression.sh" +
      " --container-name " + getContainerName("regression") +
      " --test-name " + name +
      " --distro " + platform +
      " --regression-branch $$REGRESSION_REF" +
      " --regression-timeout $${REGRESSION_TIMEOUT}",
    ],
  },
  regressionlog:: {
    name: "regressionlog",
    depends_on: [regression_tests[std.length(regression_tests) - 1]],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      reportTestStage(getContainerName("regression"), result, "regression"),
    ],
    when: {
      status: ["success", "failure"],
    },
  },
  dockerfile:: {
    name: "dockerfile",
    depends_on: ["publish pkg", "publish cmapi build"],
    //failure: 'ignore',
    image: "alpine/git:2.49.0",
    environment: {
      DOCKER_BRANCH_REF: "${DRONE_SOURCE_BRANCH}",
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
      "export DOCKER_REF=$${DOCKER_REF:-$$DOCKER_REF_AUX}",
      'echo "$$DOCKER_REF"',
      "git clone --branch $$DOCKER_REF --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-docker docker",
      "touch docker/.secrets",
    ],
  },
  dockerhub:: {
    name: "dockerhub",
    depends_on: ["dockerfile"],
    //failure: 'ignore',
    image: "plugins/docker",
    environment: {
      VERSION: container_version,
      MCS_REPO: "columnstore",
      DEV: "true",
      // branchp has slash if not empty
      MCS_BASEURL: "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/" + result + "/",
      CMAPI_REPO: "cmapi",
      CMAPI_BASEURL: "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/" + result + "/",
    },
    settings: {
      repo: "mariadb/enterprise-columnstore-dev",
      context: "docker",
      dockerfile: "docker/Dockerfile",
      build_args_from_env: ["VERSION", "MCS_REPO", "MCS_BASEURL", "CMAPI_REPO", "CMAPI_BASEURL", "DEV"],
      tags: container_tags,
      username: {
        from_secret: "dockerhub_user",
      },
      password: {
        from_secret: "dockerhub_password",
      },
    },
  },
  cmapitest:: {
    name: "cmapi test",
    depends_on: ["publish cmapi build"],
    image: "docker:git",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    environment: {
      PYTHONPATH: "/usr/share/columnstore/cmapi/deps",
    },
    commands: [
      prepareTestContainer(getContainerName("cmapi"), result, true),
      installCmapi(getContainerName("cmapi"), pkg_format),
      "cd cmapi",
      "for i in mcs_node_control cmapi_server failover; do docker cp $${i}/test cmapi$${DRONE_BUILD_NUMBER}:" + cmapi_path + "/$${i}/; done",
      "docker cp run_tests.py cmapi$${DRONE_BUILD_NUMBER}:" + cmapi_path + "/",
      execInnerDocker("systemctl start mariadb-columnstore-cmapi", getContainerName("cmapi")),
      // set API key to /etc/columnstore/cmapi_server.conf
      execInnerDocker('bash -c "mcs cluster set api-key --key somekey123"', getContainerName("cmapi")),
      // copy cmapi conf file for test purposes (there are api key already set inside)
      execInnerDocker('bash -c "cp %s/cmapi_server.conf %s/cmapi_server/"' % [etc_path, cmapi_path], getContainerName("cmapi")),
      execInnerDocker("systemctl stop mariadb-columnstore-cmapi", getContainerName("cmapi")),
      execInnerDocker('bash -c "cd ' + cmapi_path + ' && python/bin/python3 run_tests.py"', getContainerName("cmapi")),
    ],
  },
  cmapilog:: {
    name: "cmapilog",
    depends_on: ["cmapi test"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
    commands: [
      reportTestStage(getContainerName("cmapi"), result, "cmapi"),
    ],
    when: {
      status: ["success", "failure"],
    },
  },
  multi_node_mtr:: {
    name: "mtr",
    depends_on: ["dockerhub"],
    //failure: 'ignore',
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker],
    environment: {
      DOCKER_LOGIN: {
        from_secret: "dockerhub_user",
      },
      DOCKER_PASSWORD: {
        from_secret: "dockerhub_password",
      },
      MCS_IMAGE_NAME: "mariadb/enterprise-columnstore-dev:" + container_tags[0],
    },
    commands: [
      "echo $$DOCKER_PASSWORD | docker login --username $$DOCKER_LOGIN --password-stdin",
      "cd docker",
      "cp .env_example .env",
      'sed -i "/^MCS_IMAGE_NAME=/s/=.*/=${MCS_IMAGE_NAME}/" .env',
      'sed -i "/^MAXSCALE=/s/=.*/=false/" .env',
      "docker-compose up -d",
      "docker exec mcs1 provision mcs1 mcs2 mcs3",
      "docker cp ../mysql-test/columnstore mcs1:" + mtr_path + "/suite/",
      "docker exec -t mcs1 chown mysql:mysql -R " + mtr_path,
      'docker exec -t mcs1 mariadb -e "create database if not exists test;"',
      // delay for manual debugging on live instance
      "sleep $${COMPOSE_DELAY_SECONDS:-1s}",
      'docker exec -t mcs1 bash -c "cd ' + mtr_path + " && ./mtr --extern socket=" + socket_path + ' --force --print-core=detailed --print-method=gdb --max-test-fail=0 --suite=columnstore/basic,columnstore/bugfixes"',
    ],
  },

  kind: "pipeline",
  type: "docker",
  name: std.join(" ", [branch, platform, event, arch, server, customBootstrapParams, customBuildEnvCommandsMapKey]),
  platform: { arch: arch },
  // [if arch == 'arm64' then 'node']: { arch: 'arm64' },
  clone: { depth: 10 },
  steps: [
           {
             name: "submodules",
             image: "alpine/git:2.49.0",
             commands: [
               "git submodule update --init --recursive",
               "git config cmake.update-submodules no",
               "git rev-parse --abbrev-ref HEAD && git rev-parse HEAD",
             ],
           },
           {
             name: "clone-mdb",
             image: "alpine/git:2.49.0",
             volumes: [pipeline._volumes.mdb],
             environment: {
               SERVER_REF: "${SERVER_REF:-" + server + "}",
               SERVER_REMOTE: "${SERVER_REMOTE:-" + server_remote + "}",
               SERVER_SHA: "${SERVER_SHA:-" + server + "}",
             },
             commands: echo_running_on +
             [
              "echo $$SERVER_REF",
               "echo $$SERVER_REMOTE",
               "mkdir -p /mdb/" + builddir + " && cd /mdb/" + builddir,
               'git config --global url."https://github.com/".insteadOf git@github.com:',
               'git -c submodule."storage/rocksdb/rocksdb".update=none -c submodule."wsrep-lib".update=none -c submodule."storage/columnstore/columnstore".update=none clone --recurse-submodules --depth 200 --branch $$SERVER_REF $$SERVER_REMOTE .',
               "git reset --hard $$SERVER_SHA",
               "git rev-parse --abbrev-ref HEAD && git rev-parse HEAD",
               "git config cmake.update-submodules no",
               "rm -rf storage/columnstore/columnstore",
               "cp -r /drone/src /mdb/" + builddir + "/storage/columnstore/columnstore",
             ],
           },
           {
             name: "build",
             depends_on: ["clone-mdb"],
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: "noninteractive",
               AWS_ACCESS_KEY_ID: {
                 from_secret: "aws_access_key_id",
               },
               AWS_SECRET_ACCESS_KEY: {
                 from_secret: "aws_secret_access_key",
               },
               AWS_REGION: "us-east-1",
               AWS_DEFAULT_REGION: "us-east-1",
               SCCACHE_BUCKET: "cs-sccache",
               SCCACHE_REGION: "us-east-1",
               SCCACHE_S3_USE_SSL: "true",
               SCCACHE_S3_KEY_PREFIX: result + branch + server + arch + "${DRONE_PULL_REQUEST}",
               //SCCACHE_ERROR_LOG: '/tmp/sccache_log.txt',
               //SCCACHE_LOG: 'debug',
             },
             commands: [
                         "mkdir /mdb/" + builddir + "/" + result,
                       ]
                       + get_sccache
                       + customEnvCommands(customBuildEnvCommandsMapKey, builddir) +
                       [
                        'bash -c "set -o pipefail && bash /mdb/' + builddir + "/storage/columnstore/columnstore/build/bootstrap_mcs.sh " +
                         "--build-type RelWithDebInfo " +
                         "--distro " + platform + " " +
                         "--build-packages --install-deps --sccache " +
                         " " + customBootstrapParams +
                         " " + customBootstrapParamsForExisitingPipelines(platform) + " | " +
                         "/mdb/" + builddir + "/storage/columnstore/columnstore/build/ansi2txt.sh " +
                         "/mdb/" + builddir + "/" + result + '/build.log"',
                         "sccache --show-stats",
                       ],
           },
           {
             name: "cmapi build",
             depends_on: ["clone-mdb"],
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: "noninteractive",
             },
             commands: [
               "bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/build_cmapi.sh --distro " + platform + " --arch " + arch,
             ],
           },
           {
             name: "createrepo",
             depends_on: ["build", "cmapi build"],
             image: img,
             when: {
               status: ["success", "failure"],
             },
             volumes: [pipeline._volumes.mdb],
             commands: [
               "bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/createrepo.sh --result " + result,
             ],
           },
           {
             name: "unittests",
             depends_on: ["createrepo"],
             image: img,
             volumes: [pipeline._volumes.mdb],
             environment: {
               DEBIAN_FRONTEND: "noninteractive",
             },
             commands: [
               "cd /mdb/" + builddir,
               testPreparation(platform),
               testRun(platform),
             ],
           },
           {
             name: "pkg",
             depends_on: ["unittests"],
             image: "alpine/git:2.49.0",
             when: {
               status: ["success", "failure"],
             },
             volumes: [pipeline._volumes.mdb],
             environment: {
               SERVER_REF: "${SERVER_REF:-" + server + "}",
               SERVER_REMOTE: "${SERVER_REMOTE:-" + server_remote + "}",
             },
             commands: [
               "cd /mdb/" + builddir,
               'echo "engine: $DRONE_COMMIT" > buildinfo.txt',
               'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
               'echo "buildNo: $DRONE_BUILD_NUMBER" >> buildinfo.txt',
               'echo "serverBranch: $$SERVER_REF" >> buildinfo.txt',
               'echo "serverRepo: $$SERVER_REMOTE" >> buildinfo.txt',
               'echo "engineBranch: $DRONE_SOURCE_BRANCH" >> buildinfo.txt',
               'echo "engineRepo: https://github.com/$DRONE_REPO" >> buildinfo.txt',
               "mv buildinfo.txt ./%s/" % result,
               "yes | cp -vr ./%s/. /drone/src/%s/" % [result, result],
               "ls -l /drone/src/" + result,
               'echo "check columnstore package:"',
               "ls -l /drone/src/%s | grep columnstore" % result,
             ],
           },
         ] +
         [pipeline.publish("cmapi build")] +
         [pipeline.publish()] +
         (if (event == "cron") then [pipeline.publish("pkg latest", "latest")] else []) +
         [pipeline.smoke] +
         [pipeline.smokelog] +
         [pipeline.publish("smokelog")] +
         [pipeline.cmapitest] +
         [pipeline.cmapilog] +
         [pipeline.publish("cmapilog")] +
         (if (platform == "rockylinux:8" && arch == "amd64") then [pipeline.dockerfile] + [pipeline.dockerhub] + [pipeline.multi_node_mtr] else [pipeline.mtr] + [pipeline.mtrlog] + [pipeline.publish("mtrlog")]) +
         [pipeline.regression(regression_tests[i], if (i == 0) then ["mtr", "publish pkg", "publish cmapi build"] else [regression_tests[i - 1]]) for i in indexes(regression_tests)] +
         [pipeline.regressionlog] +
         [pipeline.publish("regressionlog")] +
         // [pipeline.upgrade(mdb_server_versions[i]) for i in indexes(mdb_server_versions)] +
         // (if (std.length(mdb_server_versions) == 0) then [] else [pipeline.upgradelog] + [pipeline.publish("upgradelog")]) +
         (if (event == "cron") then [pipeline.publish("regressionlog latest", "latest")] else []),

  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: "/var/run/docker.sock" } }],
  trigger: {
    event: [event],
    branch: [branch],
  },
  //  + (if event == 'cron' then {
  //        cron: ['nightly-' + std.strReplace(branch, '.', '-')],
  //      } else {}),
};

local FinalPipeline(branch, event) = {
  kind: "pipeline",
  name: std.join(" ", ["after", branch, event]),
  steps: [
    {
      name: "notify",
      image: "plugins/slack",
      settings: {
        webhook: {
          from_secret: "slack_webhook",
        },
        template: "*" + event + (if event == "pull_request" then " <https://github.com/{{repo.owner}}/{{repo.name}}/pull/{{build.pull}}|#{{build.pull}}>" else "") +
                  " build <{{build.link}}|{{build.number}}> {{#success build.status}}succeeded{{else}}failed{{/success}}*.\n\n*Branch*: <https://github.com/{{repo.owner}}/{{repo.name}}/tree/{{build.branch}}|{{build.branch}}>\n*Commit*: <https://github.com/{{repo.owner}}/{{repo.name}}/commit/{{build.commit}}|{{truncate build.commit 8}}> {{truncate build.message.title 100 }}\n*Author*: {{ build.author }}\n*Duration*: {{since build.started}}\n*Artifacts*: https://cspkg.s3.amazonaws.com/index.html?prefix={{build.branch}}/{{build.event}}/{{build.number}}",
      },
    },
  ],
  trigger: {
    event: [event],
    branch: [branch],
    status: [
      "success",
      "failure",
    ],
  } + (if event == "cron" then { cron: ["nightly-" + std.strReplace(branch, ".", "-")] } else {}),
  depends_on: std.map(function(p) std.join(" ", [branch, p, event, "amd64", "10.6-enterprise", "", ""]), platforms[current_branch]),
  // +std.map(function(p) std.join(" ", [branch, p, event, "arm64", "10.6-enterprise", "", ""]), platforms_arm.develop),
};

[
  Pipeline(b, p, e, "amd64", s)
  for b in std.objectFields(platforms)
  for p in platforms[b]
  for s in servers[b]
  for e in events
] +
// [
//   Pipeline(b, p, e, "arm64", s)
//   for b in std.objectFields(platforms_arm)
//   for p in platforms_arm[b]
//   for s in servers[b]
//   for e in events
// ] +

[
  FinalPipeline(b, "cron")
  for b in std.objectFields(platforms)
] +

[
  Pipeline(any_branch, p, "custom", "amd64", "10.6-enterprise")
  for p in platforms_custom
] +
// [
//   Pipeline(any_branch, p, "custom", "arm64", "10.6-enterprise")
//   for p in platforms_arm_custom
// ]
// +
[
  Pipeline(b, platform, triggeringEvent, a, server, "", buildenv)
  for a in ["amd64"]
  for b in std.objectFields(platforms)
  for platform in ["ubuntu:24.04"]
  for buildenv in std.objectFields(customEnvCommandsMap)
  for triggeringEvent in events
  for server in servers[current_branch]
]
