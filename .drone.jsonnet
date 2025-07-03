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

local customEnvCommandsMap = {
  // 'clang-18': ['apt install -y clang-18', 'export CC=/usr/bin/clang-18', 'export CXX=/usr/bin/clang++-18'],
  "clang-20": [
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

local make_clickable_link(link) = "echo -e '\\e]8;;" +  link + "\\e\\\\" +  link + "\\e]8;;\\e\\\\'";
local echo_running_on = ["echo running on ${DRONE_STAGE_MACHINE}",
      make_clickable_link("https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#Instances:search=:${DRONE_STAGE_MACHINE};v=3;$case=tags:true%5C,client:false;$regex=tags:false%5C,client:false;sort=desc:launchTime")];

local Pipeline(branch, platform, event, arch="amd64", server="10.6-enterprise", customBootstrapParams="", customBuildEnvCommandsMapKey="") = {
  local pkg_format = if (std.split(platform, ":")[0] == "rockylinux") then "rpm" else "deb",
  local img = if (platform == "rockylinux:8") then platform else "detravi/" + std.strReplace(platform, "/", "-"),
  local branch_ref = if (branch == any_branch) then current_branch else branch,

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
      "sleep 10",
      "ls " + result,
      '[ -z "$(ls -A "' + result + '")" ] && echo Nothing to publish! && exit 1',

      "aws s3 sync " + result + "/" + " s3://cspkg/" + branchp + eventp + "/" + server + "/" + arch + "/" + result + " --only-show-errors --debug",
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

  local getContainerName(stepname) = stepname + "$${DRONE_BUILD_NUMBER}",

  local prepareTestContainer(containerName, result, do_setup) =
    'sh -c "apk add bash && bash /mdb/' + builddir + "/storage/columnstore/columnstore/build/prepare_test_container.sh" +
    " --container-name " + containerName +
    " --docker-image " + img +
    " --result-path " + result +
    " --packages-url " + packages_url +
    " --do-setup " + std.toString(do_setup) + '"',

  local reportTestStage(containerName, result, stage) =
    'sh -c "apk add bash && bash /mdb/' + builddir + '/storage/columnstore/columnstore/build/report_test_stage.sh' +
    ' --container-name ' + containerName +
    ' --result-path ' + result +
    ' --stage ' + stage + '"',


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
      "bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/run_smoke.sh" +
      ' --container-name ' + getContainerName("smoke") +
      ' --result-path ' + result,
    ],
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
      prepareTestContainer(getContainerName("upgrade") + version, result, false),

      execInnerDocker('bash -c "./upgrade_setup_' + pkg_format + '.sh '
          + version + ' '
          + result + ' '
          + arch + ' '
          + repo_pkg_url_no_res
          + ' $${UPGRADE_TOKEN}"',
        getContainerName("upgrade") + version
      )
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
      ' --result-path ' + result +
      ' --triggering-event ' + event,
    ],
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

      "apk add bash && bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/run_cmapi_test.sh" +
      " --container-name " + getContainerName("cmapi") +
      " --result-path " + result +
      " --pkg-format " + pkg_format,
    ],
  },
  multi_node_mtr:: {
    name: "mtr",
    depends_on: ["dockerhub"],
    image: "docker:28.2.2",
    volumes: [pipeline._volumes.docker, pipeline._volumes.mdb],
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

      "apk add bash && bash /mdb/" + builddir + "/storage/columnstore/columnstore/build/run_multi_node_mtr.sh " +
      "--columnstore-image-name $${MCS_IMAGE_NAME}" +
      "--distro " + platform,
    ],
  },

  kind: "pipeline",
  type: "docker",
  name: std.join(" ", [branch, platform, event, arch, server, customBootstrapParams, customBuildEnvCommandsMapKey]),
  platform: { arch: arch },
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
             },
             commands: [
                         "mkdir /mdb/" + builddir + "/" + result,
                       ]
                       + customEnvCommands(customBuildEnvCommandsMapKey, builddir) +
                       [
                        'bash -c "set -o pipefail && bash /mdb/' + builddir + "/storage/columnstore/columnstore/build/bootstrap_mcs.sh " +
                         "--build-type RelWithDebInfo " +
                         "--distro " + platform + " " +
                         "--build-packages --install-deps --sccache --sccache-arch " + arch +
                         " " + customBootstrapParams +
                         " " + customBootstrapParamsForExisitingPipelines(platform) + " | " +
                         "/mdb/" + builddir + "/storage/columnstore/columnstore/build/ansi2txt.sh " +
                         "/mdb/" + builddir + "/" + result + '/build.log "',
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
             name: "pkg",
             depends_on: ["createrepo"],
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
         [pipeline.publish("smoke")] +
         [pipeline.cmapitest] +
         [pipeline.publish("cmapi test")] +
         (if (platform == "rockylinux:8" && arch == "amd64") then [pipeline.dockerfile] + [pipeline.dockerhub] + [pipeline.multi_node_mtr] else [pipeline.mtr] + [pipeline.publish("mtr")]) +
         [pipeline.regression(regression_tests[i], if (i == 0) then ["mtr", "publish pkg", "publish cmapi build"] else [regression_tests[i - 1]]) for i in indexes(regression_tests)] +
         [pipeline.regressionlog] +
         // [pipeline.upgrade(mdb_server_versions[i]) for i in indexes(mdb_server_versions)] +
         // (if (std.length(mdb_server_versions) == 0) then [] else [pipeline.upgradelog] + [pipeline.publish("upgradelog")]) +
         (if (event == "cron") then [pipeline.publish("regressionlog latest", "latest")] else [pipeline.publish("regressionlog")]),

  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: "/var/run/docker.sock" } }],
  trigger: {
    event: [event],
    branch: [branch],
  },
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
  for p in platforms[current_branch]
] +
// [
//   Pipeline(any_branch, p, "custom", "arm64", "10.6-enterprise")
//   for p in platforms_arm[current_branch];
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
