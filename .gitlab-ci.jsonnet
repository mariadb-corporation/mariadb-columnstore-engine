local events = ["pull_request", "cron"];
local current_branch = "stable-23.10";
local servers = { [current_branch]: ["10.6-enterprise"] };
local platforms = {
  [current_branch]: ["rockylinux:8", "rockylinux:9", "debian:12", "ubuntu:22.04", "ubuntu:24.04"]
};
local archs = ["amd64"];
local builddir = "verylongdirnameforverystrangecpackbehavior";

local get_build_command(command) = "bash ./mdb/" + builddir + "/storage/columnstore/columnstore/build/" + command + " ";

local clang(version) = [
  get_build_command("install_clang_deb.sh " + version),
  get_build_command("update-clang-version.sh " + version + " 100"),
  get_build_command("install_libc++.sh " + version),
  "export CC=/usr/bin/clang",
  "export CXX=/usr/bin/clang++"
];
local customEnvCommandsMap = { "clang-20": clang("20") };
local customEnvCommands(envkey, builddir) =
  if std.objectHas(customEnvCommandsMap, envkey) then customEnvCommandsMap[envkey] else [];

local customBootstrapParamsForExisitingPipelines(envkey) =
  local customBootstrapMap = { "ubuntu:24.04": "--custom-cmake-flags '-DCOLUMNSTORE_ASAN_FOR_UNITTESTS=YES'" };
  if std.objectHas(customBootstrapMap, envkey) then customBootstrapMap[envkey] else "";

local customBootstrapParamsForAdditionalPipelinesMap = {
  "ASan": "--asan", "TSAN": "--tsan", "UBSan": "--ubsan", "MSan": "--msan",
  "libcpp": "--libcpp --skip-unit-tests", "gcc-toolset": "--gcc-toolset-for-rocky-8"
};
local customBuildFlags(buildKey) =
  if std.objectHas(customBootstrapParamsForAdditionalPipelinesMap, buildKey)
  then customBootstrapParamsForAdditionalPipelinesMap[buildKey] else "";

local any_branch = "**";
local mtr_suite_list = "basic,bugfixes";
local mtr_full_set = "basic,bugfixes,devregression,autopilot,extended,multinode,oracle,1pmonly";

local full_regression_tests = [
  "test000.sh", "test001.sh", "test005.sh", "test006.sh", "test007.sh", "test008.sh",
  "test009.sh", "test010.sh", "test011.sh", "test012.sh", "test013.sh", "test014.sh",
  "test023.sh", "test201.sh", "test202.sh", "test203.sh", "test204.sh", "test210.sh",
  "test211.sh", "test212.sh", "test297.sh", "test299.sh", "test400.sh", "test500.sh"
];

local upgrade_test_lists = {
  "rockylinux8": { "amd64": ["10.6.4-1", "10.6.15-10"], "arm64": ["10.6.15-10"] },
  "rockylinux9": { "amd64": ["10.6.9-5", "10.6.15-10"], "arm64": ["10.6.15-10"] },
  "debian12": { "amd64": [], "arm64": [] },
  "ubuntu20.04": { "amd64": ["10.6.4-1", "10.6.15-10"], "arm64": ["10.6.15-10"] },
  "ubuntu22.04": { "amd64": ["10.6.9-5", "10.6.15-10"], "arm64": ["10.6.15-10"] },
  "ubuntu24.04": { "amd64": [], "arm64": [] }
};

local make_clickable_link(link) = "echo -e '\\e]8;;" + link + "\\e\\\\" + link + "\\e]8;;\\e\\\\'";
local echo_running_on = ["echo running on ${CI_RUNNER_ID}", make_clickable_link("https://gitlab.com/")];

local jobName(step, params) = step + "_" + std.join("_", [params.branch, params.platform, params.event, params.arch, params.server, params.customParams, params.customEnv]);

local generateJob(stepName, image, script, dependsOn, params, variables={}, artifacts={ paths: ["./mdb"] }, services=[]) = {
  [jobName(stepName, params)]: {
    stage: stepName,
    image: image,
    script: script,
    variables: variables,
    artifacts: artifacts,
    services: services,
    needs: [jobName(dep, params) for dep in dependsOn],
    rules: [
      {
        ["if"]: local source = if params.event == "pull_request" then "merge_request_event" else "schedule";
            local condition = if params.branch == "**"
              then '$CI_PIPELINE_SOURCE == "' + source + '"'
              else '$CI_COMMIT_BRANCH == "' + params.branch + '" && $CI_PIPELINE_SOURCE == "' + source + '"';
            condition,
        ["when"]: "always"
      }
    ],
    allow_failure: std.member(params.ignoreFailureStepList, stepName) || (stepName == "mtrlog" && std.member(params.ignoreFailureStepList, "mtr")) || (std.startsWith(stepName, "test") && std.member(params.ignoreFailureStepList, "regression"))
  }
};

local Pipeline(branch, platform, event, arch="amd64", server="10.6-enterprise", customBootstrapParamsKey="", customBuildEnvCommandsMapKey="", ignoreFailureStepList=[]) = {
  local params = { branch: branch, platform: platform, event: event, arch: arch, server: server, customParams: customBootstrapParamsKey, customEnv: customBuildEnvCommandsMapKey, ignoreFailureStepList: ignoreFailureStepList },
  local pkg_format = if std.split(platform, ":")[0] == "rockylinux" then "rpm" else "deb",
  local img = if platform == "rockylinux:8" then platform else "detravi/" + std.strReplace(platform, "/", "-"),
  local branch_ref = if branch == any_branch then current_branch else branch,
  local branchp = if branch == "**" then "" else branch + "/",
  local brancht = if branch == "**" then "" else branch + "-",
  local platformKey = std.strReplace(std.strReplace(platform, ":", ""), "/", "-"),
  local result = platformKey +
    (if customBuildEnvCommandsMapKey != "" then "_" + customBuildEnvCommandsMapKey else "") +
    (if customBootstrapParamsKey != "" then "_" + customBootstrapParamsKey else ""),
  local packages_url = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${CI_PIPELINE_ID}/" + server,
  local publish_pkg_url = "https://cspkg.s3.amazonaws.com/index.html?prefix=" + branchp + event + "/${CI_PIPELINE_ID}/" + server + "/" + arch + "/" + result + "/",
  local repo_pkg_url_no_res = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${CI_PIPELINE_ID}/" + server + "/" + arch + "/",
  local container_tags = if event == "cron" then [brancht + std.strReplace(event, "_", "-") + "${CI_PIPELINE_ID}", brancht] else [brancht + std.strReplace(event, "_", "-") + "${CI_PIPELINE_ID}"],
  local container_version = branchp + event + "/${CI_PIPELINE_ID}/" + server + "/" + arch,
  local server_remote = if std.endsWith(server, "enterprise") then "https://github.com/mariadb-corporation/MariaDBEnterprise" else "https://github.com/MariaDB/server",

  local publish(step_prefix="pkg", eventp=event + "/${CI_PIPELINE_ID}") = generateJob(
    "publish " + step_prefix,
    "amazon/aws-cli:2.22.30",
    [
      "sleep 10",
      "ls -lR ./mdb/" + builddir + "/" + result,
      "source ./mdb/" + builddir + "/storage/columnstore/columnstore/VERSION && " +
      "CURRENT_VERSION=${COLUMNSTORE_VERSION_MAJOR}.${COLUMNSTORE_VERSION_MINOR}.${COLUMNSTORE_VERSION_PATCH} && " +
      "aws s3 rm s3://cspkg/" + branchp + eventp + "/" + server + "/" + arch + "/" + result + "/ " +
      "--recursive --exclude \"*\" --include \"*columnstore*.deb\" --include \"*columnstore*.rpm\" " +
      "--exclude \"*${CURRENT_VERSION}*.deb\" --exclude \"*${CURRENT_VERSION}*.rpm\" --only-show-errors",
      "aws s3 sync ./mdb/" + builddir + "/" + result + "/ s3://cspkg/" + branchp + eventp + "/" + server + "/" + arch + "/" + result + " --only-show-errors",
      'echo "Data uploaded to: ' + publish_pkg_url + '"',
      make_clickable_link(publish_pkg_url)
    ],
    [std.strReplace(step_prefix, " latest", ""), "createrepo"],
    params,
    {
      AWS_ACCESS_KEY_ID: "$AWS_ACCESS_KEY_ID",
      AWS_SECRET_ACCESS_KEY: "$AWS_SECRET_ACCESS_KEY",
      AWS_REGION: "us-east-1",
      AWS_DEFAULT_REGION: "us-east-1"
    }
  ),

  local regression_tests = if event == "cron" then full_regression_tests else ["test000.sh", "test001.sh"],

  local mdb_server_versions = upgrade_test_lists[platformKey][arch],
  local indexes(arr) = std.range(0, std.length(arr) - 1),
  local execInnerDocker(command, containerName, flags="") = "docker exec " + flags + " -t " + containerName + " " + command,
  local getContainerName(stepname) = stepname + "${CI_PIPELINE_ID}",
  local prepareTestContainer(containerName, result, do_setup) =
    'sh -c "apk add bash && ' + get_build_command("prepare_test_container.sh") +
    " --container-name " + containerName +
    " --docker-image " + img +
    " --result-path " + result +
    " --packages-url " + packages_url +
    " --do-setup " + std.toString(do_setup) +
    (if result == "ubuntu24.04_clang-20_libcpp" then " --install-libcpp " else "") + '"',
  local reportTestStage(containerName, result, stage) =
    'sh -c "apk add bash && ' + get_build_command("report_test_stage.sh") +
    ' --container-name ' + containerName +
    ' --result-path ' + result +
    ' --stage ' + stage + '"',

  jobs:
    generateJob("submodules", "alpine/git:2.49.0", [
      "git submodule update --init --recursive",
      "git config cmake.update-submodules no",
      "git rev-parse --abbrev-ref HEAD && git rev-parse HEAD"
    ], [], params, {}) +
    generateJob("clone-mdb", "alpine/git:2.49.0", echo_running_on + [
      "echo $SERVER_REF",
      "echo $SERVER_REMOTE",
      "mkdir -p ./mdb/" + builddir + " && cd ./mdb/" + builddir,
      'git config --global url."https://github.com/".insteadOf git@github.com:',
      'git -c submodule."storage/rocksdb/rocksdb".update=none -c submodule."wsrep-lib".update=none -c submodule."storage/columnstore/columnstore".update=none clone --recurse-submodules --depth 200 --branch $SERVER_REF $SERVER_REMOTE .',
      "git reset --hard $SERVER_SHA",
      "git rev-parse --abbrev-ref HEAD && git rev-parse HEAD",
      "git config cmake.update-submodules no",
      "rm -rf storage/columnstore/columnstore",
      "cp -r $CI_PROJECT_DIR ./mdb/" + builddir + "/storage/columnstore/columnstore"
    ], ["submodules"], params, { SERVER_REF: server, SERVER_REMOTE: server_remote, SERVER_SHA: server }) +
    generateJob("build", img, [
      "mkdir ./mdb/" + builddir + "/" + result
    ] + customEnvCommands(customBuildEnvCommandsMapKey, builddir) + [
      'bash -c "set -o pipefail && ' + get_build_command("bootstrap_mcs.sh") +
      "--build-type RelWithDebInfo --distro " + platform + " --build-packages --install-deps --sccache " +
      "--build-path ./mdb/" + builddir + "/builddir " + customBootstrapParamsForExisitingPipelines(platform) + " " +
      customBuildFlags(customBootstrapParamsKey) + " | " + get_build_command("ansi2txt.sh") +
      "./mdb/" + builddir + "/" + result + '/build.log "'
    ], ["clone-mdb"], params, {
      DEBIAN_FRONTEND: "noninteractive",
      AWS_ACCESS_KEY_ID: "$AWS_ACCESS_KEY_ID",
      AWS_SECRET_ACCESS_KEY: "$AWS_SECRET_ACCESS_KEY",
      AWS_REGION: "us-east-1",
      AWS_DEFAULT_REGION: "us-east-1",
      SCCACHE_BUCKET: "cs-sccache",
      SCCACHE_REGION: "us-east-1",
      SCCACHE_S3_USE_SSL: "true",
      SCCACHE_S3_KEY_PREFIX: result + branch + server + arch
    }) +
    generateJob("cmapi build", img, [
      get_build_command("build_cmapi.sh") + " --distro " + platform
    ], ["clone-mdb"], params, { DEBIAN_FRONTEND: "noninteractive" }) +
    generateJob("createrepo", img, [
      get_build_command("createrepo.sh") + " --result " + result
    ], ["build", "cmapi build"], params) +
    generateJob("pkg", "alpine/git:2.49.0", [
      "cd ./mdb/" + builddir,
      'echo "engine: $CI_COMMIT_SHA" > buildinfo.txt',
      'echo "server: $(git rev-parse HEAD)" >> buildinfo.txt',
      'echo "buildNo: $CI_PIPELINE_ID" >> buildinfo.txt',
      'echo "serverBranch: $SERVER_REF" >> buildinfo.txt',
      'echo "serverRepo: $SERVER_REMOTE" >> buildinfo.txt',
      'echo "engineBranch: $CI_COMMIT_REF_NAME" >> buildinfo.txt',
      'echo "engineRepo: https://gitlab.com/$CI_PROJECT_PATH" >> buildinfo.txt',
      "mv buildinfo.txt ./%s/" % result,
      "yes | cp -vr ./%s/. $CI_PROJECT_DIR/%s/" % [result, result],
      "ls -l $CI_PROJECT_DIR/" + result,
      'echo "check columnstore package:"',
      "ls -l $CI_PROJECT_DIR/%s | grep columnstore" % result
    ], ["createrepo"], params, { SERVER_REF: server, SERVER_REMOTE: server_remote }) +
    publish("cmapi build") +
    publish() +
    (if event == "cron" then publish("pkg latest", "latest") else {}) +
    generateJob("smoke", "docker:28.2.2", [
      prepareTestContainer(getContainerName("smoke"), result, true),
      get_build_command("run_smoke.sh") + ' --container-name ' + getContainerName("smoke")
    ], ["publish pkg"], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
    generateJob("smokelog", "docker:28.2.2", [
      reportTestStage(getContainerName("smoke"), result, "smoke")
    ], ["smoke"], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
    publish("smokelog") +
    generateJob("cmapi test", "docker:git", [
      prepareTestContainer(getContainerName("cmapi"), result, true),
      "apk add bash && " + get_build_command("run_cmapi_test.sh") +
      " --container-name " + getContainerName("cmapi") + " --pkg-format " + pkg_format
    ], ["publish cmapi build"], params, { PYTHONPATH: "/usr/share/columnstore/cmapi/deps" }, { paths: ["./mdb"] }, ["docker:dind"]) +
    generateJob("cmapilog", "docker:28.2.2", [
      reportTestStage(getContainerName("cmapi"), result, "cmapi")
    ], ["cmapi test"], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
    publish("cmapilog") +
    (if platform == "rockylinux:8" && arch == "amd64" && customBootstrapParamsKey == "gcc-toolset"
      then generateJob("dockerfile", "alpine/git:2.49.0", [
          'echo "$DOCKER_REF"',
          'echo "$CI_COMMIT_REF_NAME"',
          'export DOCKER_REF=${DOCKER_REF:-$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-docker --h --sort origin "refs/heads/$CI_COMMIT_REF_NAME" | grep -E -o "[^/]+$")}',
          'echo "$DOCKER_REF"',
          "export DOCKER_REF=${DOCKER_REF:-$DOCKER_REF_AUX}",
          'echo "$DOCKER_REF"',
          "git clone --branch $DOCKER_REF --depth 1 https://github.com/mariadb-corporation/mariadb-columnstore-docker docker",
          "touch docker/.secrets"
        ], ["publish pkg", "publish cmapi build"], params, { DOCKER_REF_AUX: branch_ref }) +
        generateJob("dockerhub", "docker:28.2.2", [
          "docker login -u $DOCKERHUB_USER -p $DOCKERHUB_PASSWORD",
          "docker build -t mariadb/enterprise-columnstore-dev:" + container_tags[0] + " " +
          "--build-arg VERSION=" + container_version + " " +
          "--build-arg MCS_REPO=columnstore " +
          "--build-arg MCS_BASEURL=https://cspkg.s3.amazonaws.com/" + branchp + event + "/${CI_PIPELINE_ID}/" + server + "/" + arch + "/" + result + "/ " +
          "--build-arg CMAPI_REPO=cmapi " +
          "--build-arg CMAPI_BASEURL=https://cspkg.s3.amazonaws.com/" + branchp + event + "/${CI_PIPELINE_ID}/" + server + "/" + arch + "/" + result + "/ " +
          "--build-arg DEV=true " +
          "-f docker/Dockerfile docker",
          "docker push mariadb/enterprise-columnstore-dev:" + container_tags[0]
        ] + (if event == "cron" then ["docker tag mariadb/enterprise-columnstore-dev:" + container_tags[0] + " mariadb/enterprise-columnstore-dev:" + container_tags[1], "docker push mariadb/enterprise-columnstore-dev:" + container_tags[1]] else []),
        ["dockerfile"], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
        generateJob("mtr", "docker:28.2.2", [
          "echo $DOCKER_PASSWORD | docker login --username $DOCKER_LOGIN --password-stdin",
          "apk add bash && " + get_build_command("run_multi_node_mtr.sh") +
          " --columnstore-image-name ${MCS_IMAGE_NAME} --distro " + platform
        ], ["dockerhub"], params, {
          DOCKER_LOGIN: "$DOCKERHUB_USER",
          DOCKER_PASSWORD: "$DOCKERHUB_PASSWORD",
          MCS_IMAGE_NAME: "mariadb/enterprise-columnstore-dev:" + container_tags[0]
        }, { paths: ["./mdb"] }, ["docker:dind"])
      else generateJob("mtr", "docker:git", [
          prepareTestContainer(getContainerName("mtr"), result, true),
          'MTR_SUITE_LIST=$([ "$MTR_FULL_SUITE" == true ] && echo "' + mtr_full_set + '" || echo "$MTR_SUITE_LIST")',
          'apk add bash && ' + get_build_command("run_mtr.sh") +
          ' --container-name ' + getContainerName("mtr") + ' --distro ' + platform +
          ' --suite-list ${MTR_SUITE_LIST} --triggering-event ' + event
        ], ["smoke"], params, { MTR_SUITE_LIST: mtr_suite_list, MTR_FULL_SUITE: "false" }, { paths: ["./mdb"] }, ["docker:dind"]) +
        generateJob("mtrlog", "docker:28.2.2", [
          reportTestStage(getContainerName("mtr"), result, "mtr")
        ], ["mtr"], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
        publish("mtrlog")
    ) +
    std.foldl(function(acc, i) acc + generateJob(regression_tests[i], "docker:git", [
        prepareTestContainer(getContainerName("regression"), result, true),
        'export REGRESSION_REF=${REGRESSION_REF:-$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-regression-test --h --sort origin "refs/heads/$CI_COMMIT_REF_NAME" | grep -E -o "[^/]+$")}',
        "export REGRESSION_REF=${REGRESSION_REF:-$REGRESSION_REF_AUX}",
        'echo "$REGRESSION_REF"',
        "apk add bash && " + get_build_command("run_regression.sh") +
        " --container-name " + getContainerName("regression") + " --test-name " + regression_tests[i] +
        " --distro " + platform + " --regression-branch $REGRESSION_REF --regression-timeout ${REGRESSION_TIMEOUT}"
      ], if i == 0 then ["mtr", "publish pkg", "publish cmapi build"] else [regression_tests[i - 1]], params, {
        REGRESSION_BRANCH_REF: "${CI_COMMIT_REF_NAME}",
        REGRESSION_REF_AUX: branch_ref,
        REGRESSION_TIMEOUT: "$REGRESSION_TIMEOUT"
      }, { paths: ["./mdb"] }, ["docker:dind"]), indexes(regression_tests), {}) +
    generateJob("regressionlog", "docker:28.2.2", [
      reportTestStage(getContainerName("regression"), result, "regression")
    ], [regression_tests[std.length(regression_tests) - 1]], params, {}, { paths: ["./mdb"] }, ["docker:dind"]) +
    publish("regressionlog") +
    (if event == "cron" then publish("regressionlog latest", "latest") else {})
};

local AllPipelines = [
  Pipeline(b, p, e, a, s)
  for b in std.objectFields(platforms)
  for p in platforms[b]
  for s in servers[b]
  for e in events
  for a in archs
] + [
  Pipeline(any_branch, p, "custom", a, server)
  for p in platforms[current_branch]
  for server in servers[current_branch]
  for a in archs
] + [
  Pipeline(b, platform, triggeringEvent, a, server, "", buildenv)
  for a in ["amd64"]
  for b in std.objectFields(platforms)
  for platform in ["ubuntu:24.04"]
  for buildenv in std.objectFields(customEnvCommandsMap)
  for triggeringEvent in events
  for server in servers[current_branch]
] + [
  Pipeline(b, platform, triggeringEvent, a, server, flag, envcommand, ["regression", "mtr"])
  for a in ["amd64"]
  for b in std.objectFields(platforms)
  for platform in ["ubuntu:24.04"]
  for flag in ["libcpp"]
  for envcommand in ["clang-20"]
  for triggeringEvent in events
  for server in servers[current_branch]
] + [
  Pipeline(b, platform, triggeringEvent, a, server, flag, "", ["regression", "mtr"])
  for a in ["amd64"]
  for b in std.objectFields(platforms)
  for platform in ["ubuntu:24.04"]
  for flag in ["ASan", "UBSan"]
  for triggeringEvent in events
  for server in servers[current_branch]
] + [
  Pipeline(b, platform, triggeringEvent, a, server, flag, "")
  for a in ["amd64"]
  for b in std.objectFields(platforms)
  for platform in ["rockylinux:8"]
  for flag in ["gcc-toolset"]
  for triggeringEvent in events
  for server in servers[current_branch]
];

local FinalPipeline(branch, event) = {
  [jobName("notify", { branch: branch, platform: "", event: event, arch: "", server: "", customParams: "", customEnv: "", ignoreFailureStepList: [] })]: {
    stage: "notify",
    image: "curlimages/curl",
    script: [
      "curl -X POST -H 'Content-type: application/json' --data '{\"text\":\"*" +
      event + " build <$CI_JOB_URL|$CI_PIPELINE_ID> ${CI_PIPELINE_STATUS:-unknown}*.\\n\\n*Branch*: $CI_COMMIT_REF_NAME\\n*Commit*: $CI_COMMIT_SHA\\n*Author*: $GITLAB_USER_NAME\\n*Duration*: $CI_JOB_DURATION\"}' $SLACK_WEBHOOK"
    ],
    rules: [
      {
        ["if"]: local source = if event == "pull_request" then "merge_request_event" else "schedule";
            local condition = if branch == "**"
              then '$CI_PIPELINE_SOURCE == "' + source + '"'
              else '$CI_COMMIT_BRANCH == "' + branch + '" && $CI_PIPELINE_SOURCE == "' + source + '"';
            condition,
        ["when"]: "always"
      }
    ]
  }
};

local allJobsArray = [p.jobs for p in AllPipelines] + [FinalPipeline(b, "cron") for b in std.objectFields(platforms)];
local allJobs = std.foldl(function(acc, job) acc + job, allJobsArray, {});

{
  stages: [
    "submodules", "clone-mdb", "build", "cmapi build", "createrepo", "pkg",
    "publish cmapi build", "publish pkg", "publish pkg latest", "smoke", "smokelog",
    "publish smokelog", "cmapi test", "cmapilog", "publish cmapilog", "dockerfile", "dockerhub", "mtr",
    "mtrlog", "publish mtrlog"
  ] + [r for r in full_regression_tests] + [
    "regressionlog", "publish regressionlog", "publish regressionlog latest", "notify"
  ]
} + allJobs