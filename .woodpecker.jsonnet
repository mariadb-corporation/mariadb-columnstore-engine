// =============================================================================
// MariaDB ColumnStore CI pipeline — Woodpecker, generated via Jsonnet.
// =============================================================================
//
// Regenerate .woodpecker/*.yml after editing this file:
//
//     ./scripts/ci/generate_woodpecker.sh
//
// This replaces .drone.jsonnet, removed in the same commit so Drone stops
// running the old pipeline alongside this one.  The one structural
// difference: .drone.jsonnet generated EVERY pipeline of the matrix for
// BOTH events (`events = ["pull_request", "cron"]`), so a PR paid for the
// full nightly matrix.  Here the matrix is split by trigger:
//
//   prMatrix       — what every pull request pays for.  Deliberately small.
//   nightlyMatrix  — the broad matrix (all platforms, extra servers,
//                    sanitizers, clang/libc++/gcc-toolset, regression,
//                    upgrade).  Currently EMPTY: nightly is not generated
//                    yet, only designed for.  Filling this list in is the
//                    only change needed to turn nightly on.
//
// Workflow topology (one generated .woodpecker/*.yml per entry):
//
//   1. submodules              — init the engine's own submodules
//   2. check-jsonnet-ymls-sync — fail early if .woodpecker/*.yml is stale
//   3. clone-mdb               — clone the server, drop this checkout into
//                                storage/columnstore/columnstore
//   4. build                   — bootstrap_mcs.sh: build + package MCS
//   4b. cmapi-build            — build the CMAPI package (parallel with build)
//   5. createrepo / pkg        — assemble the repo + buildinfo into the
//                                workspace result dir
//   6. publish-*               — aws s3 sync the result dir to cspkg
//   7. test groups             — smoke / cmapi / mtr (see testGroups below);
//                                each group is <run> + <log> + publish-<log>
//
// Every test group installs the packages the pipeline just published, so
// the publish steps are load-bearing, not just artefact retention.
//
// Naming: Drone step names had spaces ("cmapi build", "publish pkg").
// Woodpecker resolves depends_on by exact name, so they are dashed here
// ("cmapi-build", "publish-pkg").
//
// Env var mapping from Drone (used throughout):
//   DRONE_BUILD_NUMBER  -> CI_PIPELINE_NUMBER
//   DRONE_COMMIT        -> CI_COMMIT_SHA
//   DRONE_SOURCE_BRANCH -> CI_COMMIT_SOURCE_BRANCH
//   DRONE_REPO          -> CI_REPO
//   DRONE_STAGE_MACHINE -> CI_MACHINE
//   /drone/src          -> $CI_WORKSPACE

// Optional-field accessor.  std.get() would do, but it is jsonnet >= 0.20 and
// the sync check runs against whatever jsonnet the CI image ships.
local field(o, k, d) = if std.objectHas(o, k) then o[k] else d;

local current_branch = 'stable-23.10';

local builddir = 'verylongdirnameforverystrangecpackbehavior';

// The MariaDB build tree lives beside the repo checkout, inside Woodpecker's
// workspace volume. That volume is created per pipeline run and removed with
// it, which is exactly what Drone's `{name: "mdb", temp: {}}` gave us: two
// concurrent runs of the same variant can no longer share (and wipe) one
// tree, and nothing accumulates on the agent hosts.
//
// It is reached through a /mdb symlink rather than by its real path, because
// /mdb is hardcoded inside the build scripts themselves -- createrepo.sh
// (which is what caught this), run_regression.sh, run_infer.sh -- and in
// whatever else grows one later. A symlink keeps every such path working
// untouched, under Woodpecker and under any other runner. Each step gets a
// fresh container, so each one that touches the tree has to re-create it;
// `rm -rf` clears a stale symlink only, never the tree it points at.
local mdbRoot = '/mdb';

local linkMdb = [
  'mkdir -p "$${CI_WORKSPACE}/../mdb"',
  'rm -rf /mdb',
  'ln -s "$${CI_WORKSPACE}/../mdb" /mdb',
];

local get_build_command(command) =
  'bash ' + mdbRoot + '/' + builddir + '/storage/columnstore/columnstore/build/' + command + ' ';

// ---------------------------------------------------------------------------
// Build variants.  `flag` goes to bootstrap_mcs.sh, `env` prepends a compiler
// bootstrap to the build step.  The PR matrix uses neither; they exist so the
// nightly matrix can be filled in without touching any step definition.
// ---------------------------------------------------------------------------
local clang(version) = [
  get_build_command('install_clang_deb.sh ' + version),
  get_build_command('update-clang-version.sh ' + version + ' 100'),
  get_build_command('install_libc++.sh ' + version),
  'export CC=/usr/bin/clang',
  'export CXX=/usr/bin/clang++',
];

local customEnvCommandsMap = {
  'clang-20': clang('20'),
};

local customEnvCommands(envkey) =
  if std.objectHas(customEnvCommandsMap, envkey) then customEnvCommandsMap[envkey] else [];

// 'Regr' variants are the regression half of the split sanitizer stages.
// Distinct keys give them their own result path, so the two halves
// publish/install independently.
local customBuildFlagsMap = {
  ASan: '--asan',
  ASanRegr: '--asan',
  TSAN: '--tsan',
  UBSan: '--ubsan',
  UBSanRegr: '--ubsan',
  MSan: '--msan',
  libcpp: '--libcpp --skip-unit-tests',
  'gcc-toolset': '--gcc-toolset-for-rocky-8',
};

local customBuildFlags(buildKey) =
  if std.objectHas(customBuildFlagsMap, buildKey) then customBuildFlagsMap[buildKey] else '';

// errorprone if we pass --custom-cmake-flags twice, the last one will win
local customBootstrapParamsMap = {
  //'ubuntu:24.04': "--custom-cmake-flags '-DCOLUMNSTORE_ASAN_FOR_UNITTESTS=YES'",
};

local customBootstrapParams(platform) =
  if std.objectHas(customBootstrapParamsMap, platform) then customBootstrapParamsMap[platform] else '';

local default_upgrade_versions = ['10.6.15-10', '10.6.24-20'];
local default_arch_versions = {
  arm64: default_upgrade_versions,
  amd64: default_upgrade_versions,
};

local upgrade_test_lists = {
  rockylinux8: default_arch_versions,
  rockylinux9: default_arch_versions,
  rockylinux10: default_arch_versions,
  debian12: default_arch_versions,
  debian13: default_arch_versions,
  'ubuntu22.04': default_arch_versions,
  'ubuntu24.04': default_arch_versions,
  'ubuntu26.04': default_arch_versions,
};

// Normalise signal-killed exits (>=128) to 1 so the step records a plain
// failure instead of a killed status; `failure: ignore` then suppresses
// pipeline failure while the step itself remains red (visible in the UI).
local normaliseKilledExit = ' || { ec=$$?; [ $$ec -ge 128 ] && exit 1; exit $$ec; }';

// Per-step hard time limits (seconds). A hung command then fails red with its
// logs still collected by the corresponding *log step, instead of stalling
// the stage until the CI-wide limit kills it with no diagnostics.
local prepare_step_timeout = '1800';  // 30m; package installs normally ~10m
local smoke_step_timeout = '1800';  // 30m; normally minutes
local mtr_step_timeout = '18000';  // 5h; ASan full suite runs ~4h sometimes
local cmapi_step_timeout = '3600';  // 1h; ~15m under ASan
local upgrade_step_timeout = '3600';  // 1h; normally minutes
local regression_step_timeout = '10800';  // 3h per regression test: slow for sanitizer builds

// Plain echo, no OSC-8 hyperlink. Drone could emit one because it substituted
// its ${DRONE_*} vars at config time, leaving the escape sequence safely inside
// shell single quotes. Here the CI_* vars have to be expanded by the shell at
// runtime, which needs double quotes -- and the `\\` that OSC-8 needs before the
// closing quote is collapsed to a single `\` on its way to the shell, so it
// escaped the quote instead and the step died with "unterminated quoted string".
// The URL alone carries the same information and survives any quoting, so the
// escape sequence is not worth the fragility. `$` in the URL is written %24 for
// the same reason: nothing then needs escaping at all.
local echo_running_on = [
  'echo running on $${CI_MACHINE}',
  'echo "https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#Instances:search=:$${CI_MACHINE};v=3;%24case=tags:true%5C,client:false;%24regex=tags:false%5C,client:false;sort=desc:launchTime"',
];

local awsEnv = {
  AWS_ACCESS_KEY_ID: { from_secret: 'aws_access_key_id' },
  AWS_SECRET_ACCESS_KEY: { from_secret: 'aws_secret_access_key' },
  AWS_REGION: 'us-east-1',
  AWS_DEFAULT_REGION: 'us-east-1',
};

local dockerSocket = '/var/run/docker.sock:/var/run/docker.sock';

// ---------------------------------------------------------------------------
// Pipeline.
//
// spec fields:
//   platform     — distro image key, e.g. 'ubuntu:26.04'
//   server       — MariaDB server branch, e.g. '11.8-enterprise'
//   arch         — 'amd64' (arm64 runners are not wired up yet)
//   event        — 'pull_request' or 'cron'; drives both the trigger and the
//                  S3 artefact prefix
//   buildKey     — key into customBuildFlagsMap, '' for a vanilla build
//   envKey       — key into customEnvCommandsMap, '' for the distro default
//   testGroups   — which test groups to run, any of
//                  'smoke' / 'cmapi' / 'mtr' / 'upgrade' / 'regression'
//   ignoreFailure — step names whose failure must not fail the pipeline.
//                  Defaults to the known-flaky survivability test.
// ---------------------------------------------------------------------------
local Pipeline(spec) = {
  local platform = spec.platform,
  local server = spec.server,
  local arch = field(spec, 'arch', 'amd64'),
  local event = spec.event,
  local buildKey = field(spec, 'buildKey', ''),
  local envKey = field(spec, 'envKey', ''),
  local testGroups = field(spec, 'testGroups', ['smoke', 'cmapi', 'mtr']),
  local ignoreFailure = field(spec, 'ignoreFailure', ['test400.sh']),

  local pkg_format = if std.split(platform, ':')[0] == 'rockylinux' then 'rpm' else 'deb',
  local img = if platform == 'rockylinux:8' then platform else 'detravi/' + std.strReplace(platform, '/', '-'),
  local platformKey = std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),

  // The per-pipeline artefact directory name; also the S3 leaf and the
  // sccache key component, so every variant stays isolated from the others.
  local result = platformKey +
                 (if envKey != '' then '_' + envKey else '') +
                 (if buildKey != '' then '_' + buildKey else ''),

  local branchp = current_branch + '/',
  local eventDir = event + '/$${CI_PIPELINE_NUMBER}',

  local packages_url = 'https://cspkg.s3.amazonaws.com/' + branchp + eventDir + '/' + server,
  local publish_pkg_url = 'https://cspkg.s3.amazonaws.com/index.html?prefix=' + branchp + eventDir + '/' + server + '/' + arch + '/' + result + '/',
  local repo_pkg_url_no_res = 'https://cspkg.s3.amazonaws.com/' + branchp + eventDir + '/' + server + '/' + arch + '/',

  local server_remote = if std.endsWith(server, 'enterprise')
    then 'https://github.com/mariadb-corporation/MariaDBEnterprise'
    else 'https://github.com/MariaDB/server',

  local pipeline = self,

  local ignored(name) = std.member(ignoreFailure, name),

  // A step whose failure is tolerated must still be visibly red, hence
  // `failure: ignore` rather than a swallowed exit code.
  local tolerate(name) = if ignored(name) then { failure: 'ignore' } else {},
  local tolerateExit(name) = if ignored(name) then normaliseKilledExit else '',

  // Log-collecting steps run whether or not the tested step passed.
  local alwaysRun = { when: [{ status: ['success', 'failure'] }] },

  publish(anchor, nameSuffix='', eventPath=eventDir):: {
    name: 'publish-' + anchor + nameSuffix,
    depends_on: [anchor, 'createrepo'],
    image: 'amazon/aws-cli:2.22.30',
    environment: awsEnv,
    commands: [
      'sleep 10',
      'ls -lR ' + result,

      // Drop stale .deb/.rpm of OTHER versions from this prefix, so a rebuilt
      // package number never leaves two candidate versions in one repo.
      'source ' + mdbRoot + '/' + builddir + '/storage/columnstore/columnstore/VERSION && ' +
      'CURRENT_VERSION=$${COLUMNSTORE_VERSION_MAJOR}.$${COLUMNSTORE_VERSION_MINOR}.$${COLUMNSTORE_VERSION_PATCH} && ' +
      'aws s3 rm s3://cspkg/' + branchp + eventPath + '/' + server + '/' + arch + '/' + result + '/ ' +
      '--recursive ' +
      '--exclude "*" ' +
      '--include "*columnstore*.deb" ' +
      '--include "*columnstore*.rpm" ' +
      '--exclude "*$${CURRENT_VERSION}*.deb" ' +
      '--exclude "*$${CURRENT_VERSION}*.rpm" ' +
      '--only-show-errors',

      'aws s3 sync ' + result + '/ s3://cspkg/' + branchp + eventPath + '/' + server + '/' + arch + '/' + result + ' --only-show-errors',
      'echo "Data uploaded to: ' + publish_pkg_url + '"',
    ],
  } + alwaysRun,

  // The nightly regression sweep vs. the short PR smoke set.
  local regression_tests = if event == 'cron' then [
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
    //  "test222.sh", FIXME: restore the test
    'test297.sh',
    'test299.sh',
    'test400.sh',
    'test500.sh',
  ] else [
    'test000.sh',
    'test001.sh',
  ],

  local mdb_server_versions = upgrade_test_lists[platformKey][arch],

  local indexes(arr) = std.range(0, std.length(arr) - 1),

  local execInnerDocker(command, containerName, flags='') =
    'docker exec ' + flags + ' -t ' + containerName + ' ' + command,

  // `result` disambiguates the workflows of a single pipeline: they share
  // CI_PIPELINE_NUMBER and bind-mount the same host docker socket, so a bare
  // "smoke<N>" would collide -- and report_test_stage.sh's cleanup trap
  // (docker rm -f) would kill the other workflow's container.
  local getContainerName(stepname) = stepname + '-' + result + '$${CI_PIPELINE_NUMBER}',

  local prepareTestContainer(containerName, do_setup, do_install, do_install_builddeps) =
    'sh -c "apk add bash && timeout ' + prepare_step_timeout + ' ' + get_build_command('prepare_test_container.sh') +
    ' --container-name ' + containerName +
    ' --docker-image ' + img +
    ' --result-path ' + result +
    ' --packages-url ' + packages_url +
    ' --do-setup ' + std.toString(do_setup) +
    ' --do-install ' + std.toString(do_install) +
    ' --do-install-builddeps ' + std.toString(do_install_builddeps) +
    (if result == 'ubuntu24.04_clang-20_libcpp' then ' --install-libcpp ' else '') +
    '"',

  local reportTestStage(containerName, stage) =
    'sh -c "apk add bash && ' + get_build_command('report_test_stage.sh') +
    ' --container-name ' + containerName +
    ' --result-path ' + result +
    ' --stage ' + stage + '"',

  // ---- test steps -------------------------------------------------------
  smoke:: {
    name: 'smoke',
    depends_on: ['publish-pkg'],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: [
      prepareTestContainer(getContainerName('smoke'), true, true, true),
      'timeout ' + smoke_step_timeout + ' ' +
      get_build_command('run_smoke.sh') +
      ' --container-name ' + getContainerName('smoke') +
      tolerateExit('smoke'),
    ],
  } + tolerate('smoke'),

  smokelog:: {
    name: 'smokelog',
    depends_on: ['smoke'],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: [reportTestStage(getContainerName('smoke'), 'smoke')],
  } + alwaysRun + tolerate('smoke'),

  cmapitest:: {
    name: 'cmapi-test',
    depends_on: ['publish-cmapi-build'],
    image: 'docker:git',
    volumes: [dockerSocket],
    environment: {
      PYTHONPATH: '/usr/share/columnstore/cmapi/deps',
    },
    commands: [
      prepareTestContainer(getContainerName('cmapi'), true, true, true),
      'apk add bash && ' +
      'timeout ' + cmapi_step_timeout + ' ' +
      get_build_command('run_cmapi_test.sh') +
      ' --container-name ' + getContainerName('cmapi') +
      ' --pkg-format ' + pkg_format +
      tolerateExit('cmapi-test'),
    ],
  } + tolerate('cmapi-test'),

  cmapilog:: {
    name: 'cmapilog',
    depends_on: ['cmapi-test'],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: [reportTestStage(getContainerName('cmapi'), 'cmapi')],
  } + alwaysRun + tolerate('cmapi-test'),

  mcs_cli_docs_check:: {
    name: 'mcs-cli-docs-check',
    depends_on: ['publish-cmapi-build'],
    image: 'docker:git',
    volumes: [dockerSocket],
    commands: [
      prepareTestContainer(getContainerName('cmapi-docs'), true, true, true),
      'apk add bash && ' +
      get_build_command('check_mcs_cli_docs.sh') +
      ' --container-name ' + getContainerName('cmapi-docs'),
    ],
  },

  mtr:: {
    name: 'mtr',
    // Serialised behind smoke on purpose: both drive a container on the same
    // runner, and a broken build should surface as a smoke failure first.
    // Guarded like regression's mtr dependency: a testGroups list without
    // 'smoke' would otherwise emit a dangling depends_on, which Woodpecker
    // rejects outright.
    depends_on: if std.member(testGroups, 'smoke') then ['smoke'] else ['publish-pkg'],
    image: 'docker:git',
    volumes: [dockerSocket],
    // Custom pipeline variables from a manual run are exposed as step env
    // vars but do NOT feed Woodpecker's `environment:` substitution, so the
    // default is applied in the shell below instead of here.
    environment: {
      MTR_FULL_SUITE: '${MTR_FULL_SUITE}',
    },
    commands: [
      ': "$${MTR_FULL_SUITE:=false}"',
      prepareTestContainer(getContainerName('mtr'), true, true, true),
      'apk add bash && ' +
      'timeout ' + mtr_step_timeout + ' ' +
      get_build_command('run_mtr.sh') +
      ' --container-name ' + getContainerName('mtr') +
      ' --distro ' + platform +
      ' --run-as-extern' +
      ' --triggering-event ' + event +
      ' --full-mtr $${MTR_FULL_SUITE}' +
      (if std.endsWith(result, 'ASan') then ' --run-as-extern' else '') +
      tolerateExit('mtr'),
    ],
  } + tolerate('mtr'),

  mtrlog:: {
    name: 'mtrlog',
    depends_on: ['mtr'],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: [reportTestStage(getContainerName('mtr'), 'mtr')],
  } + alwaysRun + tolerate('mtr'),

  upgrade(version):: {
    name: 'upgrade-test-from-' + version,
    depends_on: ['publish-pkg', 'publish-cmapi-build'],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    environment: {
      UPGRADE_TOKEN: { from_secret: 'es_token' },
    },
    commands: [
      prepareTestContainer(getContainerName('upgrade') + version, false, false, false),
      'timeout ' + upgrade_step_timeout + ' ' +
      execInnerDocker(
        'bash -c "./upgrade_setup_' + pkg_format + '.sh ' +
        version + ' ' + result + ' ' + arch + ' ' + repo_pkg_url_no_res +
        ' $${UPGRADE_TOKEN} ' + server + '"',
        getContainerName('upgrade') + version
      ) + tolerateExit('upgrade'),
    ],
  } + tolerate('upgrade'),

  upgradelog:: {
    name: 'upgradelog',
    depends_on: std.map(function(v) 'upgrade-test-from-' + v, mdb_server_versions),
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: ['echo'] + std.map(
      function(ver) reportTestStage(getContainerName('upgrade') + ver, 'upgrade_' + ver),
      mdb_server_versions
    ),
  } + alwaysRun + tolerate('upgrade'),

  regression(name, depends_on):: {
    name: name,
    depends_on: depends_on,
    image: 'docker:git',
    volumes: [dockerSocket],
    environment: {
      REGRESSION_TIMEOUT: { from_secret: 'regression_timeout' },
      REGRESSION_REF_AUX: current_branch,
    },
    commands: [
      prepareTestContainer(getContainerName('regression'), true, true, true),

      // REGRESSION_REF can be empty if there is no appropriate branch in the
      // regression repository; then try a branch named as the one we PR,
      // and finally fall back to the engine branch.
      'export REGRESSION_REF=$${REGRESSION_REF:-$$(git ls-remote https://github.com/mariadb-corporation/mariadb-columnstore-regression-test --h --sort origin "refs/heads/$${CI_COMMIT_SOURCE_BRANCH}" | sed "s#.*refs/heads/##")}',
      'export REGRESSION_REF=$${REGRESSION_REF:-$$REGRESSION_REF_AUX}',
      'echo "$$REGRESSION_REF"',

      'apk add bash && ' +
      'timeout ' + regression_step_timeout + ' ' +
      get_build_command('run_regression.sh') +
      ' --container-name ' + getContainerName('regression') +
      ' --test-name ' + name +
      (if ignored(name) || ignored('regression') then ' --ignore-cores' else '') +
      ' --distro ' + platform +
      ' --regression-branch $$REGRESSION_REF' +
      ' --regression-timeout $${REGRESSION_TIMEOUT}' +
      (if ignored(name) || ignored('regression') then normaliseKilledExit else ''),
    ],
  } + alwaysRun + (if ignored(name) || ignored('regression') then { failure: 'ignore' } else {}),

  regressionlog:: {
    name: 'regressionlog',
    depends_on: [regression_tests[std.length(regression_tests) - 1]],
    image: 'docker:28.2.2',
    volumes: [dockerSocket],
    commands: [reportTestStage(getContainerName('regression'), 'regression')],
  } + alwaysRun + tolerate('regression'),

  // ---- test step groups -------------------------------------------------
  local smoke_steps = [pipeline.smoke, pipeline.smokelog, pipeline.publish('smokelog')],
  local cmapi_steps = [pipeline.cmapitest, pipeline.cmapilog, pipeline.publish('cmapilog')] +
                      (if platform == 'rockylinux:9' && arch == 'amd64' && server == '10.6-enterprise'
                       then [pipeline.mcs_cli_docs_check] else []),
  local mtr_steps = [pipeline.mtr, pipeline.mtrlog, pipeline.publish('mtrlog')],
  // The regression chain is serial: the first test waits for the given
  // dependencies, every other test waits for the previous one.  When mtr also
  // runs, regression additionally waits for it, serialising the two heavy
  // test groups onto one runner.
  local regression_first_deps = ['publish-pkg', 'publish-cmapi-build'] +
                                (if std.member(testGroups, 'mtr') then ['mtr'] else []),
  local regression_steps =
    [
      pipeline.regression(regression_tests[i],
                          if i == 0 then regression_first_deps else [regression_tests[i - 1]])
      for i in indexes(regression_tests)
    ] +
    [pipeline.regressionlog, pipeline.publish('regressionlog')],
  local upgrade_steps =
    [pipeline.upgrade(mdb_server_versions[i]) for i in indexes(mdb_server_versions)] +
    (if std.length(mdb_server_versions) == 0 then []
     else [pipeline.upgradelog, pipeline.publish('upgradelog')]),

  local groupSteps = {
    smoke: smoke_steps,
    cmapi: cmapi_steps,
    mtr: mtr_steps,
    upgrade: upgrade_steps,
    regression: regression_steps,
  },
  local test_steps = std.flattenArrays([groupSteps[g] for g in testGroups]),

  // `pkg latest` is a moving pointer to the newest nightly artefacts; a PR
  // must never move it.
  local publish_latest_steps = if event == 'cron' then [pipeline.publish('pkg', '-latest', 'latest')] else [],

  // ---- pipeline ---------------------------------------------------------
  name: std.join('-', [
    if event == 'pull_request' then 'pr' else event,
    platformKey,
    server,
  ] + (if buildKey != '' then [buildKey] else [])
    + (if envKey != '' then [envKey] else [])),

  labels: {
    platform: 'linux/' + arch,
  },

  when: [
    if event == 'pull_request'
    then { event: ['pull_request', 'manual'], branch: current_branch }
    // NB: no `cron:` key, so a cron pipeline here matches ANY cron job
    // configured for the repo. Add `cron: '<job name>'` once more than one
    // schedule exists (Woodpecker matches the job name literally).
    else { event: ['cron'], branch: current_branch },
  ],

  clone: {
    git: {
      image: 'woodpeckerci/plugin-git',
      settings: {
        recursive: false,
        depth: 10,
        partial: false,
      },
    },
  },

  // Prepended in one place rather than per step: every step but the first two
  // reaches the build tree through /mdb, and a step that silently lacked the
  // symlink would fail deep in a build script rather than obviously here.
  local withMdbLink(step) =
    if std.member(['submodules', 'check-jsonnet-ymls-sync'], step.name)
    then step
    else step { commands: linkMdb + step.commands },

  steps: std.map(withMdbLink, [
    {
      name: 'submodules',
      image: 'alpine/git:2.49.0',
      commands: [
        'git config --global --add safe.directory "$${CI_WORKSPACE}"',
        'git submodule update --init --recursive',
        'git config cmake.update-submodules no',
        'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
      ],
    },
    {
      name: 'check-jsonnet-ymls-sync',
      depends_on: ['submodules'],
      image: 'alpine/git:2.49.0',
      commands: [
        'apk add --no-cache -q bash jsonnet python3 py3-yaml',
        'bash ./scripts/ci/generate_woodpecker.sh --check',
      ],
    },
    {
      name: 'clone-mdb',
      depends_on: ['check-jsonnet-ymls-sync'],
      image: 'alpine/git:2.49.0',
        // Bare pass-through + shell defaults: a manual run's custom variables
      // reach the step environment but not Woodpecker's `environment:`
      // substitution, so defaulting here would silently win over them.
      environment: {
        SERVER_REF: '${SERVER_REF}',
        SERVER_REMOTE: '${SERVER_REMOTE}',
        SERVER_SHA: '${SERVER_SHA}',
        GITHUB_TOKEN: { from_secret: 'github_token' },
      },
      commands: echo_running_on + [
        ': "$${SERVER_REF:=' + server + '}"',
        ': "$${SERVER_REMOTE:=' + server_remote + '}"',
        ': "$${SERVER_SHA:=' + server + '}"',
        'export SERVER_REF SERVER_REMOTE SERVER_SHA',
        'echo $$SERVER_REF',
        'echo $$SERVER_REMOTE',
        // MariaDBEnterprise is private. Drone injected a netrc into every step
        // container, so the clone just worked; Woodpecker hands netrc to the
        // clone plugin only, hence the explicit credential here.  A netrc
        // (rather than a token-bearing remote URL) keeps the token out of
        // .git/config, which travels with the build tree, and still covers the
        // recursive submodule clones, which fetch their own URLs.
        ': "$${GITHUB_TOKEN:?github_token secret is required to clone ' + server_remote + '}"',
        'printf "machine github.com login x-access-token password %s\\n" "$$GITHUB_TOKEN" > $$HOME/.netrc',
        'chmod 600 $$HOME/.netrc',
        // The volume is reused across runs of this variant; start clean.
        'rm -rf /mdb/* || true',
        'mkdir -p ' + mdbRoot + '/' + builddir + ' && cd ' + mdbRoot + '/' + builddir,
        // Rewrite ssh submodule URLs to https so they go through the netrc too.
        'git config --global url."https://github.com/".insteadOf git@github.com:',
        'git -c submodule."storage/rocksdb/rocksdb".update=none -c submodule."wsrep-lib".update=none -c submodule."storage/columnstore/columnstore".update=none clone --recurse-submodules --depth 200 --branch $$SERVER_REF $$SERVER_REMOTE .',
        'git reset --hard $$SERVER_SHA',
        'git rev-parse --abbrev-ref HEAD && git rev-parse HEAD',
        'git config cmake.update-submodules no',
        'rm -rf storage/columnstore/columnstore',
        'cp -r "$${CI_WORKSPACE}" ' + mdbRoot + '/' + builddir + '/storage/columnstore/columnstore',
      ],
    },
    {
      name: 'build',
      depends_on: ['clone-mdb'],
      image: img,
        environment: awsEnv {
        DEBIAN_FRONTEND: 'noninteractive',
        SCCACHE_BUCKET: 'cs-sccache',
        SCCACHE_REGION: 'us-east-1',
        SCCACHE_S3_USE_SSL: 'true',
        SCCACHE_S3_KEY_PREFIX: result + current_branch + server + arch,
      },
      // errorprone if we pass --custom-cmake-flags twice, the last one will win
      commands: ['mkdir ' + mdbRoot + '/' + builddir + '/' + result] +
                customEnvCommands(envKey) +
                [
                  'bash -c "set -o pipefail && ' +
                  get_build_command('bootstrap_mcs.sh') +
                  '--build-type RelWithDebInfo ' +
                  '--distro ' + platform + ' ' +
                  '--build-packages --install-deps --sccache ' +
                  '--build-path ' + mdbRoot + '/' + builddir + '/builddir ' +
                  ' ' + customBootstrapParams(platform) +
                  ' ' + customBuildFlags(buildKey) +
                  ' 2>&1 | ' + get_build_command('ansi2txt.sh') +
                  mdbRoot + '/' + builddir + '/' + result + '/build.log "',
                ],
    },
    {
      name: 'cmapi-build',
      depends_on: ['clone-mdb'],
      image: img,
        environment: {
        DEBIAN_FRONTEND: 'noninteractive',
      },
      commands: [
        // Exported here rather than set in `environment:`: values there are
        // substituted at config-parse time only, so a $$-escaped CI_* var
        // would reach build_cmapi.sh as the literal string "${CI_COMMIT_SHA}"
        // and get baked into the package version. In a command the shell
        // expands it against the CI_* vars Woodpecker injects at runtime.
        'export CMAPI_GIT_REVISION="$${CI_COMMIT_SHA}"',
        get_build_command('build_cmapi.sh') + ' --distro ' + platform,
      ],
    },
    {
      name: 'createrepo',
      depends_on: ['build', 'cmapi-build'],
      image: img,
        commands: [
        get_build_command('createrepo.sh') + ' --result ' + result,
      ],
    } + alwaysRun,
    {
      name: 'pkg',
      depends_on: ['createrepo'],
      image: 'alpine/git:2.49.0',
        environment: {
        SERVER_REF: '${SERVER_REF}',
        SERVER_REMOTE: '${SERVER_REMOTE}',
      },
      commands: [
        ': "$${SERVER_REF:=' + server + '}"',
        ': "$${SERVER_REMOTE:=' + server_remote + '}"',
        'cd ' + mdbRoot + '/' + builddir,
        'echo "engine: $${CI_COMMIT_SHA}" > buildinfo.txt',
        'echo "server: $$(git rev-parse HEAD)" >> buildinfo.txt',
        'echo "buildNo: $${CI_PIPELINE_NUMBER}" >> buildinfo.txt',
        'echo "serverBranch: $$SERVER_REF" >> buildinfo.txt',
        'echo "serverRepo: $$SERVER_REMOTE" >> buildinfo.txt',
        'echo "engineBranch: $${CI_COMMIT_SOURCE_BRANCH}" >> buildinfo.txt',
        'echo "engineRepo: $${CI_REPO_URL}" >> buildinfo.txt',
        'mv buildinfo.txt ./' + result + '/',
        'yes | cp -vr ./' + result + '/. "$${CI_WORKSPACE}/' + result + '/"',
        'ls -l "$${CI_WORKSPACE}/' + result + '"',
        'echo "check columnstore package:"',
        'ls -l "$${CI_WORKSPACE}/' + result + '" | grep columnstore',
      ],
    } + alwaysRun,
  ] + [
    pipeline.publish('cmapi-build'),
    pipeline.publish('pkg'),
  ] + publish_latest_steps + test_steps),
};

// ---------------------------------------------------------------------------
// The per-PR matrix.  Small on purpose: a pull request pays for two distro
// families (deb + rpm) on the newest supported server, and for the three
// fast test groups only.  Everything else belongs to nightly.
// ---------------------------------------------------------------------------
local prMatrix = [
  { platform: 'ubuntu:26.04', server: '11.8-enterprise', event: 'pull_request', testGroups: ['smoke', 'cmapi', 'mtr'] },
  { platform: 'rockylinux:9', server: '11.8-enterprise', event: 'pull_request', testGroups: ['smoke', 'cmapi', 'mtr'] },
];

// ---------------------------------------------------------------------------
// The nightly matrix.  EMPTY BY DESIGN — nightly is not generated yet.
//
// Everything a nightly entry needs is already implemented above (the broad
// platform/server sweep, the sanitizer and compiler variants via
// buildKey/envKey, the `upgrade` and `regression` test groups, the `latest`
// publish that only fires on `event: 'cron'`).  Turning nightly on is a
// matter of listing the entries here and regenerating, e.g.:
//
//   local nightlyMatrix = [
//     { platform: p, server: '10.6-enterprise', event: 'cron',
//       testGroups: ['smoke', 'cmapi', 'mtr', 'upgrade', 'regression'] }
//     for p in ['rockylinux:8', 'rockylinux:9', 'rockylinux:10',
//               'debian:12', 'ubuntu:22.04', 'ubuntu:24.04']
//   ] + [
//     { platform: 'ubuntu:24.04', server: '10.6-enterprise', event: 'cron',
//       buildKey: k, testGroups: ['smoke', 'cmapi', 'mtr'],
//       ignoreFailure: ['cmapi-test', 'upgrade', 'test400.sh'] }
//     for k in ['ASan', 'UBSan']
//   ];
//
// Still unported from .drone.jsonnet, and needed before the nightly list is
// filled in: the dockerfile/dockerhub publish chain with the multi-node MTR
// that depends on it (rockylinux:8 + gcc-toolset only), the Infer/SAST
// pipeline, and the Slack notification pipeline that fans in from every
// nightly workflow.
// ---------------------------------------------------------------------------
local nightlyMatrix = [];

std.map(Pipeline, prMatrix + nightlyMatrix)
