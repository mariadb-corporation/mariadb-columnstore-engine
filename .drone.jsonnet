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

local builddir = 'verylongdirnameforverystrangecpackbehavior';

local Pipeline(branch, platform, event, arch='amd64', server='10.6-enterprise') = {
  local pkg_format = if (std.split(platform, ':')[0] == 'rockylinux') then 'rpm' else 'deb',
  local branchp = if (branch == '**') then '' else branch + '/',
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
  local img = if (platform == 'rockylinux:8') then platform else 'detravi/' + std.strReplace(platform, '/', '-'),
  local init = if (pkg_format == 'rpm') then '/usr/lib/systemd/systemd' else 'systemd',
  local ready_packages_url = 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/10982/' + server,
  local packages_url = 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/${DRONE_BUILD_NUMBER}/' + server,

  local publish_pkg_url = "https://cspkg.s3.amazonaws.com/index.html?prefix=" + branchp + event + "/10982/" + server + "/" + arch + "/" + result + "/",
  local smoke_docker_name = 'fdb_smoke_1332244444223_' + result,
  local pipeline = self,

  local execInnerDocker(command, dockerImage, flags = '') =
    'docker exec ' + flags + ' -t ' + dockerImage + ' ' + command,

  local installRpmDeb(pkg_format, rpmpackages, debpackages) =
    if (pkg_format == 'rpm')
      then ' bash -c "yum install -y ' + rpmpackages + '"'
      else ' bash -c "apt update --yes && apt install -y ' + debpackages + '"',


  publish(step_prefix='pkg',  eventp=event + '/${DRONE_BUILD_NUMBER}'):: {
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
  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event, arch, server]),
  platform: { arch: arch },
  // [if arch == 'arm64' then 'node']: { arch: 'arm64' },
  clone: { depth: 10 },
  // steps: [
  //          {
  //            name: 'build_fdb',
  //            image: img,
  //            volumes: [pipeline._volumes.mdb],
  //            commands: [
  //              if (pkg_format == 'rpm') then 'yum install -y -q wget' else 'apt update --yes && apt install -q -y wget',
  //              'wget https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/fdb_build/tests/scripts/fdb_build.sh',
  //              'bash fdb_build.sh',
  //              'mkdir -p  /drone/src/' + result,
  //              if (pkg_format == 'rpm') then 'cp /drone/src/fdb_build/packages/*.rpm /drone/src/' + result else 'cp /drone/src/fdb_build/packages/*.deb /drone/src/' + result,
  //            ],
  //          },
  //       ] +
  //       [pipeline.publish('build_fdb')] +
  //       [
  //          {
  //            name: 'publish pkg url',
  //            depends_on: ['publish build_fdb'],
  //            image: 'alpine/git',
  //            commands: [
  //              "echo -e '\\e]8;;" + publish_pkg_url + '\\e\\\\' + publish_pkg_url + "\\e]8;;\\e\\\\'",
  //              "echo 'for installation run:'",
  //              "echo 'export OS="+result+"'",
  //              "echo 'export PACKAGES_URL="+packages_url+"'",
  //            ],
  //          },
  //        ] +

         steps: [
          {
             name: 'smoke check installation',
             depends_on: [],
             image: 'docker',
             volumes: [pipeline._volumes.docker],
             commands: [
                'docker run --memory 3g --env OS=' + result + ' --env PACKAGES_URL=' + ready_packages_url + ' --env DEBIAN_FRONTEND=noninteractive --name ' + smoke_docker_name + ' --ulimit core=-1 --privileged --detach ' + img + ' ' + init + ' --unit=basic.target',
                'wget https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/develop/setup-repo.sh -O setup-repo.sh',
                'docker cp setup-repo.sh ' + smoke_docker_name  +  ':/',
                execInnerDocker('bash /setup-repo.sh', smoke_docker_name),
                execInnerDocker(installRpmDeb(pkg_format, 'foundationdb-server', 'foundationdb-clients jq'), smoke_docker_name),
                execInnerDocker("fdbcli --exec 'writemode on; set foo bar; get foo'", smoke_docker_name),
                execInnerDocker('service foundationdb status', smoke_docker_name),
             ],
           },
         ]
        ,

  volumes: [pipeline._volumes.mdb { temp: {} }, pipeline._volumes.docker { host: { path: '/var/run/docker.sock' } }],
  trigger: {
    event: [event],
    branch: [branch],
  } + (if event == 'cron' then {
         cron: ['nightly-' + std.strReplace(branch, '.', '-')],
       } else {}),
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
  Pipeline(any_branch, p, 'custom', 'amd64', '10.6-enterprise')
  for p in platforms_custom
] +
[
  Pipeline(any_branch, p, 'custom', 'arm64', '10.6-enterprise')
  for p in platforms_arm_custom
]
