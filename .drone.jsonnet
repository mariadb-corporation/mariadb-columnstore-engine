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
  local brancht = if (branch == '**') then '' else branch + '-',
  local result = std.strReplace(std.strReplace(platform, ':', ''), '/', '-'),
  local img = if (platform == 'rockylinux:8') then platform else 'detravi/' + std.strReplace(platform, '/', '-'),
  local packages_url = 'https://cspkg.s3.amazonaws.com/' + branchp + event + '/${DRONE_BUILD_NUMBER}/' + server,
  local publish_pkg_url = "https://cspkg.s3.amazonaws.com/index.html?prefix=" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/" + result + "/",
  local repo_pkg_url_no_res = "https://cspkg.s3.amazonaws.com/" + branchp + event + "/${DRONE_BUILD_NUMBER}/" + server + "/" + arch + "/",
  local container_tags = if (event == 'cron') then [brancht + std.strReplace(event, '_', '-') + '${DRONE_BUILD_NUMBER}', brancht] else [brancht + std.strReplace(event, '_', '-') + '${DRONE_BUILD_NUMBER}'],
  local container_version = branchp + event + '/${DRONE_BUILD_NUMBER}/' + server + '/' + arch,

  local server_remote = if (std.endsWith(server, 'enterprise')) then 'https://github.com/mariadb-corporation/MariaDBEnterprise' else 'https://github.com/MariaDB/server',
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
  kind: 'pipeline',
  type: 'docker',
  name: std.join(' ', [branch, platform, event, arch, server]),
  platform: { arch: arch },
  // [if arch == 'arm64' then 'node']: { arch: 'arm64' },
  clone: { depth: 10 },
  steps: [
           {
             name: 'build_fdb',
             image: img,
             volumes: [pipeline._volumes.mdb],
             commands: [
               if (pkg_format == 'rpm') then 'yum install -y wget' else 'apt update --yes && apt install -y wget',
               'wget https://raw.githubusercontent.com/mariadb-corporation/mariadb-columnstore-engine/fdb_build/tests/scripts/fdb_build.sh',
               'ls -al',
               'bash fdb_build.sh'
             ],
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
