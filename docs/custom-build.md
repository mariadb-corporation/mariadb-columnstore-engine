
How to trigger a custom build
=============================

 - Open <https://ci.columnstore.mariadb.net>.

 - Click the Continue button to login via github.

 - After you logged in, select mariadb/mariadb-columnstore-engine repository. Please note that recipes below do not work for branches in forked repositories. The branch you want to build against should be in the main engine repository.

 - Click the New Build button on the top right corner.

 - Fill the  Branch field (branch you want to build).

 - Fill desired parameters in key-value style.

Supported parameters with some their values for develop/develop-6 branches:
| parameter name  | develop | develop-6 |
|--|--|--|
|`SERVER_REF`  | 10.9 |10.6-enterprise |
|`SERVER_SHA` | 10.9| 10.6-enterprise  |
|`SERVER_REMOTE` | https://github.com/MariaDB/server|https://github.com/mariadb-corporation/MariaDBEnterprise |
|`REGRESSION_REF` |develop |develop-6 |
|`REGRESSION_TESTS` |`test000.sh,test001.sh` |`test000.sh,test001.sh` |
|`BUILD_DELAY_SECONDS` |0 |0 | 
|`SMOKE_DELAY_SECONDS` |0 |0 | 
|`MTR_DELAY_SECONDS` |0 |0 |
| `REGRESSION_DELAY_SECONDS`|0 |0 |
| `MTR_SUITE_LIST`|`basic,bugfixes` |`basic,bugfixes` |
| `MTR_FULL_SUITE`|`false` |`false` |
`REGRESSION_TESTS` parameter has an empty value on `cron` (nightly) builds and it passed to build just as an argument to regression script like that:
`./go.sh --tests=${REGRESSION_TESTS}`
So you can set it to `test000.sh,test001.sh` for example (comma separated list).
Build artifacts (packages and tests results) will be available [here](https://cspkg.s3.amazonaws.com/index.html?prefix=custom/%5D).

Trigger a build against external packages (built by external ci-systems like Jenkins)
=============================

 - Start build just like a regular custom build, but choose branch `external-packages`.

 - Add `EXTERNAL_PACKAGES_URL` variable. For example, if you want to run tests for packages from URL `https://es-repo.mariadb.net/jenkins/ENTERPRISE/bb-10.6.9-5-cs-22.08.1-2/a71ceba3a33888a62ee0a783adab8b34ffc9c046/`, you should set
`EXTERNAL_PACKAGES_URL=https://es-repo.mariadb.net/jenkins/ENTERPRISE/10.6-enterprise-undo/d296529db9a1e31eab398b5c65fc72e33d0d6a8a`.

|parameter name  | mandatory  |default value |
|--|--|--|
|`EXTERNAL_PACKAGES_URL`  | true | |
|`REGRESSION_REF` |false |`develop` |

Get into the live build on mtr/regression steps
===============================================

Prerequisites:

-   [docker binary](https://docs.docker.com/engine/install/) (we need only client, no need to use docker daemon)

-   [drone cli binary](https://docs.drone.io/cli/install/) 

-   [your personal drone token](https://ci.columnstore.mariadb.net/account) 

-   run you custom build number with `MTR_DELAY_SECONDS` or `REGRESSION_DELAY_SECONDS` parameters and note build number.
Build number example:
![](https://lh4.googleusercontent.com/bUXokNezygP7Xx8KqIAYrJEXzFJua6QqP1aDKkr2LTmb3VXASem8MYSzYfB3K3ZySmJTs6ylfh37oYsnFMp0arVT4iNZonJH4kClFlzja_Un89g9n9En6M8kw-VM4VwF3d_ONI18I00Zdsbard1MTmg)

1.  Export environment variables:
    ```Shell
    export DRONE_AUTOSCALER=https://autoscaler.columnstore.mariadb.net
    export DRONE_SERVER=https://ci.columnstore.mariadb.net
    export DRONE_TOKEN=your-personal-token-from-drone-ui-account-page
```
	Note Use https://autoscaler-arm.columnstore.mariadb.net as ARM autoscaler.
2.  Run:
```Shell
for i in $(drone server ls); do eval "$(drone server env $i)" && drone server info $i --format="{{ .Name }}" && docker ps --format="{{ .Image }} {{ .Names }}" --filter=name=5107; done
```
    Where 5107 is your build number.
    You should see some output looks like this:

    ![](https://lh5.googleusercontent.com/O5gbs6bHH9PnlqP_R-nUkGUM_V98c9s9AvDhEDcNx0R22Wlpka4O1-G7GkdZCJNxzxmsMLn5rlRKcYjRakOgF4FQkVZrCSVYQueaqxaL8-lmQg45Yc6ZOEIUOUZhiXe4YQNid1L3N4YqlDiNjSq4FfE)

3.  Run:
   ```
   eval "$(drone server env agent-A4kVtsDU)"
   ```
4. Run:
```
  docker exec -it regression5107 bash
```
