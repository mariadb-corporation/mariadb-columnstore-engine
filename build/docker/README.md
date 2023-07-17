# General synopsis

There are two Dockerfiles and two scripts.

The ``start`` script uses Dockerfile_start and creates initial Rocky Linux 8 Docker image, with MDB, columnstore and regression test suite copied into container, ready to be built. The building process will not be executed. The name of container is "do-not-publish-roli8-curr-mdb".

Please note that MDB, columnstore and regression test suite are copied as is, with all your changes and/or ``git branch`` status. So you may test a vanilla build or experiment with your own.

The ``build-run`` script will work on "do-not-publish-roli8-curr-mdb" container. It also will copy MDB, columnstore and regression test suite and execute a build. After a successful build it will execute ``bash -c $*`` inside container, which would allow you to run

TODO: FINISH
