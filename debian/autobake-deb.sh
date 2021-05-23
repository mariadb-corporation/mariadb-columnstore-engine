#!/bin/bash
#
# Build MariaDB ColumnStore .deb packages for test and release at mariadb.org
#
# This debian/autobake-deb.sh file is a copy from mariadb-server, but updated to be
# ColumnStore specific to avoid pulling in any excess build dependencies or
# building anything else than what is required to generate mariadb-plugin-columnstore.
#
# Purpose of this script:
# Always keep the actual packaging as up-to-date as possible following the latest
# Debian policy and targeting Debian Sid. Then case-by-case run in autobake-deb.sh
# tests for backwards compatibility and strip away parts on older builders or
# specfic build environments.

# Exit immediately on any error
set -e

# This file is invocated from Buildbot and Travis-CI to build deb packages.
# As both of those CI systems have many parallel jobs that include different
# parts of the test suite, we don't need to run the mysql-test-run at all when
# building the deb packages here.
export DEB_BUILD_OPTIONS="nocheck $DEB_BUILD_OPTIONS"

# General CI optimizations to keep build output smaller
if [[ $TRAVIS ]] || [[ $GITLAB_CI ]]
then
  # On both Travis and Gitlab the output log must stay under 4MB so make the
  # build less verbose
  sed '/Add support for verbose builds/,/^$/d' -i debian/rules
fi

# From Debian Buster/Ubuntu Bionic, libcurl4 replaces libcurl3
if ! apt-cache madison libcurl4 | grep 'libcurl4' >/dev/null 2>&1
then
  sed 's/libcurl4/libcurl3/g' -i debian/control
fi

# From Debian Bullseye/Ubuntu Groovy, liburing replaces libaio
if ! apt-cache madison liburing-dev | grep 'liburing-dev' >/dev/null 2>&1
then
  sed 's/liburing-dev/libaio-dev/g' -i debian/control
  sed '/-DIGNORE_AIO_CHECK=YES/d' -i debian/rules
  sed '/-DWITH_URING=yes/d' -i debian/rules
fi

# Check if build dependencies are met and if not, attempt to install them
# NOTE! This must happen as the last step after all debian/control
# customizations are done.
if ! dpkg-checkbuilddeps
then
  # Install dependencies on the fly if running with root permissions
  if (( $EUID == 0 ))
  then
    echo "Automatically install all build dependencies as defined in debian/control"
    apt-get update
    apt-get install --yes --no-install-recommends build-essential devscripts ccache equivs
    mk-build-deps debian/control -t "apt-get -y -o Debug::pkgProblemResolver=yes --no-install-recommends" -r -i
  else
    echo "ERROR: Missing build dependencies as defined in debian/control"
    exit 1
  fi
fi

# Adjust changelog, add new version
echo "Incrementing changelog and starting build scripts"

# Find major.minor version
source ./VERSION
UPSTREAM="${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_PATCH}${MYSQL_VERSION_EXTRA}"
PATCHLEVEL="+columnstore"
LOGSTRING="MariaDB ColumnStore build"
CODENAME="$(lsb_release -sc)"
EPOCH="1:"
VERSION="${EPOCH}${UPSTREAM}${PATCHLEVEL}~${CODENAME}"

dch -b -D "${CODENAME}" -v "${VERSION}" "Automatic build with ${LOGSTRING}."

echo "Creating package version ${VERSION} ... "

# On Travis CI and Gitlab-CI, use -b to build binary only packages as there is
# no need to waste time on generating the source package.
if [[ $TRAVIS ]]
then
  BUILDPACKAGE_FLAGS="-b"
fi

# Use eatmydata is available to build faster with less I/O, skipping fsync()
# during the entire build process (safe because a build can always be restarted)
if which eatmydata > /dev/null
then
  BUILDPACKAGE_PREPEND=eatmydata
fi

# Build the package
# Pass -I so that .git and other unnecessary temporary and source control files
# will be ignored by dpkg-source when creating the tar.gz source package.
fakeroot $BUILDPACKAGE_PREPEND dpkg-buildpackage -us -uc -I $BUILDPACKAGE_FLAGS

# If the step above fails due to missing dependencies, you can manually run
#   sudo mk-build-deps debian/control -r -i

# Don't log package contents on Travis-CI or Gitlab-CI to save time and log size
if [[ ! $TRAVIS ]] && [[ ! $GITLAB_CI ]]
then
  echo "List package contents ..."
  cd ..
  for package in *.deb
  do
    echo "$package" | cut -d '_' -f 1
    dpkg-deb -c "$package" | awk '{print $1 " " $2 " " $6 " " $7 " " $8}' | sort -k 3
    echo "------------------------------------------------"
  done
fi

echo "Build complete"
