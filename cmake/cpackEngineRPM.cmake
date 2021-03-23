IF(RPM)

SET(CMAKE_INSTALL_PREFIX ${INSTALL_ENGINE})

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG 1)
SET(CPACK_PACKAGING_INSTALL_PREFIX ${INSTALL_ENGINE})

SET(CPACK_RPM_COMPONENT_INSTALL ON)

SET(CPACK_COMPONENTS_ALL columnstore-engine)

SET(CPACK_PACKAGE_NAME "MariaDB")
SET(ENGINE_ARCH "x86_64")

IF (NOT CPACK_RPM_PACKAGE_VERSION)
SET (CPACK_RPM_PACKAGE_VERSION ${PACKAGE_VERSION})
ENDIF()
IF (NOT CPACK_RPM_PACKAGE_RELEASE)
SET (CPACK_RPM_PACKAGE_RELEASE ${PACKAGE_RELEASE})
ENDIF()

SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_RPM_PACKAGE_VERSION}-${CPACK_RPM_PACKAGE_RELEASE}-${ENGINE_ARCH}-${RPM}")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB ColumnStore: a scale out columnar storage engine for MariaDB")
SET(CPACK_PACKAGE_URL "http://mariadb.org")

SET(CPACK_PACKAGE_SUMMARY "MariaDB ColumnStore: a scale out columnar storage engine for MariaDB")
SET(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_LICENSE "Copyright (c) 2016 MariaDB Corporation Ab., all rights reserved; redistributable under the terms of the GPL, see the file COPYING for details.")


SET(CPACK_RPM_PACKAGE_LICENSE "GPLv2")
SET(CPACK_RPM_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
SET(CPACK_RPM_PACKAGE_URL ${CPACK_PACKAGE_URL})
SET(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
SET(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
SET(CPACK_RPM_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})

SET(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}

It is GPL v2 licensed, which means you can use the it free of charge under the
conditions of the GNU General Public License Version 2 (http://www.gnu.org/licenses/).

MariaDB documentation can be found at https://mariadb.com/kb
MariaDB bug reports should be submitted through https://jira.mariadb.org 

")

SET(CPACK_RPM_columnstore-engine_PACKAGE_DESCRIPTION "MariaDB Columnstore connector binary files")
SET(CPACK_RPM_columnstore-engine_PACKAGE_SUMMARY "MariaDB ColumnStore: a scale out columnar storage engine for MariaDB")
SET(CPACK_RPM_columnstore-engine_PACKAGE_GROUP "Applications")

# "set/append array" - append a set of strings, separated by a space
MACRO(SETA var)
  FOREACH(v ${ARGN})
    SET(${var} "${${var}} ${v}")
  ENDFOREACH()
ENDMACRO(SETA)

SETA(CPACK_RPM_columnstore--engine_PACKAGE_PROVIDES "MariaDB-columnstore-engine")

#boost is a source build in CentOS 6 so don't require it as a package
SET(REDHAT_VERSION_NUMBER OFF)
SET(SUSE_VERSION_NUMBER OFF)
IF (EXISTS "/etc/redhat-release")
    file (READ "/etc/redhat-release" REDHAT_VERSION)
    string(REGEX MATCH "release ([0-9]+)" CENTOS "${REDHAT_VERSION}")
    set(REDHAT_VERSION_NUMBER "${CMAKE_MATCH_1}")
ENDIF ()
IF (EXISTS "/etc/SuSE-release")
    file (READ "/etc/SuSE-release" SUSE_VERSION)
    string(REGEX MATCH "VERSION = ([0-9]+)" SUSE "${SUSE_VERSION}")
    set(SUSE_VERSION_NUMBER "${CMAKE_MATCH_1}")
ENDIF ()
if (${REDHAT_VERSION_NUMBER} EQUAL 6)
    SETA(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES "MariaDB-columnstore-shared" "snappy" "net-tools" "MariaDB-server" "python3")
    # Disable auto require as this will also try to pull Boost via RPM
    SET(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
elseif (${SUSE_VERSION_NUMBER} EQUAL 12)
    SETA(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES "boost-devel >= 1.54.0" "libsnappy1" "net-tools" "MariaDB-server" "python3" "jemalloc")
else ()
    SETA(CPACK_RPM_columnstore-engine_PACKAGE_REQUIRES "boost >= 1.53.0" "snappy" "net-tools" "MariaDB-server" "python3" "jemalloc")
endif()

SET(CPACK_RPM_columnstore-engine_PRE_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preInstall_storage_engine.sh)
SET(CPACK_RPM_columnstore-engine_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_storage_engine.sh)
SET(CPACK_RPM_columnstore-engine_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_storage_engine.sh)
SET(CPACK_RPM_columnstore-engine_POST_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postUn_storage_engine.sh)

SET(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}
%define ignore \#
")

SET(ignored
    "%ignore /usr"
    "%ignore /usr/local"
    "%ignore /bin"
    "%ignore /lib"
    "%ignore /usr/sbin"
    "%ignore /usr/lib64/mysql"
    "%ignore /usr/lib64/mysql/plugin"
    "%ignore /etc/my.cnf.d"
    "%ignore /var/lib"
    "%ignore /var"
)

#SET(CPACK_RPM_SPEC_MORE_DEFINE "
#%define _prefix ${CMAKE_INSTALL_PREFIX}
#")

SET(CPACK_RPM_columnstore-engine_USER_FILELIST ${ignored})

INCLUDE (CPack)

ENDIF()
