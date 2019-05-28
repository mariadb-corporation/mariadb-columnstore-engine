IF(RPM)

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG 1)
SET(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

SET(CPACK_RPM_COMPONENT_INSTALL ON)

SET(CPACK_COMPONENTS_ALL platform libs storage-engine)

SET(CPACK_PACKAGE_NAME "mariadb-columnstore")
SET(ENGINE_ARCH "x86_64")

IF (NOT CPACK_RPM_PACKAGE_VERSION)
SET (CPACK_RPM_PACKAGE_VERSION ${PACKAGE_VERSION})
ENDIF()
IF (NOT CPACK_RPM_PACKAGE_RELEASE)
SET (CPACK_RPM_PACKAGE_RELEASE ${PACKAGE_RELEASE})
ENDIF()

SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_RPM_PACKAGE_VERSION}-${CPACK_RPM_PACKAGE_RELEASE}-${ENGINE_ARCH}-${RPM}")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB ColumnStore: A Scale out Columnar storage engine for MariaDB")
SET(CPACK_PACKAGE_URL "http://mariadb.org")

SET(CPACK_PACKAGE_SUMMARY "MariaDB ColumnStore: A Scale out Columnar storage engine for MariaDB")
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

SET(CPACK_RPM_platform_PACKAGE_DESCRIPTION "MariaDB-Columnstore binary files")
SET(CPACK_RPM_platform_PACKAGE_SUMMARY "MariaDB ColumnStore: A Scale out Columnar storage engine for MariaDB")
SET(CPACK_RPM_platform_PACKAGE_GROUP "Applications")

SET(CPACK_RPM_libs_PACKAGE_DESCRIPTION "MariaDB-Columnstore libraries")
SET(CPACK_RPM_libs_PACKAGE_SUMMARY "MariaDB ColumnStore: A Scale out Columnar storage engine for MariaDB")

SET(CPACK_RPM_storage-engine_PACKAGE_DESCRIPTION "MariaDB Columnstore connector binary files")
SET(CPACK_RPM_storage-engine_PACKAGE_SUMMARY "MariaDB ColumnStore: A Scale out Columnar storage engine for MariaDB")
SET(CPACK_RPM_storage-engine_PACKAGE_GROUP "Applications")

# "set/append array" - append a set of strings, separated by a space
MACRO(SETA var)
  FOREACH(v ${ARGN})
    SET(${var} "${${var}} ${v}")
  ENDFOREACH()
ENDMACRO(SETA)

SETA(CPACK_RPM_libs_PACKAGE_PROVIDES "mariadb-columnstore-libs")
SETA(CPACK_RPM_platform_PACKAGE_PROVIDES "mariadb-columnstore-platform")
SETA(CPACK_RPM_storage-engine_PACKAGE_PROVIDES "mariadb-columnstore-storage-engine")


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
    SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "mariadb-columnstore-libs" "mariadb-columnstore-shared" "snappy")
    # Disable auto require as this will also try to pull Boost via RPM
    SET(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
elseif (${SUSE_VERSION_NUMBER} EQUAL 12)
   SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "boost-devel >= 1.54.0" "mariadb-columnstore-libs" "libsnappy1" "jemalloc")
else ()
   SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "boost >= 1.53.0" "mariadb-columnstore-libs" "snappy" "jemalloc")
endif()

SETA(CPACK_RPM_storage-engine_PACKAGE_REQUIRES "mariadb-columnstore-libs")

SET(CPACK_RPM_platform_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_platform.sh)
SET(CPACK_RPM_libs_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_libs.sh)
SET(CPACK_RPM_storage-engine_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_storage_engine.sh)

SET(CPACK_RPM_platform_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_platform.sh)
SET(CPACK_RPM_libs_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_libs.sh)
SET(CPACK_RPM_storage-engine_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_storage_engine.sh)

SET(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}
%define ignore \#
")

SET(ignored
    "%ignore /usr"
    "%ignore /usr/local"
)

#SET(CPACK_RPM_SPEC_MORE_DEFINE "
#%define _prefix ${CMAKE_INSTALL_PREFIX}
#")

SET(CPACK_RPM_platform_USER_FILELIST ${ignored})

SET(CPACK_RPM_libs_USER_FILELIST ${ignored})

SET(CPACK_RPM_storage-engine_USER_FILELIST ${ignored})

INCLUDE (CPack)

ENDIF()
