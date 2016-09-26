IF(RPM)

SET(CMAKE_INSTALL_PREFIX ${INSTALL_ENGINE})

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG 1)
SET(CPACK_PACKAGING_INSTALL_PREFIX ${INSTALL_ENGINE})
CMAKE_MINIMUM_REQUIRED(VERSION 2.8.7)

SET(CPACK_RPM_COMPONENT_INSTALL ON)

SET(CPACK_COMPONENTS_ALL platform libs storage-engine)

SET(CPACK_PACKAGE_NAME "mariadb-columnstore")
SET(ENGINE_ARCH "x86_64")

IF (NOT CPACK_RPM_PACKAGE_VERSION)
SET (CPACK_RPM_PACKAGE_VERSION "1.0.0")
ENDIF()
IF (NOT CPACK_RPM_PACKAGE_RELEASE)
SET (CPACK_RPM_PACKAGE_RELEASE "0")
ENDIF()

SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_RPM_PACKAGE_VERSION}.${CPACK_RPM_PACKAGE_RELEASE}-${ENGINE_ARCH}-${RPM}")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB Columnstore: a very fast and robust SQL database server")
SET(CPACK_PACKAGE_URL "http://mariadb.org")

SET(CPACK_PACKAGE_SUMMARY "MariaDB-Columnstore software")
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
SET(CPACK_RPM_platform_PACKAGE_SUMMARY "MariaDB-Columnstore software binaries")
SET(CPACK_RPM_platform_PACKAGE_GROUP "Applications")

SET(CPACK_RPM_libs_PACKAGE_DESCRIPTION "MariaDB-Columnstore libraries")
SET(CPACK_RPM_libs_PACKAGE_SUMMARY "MariaDB-Columnstore software libraries")

SET(CPACK_RPM_storage-engine_PACKAGE_DESCRIPTION "MariaDB Columnstore connector binary files")
SET(CPACK_RPM_storage-engine_PACKAGE_SUMMARY "MariaDB-Columnstore software MariaDB connector")
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

SETA(CPACK_RPM_libs_PACKAGE_OBSOLETES "mariadb-columnstore-libs")
SETA(CPACK_RPM_platform_PACKAGE_OBSOLETES "mariadb-columnstore-platform")
SETA(CPACK_RPM_storage-engine_PACKAGE_OBSOLETES "mariadb-columnstore-storage-engine")

SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "boost >= 1.53.0" "mariadb-columnstore-libs" "net-snmp-libs")
SETA(CPACK_RPM_storage-engine_PACKAGE_REQUIRES "mariadb-columnstore-libs")

SET(CPACK_RPM_platform_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_platform.sh)
SET(CPACK_RPM_libs_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_libs.sh)
SET(CPACK_RPM_storage-engine_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_storage_engine.sh)

SET(CPACK_RPM_platform_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_platform.sh)
SET(CPACK_RPM_libs_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_libs.sh)
SET(CPACK_RPM_storage-engine_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_storage_engine.sh)


#SET(CPACK_RPM_SPEC_MORE_DEFINE "
#%define _prefix ${CMAKE_INSTALL_PREFIX}
#")


INCLUDE (CPack)

ENDIF()
