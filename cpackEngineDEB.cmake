IF(DEB)

CMAKE_MINIMUM_REQUIRED(VERSION 3.4)

SET(CMAKE_INSTALL_PREFIX ${INSTALL_ENGINE})

SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_DEBUG 1)
SET(CPACK_PACKAGING_INSTALL_PREFIX ${INSTALL_ENGINE})

# Note that this variable is DEB not DEBIAN! http://public.kitware.com/pipermail/cmake/2014-July/058030.html
SET(CPACK_DEB_COMPONENT_INSTALL ON)

SET(CPACK_COMPONENTS_ALL platform libs storage-engine)

SET(CPACK_PACKAGE_NAME "mariadb-columnstore")
SET(ENGINE_ARCH "amd64")

IF (NOT CPACK_DEBIAN_PACKAGE_VERSION)
    SET (CPACK_DEBIAN_PACKAGE_VERSION ${PACKAGE_VERSION})
ENDIF()
IF (NOT CPACK_DEBIAN_PACKAGE_RELEASE)
    SET (CPACK_DEBIAN_PACKAGE_RELEASE ${PACKAGE_RELEASE})
ENDIF()

SET(CPACK_DEBIAN_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_DEBIAN_PACKAGE_VERSION}-${CPACK_DEBIAN_PACKAGE_RELEASE}-${ENGINE_ARCH}")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB Columnstore: a very fast and robust SQL database server")
SET(CPACK_PACKAGE_URL "http://mariadb.org")
SET(CPACK_PACKAGE_CONTACT "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_SUMMARY "MariaDB-Columnstore software")
SET(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_LICENSE "Copyright (c) 2016 MariaDB Corporation Ab., all rights reserved; redistributable under the terms of the GPL, see the file COPYING for details.")


SET(CPACK_DEBIAN_PACKAGE_LICENSE "GPLv2")
SET(CPACK_DEBIAN_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_DEBIAN_PACKAGE_URL ${CPACK_PACKAGE_URL})
SET(CPACK_DEBIAN_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
SET(CPACK_DEBIAN_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
SET(CPACK_DEBIAN_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})

SET(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

SET(CPACK_DEBIAN_PLATFORM_PACKAGE_DESCRIPTION "MariaDB-Columnstore binary files")
SET(CPACK_DEBIAN_PLATFORM_PACKAGE_SUMMARY "MariaDB-Columnstore software binaries")

SET(CPACK_DEBIAN_LIBS_PACKAGE_DESCRIPTION "MariaDB-Columnstore libraries")
SET(CPACK_DEBIAN_LIBS_PACKAGE_SUMMARY "MariaDB-Columnstore software libraries")

SET(CPACK_DEBIAN_STORAGE-ENGINE_PACKAGE_DESCRIPTION "MariaDB Columnstore connector binary files")
SET(CPACK_DEBIAN_STORAGE-ENGINE_PACKAGE_SUMMARY "MariaDB-Columnstore software MariaDB connector")


SET(CPACK_DEBIAN_LIBS_PACKAGE_PROVIDES "mariadb-columnstore-libs")
SET(CPACK_DEBIAN_PLATFORM_PACKAGE_PROVIDES "mariadb-columnstore-platform")
SET(CPACK_DEBIAN_STORAGE-ENGINE_PACKAGE_PROVIDES "mariadb-columnstore-storage-engine")

set(DEBIAN_VERSION_NUMBER OFF)
if (EXISTS "/etc/debian_version")
    file (READ "/etc/debian_version" DEBIAN_VERSION)
    string(REGEX MATCH "([0-9]+).[0-9]+" DEBIAN "${DEBIAN_VERSION}")
    set(DEBIAN_VERSION_NUMBER "${CMAKE_MATCH_1}")
endif ()
if ("${DEBIAN_VERSION_NUMBER}" EQUAL "8")
    SET(CPACK_DEBIAN_PLATFORM_PACKAGE_DEPENDS "expect, perl, openssl, file, libdbi-perl, libreadline-dev, rsync, net-tools, libboost-all-dev, mariadb-columnstore-libs, mariadb-columnstore-server, libsnappy1")
elseif ("${DEBIAN_VERSION_NUMBER}" EQUAL "9")
    SET(CPACK_DEBIAN_PLATFORM_PACKAGE_DEPENDS "expect, perl, openssl, file, libdbi-perl, libreadline-dev, rsync, net-tools, libboost-all-dev, mariadb-columnstore-libs, mariadb-columnstore-server, libsnappy1v5, libreadline5")
else()
    SET(CPACK_DEBIAN_PLATFORM_PACKAGE_DEPENDS "expect, perl, openssl, file, libdbi-perl, libboost-all-dev, libreadline-dev, rsync, libsnappy1v5, net-tools")
endif ()

SET(CPACK_DEBIAN_STORAGE-ENGINE_PACKAGE_DEPENDS "mariadb-columnstore-libs")


set( CPACK_DEBIAN_LIBS_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_SOURCE_DIR}/build/debian/libs/postinst;${CMAKE_CURRENT_SOURCE_DIR}/build/debian/libs/prerm;" )
set( CPACK_DEBIAN_PLATFORM_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_SOURCE_DIR}/build/debian/platform/postinst;${CMAKE_CURRENT_SOURCE_DIR}/build/debian/platform/prerm;" )
set( CPACK_DEBIAN_STORAGE-ENGINE_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_SOURCE_DIR}/build/debian/storageEngine/postinst;${CMAKE_CURRENT_SOURCE_DIR}/build/debian/storageEngine/prerm;" )

INCLUDE (CPack)

ENDIF()
