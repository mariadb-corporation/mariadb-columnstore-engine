CMAKE_MINIMUM_REQUIRED(VERSION 3.11)

STRING(TIMESTAMP CURRENT_YEAR "%Y")

PROJECT(cmapi NONE)

SET(CPACK_PACKAGE_NAME "MariaDB-columnstore-cmapi")


# use columnstore package versioning
SET(CMAPI "YES")
LIST(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)
MESSAGE(STATUS "GET SERVER_DIR from input argument ${SERVER_DIR}")
SET(SERVER_SOURCE_DIR "${SERVER_DIR}")
INCLUDE(cmapi_misc)
# Get mysql version
GET_MYSQL_VERSION()
# get CMAPI version from Columnstore version
SET(ENGINE_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)
LIST(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../cmake)
INCLUDE(columnstore_version)


SET(CMAPI_PACKAGE_VERSION "${CMAPI_VERSION_MAJOR}.${CMAPI_VERSION_MINOR}.${CMAPI_VERSION_PATCH}")

SET(CMAPI_USER "root")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB ColumnStore CMAPI: cluster management API and command line tool.")
SET(CPACK_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}

It is GPL v2 licensed, which means you can use the it free of charge under the
conditions of the GNU General Public License Version 2 (http://www.gnu.org/licenses/).

MariaDB documentation can be found at https://mariadb.com/kb
MariaDB bug reports should be submitted through https://jira.mariadb.org

")
SET(CPACK_PACKAGE_URL "http://www.mariadb.com")
SET(CPACK_PACKAGE_CONTACT "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_SUMMARY "MariaDB ColumnStore CMAPI: cluster management API and command line tool.")
SET(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_LICENSE "Copyright (c) ${CURRENT_YEAR} MariaDB Corporation Ab.; redistributable under the terms of the GPLv2, see the file LICENSE.GPL2 for details.")

SET(BIN_DIR "/usr/bin")
SET(ETC_DIR "/etc/columnstore")
SET(SHARE_DIR "/usr/share/columnstore")
SET(CMAPI_DIR "${SHARE_DIR}/cmapi")
SET(SYSTEMD_UNIT_DIR "/usr/lib/systemd/system")
SET(SYSTEMD_ENGINE_UNIT_NAME "mariadb-columnstore")
SET(CMAPI_CONF_FILEPATH "${ETC_DIR}/cmapi_server.conf")
SET(MAN_DIR "/usr/share/man/man1")

STRING(TOLOWER ${CPACK_PACKAGE_NAME} SYSTEMD_UNIT_NAME)

CONFIGURE_FILE(VERSION.template VERSION)
CONFIGURE_FILE(service.template ${SYSTEMD_UNIT_NAME}.service)
CONFIGURE_FILE(systemd.env.template systemd.env)
CONFIGURE_FILE(postinst.template postinst)
CONFIGURE_FILE(prerm.template prerm)
CONFIGURE_FILE(conffiles.template conffiles)
CONFIGURE_FILE(mcs.template mcs)
CONFIGURE_FILE(mcs_aws.template mcs_aws)
CONFIGURE_FILE(mcs_gsutil.template mcs_gsutil)

INSTALL(DIRECTORY python deps mcs_node_control failover cmapi_server engine_files mcs_cluster_tool
            DESTINATION ${CMAPI_DIR}
            USE_SOURCE_PERMISSIONS
            PATTERN "test" EXCLUDE
            PATTERN "cmapi_server.conf" EXCLUDE
            PATTERN "README" EXCLUDE)
INSTALL(FILES LICENSE.GPL2 VERSION
            DESTINATION ${CMAPI_DIR})
INSTALL(FILES check_ready.sh
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
            DESTINATION ${CMAPI_DIR})
INSTALL(FILES cmapi_server/cmapi_server.conf systemd.env
            DESTINATION ${ETC_DIR})
INSTALL(FILES ${SYSTEMD_UNIT_NAME}.service
            DESTINATION ${SYSTEMD_UNIT_DIR})
INSTALL(FILES mcs
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
            DESTINATION ${BIN_DIR})
INSTALL(FILES mcs_aws
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
            DESTINATION ${BIN_DIR})
INSTALL(FILES mcs_gsutil
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
            DESTINATION ${BIN_DIR})
INSTALL(FILES scripts/mcs_backup_manager.sh scripts/cs_package_manager.sh scripts/columnstore_review.sh
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
            DESTINATION ${BIN_DIR})
INSTALL(FILES mcs_cluster_tool/mcs.1 DESTINATION ${MAN_DIR})

OPTION(RPM "Build an RPM" OFF)
IF(RPM)
    SET(CPACK_GENERATOR "RPM")
    # cmake wants to byte-compile all .py files.
    # this line below will prevent it of doing such
    # trying to byte-compile could produce some issues with not existing
    # python3 on some systems like centos7
    # more info:
    # https://fedoraproject.org/wiki/Changes/No_more_automagic_Python_bytecompilation#Status_quo
    # https://stackoverflow.com/questions/69988093/cmake-brp-python-bytecompile-and-python3
    SET(CPACK_RPM_SPEC_INSTALL_POST "%global _python_bytecompile_extra 0")
    SET(CPACK_RPM_SPEC_MORE_DEFINE "%global _python_bytecompile_extra 0")
    # Turn off the brp-python-bytecompile script
    # for every release including EPEL
    # more info here:
    # https://pagure.io/packaging-committee/issue/755
    SET(CPACK_RPM_SPEC_INSTALL_POST "%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')")
    SET(CPACK_RPM_SPEC_MORE_DEFINE "%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')")
    # otherwise it could be solved to install python3 in centos7 and adding this line:
    # SET(CPACK_RPM_SPEC_MORE_DEFINE "%define __python %{__python3}")
    # example here:
    # https://github.com/irods/irods/pull/6347/files
    # but it's doesn't work because of some CPACK versions don't add definitions
    # to the spec file using CPACK_RPM_SPEC_MORE_DEFINE

    SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})

    SET(CPACK_RPM_PACKAGE_LICENSE "GPLv2")
    SET(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
    SET(CPACK_RPM_PACKAGE_URL ${CPACK_PACKAGE_URL})
    SET(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
    SET(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
    SET(CPACK_RPM_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})
    SET(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

    SET(CPACK_RPM_SPEC_MORE_DEFINE "%undefine __brp_mangle_shebangs")
    SET(CPACK_RPM_PACKAGE_AUTOREQ "no")

    SET(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/postinst)
    SET(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/prerm)
    SET(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION ${ETC_DIR} ${SHARE_DIR})
    SET(CPACK_RPM_USER_FILELIST "%config(noreplace) ${CMAPI_CONF_FILEPATH}")

    SET(CPACK_RPM_PACKAGE_OBSOLETES "mariadb-columnstore-cmapi")
    SET(CPACK_RPM_PACKAGE_REQUIRES "curl")
    STRING(REPLACE "-" "_" SERVER_VERSION ${SERVER_VERSION})
    get_linux_lsb_release_information()

    STRING(REGEX MATCH "^." OS_VERSION_MAJOR "${LSB_RELEASE_VERSION_SHORT}")
    MESSAGE(STATUS ${OS_VERSION_MAJOR})
    IF (LSB_RELEASE_ID_SHORT MATCHES "centos|rocky|rhel|alma|RedHatEnterprise")
        SET(OS_NAME_SHORT "el")
        IF (OS_VERSION_MAJOR MATCHES "9")
	    SET(CPACK_RPM_PACKAGE_REQUIRES "libxcrypt-compat")
        ENDIF()
    ELSE()
        SET(OS_NAME_SHORT "unknown")
    ENDIF()
    SET(OS_VERSION "${OS_NAME_SHORT}${OS_VERSION_MAJOR}")
    MESSAGE(STATUS "OS_VERSION ${OS_VERSION}")

    SET(CPACK_RPM_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${SERVER_VERSION}_${CMAPI_PACKAGE_VERSION}-${CMAPI_VERSION_RELEASE}.${OS_VERSION}.${CMAKE_HOST_SYSTEM_PROCESSOR}")
    MESSAGE(STATUS "CPACK_RPM_PACKAGE_FILE_NAME ${CPACK_RPM_PACKAGE_FILE_NAME}")
    SET(CPACK_PACKAGE_FILE_NAME ${CPACK_RPM_PACKAGE_FILE_NAME})
    # version and release the same as in Columnstore engine package
    SET(CPACK_RPM_PACKAGE_VERSION "${SERVER_VERSION}_${CMAPI_PACKAGE_VERSION}")
    SET(CPACK_RPM_PACKAGE_RELEASE "${CMAPI_VERSION_RELEASE}.${OS_VERSION}")
ENDIF()

OPTION(DEB "Build a DEB" OFF)
IF(DEB)
    SET(CPACK_GENERATOR "DEB")

    # TODO: different names in deb and rpm packages, fix it in next releases.
    STRING(TOLOWER ${CPACK_PACKAGE_NAME} CPACK_DEBIAN_PACKAGE_NAME)
    STRING(TOLOWER ${CPACK_PACKAGE_NAME} CPACK_PACKAGE_NAME)
    SET(CPACK_DEBIAN_PACKAGE_LICENSE "GPLv2")
    SET(CPACK_DEBIAN_PACKAGE_URL ${CPACK_PACKAGE_URL})
    SET(CPACK_DEBIAN_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
    SET(CPACK_DEBIAN_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
    SET(CPACK_DEBIAN_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})
    SET(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

    SET(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_SOURCE_DIR}/prerm;${CMAKE_CURRENT_SOURCE_DIR}/postinst;${CMAKE_CURRENT_SOURCE_DIR}/conffiles")

    SET(CPACK_DEBIAN_PACKAGE_REPLACES "mariadb-columnstore-cmapi")
    SET(CPACK_DEBIAN_PACKAGE_DEPENDS "curl")
    STRING(REPLACE "-" "." SERVER_VERSION ${SERVER_VERSION})
    SET(PATCHLEVEL "+maria")
    get_linux_lsb_release_information()
    STRING(REGEX MATCH "^..." LSBID ${LSB_RELEASE_ID_SHORT})
    MESSAGE(STATUS "LSBID ${LSBID}")
    STRING(REPLACE "." "" LSBVERSION ${LSB_RELEASE_VERSION_SHORT})
    MESSAGE(STATUS "LSBVERSION ${LSBVERSION}")
    EXECUTE_PROCESS(COMMAND dpkg-architecture -q DEB_BUILD_ARCH OUTPUT_VARIABLE ARCHITECTURE OUTPUT_STRIP_TRAILING_WHITESPACE)
    MESSAGE(STATUS "ARCHITECTURE detected ${ARCHITECTURE}")
    SET(CPACK_DEBIAN_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}_${SERVER_VERSION}-${CMAPI_PACKAGE_VERSION}${PATCHLEVEL}~${LSBID}${LSBVERSION}_${ARCHITECTURE}")
    MESSAGE(STATUS "CPACK_DEBIAN_PACKAGE_FILE_NAME ${CPACK_DEBIAN_PACKAGE_FILE_NAME}")
    SET(CPACK_PACKAGE_FILE_NAME ${CPACK_DEBIAN_PACKAGE_FILE_NAME})
    # making possible to store several cmapi packages in one repo (same version as in
    # Columnstore engine package)
    SET(CPACK_DEBIAN_PACKAGE_VERSION "${SERVER_VERSION}-${CMAPI_PACKAGE_VERSION}${PATCHLEVEL}~${LSBID}${LSBVERSION}")
    ENDIF()

INCLUDE (CPack)
