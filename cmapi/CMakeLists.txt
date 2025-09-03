cmake_minimum_required(VERSION 3.11)

string(TIMESTAMP CURRENT_YEAR "%Y")

project(cmapi NONE)

set(CPACK_PACKAGE_NAME "MariaDB-columnstore-cmapi")

# use columnstore package versioning
set(CMAPI "YES")
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)
message(STATUS "GET SERVER_DIR from input argument ${SERVER_DIR}")
set(SERVER_SOURCE_DIR "${SERVER_DIR}")
include(cmapi_misc)
# Get mysql version
get_mysql_version()
# get CMAPI version from Columnstore version
set(ENGINE_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/..)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../cmake)
include(columnstore_version)

set(CMAPI_PACKAGE_VERSION "${CMAPI_VERSION_MAJOR}.${CMAPI_VERSION_MINOR}.${CMAPI_VERSION_PATCH}")

# Git revision to embed into VERSION (may be provided via -DCMAPI_GIT_REVISION or env)
if(NOT DEFINED CMAPI_GIT_REVISION OR "${CMAPI_GIT_REVISION}" STREQUAL "")
    if(DEFINED ENV{CMAPI_GIT_REVISION} AND NOT "$ENV{CMAPI_GIT_REVISION}" STREQUAL "")
        set(CMAPI_GIT_REVISION "$ENV{CMAPI_GIT_REVISION}")
    elseif(DEFINED ENV{DRONE_COMMIT} AND NOT "$ENV{DRONE_COMMIT}" STREQUAL "")
        set(CMAPI_GIT_REVISION "$ENV{DRONE_COMMIT}")
    else()
        set(CMAPI_GIT_REVISION "unknown")
    endif()
endif()

set(CMAPI_USER "root")

set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB ColumnStore CMAPI: cluster management API and command line tool.")
set(CPACK_PACKAGE_DESCRIPTION
    "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}

It is GPL v2 licensed, which means you can use the it free of charge under the
conditions of the GNU General Public License Version 2 (http://www.gnu.org/licenses/).

MariaDB documentation can be found at https://mariadb.com/kb
MariaDB bug reports should be submitted through https://jira.mariadb.org

"
)
set(CPACK_PACKAGE_URL "http://www.mariadb.com")
set(CPACK_PACKAGE_CONTACT "MariaDB Corporation Ab")
set(CPACK_PACKAGE_SUMMARY "MariaDB ColumnStore CMAPI: cluster management API and command line tool.")
set(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
set(CPACK_PACKAGE_LICENSE
    "Copyright (c) ${CURRENT_YEAR} MariaDB Corporation Ab.; redistributable under the terms of the GPLv2, see the file LICENSE.GPL2 for details."
)

set(BIN_DIR "/usr/bin")
set(ETC_DIR "/etc/columnstore")
set(SHARE_DIR "/usr/share/columnstore")
set(CMAPI_DIR "${SHARE_DIR}/cmapi")
set(SYSTEMD_UNIT_DIR "/usr/lib/systemd/system")
set(SYSTEMD_ENGINE_UNIT_NAME "mariadb-columnstore")
set(CMAPI_CONF_FILEPATH "${ETC_DIR}/cmapi_server.conf")
set(MAN_DIR "/usr/share/man/man1")

string(TOLOWER ${CPACK_PACKAGE_NAME} SYSTEMD_UNIT_NAME)

configure_file(VERSION.template VERSION)
configure_file(service.template ${SYSTEMD_UNIT_NAME}.service)
configure_file(systemd.env.template systemd.env)
configure_file(postinst.template postinst)
configure_file(prerm.template prerm)
configure_file(conffiles.template conffiles)
configure_file(mcs.template mcs)
configure_file(mcs_aws.template mcs_aws)
configure_file(mcs_gsutil.template mcs_gsutil)

install(
    DIRECTORY python
              deps
              mcs_node_control
              failover
              cmapi_server
              engine_files
              mcs_cluster_tool
              tracing
    DESTINATION ${CMAPI_DIR}
    USE_SOURCE_PERMISSIONS
    PATTERN "test" EXCLUDE
    PATTERN "cmapi_server.conf" EXCLUDE
    PATTERN "README" EXCLUDE
)
install(FILES LICENSE.GPL2 VERSION DESTINATION ${CMAPI_DIR})
install(
    FILES check_ready.sh
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION ${CMAPI_DIR}
)
install(FILES cmapi_server/cmapi_server.conf systemd.env DESTINATION ${ETC_DIR})
install(FILES ${SYSTEMD_UNIT_NAME}.service DESTINATION ${SYSTEMD_UNIT_DIR})
install(
    FILES mcs
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION ${BIN_DIR}
)
install(
    FILES mcs_aws
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION ${BIN_DIR}
)
install(
    FILES mcs_gsutil
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION ${BIN_DIR}
)
install(
    FILES scripts/mcs_backup_manager.sh scripts/cs_package_manager.sh scripts/columnstore_review.sh
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    DESTINATION ${BIN_DIR}
)
install(FILES mcs_cluster_tool/mcs.1 DESTINATION ${MAN_DIR})

option(RPM "Build an RPM" OFF)
if(RPM)
    set(CPACK_GENERATOR "RPM")
    # cmake wants to byte-compile all .py files. this line below will prevent it of doing such trying to byte-compile
    # could produce some issues with not existing python3 on some systems like centos7 more info:
    # https://fedoraproject.org/wiki/Changes/No_more_automagic_Python_bytecompilation#Status_quo
    # https://stackoverflow.com/questions/69988093/cmake-brp-python-bytecompile-and-python3
    set(CPACK_RPM_SPEC_INSTALL_POST "%global _python_bytecompile_extra 0")
    set(CPACK_RPM_SPEC_MORE_DEFINE "%global _python_bytecompile_extra 0")
    # Turn off the brp-python-bytecompile script for every release including EPEL more info here:
    # https://pagure.io/packaging-committee/issue/755
    set(CPACK_RPM_SPEC_INSTALL_POST
        "%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')"
    )
    set(CPACK_RPM_SPEC_MORE_DEFINE
        "%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')"
    )
    # otherwise it could be solved to install python3 in centos7 and adding this line: SET(CPACK_RPM_SPEC_MORE_DEFINE
    # "%define __python %{__python3}") example here: https://github.com/irods/irods/pull/6347/files but it's doesn't
    # work because of some CPACK versions don't add definitions to the spec file using CPACK_RPM_SPEC_MORE_DEFINE

    set(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})

    set(CPACK_RPM_PACKAGE_LICENSE "GPLv2")
    set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
    set(CPACK_RPM_PACKAGE_URL ${CPACK_PACKAGE_URL})
    set(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
    set(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
    set(CPACK_RPM_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})
    set(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

    set(CPACK_RPM_SPEC_MORE_DEFINE "%undefine __brp_mangle_shebangs")
    set(CPACK_RPM_PACKAGE_AUTOREQ "no")

    set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/postinst)
    set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/prerm)
    set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION ${ETC_DIR} ${SHARE_DIR})
    set(CPACK_RPM_USER_FILELIST "%config(noreplace) ${CMAPI_CONF_FILEPATH}")

    set(CPACK_RPM_PACKAGE_OBSOLETES "mariadb-columnstore-cmapi")
    set(CPACK_RPM_PACKAGE_REQUIRES "curl")
    string(REPLACE "-" "_" SERVER_VERSION ${SERVER_VERSION})
    get_linux_lsb_release_information()

    string(REGEX MATCH "^[0-9]+" OS_VERSION_MAJOR "${LSB_RELEASE_VERSION_SHORT}")
    message(STATUS ${OS_VERSION_MAJOR})
    if(LSB_RELEASE_ID_SHORT MATCHES "centos|rocky|rhel|alma|RedHatEnterprise")
        set(OS_NAME_SHORT "el")
        if(OS_VERSION_MAJOR GREATER_EQUAL 9)
            set(CPACK_RPM_PACKAGE_REQUIRES "libxcrypt-compat")
        endif()
    else()
        set(OS_NAME_SHORT "unknown")
    endif()
    set(OS_VERSION "${OS_NAME_SHORT}${OS_VERSION_MAJOR}")
    message(STATUS "OS_VERSION ${OS_VERSION}")

    set(CPACK_RPM_PACKAGE_FILE_NAME
        "${CPACK_PACKAGE_NAME}-${SERVER_VERSION}_${CMAPI_PACKAGE_VERSION}-${CMAPI_VERSION_RELEASE}.${OS_VERSION}.${CMAKE_HOST_SYSTEM_PROCESSOR}"
    )
    message(STATUS "CPACK_RPM_PACKAGE_FILE_NAME ${CPACK_RPM_PACKAGE_FILE_NAME}")
    set(CPACK_PACKAGE_FILE_NAME ${CPACK_RPM_PACKAGE_FILE_NAME})
    # version and release the same as in Columnstore engine package
    set(CPACK_RPM_PACKAGE_VERSION "${SERVER_VERSION}_${CMAPI_PACKAGE_VERSION}")
    set(CPACK_RPM_PACKAGE_RELEASE "${CMAPI_VERSION_RELEASE}.${OS_VERSION}")
endif()

option(DEB "Build a DEB" OFF)
if(DEB)
    set(CPACK_GENERATOR "DEB")

    # TODO: different names in deb and rpm packages, fix it in next releases.
    string(TOLOWER ${CPACK_PACKAGE_NAME} CPACK_DEBIAN_PACKAGE_NAME)
    string(TOLOWER ${CPACK_PACKAGE_NAME} CPACK_PACKAGE_NAME)
    set(CPACK_DEBIAN_PACKAGE_LICENSE "GPLv2")
    set(CPACK_DEBIAN_PACKAGE_URL ${CPACK_PACKAGE_URL})
    set(CPACK_DEBIAN_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
    set(CPACK_DEBIAN_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
    set(CPACK_DEBIAN_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})
    set(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

    set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
        "${CMAKE_CURRENT_SOURCE_DIR}/prerm;${CMAKE_CURRENT_SOURCE_DIR}/postinst;${CMAKE_CURRENT_SOURCE_DIR}/conffiles"
    )

    set(CPACK_DEBIAN_PACKAGE_REPLACES "mariadb-columnstore-cmapi")
    set(CPACK_DEBIAN_PACKAGE_DEPENDS "curl")
    string(REPLACE "-" "." SERVER_VERSION ${SERVER_VERSION})
    set(PATCHLEVEL "+maria")
    get_linux_lsb_release_information()
    string(REGEX MATCH "^..." LSBID ${LSB_RELEASE_ID_SHORT})
    message(STATUS "LSBID ${LSBID}")
    string(REPLACE "." "" LSBVERSION ${LSB_RELEASE_VERSION_SHORT})
    message(STATUS "LSBVERSION ${LSBVERSION}")
    execute_process(
        COMMAND dpkg-architecture -q DEB_BUILD_ARCH
        OUTPUT_VARIABLE ARCHITECTURE
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "ARCHITECTURE detected ${ARCHITECTURE}")
    set(CPACK_DEBIAN_PACKAGE_FILE_NAME
        "${CPACK_PACKAGE_NAME}_${SERVER_VERSION}-${CMAPI_PACKAGE_VERSION}${PATCHLEVEL}~${LSBID}${LSBVERSION}_${ARCHITECTURE}"
    )
    message(STATUS "CPACK_DEBIAN_PACKAGE_FILE_NAME ${CPACK_DEBIAN_PACKAGE_FILE_NAME}")
    set(CPACK_PACKAGE_FILE_NAME ${CPACK_DEBIAN_PACKAGE_FILE_NAME})
    # making possible to store several cmapi packages in one repo (same version as in Columnstore engine package)
    set(CPACK_DEBIAN_PACKAGE_VERSION "${SERVER_VERSION}-${CMAPI_PACKAGE_VERSION}${PATCHLEVEL}~${LSBID}${LSBVERSION}")
endif()

include(CPack)
