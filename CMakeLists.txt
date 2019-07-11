cmake_minimum_required(VERSION 2.8.12)
project(storagemanager)

# Generate "something" to trigger cmake rerun when VERSION changes
CONFIGURE_FILE(
  ${CMAKE_SOURCE_DIR}/VERSION
  ${CMAKE_BINARY_DIR}/VERSION.dep
)

# Read value for a variable from VERSION.

MACRO(STORAGEMANAGER_GET_CONFIG_VALUE keyword var)
  IF(NOT ${var})
    FILE (STRINGS ${CMAKE_SOURCE_DIR}/VERSION str REGEX "^[ ]*${keyword}=")
    IF(str)
      STRING(REPLACE "${keyword}=" "" str ${str})
      STRING(REGEX REPLACE  "[ ].*" ""  str "${str}")
      SET(${var} ${str})
    ENDIF()
  ENDIF()
ENDMACRO()

MACRO(GET_STORAGEMANAGER_VERSION)
  STORAGEMANAGER_GET_CONFIG_VALUE("STORAGEMANAGER_VERSION_MAJOR" MAJOR_VERSION)
  STORAGEMANAGER_GET_CONFIG_VALUE("STORAGEMANAGER_VERSION_MINOR" MINOR_VERSION)
  STORAGEMANAGER_GET_CONFIG_VALUE("STORAGEMANAGER_VERSION_PATCH" PATCH_VERSION)
  STORAGEMANAGER_GET_CONFIG_VALUE("STORAGEMANAGER_VERSION_EXTRA" EXTRA_VERSION)
  STORAGEMANAGER_GET_CONFIG_VALUE("STORAGEMANAGER_VERSION_RELEASE" RELEASE_VERSION)
  

  IF(NOT "${MAJOR_VERSION}" MATCHES "[0-9]+" OR
    NOT "${MINOR_VERSION}" MATCHES "[0-9]+" OR
    NOT "${PATCH_VERSION}" MATCHES "[0-9]+")
    MESSAGE(FATAL_ERROR "VERSION file cannot be parsed.")
  ENDIF()

  SET(VERSION "${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}${EXTRA_VERSION}")
  MESSAGE(STATUS "StorageManager ${VERSION}")
  SET(CPACK_PACKAGE_VERSION_MAJOR ${MAJOR_VERSION})
  SET(CPACK_PACKAGE_VERSION_MINOR ${MINOR_VERSION})
  SET(CPACK_PACKAGE_VERSION_PATCH ${PATCH_VERSION}${EXTRA_VERSION})
  SET(PACKAGE_VERSION "${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}${EXTRA_VERSION}")
  SET(PACKAGE_RELEASE "${RELEASE_VERSION}")
ENDMACRO()

exec_program("git"
    ${CMAKE_CURRENT_SOURCE_DIR}
    ARGS "describe --match=NeVeRmAtCh --always --dirty"
    OUTPUT_VARIABLE GIT_VERSION)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/gitVersionSM.in ${CMAKE_CURRENT_BINARY_DIR}/gitVersionSM IMMEDIATE)

# Get columnstore version
GET_STORAGEMANAGER_VERSION()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(include ${CMAKE_BINARY_DIR}/include)

set(storagemanager_SRCS 
    src/AppendTask.cpp
    src/ClientRequestProcessor.cpp
    src/ListDirectoryTask.cpp
    src/OpenTask.cpp
    src/PingTask.cpp
    src/PosixTask.cpp
    src/ProcessTask.cpp
    src/ReadTask.cpp
    src/StatTask.cpp
    src/ThreadPool.cpp
    src/TruncateTask.cpp
    src/UnlinkTask.cpp
    src/WriteTask.cpp
    src/CopyTask.cpp
    src/IOCoordinator.cpp
    src/SessionManager.cpp
    src/Config.cpp
    src/CloudStorage.cpp
    src/S3Storage.cpp
    src/LocalStorage.cpp
    src/Cache.cpp
    src/SMLogging.cpp
    src/Downloader.cpp
    src/Synchronizer.cpp
    src/RWLock.cpp
    src/MetadataFile.cpp
    src/Replicator.cpp
    src/Utilities.cpp
)

option(TRACE "Enable some tracing output" OFF)
option(LOG_TO_STDERR "Make the logging system also print to stderr" OFF)
if (TRACE)
    add_definitions(-DSM_TRACE)
endif()

if (CMAKE_BUILD_TYPE STREQUAL Debug)
    add_definitions(-DDEBUG -Werror -Wall)
    set(S3_CONFIGURE_OPT --enable-debug)
endif()

if (LOG_TO_STDERR)
    add_definitions(-DDEBUG)
endif()

set(S3API_DIR ${CMAKE_SOURCE_DIR}/libmarias3)

include(ExternalProject)
ExternalProject_Add(ms3
    SOURCE_DIR ${S3API_DIR}
    CONFIGURE_COMMAND autoreconf -fi ${S3API_DIR} && ${S3API_DIR}/configure --enable-shared --disable-static --prefix=${CMAKE_BINARY_DIR} ${S3_CONFIGURE_OPT}
    BUILD_COMMAND make
    BUILD_IN_SOURCE 0
    INSTALL_DIR ${CMAKE_INSTALL_PREFIX}
    EXCLUDE_FROM_ALL TRUE
)

#add_library(marias3 SHARED IMPORTED)
#set_property(TARGET marias3 PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/lib/libmarias3.so.3.0.2)

set(S3API_DEPS marias3 curl xml2 mhash)

# get linkage right
link_directories(${CMAKE_BINARY_DIR}/lib)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_library(storagemanager SHARED ${storagemanager_SRCS})
target_compile_definitions(storagemanager PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
target_link_libraries(storagemanager boost_system boost_thread boost_filesystem boost_regex pthread ${S3API_DEPS})
add_dependencies(storagemanager ms3)

add_executable(StorageManager src/main.cpp)
target_link_libraries(StorageManager storagemanager)
set_property(TARGET StorageManager PROPERTY CXX_STANDARD 11)

# There's probably a better way to do this, but this works, so moving on
# for now.
add_custom_target(test_files
    mkdir -p ${CMAKE_BINARY_DIR}/test_data && cp -R ${CMAKE_SOURCE_DIR}/test_data/* ${CMAKE_BINARY_DIR}/test_data
)
add_executable(unit_tests src/unit_tests.cpp)
target_compile_definitions(unit_tests PUBLIC BOOST_NO_CXX11_SCOPED_ENUMS)
add_dependencies(unit_tests test_files)
target_link_libraries(unit_tests storagemanager)
set_property(TARGET unit_tests PROPERTY CXX_STANDARD 11)

add_executable(smcat src/smcat.cpp)
target_link_libraries(smcat storagemanager)

add_executable(smput src/smput.cpp)
target_link_libraries(smput storagemanager)

add_executable(smls src/smls.cpp)
target_link_libraries(smls storagemanager)

add_executable(smrm src/smrm.cpp)
target_link_libraries(smrm storagemanager)

install(TARGETS StorageManager smcat smput smls smrm storagemanager
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

install(DIRECTORY ${CMAKE_BINARY_DIR}/lib DESTINATION ${CMAKE_INSTALL_PREFIX} FILES_MATCHING PATTERN "libmarias3*")

install(FILES storagemanager.cnf DESTINATION ${CMAKE_INSTALL_PREFIX}/etc)

SET(CPACK_PACKAGE_NAME "mariadb-storagemanager")
SET(ENGINE_ARCH "amd64")

SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${PACKAGE_VERSION}-${PACKAGE_RELEASE}")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB StorageManager: ")
SET(CPACK_PACKAGE_URL "http://www.mariadb.com")
SET(CPACK_PACKAGE_CONTACT "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_SUMMARY "MariaDB StorageManager: ")
SET(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_LICENSE "Copyright (c) 2018 MariaDB Corporation Ab., all rights reserved; redistributable under the terms of the BSL, see the file LICENSE.txt for details.")

IF(DEB)

  SET(CPACK_GENERATOR "DEB")

  SET(ENGINE_ARCH "amd64")

  IF (NOT CPACK_DEBIAN_PACKAGE_VERSION)
      SET (CPACK_DEBIAN_PACKAGE_VERSION ${PACKAGE_VERSION})
  ENDIF()
  IF (NOT CPACK_DEBIAN_PACKAGE_RELEASE)
      SET (CPACK_DEBIAN_PACKAGE_RELEASE ${PACKAGE_RELEASE})
  ENDIF()

  SET(CPACK_DEBIAN_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
  SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}-${ENGINE_ARCH}-${DEB}")

  SET(CPACK_DEBIAN_PACKAGE_LICENSE "BSL")
  SET(CPACK_DEBIAN_PACKAGE_RELOCATABLE FALSE)
  SET(CPACK_DEBIAN_PACKAGE_URL ${CPACK_PACKAGE_URL})
  SET(CPACK_DEBIAN_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
  SET(CPACK_DEBIAN_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
  SET(CPACK_DEBIAN_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})

  SET(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

  SET(CPACK_DEBIAN_PACKAGE_PROVIDES "mariadb-storagemanager")
  SET(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

  INCLUDE (CPack)

ENDIF()

IF(RPM)

    SET(CPACK_GENERATOR "RPM")

    SET(ENGINE_ARCH "x86_64")

    SET(CPACK_RPM_PACKAGE_AUTOREQ "no")
    IF (NOT CPACK_RPM_PACKAGE_VERSION)
        SET (CPACK_RPM_PACKAGE_VERSION ${PACKAGE_VERSION})
    ENDIF()
    IF (NOT CPACK_RPM_PACKAGE_RELEASE)
        SET (CPACK_RPM_PACKAGE_RELEASE ${PACKAGE_RELEASE})
    ENDIF()

    SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
    SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}-${ENGINE_ARCH}-${RPM}")

    SET(CPACK_RPM_PACKAGE_LICENSE "BSL")
    SET(CPACK_RPM_PACKAGE_RELOCATABLE FALSE)
    SET(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
    SET(CPACK_RPM_PACKAGE_URL ${CPACK_PACKAGE_URL})
    SET(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
    SET(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
    SET(CPACK_RPM_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})

    SET(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})

    # "set/append array" - append a set of strings, separated by a space
    MACRO(SETA var)
        FOREACH(v ${ARGN})
            SET(${var} "${${var}} ${v}")
        ENDFOREACH()
    ENDMACRO(SETA)

    SETA(CPACK_RPM_PACKAGE_PROVIDES "mariadb-storagemanager")

    SET(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}
        %define ignore \#
    ")

    SET(ignored
        "%ignore /usr"
        "%ignore /usr/local"
        "%ignore /usr/local/mariadb/columnstore/lib"
    )

    SET(CPACK_RPM_USER_FILELIST ${ignored})

    INCLUDE (CPack)

ENDIF()

