
cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(columnstore
  LANGUAGES C CXX
  VERSION 1.4.0.0
  DESCRIPTION "Massively parallel and distributed columnar storage engine"
  HOMEPAGE_URL "https://mariadb.com/kb/en/library/mariadb-columnstore/")
set(RELEASE_VERSION 1)




list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)


find_package(Flex REQUIRED)
find_package(Perl REQUIRED)
find_package(Python COMPONENTS Interpreter)
find_package(Threads REQUIRED)
find_package(Boost 1.53.0 REQUIRED COMPONENTS system filesystem thread regex date_time)
find_package(BISON REQUIRED)
find_package(Readline REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(Snappy REQUIRED)
find_package(JeMalloc REQUIRED)
find_package(Git)


include(CMakeDependentOption)
include(CMakeGraphVizOptions)
include(ExternalProject)
include(CheckCXXSourceCompiles)
include(CheckCXXCompilerFlag)
include(colorize)


string(TIMESTAMP _tmp "%b %d %y %H:%M:%S")
status("${_tmp}")
unset(_tmp)
status("ColumnStore ver. ${PROJECT_VERSION} rel. ${RELEASE_VERSION}")

set(GIT_HASH "Unknown")
if(Git_FOUND AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h
    RESULT_VARIABLE _reterr 
    OUTPUT_VARIABLE GIT_HASH 
    ERROR_VARIABLE _stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

  if(_reterr)
    error("Git failed with ${_reterr}: ${_stderr}")
  endif()
endif()

status("Git hash: ${GIT_HASH}")
status("CMake version ${CMAKE_VERSION}")

set(CMAKE_VERBOSE_MAKEFILE TRUE)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS FALSE)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE TRUE)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/obj)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/.external)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/.external)


option(BUILD_MISSING_TOOLS "Attempt to download and build missing tools during compilation?" TRUE)


option(CMAKE_POSITION_INDEPENDENT_CODE "Enable position independent code" TRUE)
mark_as_advanced(CMAKE_POSITION_INDEPENDENT_CODE)


option(CMAKE_INTERPROCEDURAL_OPTIMIZATION "Enable link-time optimizations" TRUE)
mark_as_advanced(CMAKE_INTERPROCEDURAL_OPTIMIZATION)


if(NOT SERVER_BUILD_DIR)
  if(SERVER_BUILD_INCLUDE_DIR)
    set(SERVER_BUILD_DIR ${SERVER_BUILD_INCLUDE_DIR}/..)
  else()
    error("Missing parameter: SERVER_BUILD_DIR")
  endif()
endif()

find_path(SERVER_BUILD_DIR CMakeCache.txt 
  HINTS ${SERVER_BUILD_DIR} ${CMAKE_CURRENT_BINARY_DIR}/${SERVER_BUILD_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${SERVER_BUILD_DIR}
  DOC "Path to MariaDB Server CMakeCache.txt"
  NO_DEFAULT_PATH
  )

if(NOT EXISTS "${SERVER_BUILD_DIR}/CMakeCache.txt")
  error("Required parameter must point to the server build directory: SERVER_BUILD_DIR")
endif()

load_cache("${SERVER_BUILD_DIR}" READ_WITH_PREFIX SERVER_ 
  MySQL_SOURCE_DIR MySQL_BINARY_DIR CMAKE_BUILD_TYPE CMAKE_INSTALL_PREFIX
  mariadb-connector-c_BINARY_DIR mariadb-connector-c_SOURCE_DIR
  )

status("Server build directory: ${SERVER_BUILD_DIR}")

set(SERVER_SOURCE_DIR "${SERVER_MySQL_SOURCE_DIR}" CACHE PATH "Location of the server source folder" FORCE)
set(SERVER_INSTALL_PREFIX "${SERVER_CMAKE_INSTALL_PREFIX}" CACHE PATH "Installation prefix" FORCE)
set(CMAKE_BUILD_TYPE ${SERVER_CMAKE_BUILD_TYPE} CACHE STRING "Build configuration type" FORCE)
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
set(_DEBUG FALSE)
if("Debug" STREQUAL "${CMAKE_BUILD_TYPE}")
  set(_DEBUG TRUE)
endif()
status("Build type: ${CMAKE_BUILD_TYPE}")




# Distinguish between community and non-community builds, with the
# default being a community build. This does not impact the feature
# set that will be compiled in; it's merely provided as a hint to
# custom packaging steps.
OPTION(COMMUNITY_BUILD "Set to true if this is a community build" ON) 


option(BUILD_TESTING "Build unit tests" FALSE)
if(BUILD_TESTING)
  include(CTest)
  include(CTestCoverageCollectGCOV)
  include(cppunit)
endif()

if("GNU" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(gcc)
elseif("Clang" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(clang)
elseif("Intel" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(intel)
endif()


include(check_compiler_flag)
include(ccache)
include(pch)
include(iwyu)
include(cpplint)
include(cppcheck)
include(clang-tidy)


SET (PACKAGE columnstore)
SET (PACKAGE_NAME columnstore)
SET (PACKAGE_TARNAME columnstore)
SET (PACKAGE_BUGREPORT support@mariadb.com)
SET (PACKAGE_URL "")

SET (PACKAGE_STRING columnstore-${PACKAGE_VERSION})

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/usr/local/mariadb/columnstore" CACHE PATH "Installation prefix" FORCE)
endif()

# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib;${CMAKE_INSTALL_PREFIX}/mysql/lib")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
ENDIF()

LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${SERVER_SOURCE_ROOT_DIR}/libmysql/" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_RPATH};${CMAKE_INSTALL_PREFIX}/mysql/lib")
ENDIF()


INCLUDE (configureEngine)

# releasenum is used by external scripts for various tasks. Leave it alone.
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/build/releasenum.in ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum IMMEDIATE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum DESTINATION ${CMAKE_INSTALL_PREFIX} COMPONENT platform)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/columnstore.h.in ${CMAKE_CURRENT_BINARY_DIR}/columnstore.h @ONLY)
add_compile_options(-include ${CMAKE_CURRENT_BINARY_DIR}/columnstore.h)

SET (ENGINE_LIBDIR     lib)
SET (ENGINE_BINDIR     bin)
SET (ENGINE_INCDIR     include)
SET (ENGINE_ETCDIR     etc)
SET (ENGINE_SYSCONFDIR etc)
SET (ENGINE_MANDIR     man)
SET (ENGINE_SBINDIR    sbin)
SET (ENGINE_SHAREDIR   share)
SET (ENGINE_POSTDIR    post)
SET (ENGINE_LOCALDIR   local)
SET (ENGINE_MYSQLDIR   mysql)
SET (ENGINE_TOOLSDIR   tools)

add_library(columnstore_h INTERFACE)
target_include_directories(columnstore_h INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

add_library(mariadb_h INTERFACE)
target_include_directories(mariadb_h INTERFACE "${SERVER_MySQL_SOURCE_DIR}/include;${SERVER_MySQL_BINARY_DIR}/include;${SERVER_mariadb-connector-c_BINARY_DIR}/include;${SERVER_mariadb-connector-c_SOURCE_DIR}/include")

add_library(mariadb STATIC IMPORTED)
set_target_properties(mariadb PROPERTIES
  IMPORTED_LOCATION ${SERVER_mariadb-connector-c_BINARY_DIR}/libmariadb/libmariadb.so
  INTERFACE_INCLUDE_DIRECTORIES "${SERVER_MySQL_SOURCE_DIR}/include;${SERVER_MySQL_BINARY_DIR}/include;${SERVER_mariadb-connector-c_BINARY_DIR}/include;${SERVER_mariadb-connector-c_SOURCE_DIR}/include"
  INTERFACE_COMPILE_DEFINITIONS "SAFE_MUTEX;SAFEMALLOC"
  INTERFACE_COMPILE_DEFINITIONS $<IF:$<BOOL:${_DEBUG}>,ENABLED_DEBUG_SYNC,DBUG_OFF>
)


add_library(mariadb_sql INTERFACE)
target_include_directories(mariadb_sql INTERFACE ${SERVER_SOURCE_DIR}/sql)

add_library(mariadb_pcre INTERFACE)
target_include_directories(mariadb_pcre INTERFACE ${SERVER_BUILD_DIR}/pcre)


add_library(alarmmanager_h      INTERFACE)
add_library(dbbc_h              INTERFACE)
add_library(batchloader_h       INTERFACE)
add_library(brm_h               INTERFACE)
add_library(cacheutils_h        INTERFACE)
add_library(calmysql_h          INTERFACE)
add_library(common_h            INTERFACE)
add_library(compress_h          INTERFACE)
add_library(configcpp_h         INTERFACE)
add_library(crashtrace_h        INTERFACE)
add_library(dataconvert_h       INTERFACE)
add_library(ddlcleanuputil_h    INTERFACE)
add_library(ddlpackage_h        INTERFACE)
add_library(ddlpackageproc_h    INTERFACE)
add_library(dmlpackage_h        INTERFACE)
add_library(dmlpackageproc_h    INTERFACE)
add_library(execplan_h          INTERFACE)
add_library(funcexp_h           INTERFACE)
add_library(idbboot_h           INTERFACE)
add_library(idbdatafile_h       INTERFACE)
add_library(joblist_h           INTERFACE)
add_library(joiner_h            INTERFACE)
add_library(mysql_client_h      INTERFACE)
add_library(loggingcpp_h        INTERFACE)
add_library(messageqcpp_h       INTERFACE)
add_library(oamcpp_h            INTERFACE)
add_library(primproc_h          INTERFACE)
add_library(processor_h         INTERFACE)
add_library(querystats_h        INTERFACE)
add_library(querytele_h         INTERFACE)
add_library(rowgroup_h          INTERFACE)
add_library(rwlock_h            INTERFACE)
add_library(threadpool_h        INTERFACE)
add_library(udfsdk_h            INTERFACE)
add_library(writeengineclient_h INTERFACE)
add_library(writeengine_h       INTERFACE)
add_library(we_shared_h         INTERFACE)
add_library(we_xml_h            INTERFACE)
add_library(we_server_h         INTERFACE)


function(cs_add_library)
  add_library(${ARGN})
endfunction()

function(cs_add_executable)
  add_executable(${ARGN})
endfunction()



add_subdirectory(utils/startup)
add_subdirectory(utils/loggingcpp)
add_subdirectory(utils/common)
add_subdirectory(utils/configcpp)
add_subdirectory(utils/messageqcpp)
add_subdirectory(utils/threadpool)
add_subdirectory(utils/rwlock)
add_subdirectory(utils/dataconvert)
add_subdirectory(utils/joiner)
add_subdirectory(utils/funcexp)
add_subdirectory(utils/rowgroup)
add_subdirectory(utils/cacheutils)
add_subdirectory(utils/udfsdk)
add_subdirectory(utils/compress)
add_subdirectory(utils/batchloader)
add_subdirectory(utils/ddlcleanup)
add_subdirectory(utils/querystats)
add_subdirectory(utils/windowfunction)
add_subdirectory(oamapps/alarmmanager)
add_subdirectory(oam/oamcpp)
add_subdirectory(utils/idbdatafile)
add_subdirectory(utils/idbhdfs/hdfs-20)
add_subdirectory(utils/idbhdfs/hdfs-12)
add_subdirectory(utils/thrift)
add_subdirectory(utils/querytele)
add_subdirectory(utils/regr)
add_subdirectory(dbcon/execplan)
add_subdirectory(dbcon/joblist)
add_subdirectory(versioning/BRM)
add_subdirectory(oam/etc)
add_subdirectory(oam/post)
add_subdirectory(oam/install_scripts)
add_subdirectory(oam/cloud)
add_subdirectory(writeengine/shared)
add_subdirectory(writeengine/wrapper)
add_subdirectory(writeengine/client)
add_subdirectory(writeengine/xml)
add_subdirectory(writeengine/redistribute)
add_subdirectory(dbcon/ddlpackage)
add_subdirectory(dbcon/ddlpackageproc)
add_subdirectory(dbcon/dmlpackage)
add_subdirectory(dbcon/dmlpackageproc)
add_subdirectory(dbcon/mysql)
add_subdirectory(exemgr)
add_subdirectory(ddlproc)
add_subdirectory(dmlproc)
add_subdirectory(procmon)
add_subdirectory(procmgr)
add_subdirectory(oamapps/mcsadmin)
add_subdirectory(oamapps/columnstoreDB)
add_subdirectory(oamapps/postConfigure)
add_subdirectory(oamapps/serverMonitor)
add_subdirectory(oamapps/columnstoreSupport)
add_subdirectory(primitives)
add_subdirectory(tools)
add_subdirectory(writeengine/server)
add_subdirectory(writeengine/bulk)
add_subdirectory(writeengine/splitter)
add_subdirectory(utils/libmysql_client)
add_subdirectory(utils/clusterTester)


# WriteEngine component tests
IF( WITH_SHARED_COMP_TESTS )
    # search for cppunit
    INCLUDE (findcppunit.cmake)
    if (NOT CPPUNIT_FOUND)
        MESSAGE(FATAL_ERROR "CPPUnit not found please install cppunit-devel for CentOS/RedHat or libcppunit-dev for Ubuntu/Debian")
    endif()
    ADD_SUBDIRECTORY(writeengine/shared)
ENDIF( WITH_SHARED_COMP_TESTS )

INCLUDE(cpackEngineRPM)
INCLUDE(cpackEngineDEB)
