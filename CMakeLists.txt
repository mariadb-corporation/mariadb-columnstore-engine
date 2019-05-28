cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

if(POLICY CMP0069)
endif()

if(POLICY CMP0083)
  cmake_policy(SET CMP0083 NEW)  # CMake > 3.13 adds linker flags for position independent executables. Old behavior does not
endif()

#[[
CMake scripts are executed twice to generate a builder. The first execution is the 'Configure' stage. Subsequent
executions are the 'Generate' stages
]]
set(CMAKE_STAGE Generate)
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_STAGE Configure)
endif()

#[[ ISA_SUB_PROJECT indicates whether this script is run as a result of add_subdirectory ]]
get_directory_property(_parentdir PARENT_DIRECTORY)
set(ISA_SUB_PROJECT FALSE)
if(EXISTS "${_parentdir}")
  set(ISA_SUB_PROJECT TRUE)
endif()

project(columnstore
  LANGUAGES C CXX
  VERSION 1.4.0.1
  DESCRIPTION "Massively parallel and distributed columnar storage engine"
  HOMEPAGE_URL "https://mariadb.com/kb/en/library/mariadb-columnstore/")


list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)

include(GNUInstallDirs)
include(CMakeDependentOption)
include(CMakeGraphVizOptions)
include(ExternalProject)
include(CheckCXXSourceCompiles)
include(CheckCXXCompilerFlag)
include(CheckIPOSupported)
include(CheckPIESupported)
include(colorize)

string(TIMESTAMP TIMESTAMP "%b %d %y %H:%M:%S")
dump(TIMESTAMP)
dump(PROJECT_VERSION)


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
find_package(OpenSSL REQUIRED)
find_package(Git)


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

dump(GIT_HASH)
dump(CMAKE_VERSION)


set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS FALSE)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE TRUE)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION TRUE)
set(REPORT_UNDEFINED_PROPERTIES ${CMAKE_CURRENT_BINARY_DIR}/undefined_cmake_variables.txt)
set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/.external)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/.external)


option(ERROR_REPORTING_ENROLLMENT "Enroll in automatic error reporting to help improve quality?" FALSE)
dump(ERROR_REPORTING_ENROLLMENT)

option(BUILD_MISSING_TOOLS "Attempt to download and build missing tools during compilation?" TRUE)
dump(BUILD_MISSING_TOOLS)

check_pie_supported(OUTPUT_VARIABLE PIE_SUPPORTED LANGUAGES CXX)
dump(CMAKE_CXX_LINK_PIE_SUPPORTED)
cmake_dependent_option(CMAKE_POSITION_INDEPENDENT_CODE "Enable position independent code" TRUE CMAKE_CXX_LINK_PIE_SUPPORTED FALSE)
mark_as_advanced(CMAKE_POSITION_INDEPENDENT_CODE)

check_ipo_supported(RESULT IPO_SUPPORTED OUTPUT _ignore)
dump(IPO_SUPPORTED)
cmake_dependent_option(CMAKE_INTERPROCEDURAL_OPTIMIZATION "Use IPO" FALSE IPO_SUPPORTED FALSE)
mark_as_advanced(CMAKE_INTERPROCEDURAL_OPTIMIZATION)


if(NOT CMAKE_BUILD_TYPE)
  set(_default_build_type)
  if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/../.git")
    set(_default_build_type Debug)
  else()
    set(_default_build_type Release)
  endif()
  set(CMAKE_BUILD_TYPE ${_default_build_type} CACHE STRING "Build configuration type" FORCE)
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)

set(SCAN_TYPE "none" CACHE INTERNAL "specifies the scan stage when run under CI/CD")

if(SERVER_BUILD_INCLUDE_DIR OR SERVER_SOURCE_ROOT_DIR)
  warning("The parameters SERVER_BUILD_INCLUDE_DIR and SERVER_SOURCE_ROOT_DIR are deprecated.
  Try configuring with no parameters to locate a server build directory or
  pass MySQL_BINARY_DIR if needed")
endif()


if(NOT MySQL_BINARY_DIR)

  find_path(MySQL_BINARY_DIR 
      NAMES CMakeCache.txt  
      PATHS ${SERVER_BUILD_INCLUDE_DIR}/.. 
            ${CMAKE_CURRENT_BINARY_DIR}/.. 
            ${CMAKE_CURRENT_BINARY_DIR}/../..
            ${CMAKE_CURRENT_BINARY_DIR}/../../..
      DOC "MariaDB Server CMake build path"
      NO_DEFAULT_PATH
      )

  if(EXISTS MySQL_BINARY_DIR)
    load_cache(${MySQL_BINARY_DIR} READ_WITH_PREFIX TESTER_ 
      MySQL_SOURCE_DIR)
    if(NOT TESTER_MySQL_SOURCE_DIR)
      error("Could not find MariaDB Server CMake build path: MySQL_BINARY_DIR (${MySQL_BINARY_DIR})")
    endif()
  endif()
endif()



if(NOT EXISTS ${MySQL_BINARY_DIR}/CMakeCache.txt)
  error("Could not find MariaDB Server CMake build path: (${MySQL_BINARY_DIR})")
endif()


if(NOT ISA_EXTERNAL_PROJECT)
  load_cache(${MySQL_BINARY_DIR} READ_WITH_PREFIX SERVER_ 
    MySQL_SOURCE_DIR CMAKE_BUILD_TYPE CMAKE_INSTALL_PREFIX
    mariadb-connector-c_BINARY_DIR mariadb-connector-c_SOURCE_DIR
    )

  set(MySQL_SOURCE_DIR "${SERVER_MySQL_SOURCE_DIR}" CACHE PATH "Location of the server source folder" FORCE)
  set(SERVER_INSTALL_PREFIX "${SERVER_CMAKE_INSTALL_PREFIX}" CACHE PATH "" FORCE)
  set(CMAKE_BUILD_TYPE ${SERVER_CMAKE_BUILD_TYPE} CACHE STRING "Build configuration type" FORCE)

  add_library(pcre INTERFACE)
  target_include_directories(pcre INTERFACE ${MySQL_BINARY_DIR}/pcre)

endif()

get_filename_component(MySQL_SOURCE_DIR ${MySQL_SOURCE_DIR} ABSOLUTE)
get_filename_component(MySQL_BINARY_DIR ${MySQL_BINARY_DIR} ABSOLUTE)

dump(MySQL_BINARY_DIR)
dump(MySQL_SOURCE_DIR)
dump(CMAKE_BUILD_TYPE)


# Distinguish between community and non-community builds, with the
# default being a community build. This does not impact the feature
# set that will be compiled in; it's merely provided as a hint to
# custom packaging steps.
option(COMMUNITY_BUILD "Set to true if this is a community build" ON) 


option(BUILD_TESTING "Build unit tests" FALSE)
if(BUILD_TESTING)
  include(CTest)
  include(CTestCoverageCollectGCOV)
  include(cppunit)
endif()

dump(CMAKE_CXX_COMPILER_ID)
if("GNU" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(gcc)
elseif("Clang" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(clang)
elseif("Intel" STREQUAL "${CMAKE_CXX_COMPILER_ID}")
  include(intel)
endif()


include(check_compiler_flag)
include(ccache)
include(iwyu)
include(cpplint)
include(cppcheck)
include(clang-tidy)
include (configureEngine)


set (PACKAGE columnstore)
set (PACKAGE_NAME columnstore)
set (PACKAGE_TARNAME columnstore)
set (PACKAGE_BUGREPORT support@mariadb.com)
set (PACKAGE_URL "")

set (PACKAGE_STRING columnstore-${PACKAGE_VERSION})

if("Configure" STREQUAL "${CMAKE_STAGE}")
  set(CMAKE_INSTALL_PREFIX "$ENV{HOME}/local/columnstore" CACHE PATH "Installation prefix" FORCE)
endif()


# generate git info for automated system tests. A downstream test suite scans these for changes as a trigger
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/gitversionEngine "${GIT_HASH}-dirty")
# releasenum is used by external scripts for various tasks. Leave it alone.
configure_file(build/releasenum.in  build/releasenum IMMEDIATE)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum DESTINATION . COMPONENT platform)

configure_file(config.h.in  config.h)

configure_file(columnstore.h.in  columnstore.h @ONLY)
add_compile_options(-include ${CMAKE_CURRENT_BINARY_DIR}/columnstore.h)

file(RELATIVE_PATH _rel ${CMAKE_INSTALL_PREFIX}/${INSTALL_BINDIR} ${CMAKE_INSTALL_PREFIX})
set(_rpath "\$ORIGIN/${_rel}")
file(TO_NATIVE_PATH "${_rpath}/${INSTALL_LIBDIR}" message_RPATH)
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH $ORIGIN "${message_RPATH}")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_BUILD_RPATH_USE_ORIGIN TRUE)

include(pch)

add_library(columnstore_h INTERFACE)
target_include_directories(columnstore_h INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

add_library(mariadb_h INTERFACE)
target_include_directories(mariadb_h INTERFACE ${MySQL_SOURCE_DIR}/include;${MySQL_BINARY_DIR}/include;${MySQL_SOURCE_DIR}/sql;${MySQL_BINARY_DIR}/sql)

add_library(mariadb STATIC IMPORTED)

target_link_libraries(mariadb
  INTERFACE mariadb_h
    OpenSSL::Crypto
    OpenSSL::SSL
    dl
)
set_target_properties(mariadb PROPERTIES
  IMPORTED_LOCATION ${MySQL_BINARY_DIR}/libmariadb/libmariadb/libmariadbclient.a
#  INTERFACE_INCLUDE_DIRECTORIES "${MySQL_SOURCE_DIR}/include;${MySQL_BINARY_DIR}/include;${MySQL_SOURCE_DIR}/sql;${MySQL_BINARY_DIR}/sql"
  INTERFACE_COMPILE_DEFINITIONS "SAFE_MUTEX;SAFEMALLOC"
  INTERFACE_COMPILE_DEFINITIONS $<IF:$<BOOL:${_DEBUG}>,ENABLED_DEBUG_SYNC,DBUG_OFF>
)

add_library(PCRE::PCRE ALIAS pcre)
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


function(cs_add_library _target)
  add_library(${ARGV})
  target_pch(${ARGV})
endfunction()

function(cs_add_executable _target)
  add_executable(${ARGV})
  target_pch(${ARGV})
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


if( BUILD_TESTING )
    add_subdirectory(writeengine/shared)
endif()

include(cpackEngineRPM)
include(cpackEngineDEB)
