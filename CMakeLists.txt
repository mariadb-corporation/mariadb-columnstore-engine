
cmake_minimum_required(VERSION 3.13)

project(columnstore VERSION 1.4.0.0)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)
include(message)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS FALSE)
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE TRUE)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/obj)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/.external)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/.external)

if(SERVER_BUILD_DIR AND NOT IS_ABSOLUTE ${SERVER_BUILD_DIR})
  set(SERVER_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SERVER_BUILD_DIR})
endif()

if(NOT SERVER_BUILD_DIR OR NOT EXISTS ${SERVER_BUILD_DIR}/CMakeCache.txt)
  error("Required parameter must point to the server build directory: SERVER_BUILD_DIR")
endif()


load_cache("${SERVER_BUILD_DIR}" READ_WITH_PREFIX SERVER_ MySQL_SOURCE_DIR MySQL_BINARY_DIR CMAKE_BUILD_TYPE CMAKE_INSTALL_PREFIX)

set(SERVER_SOURCE_DIR "${SERVER_MySQL_SOURCE_DIR}" CACHE PATH "Location of the server source folder" FORCE)
set(SERVER_INSTALL_PREFIX "${SERVER_CMAKE_INSTALL_PREFIX}" CACHE PATH "Installation prefix" FORCE)
set(CMAKE_BUILD_TYPE ${SERVER_CMAKE_BUILD_TYPE} CACHE STRING "Build configuration type" FORCE)
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
set(_DEBUG $<CONFIG:Debug>)


find_package(Git REQUIRED)

execute_process(
  COMMAND ${GIT_EXECUTABLE} describe --match=NeVeRmAtCh --always --dirty
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  RESULT_VARIABLE _reterr OUTPUT_VARIABLE GIT_CHANGESET ERROR_VARIABLE _stderr
  OUTPUT_STRIP_TRAILING_WHITESPACE
  )

if(_reterr)
  error("Git failed with ${_reterr}: ${_stderr}")
endif()

string(TIMESTAMP _build_date "%b %d %y")
string(TIMESTAMP _build_time "%H:%M:%S")

status(
"----------------------------------
ColumnStore version: ${PROJECT_VERSION}
      CMake version: ${CMAKE_VERSION}
   Compiler version: ${CMAKE_CXX_COMPILER_VERSION}
      Git Changeset: ${GIT_CHANGESET}
      Configuration: ${CMAKE_BUILD_TYPE}
               Date: ${_build_date}
               Time: ${_build_time}
--------------------------------------
"
)

option(BUILD_TESTING "Build unit tests" FALSE)
if(BUILD_TESTING)
  include(CTest)
  include(CTestCoverageCollectGCOV)
  include(cppunit)
endif()

include(CMakeDependentOption)
include(CMakeGraphVizOptions)
include(ExternalProject)
include(CheckCXXSourceCompiles)
include(CheckCXXCompilerFlag)
include(check_compiler_flag)
include(ccache)
include(pch)

find_package(Boost 1.53.0 REQUIRED COMPONENTS system filesystem thread regex date_time)
find_package(BISON REQUIRED)
find_package(Readline REQUIRED)

# Distinguish between community and non-community builds, with the
# default being a community build. This does not impact the feature
# set that will be compiled in; it's merely provided as a hint to
# custom packaging steps.
OPTION(COMMUNITY_BUILD "Set to true if this is a community build" ON) 


check_cxx_source_compiles("#include <filesystem>\n void main(){}" HAS_STD_FILESYSTEM)
check_cxx_source_compiles("#include <experimental/filesystem>\n void main(){}" HAS_STD_EXPERIMENTAL_FILESYSTEM)

INCLUDE(columnstore_version)

SET (PACKAGE columnstore)
SET (PACKAGE_NAME columnstore)
SET (PACKAGE_TARNAME columnstore)
SET (PACKAGE_BUGREPORT support@mariadb.com)
SET (PACKAGE_URL "")

SET (PACKAGE_STRING columnstore-${PACKAGE_VERSION})


SET (ENGINE_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET (INSTALL_ENGINE "/usr/local/mariadb/columnstore")

# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

SET(CMAKE_INSTALL_RPATH "${INSTALL_ENGINE}/lib;${INSTALL_ENGINE}/mysql/lib")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${INSTALL_ENGINE}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${INSTALL_ENGINE}/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")

LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${SERVER_SOURCE_ROOT_DIR}/libmysql/" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_RPATH};${INSTALL_ENGINE}/mysql/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")

INCLUDE (configureEngine)

# releasenum is used by external scripts for various tasks. Leave it alone.
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/build/releasenum.in ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum IMMEDIATE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum DESTINATION ${INSTALL_ENGINE} COMPONENT platform)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/columnstoreversion.h.in ${CMAKE_CURRENT_BINARY_DIR}/columnstoreversion.h)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/gitversionEngine.in ${CMAKE_CURRENT_BINARY_DIR}/gitversionEngine IMMEDIATE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/gitversionEngine DESTINATION ${INSTALL_ENGINE} COMPONENT platform)

FIND_PROGRAM(LEX_EXECUTABLE flex DOC "path to the flex executable")
if(NOT LEX_EXECUTABLE)
    FIND_PROGRAM(LEX_EXECUTABLE lex DOC "path to the lex executable")
    if(NOT LEX_EXECUTABLE)
        message(FATAL_ERROR "flex/lex not found!")
    endif()
endif()

FIND_PACKAGE(LibXml2 REQUIRED)

INCLUDE (FindSnappy)
if (NOT SNAPPY_FOUND)
    MESSAGE(FATAL_ERROR "Snappy not found please install snappy-devel for CentOS/RedHat or libsnappy-dev for Ubuntu/Debian")
endif()

# Jemalloc has issues with SLES 12, so disable for now
IF (EXISTS "/etc/SuSE-release")
    SET(JEMALLOC_LIBRARIES "")
ELSE ()
    INCLUDE (FindJeMalloc)
    if (NOT JEMALLOC_FOUND)
        message(FATAL_ERROR "jemalloc not found!")
        SET(JEMALLOC_LIBRARIES "")
    endif()
ENDIF ()

FIND_PROGRAM(AWK_EXECUTABLE awk DOC "path to the awk executable")
if(NOT AWK_EXECUTABLE)
    message(FATAL_ERROR "awk not found!")
endif()


set(WARNING_LEVEL all CACHE STRING "Compiler warning level")
set_property(CACHE WARNING_LEVEL PROPERTY STRINGS 0 1 2 3 all extra)
add_compile_options($<IF:$<STREQUAL:"extra",${WARNING_LEVEL}>,-Wall$<SEMICOLON>-Wextra,-W${WARNING_LEVEL}>)

option(_PEDANTIC "Pedantic warnings" TRUE)
add_compile_options($<$<BOOL:${_PEDANTIC}>:-pedantic>)

option(_WERROR "Treat warnings as errors" FALSE)
add_compile_options($<$<BOOL:${_WERROR}>:-Werror>)

set(_default_optimization 0)
if(_DEBUG)
  set(_default_optimization 3)
endif()
set(OPTIMIZATION_LEVEL ${_default_optimization} CACHE STRING "Compiler optimization level")
set_property(CACHE OPTIMIZATION_LEVEL PROPERTY STRINGS 0 1 2 3)
add_compile_options(-O${OPTIMIZATION_LEVEL})

add_compile_definitions($<$<NOT:$<BOOL:${_DEBUG}>>:DBUG_OFF>)
add_compile_definitions($<IF:$<BOOL:${_DEBUG}>,_DEBUG,NDEBUG>)
add_compile_definitions($<$<BOOL:${_DEBUG}>:ENABLED_DEBUG_SYNC>)
add_compile_definitions($<$<BOOL:${_DEBUG}>:SAFE_MUTEX>)
add_compile_definitions($<$<BOOL:${_DEBUG}>:SAFEMALLOC>)

cmake_dependent_option(_GLIBCXX_ASSERTIONS "Enable runtime assertions" TRUE "_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_ASSERTIONS}>:_GLIBCXX_ASSERTIONS>)

cmake_dependent_option(_GLIBCXX_DEBUG "Enable STL debugging (Implies _GLIBCXX_ASSERTIONS)" TRUE "_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_DEBUG}>:_GLIBCXX_DEBUG>)

cmake_dependent_option(_GLIBCXX_DEBUG_PEDANTIC "Enable pedantic STL debugging" TRUE "_GLIBCXX_DEBUG" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_DEBUG_PEDANTIC}>:_GLIBCXX_DEBUG_PEDANTIC>)

option(_GLIBCXX_CONCEPT_CHECKS "Check for standard violations in template instantiations during compile" TRUE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_CONCEPT_CHECKS}>:_GLIBCXX_CONCEPT_CHECKS>)

option(_GLIBCXX_PROFILE "Enable libstdc++ profiling" FALSE)
add_compile_definitions($<$<BOOL:${_GLIBCXX_PROFILE}>:_GLIBCXX_PROFILE>)



#[[
INCLUDE(check_compiler_flag)

MY_CHECK_AND_SET_COMPILER_FLAG("-g -O3 -fno-omit-frame-pointer -fno-strict-aliasing -Wall -fno-tree-vectorize -D_GLIBCXX_ASSERTIONS -DDBUG_OFF -DHAVE_CONFIG_H" RELEASE RELWITHDEBINFO MINSIZEREL)
MY_CHECK_AND_SET_COMPILER_FLAG("-ggdb3 -fno-omit-frame-pointer -fno-tree-vectorize -D_GLIBCXX_ASSERTIONS -DSAFE_MUTEX -DSAFEMALLOC -DENABLED_DEBUG_SYNC -O0 -Wall -D_DEBUG -DHAVE_CONFIG_H" DEBUG)

# enable security hardening features, like most distributions do
# in our benchmarks that costs about ~1% of performance, depending on the load
IF(CMAKE_C_COMPILER_VERSION VERSION_LESS "4.6")
  SET(security_default OFF)
ELSE()
  SET(security_default ON)
ENDIF()
OPTION(SECURITY_HARDENED "Use security-enhancing compiler features (stack protector, relro, etc)" ${security_default})
OPTION(SECURITY_HARDENED_NEW "Use new security-enhancing compilier features" OFF)
IF(SECURITY_HARDENED)
  # security-enhancing flags
  MY_CHECK_AND_SET_COMPILER_FLAG("-pie -fPIC")
  MY_CHECK_AND_SET_COMPILER_FLAG("-Wl,-z,relro,-z,now")
  MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-protector --param=ssp-buffer-size=4")
  MY_CHECK_AND_SET_COMPILER_FLAG("-D_FORTIFY_SOURCE=2" RELEASE RELWITHDEBINFO)
  MY_CHECK_AND_SET_COMPILER_FLAG("-fexceptions")
  IF(SECURITY_HARDENED_NEW)
    MY_CHECK_AND_SET_COMPILER_FLAG("-mcet -fcf-protection")
    MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-protector-strong")
    MY_CHECK_AND_SET_COMPILER_FLAG("-fstack-clash-protection")
  ENDIF()
ENDIF()
]]
SET (ENGINE_LDFLAGS    "-Wl,--no-as-needed -Wl,--add-needed")
SET (ENGINE_LIBDIR     "lib")
SET (ENGINE_BINDIR     "bin")
SET (ENGINE_INCDIR     "include")
SET (ENGINE_ETCDIR     "etc")
SET (ENGINE_SYSCONFDIR "etc")
SET (ENGINE_MANDIR     "man")
SET (ENGINE_SBINDIR    "sbin")
SET (ENGINE_SHAREDIR   "share")
SET (ENGINE_POSTDIR    "post")
SET (ENGINE_LOCALDIR   "local")
SET (ENGINE_MYSQLDIR   "mysql")
SET (ENGINE_TOOLSDIR   "tools")


add_library(columnstore_h INTERFACE)
target_include_directories(columnstore_h INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

add_library(mariadb INTERFACE)
target_include_directories(mariadb INTERFACE ${SERVER_BUILD_DIR}/include ${SERVER_MySQL_SOURCE_DIR}/include)
target_compile_definitions(mariadb INTERFACE SAFE_MUTEX SAFEMALLOC)

add_library(mariadb_sql INTERFACE)
target_include_directories(mariadb_sql INTERFACE ${SERVER_SOURCE_DIR}/sql)

add_library(mariadb_pcre INTERFACE)
target_include_directories(mariadb_pcre INTERFACE ${SERVER_BUILD_DIR}/pcre)


add_library(alarmmanager_h      INTERFACE)
add_library(dbbc_h              INTERFACE)
add_library(brm_h               INTERFACE)
add_library(cacheutils_h        INTERFACE)
add_library(calmysql_h          INTERFACE)
add_library(common_h            INTERFACE)
add_library(compress_h          INTERFACE)
add_library(configcpp_h         INTERFACE)
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
add_library(libmysql_client_h   INTERFACE)
add_library(loggingcpp_h        INTERFACE)
add_library(messageqcpp_h       INTERFACE)
add_library(oamcpp_h            INTERFACE)
add_library(primproc_h          INTERFACE)
add_library(querystats_h        INTERFACE)
add_library(querytele_h         INTERFACE)
add_library(rowgroup_h          INTERFACE)
add_library(rwlock_h            INTERFACE)
add_library(threadpool_h        INTERFACE)
add_library(udfsdk_h            INTERFACE)
add_library(we_client_h         INTERFACE)
add_library(we_shared_h         INTERFACE)
add_library(we_wrapper_h        INTERFACE)
add_library(we_xml_h            INTERFACE)
add_library(we_server_h         INTERFACE)



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



add_subdirectory(writeengine/shared)
add_subdirectory(utils/libmysql_client)

#[[
add_subdirectory(utils/winport)
add_subdirectory(utils/clusterTester)
add_subdirectory(versioning)
add_subdirectory(oam)
add_subdirectory(oamapps)
]]



add_executable(fnord fnord.cpp)
target_link_libraries(fnord PRIVATE brm )




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
