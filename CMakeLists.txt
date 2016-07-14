
CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
# Avoid warnings in higher versions
if("${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}" GREATER 2.6)
 CMAKE_POLICY(VERSION 2.8)
endif()

# explicitly set the policy to OLD
# (cannot use NEW, not everyone is on cmake-2.8.12 yet)
IF(POLICY CMP0022)
  CMAKE_POLICY(SET CMP0022 OLD)
ENDIF()

# We use the LOCATION target property (CMP0026)
# and get_target_property() for non-existent targets (CMP0045)
# and INSTALL_NAME_DIR (CMP0042)
IF(CMAKE_VERSION VERSION_EQUAL "3.0.0" OR
   CMAKE_VERSION VERSION_GREATER "3.0.0")
 CMAKE_POLICY(SET CMP0026 OLD)
 CMAKE_POLICY(SET CMP0045 OLD)
 CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

MESSAGE(STATUS "Running cmake version ${CMAKE_VERSION}")

#set( CMAKE_VERBOSE_MAKEFILE on )



#AC_PREREQ(2.59)
#AC_INIT(columnstore, 1.0.2, support@mariadb.com)

#AC_CONFIG_MACRO_DIR([m4])

SET (ENGINE_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})

SET (INSTALL_ENGINE "/usr/local/mariadb/columnstore")

#
# Tests for header files
#
INCLUDE (CheckIncludeFiles)
INCLUDE (CheckIncludeFileCXX)
INCLUDE (CheckCSourceCompiles)
INCLUDE (CheckCXXSourceCompiles)
INCLUDE (CheckStructHasMember)
INCLUDE (CheckLibraryExists)
INCLUDE (CheckFunctionExists)
INCLUDE (CheckCCompilerFlag)
INCLUDE (CheckCXXCompilerFlag)
INCLUDE (CheckCSourceRuns)
INCLUDE (CheckSymbolExists)
INCLUDE (CheckTypeSize)

CHECK_INCLUDE_FILES (alloca.h HAVE_ALLOCA_H)
CHECK_INCLUDE_FILES (arpa/inet.h HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES (dlfcn.h HAVE_DLFCN_H)
CHECK_INCLUDE_FILES (fcntl.h HAVE_FCNTL_H)
CHECK_INCLUDE_FILES (inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES (limits.h HAVE_LIMITS_H)
CHECK_INCLUDE_FILES (malloc.h HAVE_MALLOC_H)
CHECK_INCLUDE_FILES (memory.h HAVE_MEMORY_H)
CHECK_INCLUDE_FILES (ncurses.h HAVE_NCURSES_H)
CHECK_INCLUDE_FILES (netdb.h HAVE_NETDB_H)
CHECK_INCLUDE_FILES (netinet/in.h HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILES (readline/readline.h HAVE_READLINE_READLINE_H)
CHECK_INCLUDE_FILES (stddef.h HAVE_STDDEF_H)
CHECK_INCLUDE_FILES (stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILES (stdlib.h HAVE_STDLIB_H)
CHECK_INCLUDE_FILES (strings.h HAVE_STRINGS_H)
CHECK_INCLUDE_FILES (string.h HAVE_STRING_H)
CHECK_INCLUDE_FILES (syslog.h HAVE_SYSLOG_H)
CHECK_INCLUDE_FILES (sys/file.h HAVE_SYS_FILE_H)
CHECK_INCLUDE_FILES (sys/mount.h HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES (sys/select.h HAVE_SYS_SELECT_H)
CHECK_INCLUDE_FILES (sys/socket.h HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILES (sys/statfs.h HAVE_SYS_STATFS_H)
CHECK_INCLUDE_FILES (sys/stat.h HAVE_SYS_STAT_H)
CHECK_INCLUDE_FILES (sys/timeb.h HAVE_SYS_TIMEB_H)
CHECK_INCLUDE_FILES (sys/time.h HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES (sys/types.h HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES (sys/wait.h HAVE_SYS_WAIT_H)
CHECK_INCLUDE_FILES (unistd.h HAVE_UNISTD_H)
CHECK_INCLUDE_FILES (utime.h HAVE_UTIME_H)
CHECK_INCLUDE_FILES (values.h HAVE_VALUES_H)
CHECK_INCLUDE_FILES (vfork.h HAVE_VFORK_H)
CHECK_INCLUDE_FILES (wchar.h HAVE_WCHAR_H)
CHECK_SYMBOL_EXISTS(mbstate_t "wchar.h" HAVE_MBSTATE_T)
CHECK_INCLUDE_FILES (wctype.h HAVE_WCTYPE_H)
CHECK_INCLUDE_FILES (zlib.h HAVE_ZLIB_H)


CHECK_FUNCTION_EXISTS (getb67 CRAY_STACKSEG_END)
CHECK_FUNCTION_EXISTS (alarm HAVE_ALARM)
CHECK_FUNCTION_EXISTS (alloca HAVE_ALLOCA)
CHECK_FUNCTION_EXISTS (btowc HAVE_BTOWC)
CHECK_FUNCTION_EXISTS (dup2 HAVE_DUP2)
CHECK_FUNCTION_EXISTS (floor HAVE_FLOOR)
CHECK_FUNCTION_EXISTS (fork HAVE_FORK)
CHECK_FUNCTION_EXISTS (ftime HAVE_FTIME)
CHECK_FUNCTION_EXISTS (ftruncate HAVE_FTRUNCATE)
CHECK_FUNCTION_EXISTS (getenv HAVE_DECL_GETENV)
CHECK_FUNCTION_EXISTS (gethostbyname HAVE_GETHOSTBYNAME)
CHECK_FUNCTION_EXISTS (getpagesize HAVE_GETPAGESIZE)
CHECK_FUNCTION_EXISTS (gettimeofday HAVE_GETTIMEOFDAY)
CHECK_FUNCTION_EXISTS (inet_ntoa HAVE_INET_NTOA)
CHECK_FUNCTION_EXISTS (isascii HAVE_ISASCII)
CHECK_FUNCTION_EXISTS (localtime_r HAVE_LOCALTIME_R)
CHECK_FUNCTION_EXISTS (malloc HAVE_MALLOC)
CHECK_FUNCTION_EXISTS (mbsrtowcs HAVE_MBSRTOWCS)
CHECK_FUNCTION_EXISTS (memchr HAVE_MEMCHR)
CHECK_FUNCTION_EXISTS (memmove HAVE_MEMMOVE)
CHECK_FUNCTION_EXISTS (memcpy HAVE_MEMCPY)
CHECK_FUNCTION_EXISTS (memset HAVE_MEMSET)
CHECK_FUNCTION_EXISTS (mkdir HAVE_MKDIR)
CHECK_FUNCTION_EXISTS (pow HAVE_POW)
CHECK_FUNCTION_EXISTS (regcomp HAVE_REGCOMP)
CHECK_FUNCTION_EXISTS (rmdir HAVE_RMDIR)
CHECK_FUNCTION_EXISTS (select HAVE_SELECT)
CHECK_FUNCTION_EXISTS (setenv HAVE_SETENV)
CHECK_FUNCTION_EXISTS (setlocale HAVE_SETLOCALE)
CHECK_FUNCTION_EXISTS (socket HAVE_SOCKET)
CHECK_FUNCTION_EXISTS (strcasecmp HAVE_STRCASECMP)
CHECK_FUNCTION_EXISTS (strchr HAVE_STRCHR)
CHECK_FUNCTION_EXISTS (strcspn HAVE_STRCSPN)
CHECK_FUNCTION_EXISTS (strdup HAVE_STRDUP)
CHECK_FUNCTION_EXISTS (strerror HAVE_STRERROR)
CHECK_FUNCTION_EXISTS (strerror_r HAVE_STRERROR_R)
CHECK_FUNCTION_EXISTS (strftime HAVE_STRFTIME)
CHECK_FUNCTION_EXISTS (strrchr HAVE_STRRCHR)
CHECK_FUNCTION_EXISTS (strspn HAVE_STRSPN)
CHECK_FUNCTION_EXISTS (strstr HAVE_STRSTR)
CHECK_FUNCTION_EXISTS (strtol HAVE_STRTOL)
CHECK_FUNCTION_EXISTS (strtoul HAVE_STRTOUL)
CHECK_FUNCTION_EXISTS (strtoull HAVE_STRTOULL)
CHECK_FUNCTION_EXISTS (utime HAVE_UTIME)
CHECK_FUNCTION_EXISTS (vfork HAVE_VFORK)
CHECK_FUNCTION_EXISTS (wmempcpy HAVE_WMEMPCPY)

CHECK_TYPE_SIZE (ptrdiff_t PTRDIFF_T)

#AC_CONFIG_HEADER([config.h])
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)

#AC_ENABLE_STATIC([no])
#AC_PROG_LIBTOOL
#AC_PROG_INSTALL


#AC_PROG_LEX
#if test "x$LEX" = "x:"; then
#    AC_MSG_ERROR([Could not find a usable lex program!])
#fi

INCLUDE(bison.cmake)

SET (LEX_EXECUTABLE /usr/bin/flex)

#AC_PROG_AWK
#AC_PROG_CPP
#AC_PROG_LN_S
#AC_PROG_MAKE_SET

## FIXME:
## for quicklz (note this macro is not needed if autotools is 1.14 or 
## greater, but old versions of CentOS do not have newer version
## of autotools
#AM_PROG_CC_C_O

# Checks for header files.
#AC_FUNC_ALLOCA
#AC_HEADER_STDC
#AC_HEADER_SYS_WAIT

# Checks for typedefs, structures, and compiler characteristics.
#AC_HEADER_STAT
#AC_HEADER_STDBOOL
#AC_C_CONST
#AC_C_INLINE
#AC_TYPE_MODE_T
#AC_TYPE_OFF_T
#AC_TYPE_PID_T
#AC_C_RESTRICT
#AC_TYPE_SIZE_T
#AC_HEADER_TIME
#AC_STRUCT_TM
#AC_C_VOLATILE

# Checks for library functions.
#AC_FUNC_ERROR_AT_LINE
#AC_REPLACE_FNMATCH
#AC_FUNC_FORK
#AC_FUNC_MALLOC
#AC_FUNC_MEMCMP
#AC_FUNC_MKTIME
#AC_FUNC_SELECT_ARGTYPES
#AC_FUNC_SETVBUF_REVERSED
#AC_TYPE_SIGNAL
#AC_FUNC_STAT
#AC_FUNC_STRERROR_R
#AC_FUNC_STRFTIME
#AC_FUNC_STRTOD
#AC_FUNC_UTIME_NULL
#AC_CHECK_FUNCS([alarm dup2 floor ftime ftruncate gethostbyname getpagesize gettimeofday inet_ntoa isascii localtime_r memchr memmove memset mkdir pow regcomp rmdir select setenv setlocale socket strcasecmp strchr strcspn strdup strerror strrchr strspn strstr strtol strtoul strtoull utime])

#AM_PATH_XML2([2.6.26])
#if test "x$XML_CPPFLAGS" = "x"; then
#    AC_MSG_ERROR([Could not find a usable libxml2 development environment!])
#fi

#AC_MSG_CHECKING(whether to enable debugging)
#AC_SUBST([idb_cppflags], [' '])
#AC_ARG_WITH([debug],
#    AS_HELP_STRING([--with-debug],[include debugging symbols/no optimization]))
#if test "x$with_debug" = "xyes"; then
#    AC_SUBST([idb_cxxflags],['-ggdb3 -O0 -Wall -D_DEBUG'])
#    AC_SUBST([idb_cflags],['-ggdb3 -O0 -Wall -D _DEBUG'])
#    AC_MSG_RESULT(yes)
#else
#    AC_SUBST([idb_cxxflags],["-g0 -O3 -fno-strict-aliasing -Wall $cxx_extra_flags"])
#    AC_SUBST([idb_cflags],['-g0 -O3 -fno-strict-aliasing -Wall'])
#    AC_MSG_RESULT(no)
#fi


SET(CMAKE_CXX_FLAGS "-g0 -O3 -fno-strict-aliasing -Wall -DHAVE_CONFIG_H")
SET(CMAKE_C_FLAGS "-g0 -O3 -fno-strict-aliasing -Wall -DHAVE_CONFIG_H")

#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb3 -O0 -Wall -D_DEBUG -DHAVE_CONFIG_H")
#SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb3 -O0 -Wall -D _DEBUG -DHAVE_CONFIG_H")

#AC_SUBST([idb_ldflags],['-Wl,--rpath -Wl,${idbinstall}/lib -Wl,--no-as-needed -Wl,--add-needed'])
SET (ENGINE_LDFLAGS    "-Wl,--rpath -Wl,${INSTALL_ENGINE}/lib -Wl,--no-as-needed -Wl,--add-needed")  


SET (ENGINE_LIBDIR     "${INSTALL_ENGINE}/lib")
SET (ENGINE_BINDIR     "${INSTALL_ENGINE}/bin")
SET (ENGINE_INCDIR     "${INSTALL_ENGINE}/include")
SET (ENGINE_ETCDIR     "${INSTALL_ENGINE}/etc")
SET (ENGINE_SYSCONFDIR "${INSTALL_ENGINE}/etc")
SET (ENGINE_MANDIR     "${INSTALL_ENGINE}/man")
SET (ENGINE_SBINDIR    "${INSTALL_ENGINE}/sbin")
SET (ENGINE_SHAREDIR   "${INSTALL_ENGINE}/share")
SET (ENGINE_POSTDIR    "${INSTALL_ENGINE}/post")
SET (ENGINE_LOCALDIR   "${INSTALL_ENGINE}/local")
SET (ENGINE_MYSQLDIR   "${INSTALL_ENGINE}/mysql")
SET (ENGINE_MIBDIR     "${INSTALL_ENGINE}/share/snmp/mibs")
SET (ENGINE_TOOLSDIR   "${INSTALL_ENGINE}/tools")

SET (ENGINE_NETSNMP_LIBS    "-L${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp/snmplib/.libs -L${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp/agent/helpers/.libs -L${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp/agent/.libs/ -L${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp/apps/.libs/ -lnetsnmpmibs -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers")
SET (ENGINE_COMMON_LIBS     messageqcpp loggingcpp configcpp idbboot boost_idb xml2 pthread rt)
SET (ENGINE_OAM_LIBS        oamcpp snmpmanager ${ENGINE_NETSNMP_LIBS})
SET (ENGINE_BRM_LIBS        brm idbdatafile cacheutils rwlock ${ENGINE_OAM_LIBS} ${ENGINE_COMMON_LIBS})
SET (ENGINE_EXEC_LIBS       joblist execplan windowfunction joiner rowgroup funcexp udfsdk dataconvert common compress mysqlcl_idb querystats querytele thrift threadpool ${ENGINE_BRM_LIBS})
SET (ENGINE_WRITE_LIBS      ddlpackageproc ddlpackage dmlpackageproc dmlpackage writeengine writeengineclient idbdatafile cacheutils ${ENGINE_EXEC_LIBS})

SET (ENGINE_COMMON_LDFLAGS  "")


SET (ENGINE_UTILS_XML_INCLUDE         "${CMAKE_CURRENT_SOURCE_DIR}/utils/libxml")
SET (ENGINE_UTILS_MESSAGEQCPP_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/utils/messageqcpp")
SET (ENGINE_WE_SHARED_INCLUDE         "${CMAKE_CURRENT_SOURCE_DIR}/writeengine/shared")
SET (ENGINE_UTILS_IDBDATAFILE_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/utils/idbdatafile")
SET (ENGINE_UTILS_LOGGINGCPP_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/utils/loggingcpp")
SET (ENGINE_UTILS_CONFIGCPP_INCLUDE   "${CMAKE_CURRENT_SOURCE_DIR}/utils/configcpp")
SET (ENGINE_UTILS_COMPRESS_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/utils/compress")
SET (ENGINE_VERSIONING_BRM_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/versioning/BRM") 
SET (ENGINE_UTILS_ROWGROUP_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/utils/rowgroup") 
SET (ENGINE_UTILS_COMMON_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/utils/common")
SET (ENGINE_UTILS_DATACONVERT_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/utils/dataconvert")
SET (ENGINE_UTILS_RWLOCK_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/utils/rwlock")
SET (ENGINE_UTILS_FUNCEXP_INCLUDE     "${CMAKE_CURRENT_SOURCE_DIR}/utils/funcexp")
SET (ENGINE_SNMPD_SNMPMANAGER_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/snmpd/snmpmanager")
SET (ENGINE_UTILS_INCLUDE             "${CMAKE_CURRENT_SOURCE_DIR}/utils")
SET (ENGINE_OAM_OAMCPP_INCLUDE        "${CMAKE_CURRENT_SOURCE_DIR}/oam/oamcpp")
SET (ENGINE_DBCON_DDLPKGPROC_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/ddlpackageproc")
SET (ENGINE_DBCON_DDLPKG_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/ddlpackage")
SET (ENGINE_DBCON_EXECPLAN_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/execplan")
SET (ENGINE_UTILS_STARTUP_INCLUDE     "${CMAKE_CURRENT_SOURCE_DIR}/utils/startup")
SET (ENGINE_DBCON_JOBLIST_INCLUDE     "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/joblist")
SET (ENGINE_WE_WRAPPER_INCLUDE        "${CMAKE_CURRENT_SOURCE_DIR}/writeengine/wrapper")
SET (ENGINE_WE_SERVER_INCLUDE         "${CMAKE_CURRENT_SOURCE_DIR}/writeengine/server")
SET (ENGINE_DBCON_DMLPKG_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/dmlpackage")
SET (ENGINE_WE_CLIENT_INCLUDE         "${CMAKE_CURRENT_SOURCE_DIR}/writeengine/client")
SET (ENGINE_DBCON_DMLPKGPROC_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/dbcon/dmlpackageproc")
SET (ENGINE_UTILS_CACHEUTILS_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/utils/cacheutils")
SET (ENGINE_UTILS_MYSQLCL_INCLUDE     "${CMAKE_CURRENT_SOURCE_DIR}/utils/mysqlcl_idb")
SET (ENGINE_UTILS_QUERYTELE_INCLUDE   "${CMAKE_CURRENT_SOURCE_DIR}/utils/querytele")
SET (ENGINE_UTILS_BOOSTIDB_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/utils/boost_idb")
SET (ENGINE_UTILS_THRIFT_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/utils/thrift")
SET (ENGINE_UTILS_JOINER_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/utils/joiner")
SET (ENGINE_UTILS_THREADPOOL_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/utils/threadpool")
SET (ENGINE_UTILS_BATCHLDR_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/utils/batchloader")
SET (ENGINE_UTILS_DDLCLEANUP_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/utils/ddlcleanup")
SET (ENGINE_UTILS_QUERYSTATS_INCLUDE  "${CMAKE_CURRENT_SOURCE_DIR}/utils/querystats")
SET (ENGINE_WE_CONFIGCPP_INCLUDE      "${CMAKE_CURRENT_SOURCE_DIR}/writeengine/xml")
SET (ENGINE_SERVER_SQL_INCLUDE        "${CMAKE_CURRENT_SOURCE_DIR}/../../../sql") 
SET (ENGINE_SERVER_SQL2_INCLUDE       "${CMAKE_CURRENT_SOURCE_DIR}/../sql")
SET (ENGINE_SERVER_INCLUDE_INCLUDE    "${CMAKE_CURRENT_SOURCE_DIR}/../../../include")
SET (ENGINE_SERVER_INCLUDE_INCLUDE2   "${CMAKE_CURRENT_SOURCE_DIR}/../include")
SET (ENGINE_SERVER_PCRE_INCLUDE       "${CMAKE_CURRENT_SOURCE_DIR}/../../../pcre") 
SET (ENGINE_SERVER_PCRE_INCLUDE2      "${CMAKE_CURRENT_SOURCE_DIR}/../pcre")
SET (ENGINE_NETSNMP_INCLUDE           "${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp")
SET (ENGINE_NETSNMP_INCLUDE2          "${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp/include")

SET (ENGINE_DEFAULT_INCLUDES "." "../" "../..")

SET (ENGINE_COMMON_INCLUDES  ${ENGINE_DEFAULT_INCLUDES} ${ENGINE_DEFAULT_INCLUDES} ${ENGINE_UTILS_XML_INCLUDE} ${ENGINE_UTILS_MESSAGEQCPP_INCLUDE} ${ENGINE_WE_SHARED_INCLUDE} ${ENGINE_UTILS_IDBDATAFILE_INCLUDE} ${ENGINE_UTILS_LOGGINGCPP_INCLUDE} ${ENGINE_UTILS_CONFIGCPP_INCLUDE} ${ENGINE_UTILS_COMPRESS_INCLUDE} ${ENGINE_VERSIONING_BRM_INCLUDE} ${ENGINE_UTILS_ROWGROUP_INCLUDE} ${ENGINE_UTILS_COMMON_INCLUDE} ${ENGINE_UTILS_DATACONVERT_INCLUDE} ${ENGINE_UTILS_RWLOCK_INCLUDE} ${ENGINE_UTILS_FUNCEXP_INCLUDE} ${ENGINE_SNMPD_SNMPMANAGER_INCLUDE} ${ENGINE_UTILS_INCLUDE} ${ENGINE_OAM_OAMCPP_INCLUDE} ${ENGINE_DBCON_DDLPKGPROC_INCLUDE} ${ENGINE_DBCON_DDLPKG_INCLUDE} ${ENGINE_DBCON_EXECPLAN_INCLUDE} ${ENGINE_UTILS_STARTUP_INCLUDE} ${ENGINE_DBCON_JOBLIST_INCLUDE} ${ENGINE_WE_WRAPPER_INCLUDE} ${ENGINE_WE_SERVER_INCLUDE} ${ENGINE_DBCON_DMLPKG_INCLUDE} ${ENGINE_WE_CLIENT_INCLUDE} ${ENGINE_DBCON_DMLPKGPROC_INCLUDE} ${ENGINE_UTILS_CACHEUTILS_INCLUDE} ${ENGINE_UTILS_MYSQLCL_INCLUDE} ${ENGINE_UTILS_QUERYTELE_INCLUDE} ${ENGINE_UTILS_THRIFT_INCLUDE} ${ENGINE_UTILS_JOINER_INCLUDE} ${ENGINE_UTILS_THREADPOOL_INCLUDE} ${ENGINE_UTILS_BATCHLDR_INCLUDE} ${ENGINE_UTILS_DDLCLEANUP_INCLUDE} ${ENGINE_UTILS_QUERYSTATS_INCLUDE} ${ENGINE_WE_CONFIGCPP_INCLUDE} ${ENGINE_SERVER_SQL_INCLUDE} ${ENGINE_SERVER_INCLUDE_INCLUDE}  ${ENGINE_SERVER_PCRE_INCLUDE} ${ENGINE_SERVER_SQL2_INCLUDE} ${ENGINE_SERVER_INCLUDE_INCLUDE2} ${ENGINE_SERVER_PCRE_INCLUDE2} ${ENGINE_NETSNMP_INCLUDE} ${ENGINE_NETSNMP_INCLUDE2})


INCLUDE (ExternalProject)
ExternalProject_Add(
  libnetsnmpmibs
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp
  CONFIGURE_COMMAND ./configure --prefix=${INSTALL_ENGINE} --with-mib-modules=disman/event-mib --with-cc=gcc --with-logfile=${INSTALL_ENGINE}/log/snmpd.log --with-sys-location=Unknown --with-sys-contact=root@localhost.localdomain --with-default-snmp-version=3 --with-persistent-directory=${INSTALL_ENGINE}/var/net-snmp --without-openssl --with-ldflags=-Wl,-rpath\ -Wl,${INSTALL_ENGINE}/lib
  BUILD_COMMAND ""
  INSTALL_COMMAND make
  BUILD_IN_SOURCE 1
)

ADD_SUBDIRECTORY(utils)
ADD_SUBDIRECTORY(oam/oamcpp)
ADD_SUBDIRECTORY(snmpd)
ADD_SUBDIRECTORY(dbcon/execplan)
ADD_SUBDIRECTORY(dbcon/joblist)
ADD_SUBDIRECTORY(versioning)
ADD_SUBDIRECTORY(oam)
ADD_SUBDIRECTORY(writeengine/wrapper)
ADD_SUBDIRECTORY(writeengine/client)
ADD_SUBDIRECTORY(writeengine/xml)
ADD_SUBDIRECTORY(writeengine/redistribute)
ADD_SUBDIRECTORY(dbcon/ddlpackage)
ADD_SUBDIRECTORY(dbcon/ddlpackageproc)
ADD_SUBDIRECTORY(dbcon/dmlpackage)
ADD_SUBDIRECTORY(dbcon/dmlpackageproc)
ADD_SUBDIRECTORY(dbcon/mysql)
ADD_SUBDIRECTORY(exemgr)
ADD_SUBDIRECTORY(ddlproc)
ADD_SUBDIRECTORY(dmlproc)
ADD_SUBDIRECTORY(procmon)
ADD_SUBDIRECTORY(procmgr)
ADD_SUBDIRECTORY(oamapps)
ADD_SUBDIRECTORY(decomsvr)
ADD_SUBDIRECTORY(primitives)
ADD_SUBDIRECTORY(tools)
ADD_SUBDIRECTORY(writeengine/server)
ADD_SUBDIRECTORY(writeengine/bulk)
ADD_SUBDIRECTORY(writeengine/splitter)

########### install files ###############


#ACLOCAL_AMFLAGS = -I m4
## FIXME:
## this sets the local linker path (-rpath) so that the ColumnStore
## libraries and binaries know where to look for the necessary
## libraries.  This is almost certainly a wrong approach, as 
## MySQL should look in the plugin path for libraries, but that
## path comes from CMake, not from autotools.  For now, until
## the ColumnStore engine is integrated with CMake into the 
## MariaDB server, the prefix must be set the same for the
## MariaDB compilation and the engine compilation.  I can't see
## a way around that for now.
##
## FIXME:
## a second problem here is that this manually specifies 
## the realtime library and a custom boost library. These
## should be detected and used automatically by the autotools
## process.  A custom boost library should not be needed
## either. The system boost should be used. I am not aware if
## there are changes made to this custom boost, so I am leaving
## this AS IS for now.
#AM_LIBTOOLFLAGS = -Wl,-rpath -Wl,$(prefix)/lib -lrt -lboost_idb
#.PHONY: test coverage leakcheck docs bootstrap install net-snmp-install
#
#net-snmp/net-snmp-5.7.3/agent/.libs/libnetsnmpmibs.so:
#	cd net-snmp/net-snmp; \
#	./configure --prefix=${INSTALL_ENGINE} --with-mib-modules=disman/event-mib --with-cc=gcc --with-logfile=$(prefix)/log/snmpd.log --with-sys-location=Unknown --with-sys-contact=root@localhost.localdomain --with-default-snmp-version=3 --with-persistent-directory=$(prefix)var/net-snmp --without-openssl --with-ldflags="-Wl,-rpath -Wl,$(prefix)/lib"
#		$(MAKE) 
#
#all-local: net-snmp/net-snmp-5.7.3/agent/.libs/libnetsnmpmibs.so
#	$(MAKE) -C utils
#	$(MAKE) -C oam/oamcpp
#	$(MAKE) -C snmpd
#	$(MAKE) -C dbcon/execplan
#	$(MAKE) -C dbcon/joblist
#	$(MAKE) -C versioning
#	$(MAKE) -C oam
#	$(MAKE) -C versioning dbrm tools
#	$(MAKE) -C writeengine/wrapper
#	$(MAKE) -C writeengine/client
#	$(MAKE) -C writeengine/xml
#	$(MAKE) -C writeengine/redistribute
#	$(MAKE) -C dbcon/ddlpackage
#	$(MAKE) -C dbcon/ddlpackageproc
#	$(MAKE) -C dbcon/dmlpackage
#	$(MAKE) -C dbcon/dmlpackageproc
#	$(MAKE) -C dbcon
#	$(MAKE) -C exemgr
#	$(MAKE) -C ddlproc
#	$(MAKE) -C dmlproc
#	$(MAKE) -C procmon
#	$(MAKE) -C procmgr
#	$(MAKE) -C oamapps
#	$(MAKE) -C decomsvr
#	$(MAKE) -C primitives
#	$(MAKE) -C tools
#	$(MAKE) -C writeengine/server
#	$(MAKE) -C writeengine/bulk
#	$(MAKE) -C writeengine/splitter
#	echo $(idb_cxxflags) $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags
#
#net-snmp-install:
#	$(MAKE) -C net-snmp install
#
#install: net-snmp-install
#	$(MAKE) -C utils install
#	$(MAKE) -C oam/oamcpp install
#	$(MAKE) -C snmpd install
#	$(MAKE) -C dbcon/execplan install
#	$(MAKE) -C dbcon/joblist install
#	$(MAKE) -C versioning install
#	$(MAKE) -C oam install
#	$(MAKE) -C versioning dbrm tools install
#	$(MAKE) -C writeengine/wrapper install
#	$(MAKE) -C writeengine/client install
#	$(MAKE) -C writeengine/xml install
#	$(MAKE) -C writeengine/redistribute install
#	$(MAKE) -C dbcon/ddlpackage install
#	$(MAKE) -C dbcon/ddlpackageproc install
#	$(MAKE) -C dbcon/dmlpackage install
#	$(MAKE) -C dbcon/dmlpackageproc install
#	$(MAKE) -C dbcon install
#	$(MAKE) -C exemgr install
#	$(MAKE) -C ddlproc install
#	$(MAKE) -C dmlproc install
#	$(MAKE) -C procmon install
#	$(MAKE) -C procmgr install
#	$(MAKE) -C oamapps install
#	$(MAKE) -C decomsvr install
#	$(MAKE) -C primitives install
#	$(MAKE) -C tools install
#	$(MAKE) -C writeengine/server install
#	$(MAKE) -C writeengine/bulk install
#	$(MAKE) -C writeengine/splitter install
#
#compile: all-local
#
#test:
#
#coverage:
#
#leakcheck:
#
#docs:
#
#clean: 
#	rm -f net-snmp.built; \
#	find . -name \*.d.* -o -name \*.so -o -name \*.a -o -name \*.o -o -name \*.lib -o -name \*.sa -o -name \*.lo -o -name \*.la -exec rm {} \; -print
#
