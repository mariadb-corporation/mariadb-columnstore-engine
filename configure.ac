# $Id$
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(columnstore, 1.0.2, support@mariadb.com)
AC_CONFIG_MACRO_DIR([m4])
### we don't want the deault CXXFLAGS set by AC_INIT, but we do want to allow the user to set them
SAVE_CXXFLAGS=
if test -n "$CXXFLAGS"; then
	SAVE_CXXFLAGS="$CXXFLAGS"
fi
CXXFLAGS=
SAVE_CFLAGS=
if test -n "$CFLAGS"; then
	SAVE_CFLAGS="$CFLAGS"
fi
CFLAGS=
## AC_CONFIG_SRCDIR([configcpp/configcpp.cpp])
AM_INIT_AUTOMAKE
AC_CONFIG_HEADER([config.h])

AC_PREFIX_DEFAULT([/usr/local/mariadb/columnstore])

AC_LANG_CPLUSPLUS

# Checks for programs.
AC_PROG_CC
if test -n "$SAVE_CFLAGS"; then
	CFLAGS="$SAVE_CFLAGS"
else
	CFLAGS=
fi
AC_PROG_CXX
if test -n "$SAVE_CXXFLAGS"; then
	CXXFLAGS="$SAVE_CXXFLAGS"
else
	CXXFLAGS=
fi
AC_ENABLE_STATIC([no])
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AC_PROG_LEX
if test "x$LEX" = "x:"; then
	AC_MSG_ERROR([Could not find a usable lex program!])
fi
AC_PROG_YACC
$YACC --version >/dev/null 2>&1
if test $? -ne 0; then
	AC_MSG_ERROR([Could not find a usable yacc program!])
fi
AC_PROG_AWK
AC_PROG_CPP
AC_PROG_LN_S
AC_PROG_MAKE_SET

# FIXME:
# for quicklz (note this macro is not needed if autotools is 1.14 or 
# greater, but old versions of CentOS do not have newer version
# of autotools
AM_PROG_CC_C_O

# Checks for header files.
AC_FUNC_ALLOCA
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h malloc.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h strings.h sys/file.h sys/mount.h sys/socket.h sys/statfs.h sys/time.h sys/timeb.h syslog.h unistd.h utime.h values.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STAT
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_REPLACE_FNMATCH
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MKTIME
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_SETVBUF_REVERSED
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_FUNC_STRERROR_R
AC_FUNC_STRFTIME
AC_FUNC_STRTOD
AC_FUNC_UTIME_NULL
AC_CHECK_FUNCS([alarm dup2 floor ftime ftruncate gethostbyname getpagesize gettimeofday inet_ntoa isascii localtime_r memchr memmove memset mkdir pow regcomp rmdir select setenv setlocale socket strcasecmp strchr strcspn strdup strerror strrchr strspn strstr strtol strtoul strtoull utime])

AM_PATH_XML2([2.6.26])
if test "x$XML_CPPFLAGS" = "x"; then
	AC_MSG_ERROR([Could not find a usable libxml2 development environment!])
fi

AC_CHECK_HEADERS([zlib.h])
if test "x$ac_cv_header_zlib_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable zlib development environment!])
fi

AC_CHECK_HEADERS([readline/readline.h])
if test "x$ac_cv_header_readline_readline_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable readline development environment!])
fi

AC_CHECK_HEADERS([ncurses.h])
if test "x$ac_cv_header_ncurses_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable ncurses development environment!])
fi

#CXX_FLAG_CHECK([-Wno-unused-local-typedefs])
#CXX_FLAG_CHECK([-Wno-unused-result])
#CXX_FLAG_CHECK([-Wno-format])

AC_MSG_CHECKING(whether to enable debugging)
AC_SUBST([idb_cppflags], [' '])
AC_ARG_WITH([debug],
	AS_HELP_STRING([--with-debug],[include debugging symbols/no optimization]))
if test "x$with_debug" = "xyes"; then
	AC_SUBST([idb_cxxflags],['-ggdb3 -O0 -Wall -D_DEBUG'])
	AC_SUBST([idb_cflags],['-ggdb3 -O0 -Wall -D _DEBUG'])
	AC_MSG_RESULT(yes)
else
	AC_SUBST([idb_cxxflags],["-g0 -O3 -fno-strict-aliasing -Wall $cxx_extra_flags"])
	AC_SUBST([idb_cflags],['-g0 -O3 -fno-strict-aliasing -Wall'])
	AC_MSG_RESULT(no)
fi
AC_SUBST([idbinstall], ['${prefix}'])
AC_SUBST([idb_ldflags],['-Wl,--rpath -Wl,${idbinstall}/lib -Wl,--no-as-needed -Wl,--add-needed'])

AC_MSG_CHECKING(for CPU architecture)
uname=`uname -m`
AS_IF(
   [test `expr $uname : 'i.86'` -gt 0], [AC_SUBST([march_flags], ['-march=pentium4'])] [AC_MSG_RESULT(pentium4)],
   [AC_SUBST([march_flags], [' '])] [AC_MSG_RESULT(x86_64)]
)

AC_SUBST([libdir], ['${idbinstall}/lib'])
AC_SUBST([bindir], ['${idbinstall}/bin'])
AC_SUBST([includedir], ['${idbinstall}/include'])
AC_SUBST([etcdir], ['${idbinstall}/etc'])
AC_SUBST([sysconfdir], ['${idbinstall}/etc'])
AC_SUBST([mandir], ['${idbinstall}/man'])
AC_SUBST([sbindir], ['${idbinstall}/sbin'])
AC_SUBST([sharedir], ['${idbinstall}/share'])
AC_SUBST([postdir], ['${idbinstall}/post'])
AC_SUBST([localdir], ['${idbinstall}/local'])
AC_SUBST([mysqldir], ['${idbinstall}/mysql'])
AC_SUBST([mibdir], ['${idbinstall}/share/snmp/mibs'])
AC_SUBST([toolsdir], ['${idbinstall}/tools'])

AC_SUBST([netsnmp_libs], ['-L${abs_top_builddir}/net-snmp/net-snmp/snmplib/.libs -L${abs_top_builddir}/net-snmp/net-snmp/agent/helpers/.libs -L${abs_top_builddir}/net-snmp/net-snmp/agent/.libs/ -L${abs_top_builddir}/net-snmp/net-snmp/apps/.libs/ -lnetsnmpmibs -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers'])
AC_SUBST([idb_common_libs], ['${abs_top_builddir}/utils/messageqcpp/libmessageqcpp.la ${abs_top_builddir}/utils/loggingcpp/libloggingcpp.la ${abs_top_builddir}/utils/configcpp/libconfigcpp.la ${abs_top_builddir}/utils/startup/libidbboot.a ${abs_top_builddir}/utils/boost_idb/libboost_idb.la -lxml2 -lpthread -lrt'])
AC_SUBST([idb_oam_libs], ['${abs_top_builddir}/oam/oamcpp/liboamcpp.la ${abs_top_builddir}/snmpd/snmpmanager/libsnmpmanager.la ${netsnmp_libs}'])
AC_SUBST([idb_brm_libs], ['${abs_top_builddir}/versioning/BRM/libbrm.la ${abs_top_builddir}/utils/idbdatafile/libidbdatafile.la ${abs_top_builddir}/utils/cacheutils/libcacheutils.la ${abs_top_builddir}/utils/rwlock/librwlock.la ${idb_oam_libs} ${idb_common_libs}'])
AC_SUBST([idb_exec_libs], ['${abs_top_builddir}/dbcon/joblist/libjoblist.la ${abs_top_builddir}/dbcon/execplan/libexecplan.la ${abs_top_builddir}/utils/windowfunction/libwindowfunction.la ${abs_top_builddir}/utils/joiner/libjoiner.la ${abs_top_builddir}/utils/rowgroup/librowgroup.la ${abs_top_builddir}/utils/funcexp/libfuncexp.la ${abs_top_builddir}/utils/udfsdk/libudfsdk.la ${abs_top_builddir}/utils/dataconvert/libdataconvert.la ${abs_top_builddir}/utils/common/libcommon.la ${abs_top_builddir}/utils/compress/libcompress.la ${abs_top_builddir}/utils/mysqlcl_idb/libmysqlcl_idb.la ${abs_top_builddir}/utils/querystats/libquerystats.la ${abs_top_builddir}/utils/querytele/libquerytele.la ${abs_top_builddir}/utils/thrift/libthrift.la ${abs_top_builddir}/utils/threadpool/libthreadpool.la ${idb_brm_libs}'])
AC_SUBST([idb_write_libs], ['${abs_top_builddir}/dbcon/ddlpackageproc/libddlpackageproc.la ${abs_top_builddir}/dbcon/ddlpackage/libddlpackage.la ${abs_top_builddir}/dbcon/dmlpackageproc/libdmlpackageproc.la ${abs_top_builddir}/dbcon/dmlpackage/libdmlpackage.la ${abs_top_builddir}/writeengine/wrapper/libwriteengine.la ${abs_top_builddir}/writeengine/client/libwriteengineclient.la ${abs_top_builddir}/utils/idbdatafile/libidbdatafile.la ${abs_top_builddir}/utils/cacheutils/libcacheutils.la ${idb_exec_libs}'])
AC_SUBST([idb_common_includes], ['-I${abs_top_builddir}/utils/libxml -I${abs_top_builddir}/utils/messageqcpp -I${abs_top_builddir}/writeengine/shared -I${abs_top_builddir}/utils/idbdatafile -I${abs_top_builddir}/utils/loggingcpp -I${abs_top_builddir}/utils/configcpp -I${abs_top_builddir}/utils/compress -I${abs_top_builddir}/versioning/BRM -I${abs_top_builddir}/utils/rowgroup -I${abs_top_builddir}/utils/common -I${abs_top_builddir}/utils/dataconvert -I${abs_top_builddir}/utils/rwlock -I${abs_top_builddir}/utils/funcexp -I${abs_top_builddir}/snmpd/snmpmanager -I${abs_top_builddir}/utils -I${abs_top_builddir}/oam/oamcpp -I${abs_top_builddir}/dbcon/ddlpackageproc -I${abs_top_builddir}/dbcon/ddlpackage -I${abs_top_builddir}/dbcon/execplan -I${abs_top_builddir}/utils/startup -I${abs_top_builddir}/dbcon/joblist -I${abs_top_builddir}/writeengine/wrapper -I${abs_top_builddir}/writeengine/server -I${abs_top_builddir}/dbcon/dmlpackage -I${abs_top_builddir}/writeengine/client -I${abs_top_builddir}/dbcon/dmlpackageproc -I${abs_top_builddir}/utils/cacheutils -I${abs_top_builddir}/utils/mysqlcl_idb -I${abs_top_builddir}/utils/querytele -I${abs_top_builddir}/utils/boost_idb -I${abs_top_builddir}/utils/thrift -I${abs_top_builddir}/utils/joiner -I${abs_top_builddir}/utils/threadpool -I${abs_top_builddir}/utils/batchloader -I${abs_top_builddir}/utils/ddlcleanup -I${abs_top_builddir}/utils/querystats -I${abs_top_builddir}/writeengine/xml -I${abs_top_builddir}/../../../sql -I${abs_top_builddir}/../../../include -I${abs_top_builddir}/../../../pcre -I${abs_top_builddir}/../sql -I${abs_top_builddir}/../include -I${abs_top_builddir}/../pcre -I${abs_top_builddir}/net-snmp/net-snmp -I${abs_top_builddir}/net-snmp/net-snmp/include -I${abs_top_builddir}/snmpd/snmpmanager'])
AC_SUBST([idb_common_ldflags], [''])

AC_CONFIG_FILES([
	Makefile
	utils/Makefile
	utils/boost_idb/Makefile
	utils/startup/Makefile
	utils/common/Makefile
	utils/configcpp/Makefile
	utils/loggingcpp/Makefile
	utils/messageqcpp/Makefile
	utils/threadpool/Makefile
	utils/rwlock/Makefile
	utils/dataconvert/Makefile
	utils/joiner/Makefile
	utils/rowgroup/Makefile
	utils/cacheutils/Makefile
	utils/funcexp/Makefile
	utils/udfsdk/Makefile
	utils/compress/Makefile
	utils/ddlcleanup/Makefile
	utils/batchloader/Makefile
	utils/mysqlcl_idb/Makefile
	utils/querystats/Makefile
	utils/jemalloc/Makefile
	utils/windowfunction/Makefile
	utils/idbdatafile/Makefile
	utils/idbhdfs/Makefile
	utils/idbhdfs/hdfs-12/Makefile
	utils/idbhdfs/hdfs-20/Makefile
	utils/winport/Makefile
	utils/thrift/Makefile
	utils/querytele/Makefile
	exemgr/Makefile
	ddlproc/Makefile
	dbcon/Makefile
	dbcon/ddlpackage/Makefile
	dbcon/ddlpackageproc/Makefile
	dbcon/dmlpackage/Makefile
	dbcon/dmlpackageproc/Makefile
	dbcon/execplan/Makefile
	dbcon/joblist/Makefile
	dbcon/mysql/Makefile
	dmlproc/Makefile
	oam/Makefile
	oam/etc/Makefile
	oam/install_scripts/Makefile
	oam/oamcpp/Makefile
	oam/post/Makefile
	oam/cloud/Makefile
	oamapps/Makefile
	oamapps/mcsadmin/Makefile
	oamapps/calpontDB/Makefile
	oamapps/postConfigure/Makefile
	oamapps/serverMonitor/Makefile
	oamapps/sessionWalker/Makefile
	oamapps/traphandler/Makefile
	oamapps/sendtrap/Makefile
	oamapps/calpontSupport/Makefile
	oamapps/columnstoreDB/Makefile
	primitives/Makefile
	primitives/blockcache/Makefile
	primitives/linux-port/Makefile
	primitives/primproc/Makefile
	decomsvr/Makefile
	procmgr/Makefile
	procmon/Makefile
	net-snmp/Makefile
	snmpd/Makefile
	snmpd/etc/Makefile
	snmpd/snmpmanager/Makefile
	oamapps/columnstoreSupport/Makefile
	tools/Makefile
	tools/editem/Makefile
	tools/cplogger/Makefile
	tools/clearShm/Makefile
	tools/setConfig/Makefile
	tools/getConfig/Makefile
	tools/dbbuilder/Makefile
	tools/dbloadxml/Makefile
	tools/configMgt/Makefile
	tools/viewtablelock/Makefile
	tools/cleartablelock/Makefile
	tools/ddlcleanup/Makefile
	tools/idbmeminfo/Makefile
	versioning/Makefile
	versioning/BRM/Makefile
	writeengine/Makefile
	writeengine/shared/Makefile
	writeengine/index/Makefile
	writeengine/dictionary/Makefile
	writeengine/wrapper/Makefile
	writeengine/xml/Makefile
	writeengine/bulk/Makefile
	writeengine/client/Makefile
	writeengine/splitter/Makefile
	writeengine/server/Makefile
	writeengine/redistribute/Makefile
])
AC_OUTPUT
