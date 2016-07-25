
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

SET (PACKAGE columnstore)
SET (VERSION 1.0.2)
SET (PACKAGE_NAME columnstore)
SET (PACKAGE_TARNAME columnstore)
SET (PACKAGE_VERSION 1.0.2)
SET (PACKAGE_STRING columnstore-1.0.2)
SET (PACKAGE_BUGREPORT support@mariadb.com)
#SET (PACKAGE_URL "")

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
INCLUDE (CheckCXXSourceRuns)
INCLUDE (CheckSymbolExists)
INCLUDE (CheckCXXSymbolExists)
INCLUDE (CheckTypeSize)

CHECK_INCLUDE_FILE_CXX (alloca.h HAVE_ALLOCA_H)
CHECK_INCLUDE_FILE_CXX (arpa/inet.h HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILE_CXX (dlfcn.h HAVE_DLFCN_H)
CHECK_INCLUDE_FILE_CXX (fcntl.h HAVE_FCNTL_H)
CHECK_INCLUDE_FILE_CXX (inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILE_CXX (limits.h HAVE_LIMITS_H)
CHECK_INCLUDE_FILE_CXX (malloc.h HAVE_MALLOC_H)
CHECK_INCLUDE_FILE_CXX (memory.h HAVE_MEMORY_H)
CHECK_INCLUDE_FILE_CXX (ncurses.h HAVE_NCURSES_H)
CHECK_INCLUDE_FILE_CXX (netdb.h HAVE_NETDB_H)
CHECK_INCLUDE_FILE_CXX (netinet/in.h HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILE_CXX (readline/readline.h HAVE_READLINE_READLINE_H)
CHECK_INCLUDE_FILE_CXX (stddef.h HAVE_STDDEF_H)
CHECK_INCLUDE_FILE_CXX (stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILE_CXX (stdlib.h HAVE_STDLIB_H)
CHECK_INCLUDE_FILE_CXX (strings.h HAVE_STRINGS_H)
CHECK_INCLUDE_FILE_CXX (string.h HAVE_STRING_H)
CHECK_INCLUDE_FILE_CXX (syslog.h HAVE_SYSLOG_H)
CHECK_INCLUDE_FILE_CXX (sys/file.h HAVE_SYS_FILE_H)
CHECK_INCLUDE_FILE_CXX (sys/mount.h HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILE_CXX (sys/select.h HAVE_SYS_SELECT_H)
CHECK_INCLUDE_FILE_CXX (sys/socket.h HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILE_CXX (sys/statfs.h HAVE_SYS_STATFS_H)
CHECK_INCLUDE_FILE_CXX (sys/stat.h HAVE_SYS_STAT_H)
CHECK_INCLUDE_FILE_CXX (sys/timeb.h HAVE_SYS_TIMEB_H)
CHECK_INCLUDE_FILE_CXX (sys/time.h HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILE_CXX (sys/types.h HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILE_CXX (sys/wait.h HAVE_SYS_WAIT_H)
CHECK_INCLUDE_FILE_CXX (unistd.h HAVE_UNISTD_H)
CHECK_INCLUDE_FILE_CXX (utime.h HAVE_UTIME_H)
CHECK_INCLUDE_FILE_CXX (values.h HAVE_VALUES_H)
CHECK_INCLUDE_FILE_CXX (vfork.h HAVE_VFORK_H)
CHECK_INCLUDE_FILE_CXX (wchar.h HAVE_WCHAR_H)
CHECK_INCLUDE_FILE_CXX (wctype.h HAVE_WCTYPE_H)
CHECK_INCLUDE_FILE_CXX (zlib.h HAVE_ZLIB_H)

CHECK_FUNCTION_EXISTS (_getb67 GETB1)
CHECK_FUNCTION_EXISTS (GETB67 GETB2)
CHECK_FUNCTION_EXISTS (getb67 GETB3)

IF(GETB1)
    SET (CRAY_STACKSEG_END 1)
ELSEIF(GETB2)
    SET (CRAY_STACKSEG_END 1)
ELSEIF(GETB3)
    SET (CRAY_STACKSEG_END 1)
ENDIF()

CHECK_FUNCTION_EXISTS (alarm HAVE_ALARM)
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
CHECK_FUNCTION_EXISTS (mempcpy HAVE_MEMPCPY)
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

CHECK_CXX_SYMBOL_EXISTS (alloca alloca.h HAVE_ALLOCA)
CHECK_CXX_SYMBOL_EXISTS (strerror_r string.h HAVE_DECL_STRERROR_R)
CHECK_CXX_SYMBOL_EXISTS (tm sys/time.h TM_IN_SYS_TIME)

#AC_TYPE_SIGNAL
CHECK_TYPE_SIZE (ptrdiff_t PTRDIFF_T)
CHECK_TYPE_SIZE (_Bool __BOOL)
CHECK_TYPE_SIZE (mode_t mode_t_test)
IF(NOT HAVE_mode_t_test)
SET (mode_t int)
ENDIF()
CHECK_TYPE_SIZE(off_t off_t_test)
IF(NOT HAVE_off_t_test)
SET (off_t long int)
ENDIF()
CHECK_TYPE_SIZE(pid_t pid_t_test)
IF(NOT HAVE_pid_t_test)
SET (pid_t int)
ENDIF()
CHECK_TYPE_SIZE(size_t size_t_test)
IF(NOT HAVE_size_t_test)
SET (size_t unsigned int)
ENDIF()



CHECK_CXX_SOURCE_COMPILES(
"#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <float.h>

int
main ()
{

  ;
  return 0;
}" STDC_HEADERS)


SET (TEST_INCLUDES 
"
#include <stdio.h>")

IF (HAVE_SYS_TYPES_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <sys/types.h>")
ENDIF()
IF (HAVE_SYS_STAT_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <sys/stat.h>")
ENDIF()
IF (STDC_HEADERS)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <stdlib.h>
    # include <stddef.h>")
ELSE()
    IF()
        SET ( TEST_INCLUDES 
        "${TEST_INCLUDES}
        # include <stdlib.h>")
    ENDIF()
ENDIF()
IF (HAVE_STRING_H)
    IF(NOT STDC_HEADERS)
        IF (HAVE_MEMORY_H)
            SET ( TEST_INCLUDES 
            "${TEST_INCLUDES}
            # include <memory.h>")
        ENDIF()
    ENDIF()
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <string.h>")
ENDIF()
IF (HAVE_STRINGS_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <strings.h>")
ENDIF()
IF (HAVE_INTTYPES_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <inttypes.h>")
ENDIF()
IF (HAVE_STDINT_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <stdint.h>")
ENDIF()
IF (HAVE_UNISTD_H)
    SET ( TEST_INCLUDES 
    "${TEST_INCLUDES}
    # include <unistd.h>")
ENDIF()


CHECK_CXX_SOURCE_COMPILES(
"
${TEST_INCLUDES}
#       include <wchar.h>
int
main ()
{
mbstate_t x; return sizeof x;
  ;
  return 0;
}" HAVE_MBSTATE_T)


CHECK_CXX_SOURCE_RUNS(
"
${TEST_INCLUDES}

int main ()
{
struct stat sbuf;
  return stat (\"\", &sbuf) == 0;
  ;
  return 0;
}" STAT_EMPTY_STRING_BUG)
IF (NOT STAT_EMPTY_STRING_BUG)
SET (HAVE_STAT_EMPTY_STRING_BUG 1) 
ENDIF()


CHECK_CXX_SOURCE_COMPILES(
             "
             ${TEST_INCLUDES}
             #include <stdbool.h>
             #ifndef bool
              \"error: bool is not defined\"
             #endif
             #ifndef false
              \"error: false is not defined\"
             #endif
             #if false
              \"error: false is not 0\"
             #endif
             #ifndef true
              \"error: true is not defined\"
             #endif
             #if true != 1
              \"error: true is not 1\"
             #endif
             #ifndef __bool_true_false_are_defined
              \"error: __bool_true_false_are_defined is not defined\"
             #endif

             struct s { _Bool s: 1; _Bool t; } s;

             char a[true == 1 ? 1 : -1];
             char b[false == 0 ? 1 : -1];
             char c[__bool_true_false_are_defined == 1 ? 1 : -1];
             char d[(bool) 0.5 == true ? 1 : -1];
             /* See body of main program for 'e'.  */
             char f[(_Bool) 0.0 == false ? 1 : -1];
             char g[true];
             char h[sizeof (_Bool)];
             char i[sizeof s.t];
             enum { j = false, k = true, l = false * true, m = true * 256 };
             /* The following fails for
                HP aC++/ANSI C B3910B A.05.55 [Dec 04 2003]. */
             _Bool n[m];
             char o[sizeof n == m * sizeof n[0] ? 1 : -1];
             char p[-1 - (_Bool) 0 < 0 && -1 - (bool) 0 < 0 ? 1 : -1];
             /* Catch a bug in an HP-UX C compiler.  See
                http://gcc.gnu.org/ml/gcc-patches/2003-12/msg02303.html
                http://lists.gnu.org/archive/html/bug-coreutils/2005-11/msg00161.html
              */
             _Bool q = true;
             _Bool *pq = &q;

int
main ()
{

             bool e = &s;
             *pq |= q;
             *pq |= ! q;
             /* Refer to every declared value, to avoid compiler optimizations.  */
             return (!a + !b + !c + !d + !e + !f + !g + !h + !i + !!j + !k + !!l
                     + !m + !n + !o + !p + !q + !pq);

  ;
  return 0;
}" HAVE_STDBOOL_H)

IF (HAVE_UTIME_H)
CHECK_CXX_SOURCE_COMPILES(
"${TEST_INCLUDES}
# include <utime.h>
int
main ()
{
struct stat s, t;
  return ! (stat (\"conftest.data\", &s) == 0
        && utime (\"conftest.data\", 0) == 0
        && stat (\"conftest.data\", &t) == 0
        && t.st_mtime >= s.st_mtime
        && t.st_mtime - s.st_mtime < 120);
  ;
  return 0;
}" HAVE_UTIME_NULL)
ENDIF()

CHECK_CXX_SOURCE_COMPILES(
"
${TEST_INCLUDES}
int
main ()
{
      /* By Ruediger Kuhlmann. */
      return fork () < 0;
  ;
  return 0;
}" HAVE_WORKING_FORK)

CHECK_CXX_SOURCE_COMPILES(
"${TEST_INCLUDES}
#include <sys/wait.h>
#ifdef HAVE_VFORK_H
 include <vfork.h>
#endif
/* On some sparc systems, changes by the child to local and incoming
   argument registers are propagated back to the parent.  The compiler
   is told about this with #include <vfork.h>, but some compilers
   (e.g. gcc -O) don't grok <vfork.h>.  Test for this by using a
   static variable whose address is put into a register that is
   clobbered by the vfork.  */
static void
#ifdef __cplusplus
sparc_address_test (int arg)
# else
sparc_address_test (arg) int arg;
#endif
{
  static pid_t child;
  if (!child) {
    child = vfork ();
    if (child < 0) {
      perror (\"vfork\");
      _exit(2);
    }
    if (!child) {
      arg = getpid();
      write(-1, \"\", 0);
      _exit (arg);
    }
  }
}

int
main ()
{
  pid_t parent = getpid ();
  pid_t child;

  sparc_address_test (0);

  child = vfork ();

  if (child == 0) {
    /* Here is another test for sparc vfork register problems.  This
       test uses lots of local variables, at least as many local
       variables as main has allocated so far including compiler
       temporaries.  4 locals are enough for gcc 1.40.3 on a Solaris
       4.1.3 sparc, but we use 8 to be safe.  A buggy compiler should
       reuse the register of parent for one of the local variables,
       since it will think that parent can't possibly be used any more
       in this routine.  Assigning to the local variable will thus
       munge parent in the parent process.  */
    pid_t
      p = getpid(), p1 = getpid(), p2 = getpid(), p3 = getpid(),
      p4 = getpid(), p5 = getpid(), p6 = getpid(), p7 = getpid();
    /* Convince the compiler that p..p7 are live; otherwise, it might
       use the same hardware register for all 8 local variables.  */
    if (p != p1 || p != p2 || p != p3 || p != p4
    || p != p5 || p != p6 || p != p7)
      _exit(1);

    /* On some systems (e.g. IRIX 3.3), vfork doesn't separate parent
       from child file descriptors.  If the child closes a descriptor
       before it execs or exits, this munges the parent's descriptor
       as well.  Test for this by closing stdout in the child.  */
    _exit(close(fileno(stdout)) != 0);
  } else {
    int status;
    struct stat st;

    while (wait(&status) != child)
      ;
    return (
     /* Was there some problem with vforking?  */
     child < 0

     /* Did the child fail?  (This shouldn't happen.)  */
     || status

     /* Did the vfork/compiler bug occur?  */
     || parent != getpid()

     /* Did the file descriptor bug occur?  */
     || fstat(fileno(stdout), &st) != 0
     );
  }
}" HAVE_WORKING_VFORK)

IF (NOT HAVE_WORKING_VFORK)
SET (VFORK fork)
ENDIF()

CHECK_CXX_SOURCE_COMPILES(
"
#include <sys/types.h>
#include <signal.h>

int
main ()
{
return *(signal (0, 0)) (0) == 1;
  ;
  return 0;
}" RET_SIGNAL_TYPES)
IF (RET_SIGNAL_TYPES)
SET (RETSIGTYPE int)
ELSE()
SET (RETSIGTYPE void)
ENDIF()

#IF(NOT LSTAT_FOLLOWS_SLASHED_SYMLINK)
EXECUTE_PROCESS(
    COMMAND rm -f conftest.sym conftest.file
    COMMAND touch conftest.file
    COMMAND ln -s conftest.file conftest.sym
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
CHECK_CXX_SOURCE_RUNS(
"
${TEST_INCLUDES}
int
main ()
{
struct stat sbuf;
     /* Linux will dereference the symlink and fail, as required by POSIX.
    That is better in the sense that it means we will not
    have to compile and use the lstat wrapper.  */
     return lstat (\"conftest.sym/\", &sbuf) == 0;
  ;
  return 0;
}" LSTAT_FOLLOWS_SLASHED_SYMLINK)



SET (SELECT_INCLUDES ${TEST_INCLUDES})
IF (HAVE_SYS_SELECT_H)
SET (SELECT_INCULDES
"${SELECT_INCLUDES}
# include <sys/select.h>")
ENDIF()
IF (HAVE_SYS_SOCKET_H)
SET (SELECT_INCULDES
"${SELECT_INCLUDES}
# include <sys/select.h>")
ENDIF()


FOREACH( ARG234 "fd_set *" "int *" "void *")
FOREACH( ARG1 "int" "size_t" "unsigned long int" "unsigned int")
FOREACH( ARG5 "struct timeval *" "const struct timeval *")
CHECK_CXX_SOURCE_COMPILES(
"
${SELECT_INCLUDES}
#ifdef HAVE_SYS_SELECT_H
# include <sys/select.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
# include <sys/socket.h>
#endif

int
main ()
{
extern int select (${ARG1},
                        ${ARG234}, ${ARG234}, ${ARG234},
                        ${ARG5});
  ;
  return 0;
}
" SELECT_ARGS)
IF(SELECT_ARGS)
SET (SELECT_TYPE_ARG1 ${ARG1})
SET (SELECT_TYPE_ARG234 ${ARG234})
SET (SELECT_TYPE_ARG5 ${ARG5})
BREAK()
ENDIF()
ENDFOREACH()
IF(SELECT_ARGS)
BREAK()
ENDIF()
ENDFOREACH()
IF(SELECT_ARGS)
BREAK()
ENDIF()
ENDFOREACH()


CHECK_CXX_SOURCE_COMPILES(
"
#include <sys/types.h>
#include <sys/stat.h>

#if defined S_ISBLK && defined S_IFDIR
extern char c1[S_ISBLK (S_IFDIR) ? -1 : 1];
#endif

#if defined S_ISBLK && defined S_IFCHR
extern char c2[S_ISBLK (S_IFCHR) ? -1 : 1];
#endif

#if defined S_ISLNK && defined S_IFREG
extern char c3[S_ISLNK (S_IFREG) ? -1 : 1];
#endif

#if defined S_ISSOCK && defined S_IFREG
extern char c4[S_ISSOCK (S_IFREG) ? -1 : 1];
#endif
int main()
{
    return 0;
}
" STATS_MACROS_CHECK)
IF (NOT STATS_MACROS_CHECK)
SET (STAT_MACROS_BROKEN 1)
ENDIF()

CHECK_CXX_SOURCE_COMPILES(
"
${TEST_INCLUDES}
int
main ()
{

      char buf[100];
      char x = *strerror_r (0, buf, sizeof buf);
      char *p = strerror_r (0, buf, sizeof buf);
      return !p || x;

  ;
  return 0;
}
" STRERROR_R_CHAR_P)

CHECK_CXX_SOURCE_COMPILES(
"
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>

int
main ()
{
if ((struct tm *) 0)
return 0;
  ;
  return 0;
}
" TIME_WITH_SYS_TIME)


CHECK_CXX_SOURCE_COMPILES(
"
int
main ()
{

#ifndef __cplusplus
  /* Ultrix mips cc rejects this sort of thing.  */
  typedef int charset[2];
  const charset cs = { 0, 0 };
  /* SunOS 4.1.1 cc rejects this.  */
  char const *const *pcpcc;
  char **ppc;
  /* NEC SVR4.0.2 mips cc rejects this.  */
  struct point {int x, y;};
  static struct point const zero = {0,0};
  /* AIX XL C 1.02.0.0 rejects this.
     It does not let you subtract one const X* pointer from another in
     an arm of an if-expression whose if-part is not a constant
     expression */
  const char *g = \"string\";
  pcpcc = &g + (g ? g-g : 0);
  /* HPUX 7.0 cc rejects these. */
  ++pcpcc;
  ppc = (char**) pcpcc;
  pcpcc = (char const *const *) ppc;
  { /* SCO 3.2v4 cc rejects this sort of thing.  */
    char tx;
    char *t = &tx;
    char const *s = 0 ? (char *) 0 : (char const *) 0;

    *t++ = 0;
    if (s) return 0;
  }
  { /* Someone thinks the Sun supposedly-ANSI compiler will reject this.  */
    int x[] = {25, 17};
    const int *foo = &x[0];
    ++foo;
  }
  { /* Sun SC1.0 ANSI compiler rejects this -- but not the above. */
    typedef const int *iptr;
    iptr p = 0;
    ++p;
  }
  { /* AIX XL C 1.02.0.0 rejects this sort of thing, saying
       \"k.c\", line 2.27: 1506-025 (S) Operand must be a modifiable lvalue. */
    struct s { int j; const int *ap[3]; } bx;
    struct s *b = &bx; b->j = 5;
  }
  { /* ULTRIX-32 V3.1 (Rev 9) vcc rejects this */
    const int foo = 10;
    if (!foo) return 0;
  }
  return !cs[0] && !zero.x;
#endif

  ;
  return 0;
}
" CONST_CONFORM_CHECK)
IF (NOT CONST_CONFORM_CHECK)
SET (const "")
ENDIF()

CHECK_CXX_SOURCE_COMPILES(
"
int
main ()
{

volatile int x;
int * volatile y = (int *) 0;
return !x && !y;
  ;
  return 0;
}
" WORKING_VOLATILE)
IF (NOT WORKING_VOLATILE)
SET (volatile "")
ENDIF()

FOREACH (RESTRICT_KW __restrict __restrict__ _Restrict restrict)

CHECK_CXX_SOURCE_COMPILES(
"
typedef int * int_ptr;
    int foo (int_ptr ${RESTRICT_KW} ip) {
    return ip[0];
       }
int
main ()
{
int s[1];
    int * ${RESTRICT_KW} t = s;
    t[0] = 0;
    return foo(t)
  ;
  return 0;
}
" RESTRICT_CHECK)
IF (RESTRICT_CHECK)
SET (restrict ${RESTRICT_KW})
BREAK()
ENDIF()
ENDFOREACH()


EXECUTE_PROCESS(
    COMMAND rm -f conftest.data conftest.file conftest.sym
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )


CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)

INCLUDE(bison.cmake)

FIND_PROGRAM(LEX_EXECUTABLE flex DOC "path to the flex executable")
if(NOT LEX_EXECUTABLE)
    FIND_PROGRAM(LEX_EXECUTABLE lex DOC "path to the lex executable")
    if(NOT LEX_EXECUTABLE)
        message(FATAL_ERROR "flex/lex not found!")
    endif()
endif()

INCLUDE (FindLibXml2)

if (NOT LIBXML2_FOUND)
    MESSAGE(FATAL_ERROR "Could not find a usable libxml2 development environment!")
endif()

FIND_PROGRAM(AWK_EXECUTABLE awk DOC "path to the awk executable")
if(NOT AWK_EXECUTABLE)
    message(FATAL_ERROR "awk not found!")
endif()


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
#AC_FUNC_MEMCMP
#AC_FUNC_MKTIME
#AC_FUNC_SELECT_ARGTYPES
#AC_FUNC_SETVBUF_REVERSED
#AC_FUNC_STAT
#AC_FUNC_STRFTIME
#AC_FUNC_STRTOD
#AC_FUNC_UTIME_NULL





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
##-ggdb3 -fno-tree-vectorize -DSAFE_MUTEX -DSAFEMALLOC

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
SET (ENGINE_COMMON_LIBS     messageqcpp loggingcpp configcpp idbboot ${Boost_LIBRARIES} xml2 pthread rt)
SET (ENGINE_OAM_LIBS        oamcpp snmpmanager ${ENGINE_NETSNMP_LIBS})
SET (ENGINE_BRM_LIBS        brm idbdatafile cacheutils rwlock ${ENGINE_OAM_LIBS} ${ENGINE_COMMON_LIBS})
SET (ENGINE_EXEC_LIBS       joblist execplan windowfunction joiner rowgroup funcexp udfsdk dataconvert common compress mysqlcl_idb querystats querytele thrift threadpool ${ENGINE_BRM_LIBS})
SET (ENGINE_WRITE_LIBS      ddlpackageproc ddlpackage dmlpackageproc dmlpackage writeengine writeengineclient idbdatafile cacheutils ${ENGINE_EXEC_LIBS})

SET (ENGINE_COMMON_LDFLAGS  "")


FIND_PACKAGE(Boost 1.45.0 COMPONENTS system filesystem thread regex) 
SET (ENGINE_UTILS_BOOSTIDB_INCLUDE     "${Boost_INCLUDE_DIRS}")

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
  BUILD_COMMAND make
  INSTALL_COMMAND ""
  BUILD_IN_SOURCE 1
)
INSTALL ( CODE "EXECUTE_PROCESS(COMMAND make install WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/net-snmp/net-snmp)")

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
