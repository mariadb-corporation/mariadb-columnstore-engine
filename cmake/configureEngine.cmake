

#
# Tests for header files
#
INCLUDE (CheckIncludeFiles)
INCLUDE (CheckIncludeFileCXX)
INCLUDE (CheckCSourceCompiles)
INCLUDE (CheckCXXSourceRuns)
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
CHECK_FUNCTION_EXISTS (error_at_line HAVE_ERROR_AT_LINE)
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
CHECK_FUNCTION_EXISTS (mktime HAVE_MKTIME)
CHECK_FUNCTION_EXISTS (pow HAVE_POW)
CHECK_FUNCTION_EXISTS (regcomp HAVE_REGCOMP)
CHECK_FUNCTION_EXISTS (rmdir HAVE_RMDIR)
CHECK_FUNCTION_EXISTS (select HAVE_SELECT)
CHECK_FUNCTION_EXISTS (setenv HAVE_SETENV)
CHECK_FUNCTION_EXISTS (setlocale HAVE_SETLOCALE)
CHECK_FUNCTION_EXISTS (socket HAVE_SOCKET)
CHECK_FUNCTION_EXISTS (stat HAVE_STAT)
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
CHECK_FUNCTION_EXISTS (strtod HAVE_STRTOD)
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


FOREACH(INLINE_KW inline __inline__ __inline)
CHECK_CXX_SOURCE_COMPILES(
"
#ifndef __cplusplus
typedef int foo_t;
static ${INLINE_KW} foo_t static_foo () {return 0; }
${INLINE_KW} foo_t foo () {return 0; }
int main (){return 0;}
#endif
"  INLINE)

IF (INLINE)
SET (inline ${INLINE_KW})
BREAK()
ENDIF()
ENDFOREACH()

IF (NOT INLINE)
SET (inline "")
ENDIF()

EXECUTE_PROCESS(
    COMMAND rm -f conftest.data conftest.file conftest.sym
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    
CHECK_CXX_SOURCE_RUNS("
#include <limits>
int main()
{
    // If long double is 16 bytes and digits and exponent are 64 and 16384 respectively, then we need to mask out the 
    // unused bits, as they contain garbage. There are times we test for equality by memcmp of a buffer containing,
    // in part, the long double set here. Garbage bytes will adversly affect that compare.
    // Note: There may be compilers that store 80 bit floats in 12 bytes. We do not account for that here. I don't believe
    // there are any modern Linux compilers that do that as a default. Windows uses 64 bits, so no masking is needed.
    if (std::numeric_limits<long double>::digits == 64 
     && std::numeric_limits<long double>::max_exponent == 16384 
     && sizeof(long double) == 16)
        return 0;
    return 1; 
}"
MASK_LONGDOUBLE)
