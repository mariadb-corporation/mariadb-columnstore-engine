#
# Tests for header files
#
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)
include(CheckCSourceCompiles)
include(CheckCXXSourceRuns)
include(CheckCXXSourceCompiles)
include(CheckStructHasMember)
include(CheckLibraryExists)
include(CheckFunctionExists)
include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)
include(CheckCXXSourceRuns)
include(CheckSymbolExists)
include(CheckCXXSymbolExists)
include(CheckTypeSize)

check_include_file_cxx(alloca.h HAVE_ALLOCA_H)
check_include_file_cxx(arpa/inet.h HAVE_ARPA_INET_H)
check_include_file_cxx(dlfcn.h HAVE_DLFCN_H)
check_include_file_cxx(fcntl.h HAVE_FCNTL_H)
check_include_file_cxx(inttypes.h HAVE_INTTYPES_H)
check_include_file_cxx(limits.h HAVE_LIMITS_H)
check_include_file_cxx(malloc.h HAVE_MALLOC_H)
check_include_file_cxx(memory.h HAVE_MEMORY_H)
check_include_file_cxx(ncurses.h HAVE_NCURSES_H)
check_include_file_cxx(netdb.h HAVE_NETDB_H)
check_include_file_cxx(netinet/in.h HAVE_NETINET_IN_H)
check_include_file_cxx(stddef.h HAVE_STDDEF_H)
check_include_file_cxx(stdint.h HAVE_STDINT_H)
check_include_file_cxx(stdlib.h HAVE_STDLIB_H)
check_include_file_cxx(strings.h HAVE_STRINGS_H)
check_include_file_cxx(string.h HAVE_STRING_H)
check_include_file_cxx(syslog.h HAVE_SYSLOG_H)
check_include_file_cxx(sys/file.h HAVE_SYS_FILE_H)
check_include_file_cxx(sys/mount.h HAVE_SYS_MOUNT_H)
check_include_file_cxx(sys/select.h HAVE_SYS_SELECT_H)
check_include_file_cxx(sys/socket.h HAVE_SYS_SOCKET_H)
check_include_file_cxx(sys/statfs.h HAVE_SYS_STATFS_H)
check_include_file_cxx(sys/stat.h HAVE_SYS_STAT_H)
check_include_file_cxx(sys/timeb.h HAVE_SYS_TIMEB_H)
check_include_file_cxx(sys/time.h HAVE_SYS_TIME_H)
check_include_file_cxx(sys/types.h HAVE_SYS_TYPES_H)
check_include_file_cxx(sys/wait.h HAVE_SYS_WAIT_H)
check_include_file_cxx(unistd.h HAVE_UNISTD_H)
check_include_file_cxx(utime.h HAVE_UTIME_H)
check_include_file_cxx(values.h HAVE_VALUES_H)
check_include_file_cxx(vfork.h HAVE_VFORK_H)
check_include_file_cxx(wchar.h HAVE_WCHAR_H)
check_include_file_cxx(wctype.h HAVE_WCTYPE_H)
check_include_file_cxx(zlib.h HAVE_ZLIB_H)

check_function_exists(_getb67 GETB1)
check_function_exists(GETB67 GETB2)
check_function_exists(getb67 GETB3)

if(GETB1)
    set(CRAY_STACKSEG_END 1)
elseif(GETB2)
    set(CRAY_STACKSEG_END 1)
elseif(GETB3)
    set(CRAY_STACKSEG_END 1)
endif()

check_function_exists(alarm HAVE_ALARM)
check_function_exists(btowc HAVE_BTOWC)
check_function_exists(dup2 HAVE_DUP2)
check_function_exists(error_at_line HAVE_ERROR_AT_LINE)
check_function_exists(floor HAVE_FLOOR)
check_function_exists(fork HAVE_FORK)
check_function_exists(ftime HAVE_FTIME)
check_function_exists(ftruncate HAVE_FTRUNCATE)
check_function_exists(getenv HAVE_DECL_GETENV)
check_function_exists(gethostbyname HAVE_GETHOSTBYNAME)
check_function_exists(getpagesize HAVE_GETPAGESIZE)
check_function_exists(gettimeofday HAVE_GETTIMEOFDAY)
check_function_exists(inet_ntoa HAVE_INET_NTOA)
check_function_exists(isascii HAVE_ISASCII)
check_function_exists(localtime_r HAVE_LOCALTIME_R)
check_function_exists(malloc HAVE_MALLOC)
check_function_exists(mbsrtowcs HAVE_MBSRTOWCS)
check_function_exists(memchr HAVE_MEMCHR)
check_function_exists(memmove HAVE_MEMMOVE)
check_function_exists(mempcpy HAVE_MEMPCPY)
check_function_exists(memset HAVE_MEMSET)
check_function_exists(mkdir HAVE_MKDIR)
check_function_exists(mktime HAVE_MKTIME)
check_function_exists(pow HAVE_POW)
check_function_exists(regcomp HAVE_REGCOMP)
check_function_exists(rmdir HAVE_RMDIR)
check_function_exists(select HAVE_SELECT)
check_function_exists(setenv HAVE_SETENV)
check_function_exists(setlocale HAVE_SETLOCALE)
check_function_exists(socket HAVE_SOCKET)
check_function_exists(stat HAVE_STAT)
check_function_exists(strcasecmp HAVE_STRCASECMP)
check_function_exists(strchr HAVE_STRCHR)
check_function_exists(strcspn HAVE_STRCSPN)
check_function_exists(strdup HAVE_STRDUP)
check_function_exists(strerror HAVE_STRERROR)
check_function_exists(strerror_r HAVE_STRERROR_R)
check_function_exists(strftime HAVE_STRFTIME)
check_function_exists(strrchr HAVE_STRRCHR)
check_function_exists(strspn HAVE_STRSPN)
check_function_exists(strstr HAVE_STRSTR)
check_function_exists(strtod HAVE_STRTOD)
check_function_exists(strtol HAVE_STRTOL)
check_function_exists(strtoul HAVE_STRTOUL)
check_function_exists(strtoull HAVE_STRTOULL)
check_function_exists(utime HAVE_UTIME)
check_function_exists(vfork HAVE_VFORK)
check_function_exists(wmempcpy HAVE_WMEMPCPY)

check_cxx_symbol_exists(alloca alloca.h HAVE_ALLOCA)
check_cxx_symbol_exists(strerror_r string.h HAVE_DECL_STRERROR_R)
check_cxx_symbol_exists(tm sys/time.h TM_IN_SYS_TIME)

# AC_TYPE_SIGNAL
check_type_size(ptrdiff_t PTRDIFF_T)
check_type_size(_Bool __BOOL)
check_type_size(mode_t mode_t_test)
if(NOT HAVE_mode_t_test)
    set(mode_t int)
endif()
check_type_size(off_t off_t_test)
if(NOT HAVE_off_t_test)
    set(off_t long int)
endif()
check_type_size(pid_t pid_t_test)
if(NOT HAVE_pid_t_test)
    set(pid_t int)
endif()
check_type_size(size_t size_t_test)
if(NOT HAVE_size_t_test)
    set(size_t unsigned int)
endif()

check_cxx_source_compiles(
    "#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <float.h>

int
main ()
{

  ;
  return 0;
}"
    STDC_HEADERS
)

set(TEST_INCLUDES "
#include <stdio.h>"
)

if(HAVE_SYS_TYPES_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <sys/types.h>"
    )
endif()
if(HAVE_SYS_STAT_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <sys/stat.h>"
    )
endif()
if(STDC_HEADERS)
    set(TEST_INCLUDES
        "${TEST_INCLUDES}
    # include <stdlib.h>
    # include <stddef.h>"
    )
else()
    if()
        set(TEST_INCLUDES "${TEST_INCLUDES}
        # include <stdlib.h>"
        )
    endif()
endif()
if(HAVE_STRING_H)
    if(NOT STDC_HEADERS)
        if(HAVE_MEMORY_H)
            set(TEST_INCLUDES "${TEST_INCLUDES}
            # include <memory.h>"
            )
        endif()
    endif()
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <string.h>"
    )
endif()
if(HAVE_STRINGS_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <strings.h>"
    )
endif()
if(HAVE_INTTYPES_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <inttypes.h>"
    )
endif()
if(HAVE_STDINT_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <stdint.h>"
    )
endif()
if(HAVE_UNISTD_H)
    set(TEST_INCLUDES "${TEST_INCLUDES}
    # include <unistd.h>"
    )
endif()

check_cxx_source_compiles(
    "
${TEST_INCLUDES}
#       include <wchar.h>
int
main ()
{
mbstate_t x; return sizeof x;
  ;
  return 0;
}"
    HAVE_MBSTATE_T
)

check_cxx_source_runs(
    "
${TEST_INCLUDES}

int main ()
{
struct stat sbuf;
  return stat (\"\", &sbuf) == 0;
  ;
  return 0;
}"
    STAT_EMPTY_STRING_BUG
)
if(NOT STAT_EMPTY_STRING_BUG)
    set(HAVE_STAT_EMPTY_STRING_BUG 1)
endif()

check_cxx_source_compiles(
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
}"
    HAVE_STDBOOL_H
)

if(HAVE_UTIME_H)
    check_cxx_source_compiles(
        "${TEST_INCLUDES}
# include <utime.h>
int
main ()
{
struct stat s, t;
  return ! (stat (\"${CMAKE_CURRENT_BINARY_DIR}/conftest.data\", &s) == 0
        && utime (\"${CMAKE_CURRENT_BINARY_DIR}/conftest.data\", 0) == 0
        && stat (\"${CMAKE_CURRENT_BINARY_DIR}/conftest.data\", &t) == 0
        && t.st_mtime >= s.st_mtime
        && t.st_mtime - s.st_mtime < 120);
  ;
  return 0;
}"
        HAVE_UTIME_NULL
    )
endif()

check_cxx_source_compiles(
    "
${TEST_INCLUDES}
int
main ()
{
      /* By Ruediger Kuhlmann. */
      return fork () < 0;
  ;
  return 0;
}"
    HAVE_WORKING_FORK
)

check_cxx_source_compiles(
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
}"
    HAVE_WORKING_VFORK
)

if(NOT HAVE_WORKING_VFORK)
    set(VFORK fork)
endif()

check_cxx_source_compiles(
    "
#include <sys/types.h>
#include <signal.h>

int
main ()
{
return *(signal (0, 0)) (0) == 1;
  ;
  return 0;
}"
    RET_SIGNAL_TYPES
)
if(RET_SIGNAL_TYPES)
    set(RETSIGTYPE int)
else()
    set(RETSIGTYPE void)
endif()

# IF(NOT LSTAT_FOLLOWS_SLASHED_SYMLINK)
execute_process(
    COMMAND rm -f conftest.sym conftest.file
    COMMAND touch conftest.file
    COMMAND ln -s conftest.file conftest.sym
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
check_cxx_source_runs(
    "
${TEST_INCLUDES}
int
main ()
{
struct stat sbuf;
     /* Linux will dereference the symlink and fail, as required by POSIX.
    That is better in the sense that it means we will not
    have to compile and use the lstat wrapper.  */
     return lstat (\"${CMAKE_CURRENT_BINARY_DIR}/conftest.sym/\", &sbuf) == 0;
  ;
  return 0;
}"
    LSTAT_FOLLOWS_SLASHED_SYMLINK
)

set(SELECT_INCLUDES ${TEST_INCLUDES})
if(HAVE_SYS_SELECT_H)
    set(SELECT_INCULDES "${SELECT_INCLUDES}
# include <sys/select.h>"
    )
endif()
if(HAVE_SYS_SOCKET_H)
    set(SELECT_INCULDES "${SELECT_INCLUDES}
# include <sys/select.h>"
    )
endif()

foreach(ARG234 "fd_set *" "int *" "void *")
    foreach(ARG1 "int" "size_t" "unsigned long int" "unsigned int")
        foreach(ARG5 "struct timeval *" "const struct timeval *")
            check_cxx_source_compiles(
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
"
                SELECT_ARGS
            )
            if(SELECT_ARGS)
                set(SELECT_TYPE_ARG1 ${ARG1})
                set(SELECT_TYPE_ARG234 ${ARG234})
                set(SELECT_TYPE_ARG5 ${ARG5})
                break()
            endif()
        endforeach()
        if(SELECT_ARGS)
            break()
        endif()
    endforeach()
    if(SELECT_ARGS)
        break()
    endif()
endforeach()

check_cxx_source_compiles(
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
"
    STATS_MACROS_CHECK
)
if(NOT STATS_MACROS_CHECK)
    set(STAT_MACROS_BROKEN 1)
endif()

check_cxx_source_compiles(
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
"
    STRERROR_R_CHAR_P
)

check_cxx_source_compiles(
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
"
    TIME_WITH_SYS_TIME
)

check_cxx_source_compiles(
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
"
    CONST_CONFORM_CHECK
)
if(NOT CONST_CONFORM_CHECK)
    set(const "")
endif()

check_cxx_source_compiles(
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
"
    WORKING_VOLATILE
)
if(NOT WORKING_VOLATILE)
    set(volatile "")
endif()

foreach(RESTRICT_KW __restrict __restrict__ _Restrict restrict)

    check_cxx_source_compiles(
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
"
        RESTRICT_CHECK
    )
    if(RESTRICT_CHECK)
        set(restrict ${RESTRICT_KW})
        break()
    endif()
endforeach()

foreach(INLINE_KW inline __inline__ __inline)
    check_cxx_source_compiles(
        "
#ifndef __cplusplus
typedef int foo_t;
static ${INLINE_KW} foo_t static_foo () {return 0; }
${INLINE_KW} foo_t foo () {return 0; }
int main (){return 0;}
#endif
"
        INLINE
    )

    if(INLINE)
        set(inline ${INLINE_KW})
        break()
    endif()
endforeach()

if(NOT INLINE)
    set(inline "")
endif()

execute_process(COMMAND rm -f conftest.data conftest.file conftest.sym WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

check_cxx_source_runs(
    "
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
    MASK_LONGDOUBLE
)

find_package(Git QUIET)

if(GIT_FOUND AND EXISTS ${ENGINE_SRC_DIR}/.git)
    execute_process(
        COMMAND git describe --match=NeVeRmAtCh --always --dirty
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else()
    set(GIT_VERSION "source")
endif()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/build/releasenum.in ${CMAKE_CURRENT_BINARY_DIR}/build/releasenum IMMEDIATE)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/columnstoreversion.h.in ${CMAKE_CURRENT_SOURCE_DIR}/columnstoreversion.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/mcsconfig.h.in ${CMAKE_CURRENT_BINARY_DIR}/mcsconfig.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/gitversionEngine.in ${CMAKE_CURRENT_BINARY_DIR}/gitversionEngine IMMEDIATE)
