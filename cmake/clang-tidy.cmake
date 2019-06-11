include_guard(GLOBAL)

option(USE_CLANG_TIDY "Perform scan with clang-tidy" FALSE)

if(NOT USE_CLANG_TIDY)
  unset(CMAKE_CXX_CLANG_TIDY)
  return()
endif()

find_program(CMAKE_CXX_CLANG_TIDY NAMES clang-tidy clang-tidy.exe DOC "Location of the clang-tidy tool.")

set(CLANG_TIDY_CHECKS -*)

macro(_do_clang_tidy_check _name _doc _default _flag)
  option(DO_CLANG_TIDY_${_name}_CHECK ${_doc} ${_default})
  if(DO_CLANG_TIDY_${_name}_CHECK)
    set(CLANG_TIDY_CHECKS ${CLANG_TIDY_CHECKS},${_flag})
  endif()
endmacro()

_do_clang_tidy_check(CORE                 "Core checks"                 TRUE clang-analyzer-core*,)
_do_clang_tidy_check(CPLUSPLUS            "Core c++ checks"             TRUE clang-analyzer-cplusplus*)
_do_clang_tidy_check(DEADCODE             "Deadcode checks"             TRUE clang-analyzer-deadcode*)
_do_clang_tidy_check(LLVM_CONVENTIONS     "LLVM Conventions"            TRUE clang-analyzer-llvm*)
_do_clang_tidy_check(NULLABILITY          "Nullability correctness"     TRUE clang-analyzer-nullability*)
_do_clang_tidy_check(SECURECODE           "Core security checks"        TRUE clang-analyzer-security*)
_do_clang_tidy_check(UNIX                 "Unix specific checks"        TRUE clang-analyzer-unix*)
_do_clang_tidy_check(CPPCORE              "C++ core guidelines"         TRUE cppcoreguidelines*)
_do_clang_tidy_check(GOOGLE               "Google code guidelines"      TRUE google*)
_do_clang_tidy_check(LLVM                 "LLVM code guidelines"        TRUE llvm*)
_do_clang_tidy_check(MISC                 "Common errors"               TRUE misc-*)
_do_clang_tidy_check(MODERNIZE            "Legacy code style"           TRUE modernize-*)
_do_clang_tidy_check(PERFORMANCE          "Performance checks"          TRUE performance-*)
_do_clang_tidy_check(READABILITY          "Code readability"            TRUE readability-*)

configure_file(${CMAKE_CURRENT_LIST_DIR}/clang-tidy.in ${CMAKE_CURRENT_BINARY_DIR}/.clang-tidy @ONLY)
