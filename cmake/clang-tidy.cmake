

option(CLANG_TIDY_SCAN "Perform scan with clang-tidy" FALSE)

if(NOT CLANG_TIDY_SCAN)
  unset(CMAKE_CXX_CLANG_TIDY)
  unset(CXX_CLANG_TIDY)
  return()
endif()

find_program(CMAKE_CXX_CLANG_TIDY NAMES clang-tidy clang-tidy.exe DOC "Location of the clang-tidy tool.")