include_guard(GLOBAL)



option(USE_CLANG_TIDY "Perform scan with clang-tidy" FALSE)

if(NOT USE_CLANG_TIDY)
  unset(CMAKE_CXX_CLANG_TIDY)
  return()
endif()

find_program(CMAKE_CXX_CLANG_TIDY NAMES clang-tidy clang-tidy.exe DOC "Location of the clang-tidy tool.")

