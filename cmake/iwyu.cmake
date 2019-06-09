include_guard(GLOBAL)

option(USE_IWYU "Scan source with Include-What-You-Use" FALSE)

if(NOT USE_IWYU)
  unset(CMAKE_CXX_INCLUDE_WHAT_YOU_USE)
  return()
endif()

find_program(CMAKE_CXX_INCLUDE_WHAT_YOU_USE NAMES iwuy include-what-you-use)