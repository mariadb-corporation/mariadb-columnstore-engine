include_guard(GLOBAL)


option(USE_CPPCHECK "Scan source with cpplint" FALSE)

if(NOT USE_CPPCHECK)
  unset(CMAKE_CXX_CPPCHECK)
  return()
endif()

find_program(CMAKE_CXX_CPPCHECK NAMES cppcheck)

if(NOT CMAKE_CXX_CPPCHECK)
  warning("cppcheck not found. cppcheck disabled")
  set(USE_CPPCHECK FALSE)
  unset(CMAKE_CXX_CPPLINT)
  return()
endif()

add_custom_target(cppcheck_scan
  COMMAND ${CMAKE_CXX_CPPCHECK} --project=compile_commands.json
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} VERBATIM
  )



