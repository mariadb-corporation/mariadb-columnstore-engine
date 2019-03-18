
macro(add_catch2_target _target)
  add_executable(${_target} ${ARGN})
  target_link_libraries(${_target} PRIVATE catch2)
  add_test(${_target})
endmacro()

macro(add_catch2_test)
  get_filename_component(_exe ${ARGV0} NAME_WE)
  add_executable(${_exe} ${ARGN})
  target_link_libraries(${_exe} INTERFACE catch2)
  add_test(NAME ${_exe} COMMAND $<TARGET_FILE:${_exe}> --success)
  unset(_exe)
endmacro()


option(BUILD_UNIT_TESTS "Build unit tests" TRUE)

if(BUILD_UNIT_TESTS)
  include(catch2)
  add_subdirectory(utils)
endif()