
include(CMakePrintHelpers)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS FALSE)
set(GTEST_DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/_gtest_download )
set(GTEST_ROOT ${CMAKE_CURRENT_BINARY_DIR}/gtest)

configure_file(gtest.CMakeLists.txt.in ${GTEST_DOWNLOAD_DIR}/CMakeLists.txt @ONLY)

execute_process(
  COMMAND ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR} .
  WORKING_DIRECTORY ${GTEST_DOWNLOAD_DIR}
  RESULT_VARIABLE _cmdret
)

if(${_cmdret})
  message(FATAL_ERROR "Failure downloading GoogleTest")
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} --build .
  WORKING_DIRECTORY ${GTEST_DOWNLOAD_DIR}
  RESULT_VARIABLE _cmdret
)

if(${_cmdret})
  message(FATAL_ERROR "Failure building GoogleTest")
endif()

set(GTEST_INCLUDE_DIR ${GTEST_ROOT}/include)
find_package(GTest REQUIRED)


function(add_gtest _target)
  add_executable(${_target} ${ARGN})
  target_link_libraries(${_target} PRIVATE GTest::GTest GTest::Main)
  add_test(NAME ${_target} COMMAND $<TARGET_FILE:${_target}>)
endfunction()


add_subdirectory(common)