
set(GTEST_DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}/.gtest)
set(GTEST_ROOT ${CMAKE_CURRENT_BINARY_DIR}/gtest)


configure_file(gtest.CMakeLists.txt.in ${GTEST_DOWNLOAD_DIR}/CMakeLists.txt @ONLY)

execute_process(
  COMMAND ${CMAKE_COMMAND} -Wno-dev -G ${CMAKE_GENERATOR} .
  WORKING_DIRECTORY ${GTEST_DOWNLOAD_DIR}
  RESULT_VARIABLE _cmdret
)

if(${_cmdret})
  message(FATAL_ERROR "Failure downloading GoogleTest")
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -Wno-dev --build .
  WORKING_DIRECTORY ${GTEST_DOWNLOAD_DIR}
  RESULT_VARIABLE _cmdret
)

if(${_cmdret})
  message(FATAL_ERROR "Failure building GoogleTest")
endif()

enable_testing()
find_package(GTest REQUIRED)