
include(CTest)
enable_testing()

configure_file(${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt.catch2 ${CMAKE_CURRENT_BINARY_DIR}/.catch2/CMakeLists.txt @ONLY)

execute_process(
  COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.catch2
)

execute_process(
  COMMAND ${CMAKE_COMMAND} --build .
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.catch2
)

add_library(catch2 INTERFACE IMPORTED GLOBAL)

target_include_directories(catch2 INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/catch2/include)
