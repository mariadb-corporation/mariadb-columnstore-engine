
# boost super build (see superbuild.md)


configure_file(${CMAKE_CURRENT_LIST_DIR}/boost.CMakeLists.txt.in ${CMAKE_CURRENT_BINARY_DIR}/.boost/CMakeLists.txt @ONLY)

execute_process(
	COMMAND ${CMAKE_COMMAND} . 
    -G "${CMAKE_GENERATOR}"
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
	WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.boost
  RESULT_VARIABLE _exec_ret
)

if(${_exec_ret})
  message(FATAL_ERROR "Error ${_exec_ret} configuring boost dependency.")
endif()

execute_process(
	COMMAND ${CMAKE_COMMAND} --build .
	WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.boost
  RESULT_VARIABLE _exec_ret
)

if(${_exec_ret})
  message(FATAL_ERROR "Error ${_exec_ret} building boost dependency: ${_exec_ret}")
endif()

unset(_exec_ret)

set(BOOST_ROOT ${CMAKE_CURRENT_BINARY_DIR}/boost)

find_package(Boost 1.55.0 REQUIRED COMPONENTS system filesystem thread regex date_time) 
