
configure_file(${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt.tbb ${CMAKE_CURRENT_BINARY_DIR}/.tbb/CMakeLists.txt @ONLY)

execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" . WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.tbb/)

execute_process(COMMAND ${CMAKE_COMMAND} --build . WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/.tbb/)


set(TBB_DIR  ${CMAKE_CURRENT_BINARY_DIR}/tbb)
list(APPEND CMAKE_MODULE_PATH ${TBB_DIR}/cmake)
include(TBBGet)
include(TBBBuild)
tbb_build(TBB_ROOT ${TBB_DIR} CONFIG_DIR TBB_DIR MAKE_ARGS tbb_cpf=1)

add_library(tbb SHARED IMPORTED GLOBAL)
add_library(tbbmalloc SHARED IMPORTED GLOBAL)
add_library(tbbmalloc_proxy SHARED IMPORTED GLOBAL)

set(tbb_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_release/libtbb_preview.so.2)
set(tbbmalloc_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_release/libtbbmalloc.so.2)
set(tbbmalloc_proxy_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_release/libtbbmalloc_proxy.so.2)
if(DEBUG)
  set(tbb_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_debug/libtbb_preview_debug.so.2)
  set(tbbmalloc_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_debug/libtbbmalloc_debug.so.2)
  set(tbbmalloc_proxy_bin ${CMAKE_CURRENT_BINARY_DIR}/tbb_cmake_build/tbb_cmake_build_subdir_debug/libtbbmalloc_proxy_debug.so.2)
endif()


set_target_properties(tbb PROPERTIES IMPORTED_LOCATION ${tbb_bin})
set_target_properties(tbbmalloc PROPERTIES IMPORTED_LOCATION ${tbbmalloc_bin})
set_target_properties(tbbmalloc_proxy PROPERTIES IMPORTED_LOCATION ${tbbmalloc_proxy_bin})

install(FILES ${tbb_bin} ${tbbmalloc_bin} ${tbbmalloc_proxy_bin} DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

target_include_directories(tbb INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/tbb/include)
target_include_directories(tbbmalloc INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/tbb/include)
target_include_directories(tbbmalloc_proxy INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/tbb/include)

target_compile_definitions(tbb INTERFACE -DTBB_PREVIEW_MEMORY_POOL=1)
