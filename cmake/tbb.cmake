
set(_tbb_download_dir ${CMAKE_CURRENT_BINARY_DIR}/.tbb_download)

message("_tbb_download_dir: ${_tbb_download_dir}")


if(NOT EXISTS ${_tbb_download_dir})
  file(MAKE_DIRECTORY ${_tbb_download_dir})
  configure_file(${CMAKE_CURRENT_LIST_DIR}/tbb.CMakeLists.txt.in ${_tbb_download_dir}/CMakeLists.txt @ONLY)
endif()


execute_process(
  COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  WORKING_DIRECTORY ${_tbb_download_dir}
  )

execute_process(
  COMMAND ${CMAKE_COMMAND} --build .
  WORKING_DIRECTORY ${_tbb_download_dir}
)

unset(_tbb_download_dir )

set(tbb_dir ${CMAKE_CURRENT_BINARY_DIR}/tbb)

configure_file(${CMAKE_CURRENT_LIST_DIR}/tbb.version_string.ver.in ${tbb_dir}/include/version_string.ver @ONLY)

add_library(tbb SHARED
  ${tbb_dir}/src/tbb/concurrent_hash_map.cpp
  ${tbb_dir}/src/tbb/concurrent_queue.cpp
  ${tbb_dir}/src/tbb/concurrent_vector.cpp
  ${tbb_dir}/src/tbb/dynamic_link.cpp
  ${tbb_dir}/src/tbb/itt_notify.cpp
  ${tbb_dir}/src/tbb/cache_aligned_allocator.cpp
  ${tbb_dir}/src/tbb/pipeline.cpp
  ${tbb_dir}/src/tbb/queuing_mutex.cpp
  ${tbb_dir}/src/tbb/queuing_rw_mutex.cpp
  ${tbb_dir}/src/tbb/reader_writer_lock.cpp
  ${tbb_dir}/src/tbb/spin_rw_mutex.cpp
  ${tbb_dir}/src/tbb/x86_rtm_rw_mutex.cpp
  ${tbb_dir}/src/tbb/spin_mutex.cpp
  ${tbb_dir}/src/tbb/critical_section.cpp
  ${tbb_dir}/src/tbb/mutex.cpp
  ${tbb_dir}/src/tbb/recursive_mutex.cpp
  ${tbb_dir}/src/tbb/condition_variable.cpp
  ${tbb_dir}/src/tbb/tbb_thread.cpp
  ${tbb_dir}/src/tbb/concurrent_monitor.cpp
  ${tbb_dir}/src/tbb/semaphore.cpp
  ${tbb_dir}/src/tbb/private_server.cpp
  ${tbb_dir}/src/rml/client/rml_tbb.cpp
  ${tbb_dir}/src/tbb/tbb_misc.cpp
  ${tbb_dir}/src/tbb/tbb_misc_ex.cpp
  ${tbb_dir}/src/tbb/task.cpp
  ${tbb_dir}/src/tbb/task_group_context.cpp
  ${tbb_dir}/src/tbb/governor.cpp
  ${tbb_dir}/src/tbb/market.cpp
  ${tbb_dir}/src/tbb/arena.cpp
  ${tbb_dir}/src/tbb/scheduler.cpp
  ${tbb_dir}/src/tbb/observer_proxy.cpp
  ${tbb_dir}/src/tbb/tbb_statistics.cpp
  ${tbb_dir}/src/tbb/tbb_main.cpp
  ${tbb_dir}/src/old/concurrent_vector_v2.cpp
  ${tbb_dir}/src/old/concurrent_queue_v2.cpp
  ${tbb_dir}/src/old/spin_rw_mutex_v2.cpp
  ${tbb_dir}/src/old/task_v2.cpp
  )

target_compile_definitions(tbb
  PRIVATE USE_PTHREAD __TBB_BUILD=1
  PUBLIC TBB_PREVIEW_MEMORY_POOL=1
  )

option(TBB_ENABLE_PROFILE "Enable TBB profiling with Intel Parallel Studio" FALSE)
mark_as_advanced(TBB_ENABLE_PROFILE)
if(TBB_ENABLE_PROFILE)
  target_compile_definitions(tbb PUBLIC DO_ITT_NOTIFY)
endif()


target_include_directories(tbb
  PUBLIC ${tbb_dir}/include
  PRIVATE ${tbb_dir}/src/rml/include
  PRIVATE ${tbb_dir}/src
)

add_library(tbbmalloc SHARED
  ${tbb_dir}/src/tbbmalloc/backend.cpp
  ${tbb_dir}/src/tbbmalloc/large_objects.cpp
  ${tbb_dir}/src/tbbmalloc/backref.cpp
  ${tbb_dir}/src/tbbmalloc/tbbmalloc.cpp
  ${tbb_dir}/src/tbb/itt_notify.cpp
  ${tbb_dir}/src/tbbmalloc/frontend.cpp
  )

target_compile_definitions(tbbmalloc PRIVATE TBB_USE_DEBUG USE_PTHREAD __TBBMALLOC_BUILD=1)

target_include_directories(tbbmalloc PUBLIC ${tbb_dir}/include)


option(TBB_ENABLE_PROFILE "Enable TBB profiling with Intel Parallel Studio" FALSE)
mark_as_advanced(TBB_ENABLE_PROFILE)
if(TBB_ENABLE_PROFILE)
  target_compile_definitions(tbb PUBLIC DO_ITT_NOTIFY)
  target_compile_definitions(tbbmalloc PUBLIC DO_ITT_NOTIFY)
endif()



unset(tbb_dir)