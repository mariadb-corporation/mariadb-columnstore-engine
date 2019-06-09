
target_include_directories(compress_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(compress STATIC 
    idbcompress.cpp
    version1.cpp
	)

target_link_libraries(compress 
  INTERFACE compress_h
  PUBLIC
  PRIVATE
	  brm_h 
    common_h 
    loggingcpp_h
    idbboot_h
    columnstore_h
    Threads::Threads
    Snappy::Snappy
    rt
	)

