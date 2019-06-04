target_include_directories(joiner_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(joiner SHARED 
  joiner.cpp 
  tuplejoiner.cpp 
  joinpartition.cpp
)

target_link_libraries(joiner 
  INTERFACE joiner_h
  PUBLIC
  PRIVATE
    Boost::filesystem
    Boost::system
    joblist_h
    rowgroup_h
    common_h
    messageqcpp_h
    execplan_h
    loggingcpp_h
    udfsdk_h
    dataconvert_h
    mariadb
    configcpp_h
    compress_h
    idbboot_h
    idbdatafile_h
    brm_h
    rwlock_h    
  )

target_include_directories(joiner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(joiner PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(joiner)

install(TARGETS joiner DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


