
target_include_directories(dataconvert_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(dataconvert STATIC 
  dataconvert.cpp
)


target_link_libraries(dataconvert
  INTERFACE dataconvert_h
  PUBLIC
  PRIVATE
    execplan_h
    messageqcpp_h
    joblist_h
    loggingcpp_h
    brm_h
  )

