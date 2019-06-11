target_include_directories(processor_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(processor STATIC primitiveprocessor.cpp dictionary.cpp column.cpp)

target_link_libraries(processor 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          dataconvert_h
          dbbc_h
          execplan_h
          funcexp_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          primproc_h
          processor_h
          rwlock_h
          we_shared_h
          )

target_include_directories(processor PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})


INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})

