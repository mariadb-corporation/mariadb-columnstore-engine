target_include_directories(rwlock_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(rwlock STATIC 
  rwlock.cpp 
  rwlock_local.cpp
  )

target_link_libraries(rwlock 
  INTERFACE rwlock_h
  PRIVATE
    brm_h 
    idbboot_h
    )

#set_target_properties(rwlock PROPERTIES VERSION 1.0.0 SOVERSION 1)

#install(TARGETS rwlock DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

