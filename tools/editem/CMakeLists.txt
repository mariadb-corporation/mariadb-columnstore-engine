
cs_add_executable(editem editem.cpp)

target_link_libraries(editem 
  PRIVATE 
    brm
    common_h
    dataconvert
    joblist
  )

install(TARGETS editem DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

