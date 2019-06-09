
add_executable(editem editem.cpp)

target_link_libraries(editem 
  PRIVATE 
    brm
    common_h
    dataconvert
    joblist
  )

install(TARGETS editem DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

