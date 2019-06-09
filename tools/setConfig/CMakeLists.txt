
add_executable(setConfig main.cpp)

target_link_libraries(setConfig 
  PRIVATE 
    Boost::thread
    Boost::system
    brm
    common_h
    joblist_h
  )

install(TARGETS setConfig DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(PROGRAMS configxml.sh DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

