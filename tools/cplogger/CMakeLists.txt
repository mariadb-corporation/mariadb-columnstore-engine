
add_executable(cplogger main.cpp)

target_link_libraries(cplogger 
  PRIVATE messageqcpp_h
          loggingcpp_h)

install(TARGETS cplogger DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

