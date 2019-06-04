
add_executable(cplogger main.cpp)

target_link_libraries(cplogger 
  PRIVATE 
			loggingcpp
			configcpp
			messageqcpp
			compress
			brm
		  )

install(TARGETS cplogger DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

