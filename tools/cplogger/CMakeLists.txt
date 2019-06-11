
cs_add_executable(cplogger main.cpp)

target_link_libraries(cplogger 
  PRIVATE 
			brm
		  )

install(TARGETS cplogger DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

