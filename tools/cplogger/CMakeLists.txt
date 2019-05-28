
cs_add_executable(cplogger main.cpp)

target_link_libraries(cplogger 
  PRIVATE 
			brm
		  )

install(TARGETS cplogger DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

