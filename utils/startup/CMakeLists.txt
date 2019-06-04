
target_include_directories(idbboot_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(idbboot STATIC installdir.cpp)

target_link_libraries(idbboot 
	INTERFACE idbboot_h   
  PUBLIC
	PRIVATE
		configcpp_h
  )

