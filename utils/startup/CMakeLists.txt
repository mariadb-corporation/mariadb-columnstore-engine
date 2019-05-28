
target_include_directories(idbboot_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(idbboot STATIC installdir.cpp)

target_link_libraries(idbboot 
    PUBLIC idbboot_h   
          Boost::system
          Boost::filesystem
    PRIVATE configcpp_h
          )


INSTALL(TARGETS idbboot DESTINATION ${ENGINE_LIBDIR})
