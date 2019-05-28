
target_include_directories(configcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(configcpp SHARED configcpp.cpp xmlparser.cpp configstream.cpp)

set_target_properties(configcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_compile_definitions(configcpp PRIVATE BOOST_NO_CXX11_SCOPED_ENUMS)

target_link_libraries(configcpp 
  PUBLIC  configcpp_h
          Boost::system
          Boost::filesystem
          LibXml2::LibXml2          
          idbboot
  PRIVATE common_h          
          loggingcpp_h          
          )

  target_pch(configcpp)

install(TARGETS configcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


