
target_include_directories(configcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(configcpp STATIC 
  configcpp.cpp 
  xmlparser.cpp 
  configstream.cpp
  )

target_compile_definitions(configcpp PRIVATE BOOST_NO_CXX11_SCOPED_ENUMS)

target_link_libraries(configcpp 
  INTERFACE configcpp_h
  PUBLIC 
    messageqcpp_h
    LibXml2::LibXml2
  PRIVATE 
    Boost::filesystem
    idbboot_h
    loggingcpp_h
)

