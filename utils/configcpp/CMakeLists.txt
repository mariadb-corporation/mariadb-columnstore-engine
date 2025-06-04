set(configcpp_LIB_SRCS configcpp.cpp xmlparser.cpp)
include_directories(${ENGINE_COMMON_INCLUDES})
columnstore_library(configcpp ${configcpp_LIB_SRCS})
columnstore_link(configcpp PRIVATE LibXml2::LibXml2 boost_filesystem)
