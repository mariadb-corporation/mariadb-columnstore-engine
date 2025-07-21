include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(we_xml_STAT_SRCS we_xmlop.cpp we_xmljob.cpp we_xmlgendata.cpp we_xmlgenproc.cpp)

add_definitions(-D_FILE_OFFSET_BITS=64)

columnstore_static_library(we_xml ${we_xml_STAT_SRCS})
columnstore_link(we_xml loggingcpp)
