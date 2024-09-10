INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (we_xml_STAT_SRCS we_xmlop.cpp we_xmljob.cpp we_xmlgendata.cpp we_xmlgenproc.cpp)

ADD_DEFINITIONS (-D_FILE_OFFSET_BITS=64)

ADD_LIBRARY (we_xml STATIC ${we_xml_STAT_SRCS})

ADD_DEPENDENCIES (we_xml loggingcpp)

INSTALL (TARGETS we_xml DESTINATION ${ENGINE_LIBDIR})
