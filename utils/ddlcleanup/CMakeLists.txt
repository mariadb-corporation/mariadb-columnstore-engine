
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ddlcleanuputil_LIB_SRCS ddlcleanuputil.cpp)

add_library(ddlcleanuputil SHARED ${ddlcleanuputil_LIB_SRCS})

target_link_libraries(ddlcleanuputil ${NETSNMP_LIBRARIES})

set_target_properties(ddlcleanuputil PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS ddlcleanuputil DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

