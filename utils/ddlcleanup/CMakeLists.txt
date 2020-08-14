
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ddlcleanuputil_LIB_SRCS ddlcleanuputil.cpp)

add_library(ddlcleanuputil SHARED ${ddlcleanuputil_LIB_SRCS})

add_dependencies(ddlcleanuputil loggingcpp)

target_link_libraries(ddlcleanuputil ${NETSNMP_LIBRARIES})

install(TARGETS ddlcleanuputil DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

