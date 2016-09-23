
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(rowgroup_LIB_SRCS rowaggregation.cpp rowgroup.cpp)

#librowgroup_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(rowgroup SHARED ${rowgroup_LIB_SRCS})

target_link_libraries(rowgroup ${NETSNMP_LIBRARIES})

set_target_properties(rowgroup PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS rowgroup DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

