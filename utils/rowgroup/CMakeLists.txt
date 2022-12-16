
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(rowgroup_LIB_SRCS rowgroup.cpp rowstorage.cpp)

#librowgroup_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(rowgroup STATIC ${rowgroup_LIB_SRCS})

add_dependencies(rowgroup loggingcpp)

target_link_libraries(rowgroup ${NETSNMP_LIBRARIES})

install(TARGETS rowgroup DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
