
include_directories( ${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)


########### next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

#libprocessor_a_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(processor STATIC ${processor_STAT_SRCS})

target_link_libraries(processor ${NETSNMP_LIBRARIES})

INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})

#if (WITH_PP_SCAN_UT)
#    add_executable(pp_scan_unittest pp-scan-unittest.cpp)
#    target_link_libraries(pp_scan_unittest ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
#    install(TARGETS pp_scan_unittest DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
#endif()

