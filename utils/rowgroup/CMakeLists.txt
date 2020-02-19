
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(rowgroup_LIB_SRCS rowaggregation.cpp rowgroup.cpp)

#librowgroup_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

add_library(rowgroup SHARED ${rowgroup_LIB_SRCS})

add_dependencies(rowgroup loggingcpp)

target_link_libraries(rowgroup ${NETSNMP_LIBRARIES} funcexp)

install(TARGETS rowgroup DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

if (WITH_ROWGROUP_UT)
    add_executable(rowgroup_tests rowgroup-tests.cpp)
    target_link_libraries(rowgroup_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS}) 
    install(TARGETS rowgroup_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-platform)
endif()
