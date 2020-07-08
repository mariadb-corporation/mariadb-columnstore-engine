
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(windowfunction_LIB_SRCS
    framebound.cpp
    frameboundrange.cpp
    frameboundrow.cpp
    idborderby.cpp
    windowframe.cpp
    windowfunction.cpp
    windowfunctiontype.cpp
    wf_count.cpp
    wf_lead_lag.cpp
    wf_min_max.cpp
    wf_nth_value.cpp
    wf_ntile.cpp
    wf_percentile.cpp
    wf_ranking.cpp
    wf_row_number.cpp
    wf_stats.cpp
    wf_sum_avg.cpp
    wf_udaf.cpp)

add_library(windowfunction SHARED ${windowfunction_LIB_SRCS})

add_dependencies(windowfunction loggingcpp)

install(TARGETS windowfunction DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

if (WITH_SORTING_COMPARATORS_UT)
    add_executable(comparators_tests comparators-tests.cpp)
    target_link_libraries(comparators_tests ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    install(TARGETS comparators_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
endif()
