
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

set_target_properties(windowfunction PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS windowfunction DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

