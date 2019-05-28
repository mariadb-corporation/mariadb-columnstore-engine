

cs_add_library(windowfunction SHARED 
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


set_target_properties(windowfunction PROPERTIES VERSION 1.0.0 SOVERSION 1)


target_link_libraries(windowfunction 
  PUBLIC 
  PRIVATE
    brm_h
    common_h
    configcpp_h
    dataconvert_h
    execplan_h
    funcexp_h
    idbboot_h
    joblist_h
    loggingcpp_h
    mariadb
    messageqcpp_h
    querytele_h 
    rowgroup_h
    threadpool_h
    udfsdk_h)

install(TARGETS windowfunction DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)

