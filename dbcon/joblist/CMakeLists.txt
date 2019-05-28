target_include_directories(joblist_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(joblist SHARED 
    anydatalist.cpp
    batchprimitiveprocessor-jl.cpp
    columncommand-jl.cpp
    command-jl.cpp
    crossenginestep.cpp
    dictstep-jl.cpp
    diskjoinstep.cpp
    distributedenginecomm.cpp
    elementtype.cpp
    expressionstep.cpp
    filtercommand-jl.cpp
    filterstep.cpp
    groupconcat.cpp
    jl_logger.cpp
    jlf_common.cpp
    jlf_execplantojoblist.cpp
    jlf_graphics.cpp
    jlf_tuplejoblist.cpp
    jlf_subquery.cpp
    joblist.cpp
    joblistfactory.cpp
    jobstep.cpp
    jobstepassociation.cpp
    lbidlist.cpp
    limitedorderby.cpp
    passthrucommand-jl.cpp
    passthrustep.cpp
    pcolscan.cpp
    pcolstep.cpp
    pdictionary.cpp
    pdictionaryscan.cpp
    primitivemsg.cpp
    pseudocc-jl.cpp
    resourcedistributor.cpp
    resourcemanager.cpp
    rowestimator.cpp
    rtscommand-jl.cpp
    subquerystep.cpp
    subquerytransformer.cpp
    tablecolumn.cpp
    timestamp.cpp
    tuple-bps.cpp
    tupleaggregatestep.cpp
    tupleannexstep.cpp
    tupleconstantstep.cpp
    tuplehashjoin.cpp
    tuplehavingstep.cpp
    tupleunion.cpp
    unique32generator.cpp
    virtualtable.cpp
    windowfunctionstep.cpp)

target_link_libraries(joblist
  INTERFACE joblist_h
  
  PRIVATE
    rowgroup_h
    messageqcpp_h
    execplan_h
    udfsdk_h
    dataconvert_h
    brm_h
    mariadb
    rwlock_h
    configcpp_h
    funcexp_h
    oamcpp_h
    idbboot_h
    compress_h          
    primproc_h 
  PUBLIC
    loggingcpp
    querytele
    windowfunction
    threadpool
    alarmmanager
    joiner
    common
    udfsdk
    querystats
    mysql_client
    )

set_target_properties(joblist PROPERTIES VERSION 1.0.0 SOVERSION 1)


install(TARGETS joblist DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)

