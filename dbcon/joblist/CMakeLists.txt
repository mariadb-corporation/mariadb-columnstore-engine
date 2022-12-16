include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(joblist_LIB_SRCS
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
    jlf_common.cpp
    jlf_execplantojoblist.cpp
    jlf_graphics.cpp
    jlf_tuplejoblist.cpp
    jlf_subquery.cpp
    joblist.cpp
    joblistfactory.cpp
    jobstep.cpp
    jobstepassociation.cpp
    jsonarrayagg.cpp
    lbidlist.cpp
    limitedorderby.cpp
    passthrucommand-jl.cpp
    passthrustep.cpp
    pcolscan.cpp
    pcolstep.cpp
    pdictionary.cpp
    pdictionaryscan.cpp
    pseudocc-jl.cpp
    rowestimator.cpp
    rowaggregation.cpp
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


########### next target ###############

add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

add_library(joblist STATIC ${joblist_LIB_SRCS})

target_include_directories(joblist BEFORE PUBLIC ${OPENSSL_INCLUDE_DIR}
  ${LIBMARIADB_BININC_DIR}
  ${LIBMARIADB_SRCINC_DIR})
add_dependencies(joblist loggingcpp)

target_link_libraries(joblist joiner threadpool libmysql_client querystats compress loggingcpp querytele rowgroup funcexp windowfunction managers)

install(TARGETS joblist DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

if (WITH_ORDERBY_UT)
    add_executable(job_orderby_tests orderby-tests.cpp)
    target_link_libraries(job_orderby_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    install(TARGETS job_orderby_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
endif()

