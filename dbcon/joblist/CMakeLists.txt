include_directories(${ENGINE_COMMON_INCLUDES} ${ENGINE_SRC_DIR}/tools/passwd)
# ########## next target ###############

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
    windowfunctionstep.cpp
    ${ENGINE_SRC_DIR}/tools/passwd/secrets.cpp
)

# ########## next target ###############

columnstore_static_library(joblist_server ${joblist_LIB_SRCS})
columnstore_link(joblist_server loggingcpp)

# ########## next target ###############

add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

columnstore_library(joblist ${joblist_LIB_SRCS})
target_include_directories(
    joblist BEFORE PUBLIC ${OPENSSL_INCLUDE_DIR} ${LIBMARIADB_BININC_DIR} ${LIBMARIADB_SRCINC_DIR}
)
columnstore_link(
    joblist
    loggingcpp
    boost_thread
    oamcpp
    querytele
    messageqcpp
    statistics_manager
)

if(WITH_ORDERBY_UT)
    columnstore_executable(job_orderby_tests orderby-tests.cpp)
    columnstore_link(job_orderby_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
endif()
