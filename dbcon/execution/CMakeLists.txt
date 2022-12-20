include_directories( ${ENGINE_COMMON_INCLUDES} ${ENGINE_UTILS_UDFSDK_INCLUDE} ${ENGINE_DATATYPES_INCLUDE})

set(execplan_LIB_SRCS
    execplan/calpontsystemcatalog.cpp
    execplan/aggregatecolumn.cpp
    execplan/arithmeticcolumn.cpp
    execplan/arithmeticoperator.cpp
    execplan/calpontexecutionplan.cpp
    execplan/calpontexecutionplanfactory.cpp
    execplan/calpontselectexecutionplan.cpp
    execplan/mcsanalyzetableexecutionplan.cpp
    execplan/clientrotator.cpp
    execplan/constantcolumn.cpp
    execplan/constantfilter.cpp
    execplan/existsfilter.cpp
    execplan/expressionparser.cpp
    execplan/filter.cpp
    execplan/functioncolumn.cpp
    execplan/groupconcatcolumn.cpp
    execplan/intervalcolumn.cpp
    execplan/jsonarrayaggcolumn.cpp
    execplan/logicoperator.cpp
    execplan/mysqlexecutionplan.cpp
    execplan/objectidmanager.cpp
    execplan/objectreader.cpp
    execplan/operator.cpp
    execplan/oracleexecutionplan.cpp
    execplan/outerjoinonfilter.cpp
    execplan/predicateoperator.cpp
    execplan/pseudocolumn.cpp
    execplan/range.cpp
    execplan/returnedcolumn.cpp
    execplan/rowcolumn.cpp
    execplan/selectfilter.cpp
    execplan/sessionmanager.cpp
    execplan/simplecolumn.cpp
    execplan/simplefilter.cpp
    execplan/simplescalarfilter.cpp
    execplan/treenode.cpp
    execplan/treenodeimpl.cpp
    execplan/vendorexecutionplan.cpp
    execplan/windowfunctioncolumn.cpp
    execplan/udafcolumn.cpp)


set(joblist_LIB_SRCS
    joblist/anydatalist.cpp
    joblist/batchprimitiveprocessor-jl.cpp
    joblist/columncommand-jl.cpp
    joblist/command-jl.cpp
    joblist/crossenginestep.cpp
    joblist/dictstep-jl.cpp
    joblist/diskjoinstep.cpp
    joblist/distributedenginecomm.cpp
    joblist/elementtype.cpp
    joblist/expressionstep.cpp
    joblist/filtercommand-jl.cpp
    joblist/filterstep.cpp
    joblist/groupconcat.cpp
    joblist/jlf_common.cpp
    joblist/jlf_execplantojoblist.cpp
    joblist/jlf_graphics.cpp
    joblist/jlf_tuplejoblist.cpp
    joblist/jlf_subquery.cpp
    joblist/joblist.cpp
    joblist/joblistfactory.cpp
    joblist/jobstep.cpp
    joblist/jobstepassociation.cpp
    joblist/jsonarrayagg.cpp
    joblist/lbidlist.cpp
    joblist/limitedorderby.cpp
    joblist/passthrucommand-jl.cpp
    joblist/passthrustep.cpp
    joblist/pcolscan.cpp
    joblist/pcolstep.cpp
    joblist/pdictionary.cpp
    joblist/pdictionaryscan.cpp
    joblist/pseudocc-jl.cpp
    joblist/rowestimator.cpp
    joblist/rowaggregation.cpp
    joblist/rtscommand-jl.cpp
    joblist/subquerystep.cpp
    joblist/subquerytransformer.cpp
    joblist/tablecolumn.cpp
    joblist/timestamp.cpp
    joblist/tuple-bps.cpp
    joblist/tupleaggregatestep.cpp
    joblist/tupleannexstep.cpp
    joblist/tupleconstantstep.cpp
    joblist/tuplehashjoin.cpp
    joblist/tuplehavingstep.cpp
    joblist/tupleunion.cpp
    joblist/unique32generator.cpp
    joblist/virtualtable.cpp
    joblist/windowfunctionstep.cpp)

    add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

    add_library(execution STATIC ${joblist_LIB_SRCS} ${execplan_LIB_SRCS})

    add_dependencies(execution loggingcpp)
    target_link_libraries(execution ${NETSNMP_LIBRARIES} ${MARIADB_STRING_LIBS} ${ENGINE_DT_LIB} joiner threadpool libmysql_client querystats compress loggingcpp rowgroup funcexp windowfunction brm oamcpp dataconvert querytele udfsdk managers)
    target_include_directories(execution BEFORE PUBLIC ${OPENSSL_INCLUDE_DIR}
        ${LIBMARIADB_BININC_DIR}
        ${LIBMARIADB_SRCINC_DIR}
    )

if (WITH_ORDERBY_UT)
    add_executable(job_orderby_tests joblist/orderby-tests.cpp)
    target_link_libraries(job_orderby_tests ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${CPPUNIT_LIBRARIES} cppunit)
    install(TARGETS job_orderby_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
endif()
