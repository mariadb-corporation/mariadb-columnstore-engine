
include_directories( ${ENGINE_COMMON_INCLUDES} ${ENGINE_UTILS_UDFSDK_INCLUDE} )


########### next target ###############

set(execplan_LIB_SRCS
    calpontsystemcatalog.cpp
    aggregatecolumn.cpp
    arithmeticcolumn.cpp
    arithmeticoperator.cpp
    calpontexecutionplan.cpp
    calpontexecutionplanfactory.cpp
    calpontselectexecutionplan.cpp
    clientrotator.cpp
    constantcolumn.cpp
    constantfilter.cpp
    existsfilter.cpp
    expressionparser.cpp
    filter.cpp
    functioncolumn.cpp
    groupconcatcolumn.cpp
    intervalcolumn.cpp
    logicoperator.cpp
    mysqlexecutionplan.cpp
    objectidmanager.cpp
    objectreader.cpp
    operator.cpp
    oracleexecutionplan.cpp
    outerjoinonfilter.cpp
    predicateoperator.cpp
    pseudocolumn.cpp
    range.cpp
    returnedcolumn.cpp
    rowcolumn.cpp
    selectfilter.cpp
    sessionmanager.cpp
    simplecolumn.cpp
    simplefilter.cpp
    simplescalarfilter.cpp
    treenode.cpp
    treenodeimpl.cpp
    vendorexecutionplan.cpp
    windowfunctioncolumn.cpp
    udafcolumn.cpp)

add_library(execplan SHARED ${execplan_LIB_SRCS})

target_link_libraries(execplan ${NETSNMP_LIBRARIES})

set_target_properties(execplan PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS execplan DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

