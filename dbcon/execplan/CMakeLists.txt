target_include_directories(execplan_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(execplan SHARED 
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
    udafcolumn.cpp
    )

target_link_libraries(execplan 
	INTERFACE execplan_h
  PRIVATE
    Boost::thread
    common_h
    joblist_h
    ddlpackage_h
    brm_h
    mariadb
    oamcpp_h
    configcpp_h
    alarmmanager_h
    rwlock_h
    querystats_h
    querytele_h
    threadpool_h
  PUBLIC
    messageqcpp
    idbboot
    rowgroup
    loggingcpp
    dataconvert
    udfsdk
    funcexp
    )

set_target_properties(execplan PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(execplan)

install(TARGETS execplan DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

