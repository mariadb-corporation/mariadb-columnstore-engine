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
    udafcolumn.cpp)

target_link_libraries(execplan PUBLIC execplan_h 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          dataconvert_h
          ddlpackage_h 
          funcexp_h 
          idbboot_h
          joblist_h
          loggingcpp_h
          mariadb
          messageqcpp_h
          oamcpp_h
          querystats_h 
          querytele_h
          rowgroup_h
          threadpool_h
          udfsdk_h)

set_target_properties(execplan PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(execplan)

install(TARGETS execplan DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

