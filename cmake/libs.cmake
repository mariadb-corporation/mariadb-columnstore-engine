set(ENGINE_DT_LIB datatypes)
set(ENGINE_COMMON_LIBS
    boost_thread
    configcpp
    idbboot
    loggingcpp
    messageqcpp
    pthread
    rt
    xml2
    ${ENGINE_DT_LIB}
)
set(ENGINE_OAM_LIBS oamcpp)
set(ENGINE_BRM_LIBS brm cacheutils idbdatafile rwlock ${ENGINE_OAM_LIBS} ${ENGINE_COMMON_LIBS})

set(PLUGIN_EXEC_LIBS
    common
    compress
    dataconvert
    execplan
    funcexp
    joiner
    querytele
    regr
    rowgroup
    threadpool
    udfsdk
    windowfunction
    ${ENGINE_BRM_LIBS}
)
set(ENGINE_EXEC_LIBS joblist querystats libmysql_client ${PLUGIN_EXEC_LIBS})
set(PLUGIN_WRITE_LIBS
    cacheutils
    ddlpackage
    ddlpackageproc
    dmlpackage
    dmlpackageproc
    idbdatafile
    writeengine
    writeengineclient
)
set(ENGINE_WRITE_LIBS ${PLUGIN_WRITE_LIBS} ${ENGINE_EXEC_LIBS})
set(MARIADB_CLIENT_LIBS libmariadb)
set(MARIADB_STRING_LIBS dbug strings mysys)
