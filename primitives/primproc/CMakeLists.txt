
target_include_directories(primproc_h 
  INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}     
  #  messageqcpp_h
  )


cs_add_executable(PrimProc 
    primproc.cpp
    batchprimitiveprocessor.cpp
    bppseeder.cpp
    bppsendthread.cpp
    columncommand.cpp
    command.cpp
    dictstep.cpp
    filtercommand.cpp
    logger.cpp
    passthrucommand.cpp
    primitiveserver.cpp
    pseudocc.cpp
    rtscommand.cpp
    umsocketselector.cpp
    )

target_link_libraries(PrimProc 
  PRIVATE 
    primproc_h
    Boost::system
    Boost::thread
    brm
    common_h
    dataconvert_h
    dbbc
    funcexp
    idbboot_h
    joblist
    joiner_h
    mariadb
    processor
    rowgroup_h
    threadpool_h
    udfsdk_h
    writeengineclient_h
    writeengine
    crashtrace
    we_shared_h
    )


install(TARGETS PrimProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


