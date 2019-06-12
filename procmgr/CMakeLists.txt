
cs_add_executable(ProcMgr main.cpp processmanager.cpp )

target_link_libraries(ProcMgr 
  PRIVATE 
    brm
    columnstore_h
    common                                                                            
    crashtrace
    ddlcleanuputil_h
    execplan_h
    funcexp_h
    idbboot_h
    joblist_h
    threadpool_h
    )

install(TARGETS ProcMgr DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)

