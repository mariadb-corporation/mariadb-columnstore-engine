
cs_add_executable(ProcMon main.cpp processmonitor.cpp)

target_link_libraries(ProcMon 
  PRIVATE 
    brm
    columnstore_h
    common_h
    crashtrace
    ddlcleanuputil_h
    idbboot
    joblist_h
    Boost::thread
    )


install(TARGETS ProcMon DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

