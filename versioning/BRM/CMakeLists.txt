
target_include_directories(brm_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


cs_add_library(brm SHARED 
    autoincrementmanager.cpp
    blockresolutionmanager.cpp
    brmshmimpl.cpp
    brmtypes.cpp
    copylocks.cpp
    dbrm.cpp
    extentmap.cpp
    lbidresourcegraph.cpp
    logicalpartition.cpp
    mastersegmenttable.cpp
    oidserver.cpp
    resourcenode.cpp
    rgnode.cpp
    rwlockmonitor.cpp
    sessionmanagerserver.cpp
    shmkeys.cpp
    slavedbrmnode.cpp
    slavecomm.cpp
    tablelockserver.cpp
    tracer.cpp
    transactionnode.cpp
    undoable.cpp
    vbbm.cpp
    vss.cpp)

target_link_libraries(brm 
  INTERFACE brm_h 
  PRIVATE
    alarmmanager_h         
    Boost::system
    common_h
    dataconvert_h
    idbboot_h
    joblist_h
    Snappy::Snappy
  PUBLIC
    rt
    loggingcpp
    configcpp
    compress
    messageqcpp
    rwlock
    oamcpp
    idbdatafile
    execplan
    alarmmanager
    cacheutils
    )
  

set_target_properties(brm PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS brm DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)


# controllernode

cs_add_executable(controllernode 
  masternode.cpp 
  masterdbrmnode.cpp 
  )

target_link_libraries(controllernode
  PUBLIC 
  PRIVATE
    Boost::system
    Boost::thread
    brm
    common_h
    funcexp_h
    joblist_h
    crashtrace
    )

install(TARGETS controllernode DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# workernode

cs_add_executable(workernode slavenode.cpp)

target_link_libraries(workernode 
  PUBLIC 
  PRIVATE
    brm
    common_h
    crashtrace
    funcexp_h
    joblist_h
    Boost::thread
    )

install(TARGETS workernode DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# dbrmctl

cs_add_executable(dbrmctl dbrmctl.cpp)

target_link_libraries(dbrmctl
  PUBLIC 
  PRIVATE
		brm
    common_h          
    joblist_h
    )

install(TARGETS dbrmctl DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# reset_locks

cs_add_executable(reset_locks reset_locks.cpp)

target_link_libraries(reset_locks 
  PUBLIC 
  PRIVATE
    brm
    common_h
    configcpp_h
    execplan_h 
    joblist_h
    loggingcpp_h
    messageqcpp_h
    rwlock_h)

install(TARGETS reset_locks DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# rollback

cs_add_executable(rollback rollback.cpp)

target_link_libraries(rollback 
  PUBLIC 
  PRIVATE
    Boost::thread
    brm
    loggingcpp_h
    messageqcpp_h)

install(TARGETS rollback DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# save_brm 

cs_add_executable(save_brm save_brm.cpp)

target_link_libraries(save_brm 
  PUBLIC 
  PRIVATE
    brm
    messageqcpp_h
    loggingcpp_h)

install(TARGETS save_brm DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)


# load_brm

cs_add_executable(load_brm load_brm.cpp)

target_link_libraries(load_brm 
  PUBLIC 
  PRIVATE
    brm
    common_h
    configcpp_h
    execplan_h
    idbdatafile_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    rwlock_h)

install(TARGETS load_brm DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
