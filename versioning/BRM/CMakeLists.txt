
target_include_directories(brm_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


add_library(brm SHARED 
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
  PUBLIC brm_h 
          Boost::system
          messageqcpp
          execplan_h
          joblist_h
          loggingcpp
          rwlock
          common_h
          configcpp_h
  
          
          oamcpp_h
          alarmmanager_h
          
          idbboot
          idbdatafile_h
          dataconvert_h
          cacheutils_h
          )
  
  #[[
  alarmmanager_h
          cacheutils_h
          columnstore_h
          common_h
          configcpp_h
          dataconvert_h
          idbboot_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          rwlock_h
          )
          ]]

set_target_properties(brm PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS brm DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)


# controllernode

add_executable(controllernode 
  masternode.cpp 
  masterdbrmnode.cpp 
  ../../utils/common/crashtrace.cpp)

target_link_libraries(controllernode
  PUBLIC Boost::system
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          execplan_h 
          funcexp_h
          idbdatafile_h 
          joblist_h
          loggingcpp_h
          messageqcpp_h         
          oamcpp_h
          rwlock_h         
          )

install(TARGETS controllernode DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# workernode

add_executable(workernode slavenode.cpp  ../../utils/common/crashtrace.cpp)

target_link_libraries(workernode 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          execplan_h
          funcexp_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          rwlock_h)

install(TARGETS workernode DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# dbrmctl

add_executable(dbrmctl dbrmctl.cpp)

target_link_libraries(dbrmctl
  PRIVATE brm_h
          common_h
          configcpp_h
          execplan_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          rwlock_h)

install(TARGETS dbrmctl DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# reset_locks

add_executable(reset_locks reset_locks.cpp)

target_link_libraries(reset_locks 
  PRIVATE brm_h
          common_h
          configcpp_h
          execplan_h 
          joblist_h
          loggingcpp_h
          messageqcpp_h
          rwlock_h)

install(TARGETS reset_locks DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# rollback

add_executable(rollback rollback.cpp)

target_link_libraries(rollback 
  PRIVATE brm_h
          loggingcpp_h
          messageqcpp_h)

install(TARGETS rollback DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# save_brm 

add_executable(save_brm save_brm.cpp)

target_link_libraries(save_brm 
  PRIVATE brm_h
          messageqcpp_h
          loggingcpp_h)

install(TARGETS save_brm DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


# load_brm

add_executable(load_brm load_brm.cpp)

target_link_libraries(load_brm 
  PRIVATE brm_h
          common_h
          configcpp_h
          execplan_h
          idbdatafile_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          rwlock_h)

install(TARGETS load_brm DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
