
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(brm_LIB_SRCS
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
    vss.cpp
    ../../datatypes/mcs_datatype.cpp)

add_library(brm SHARED ${brm_LIB_SRCS})

add_dependencies(brm loggingcpp)

install(TARGETS brm DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)


########### next target ###############

set(controllernode_SRCS masternode.cpp masterdbrmnode.cpp ../../utils/common/crashtrace.cpp)

add_executable(controllernode ${controllernode_SRCS})

target_link_libraries(controllernode ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} )

install(TARGETS controllernode DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(workernode_SRCS slavenode.cpp ../../utils/common/crashtrace.cpp)

add_executable(workernode ${workernode_SRCS})

target_link_libraries(workernode ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS workernode DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(dbrmctl_SRCS dbrmctl.cpp)

add_executable(dbrmctl ${dbrmctl_SRCS})

target_link_libraries(dbrmctl ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS dbrmctl DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(reset_locks_SRCS reset_locks.cpp)

add_executable(reset_locks ${reset_locks_SRCS})

target_link_libraries(reset_locks ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

install(TARGETS reset_locks DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(rollback_SRCS rollback.cpp)

add_executable(rollback ${rollback_SRCS})

target_link_libraries(rollback ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

install(TARGETS rollback DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(save_brm_SRCS save_brm.cpp)

add_executable(save_brm ${save_brm_SRCS})

target_link_libraries(save_brm ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

install(TARGETS save_brm DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


########### next target ###############

set(load_brm_SRCS load_brm.cpp)

add_executable(load_brm ${load_brm_SRCS})

target_link_libraries(load_brm ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

install(TARGETS load_brm DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

