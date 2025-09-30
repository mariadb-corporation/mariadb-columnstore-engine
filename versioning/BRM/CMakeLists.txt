include_directories(${ENGINE_COMMON_INCLUDES})

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/shmkeys.cpp.in" "${CMAKE_CURRENT_SOURCE_DIR}/shmkeys.cpp" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/brmshmimpl.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/brmshmimpl.h" @ONLY)

# ########## next target ###############

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
    ../../datatypes/mcs_datatype.cpp
)

columnstore_library(brm ${brm_LIB_SRCS})
columnstore_link(brm loggingcpp datatypes)

# ########## next target ###############

set(controllernode_SRCS masternode.cpp masterdbrmnode.cpp ../../utils/common/crashtrace.cpp)

columnstore_executable(controllernode ${controllernode_SRCS})
columnstore_link(controllernode ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

# ########## next target ###############

set(workernode_SRCS slavenode.cpp ../../utils/common/crashtrace.cpp)

columnstore_executable(workernode ${workernode_SRCS})
columnstore_link(workernode ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

# ########## next target ###############

set(dbrmctl_SRCS dbrmctl.cpp)

columnstore_executable(dbrmctl ${dbrmctl_SRCS})
columnstore_link(dbrmctl ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

# ########## next target ###############

set(rollback_SRCS rollback.cpp)

columnstore_executable(rollback ${rollback_SRCS})
columnstore_link(rollback ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

# ########## next target ###############

set(save_brm_SRCS save_brm.cpp)

columnstore_executable(save_brm ${save_brm_SRCS})
columnstore_link(save_brm ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

# ########## next target ###############

set(load_brm_SRCS load_brm.cpp)

columnstore_executable(load_brm ${load_brm_SRCS})
columnstore_link(load_brm ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

columnstore_executable(mcs-load-em load_em.cpp)
columnstore_link(
    mcs-load-em ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES}
)

columnstore_executable(mcs-load-brm-from-file load_brm_from_file.cpp)
columnstore_link(
    mcs-load-brm-from-file
    ${ENGINE_LDFLAGS}
    ${MARIADB_CLIENT_LIBS}
    ${ENGINE_OAM_LIBS}
    ${ENGINE_EXEC_LIBS}
    ${NETSNMP_LIBRARIES}
    boost_program_options
)

columnstore_executable(mcs-shmem-locks shmem_locks.cpp)
columnstore_link(
    mcs-shmem-locks
    ${ENGINE_LDFLAGS}
    ${MARIADB_CLIENT_LIBS}
    ${ENGINE_OAM_LIBS}
    ${ENGINE_EXEC_LIBS}
    ${NETSNMP_LIBRARIES}
    boost_program_options
)

columnstore_executable(mcs-oid-client oid-client.cpp)
columnstore_link(mcs-oid-client ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} boost_program_options)
