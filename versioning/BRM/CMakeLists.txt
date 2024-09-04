INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

CONFIGURE_FILE ("${CMAKE_CURRENT_SOURCE_DIR}/shmkeys.cpp.in" "${CMAKE_CURRENT_SOURCE_DIR}/shmkeys.cpp" @ONLY)
CONFIGURE_FILE ("${CMAKE_CURRENT_SOURCE_DIR}/brmshmimpl.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/brmshmimpl.h" @ONLY)

# ########## next target ###############

SET (
  brm_LIB_SRCS
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

ADD_LIBRARY (brm SHARED ${brm_LIB_SRCS})

ADD_DEPENDENCIES (brm loggingcpp)

INSTALL (
  TARGETS brm
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (controllernode_SRCS masternode.cpp masterdbrmnode.cpp ../../utils/common/crashtrace.cpp)

ADD_EXECUTABLE (controllernode ${controllernode_SRCS})

TARGET_LINK_LIBRARIES (controllernode ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS controllernode
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (workernode_SRCS slavenode.cpp ../../utils/common/crashtrace.cpp)

ADD_EXECUTABLE (workernode ${workernode_SRCS})

TARGET_LINK_LIBRARIES (workernode ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS workernode
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (dbrmctl_SRCS dbrmctl.cpp)

ADD_EXECUTABLE (dbrmctl ${dbrmctl_SRCS})

TARGET_LINK_LIBRARIES (dbrmctl ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS dbrmctl
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (reset_locks_SRCS reset_locks.cpp)

ADD_EXECUTABLE (reset_locks ${reset_locks_SRCS})

TARGET_LINK_LIBRARIES (reset_locks ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS reset_locks
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (rollback_SRCS rollback.cpp)

ADD_EXECUTABLE (rollback ${rollback_SRCS})

TARGET_LINK_LIBRARIES (rollback ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS rollback
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (save_brm_SRCS save_brm.cpp)

ADD_EXECUTABLE (save_brm ${save_brm_SRCS})

TARGET_LINK_LIBRARIES (save_brm ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS save_brm
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)

# ########## next target ###############

SET (load_brm_SRCS load_brm.cpp)

ADD_EXECUTABLE (load_brm ${load_brm_SRCS})

TARGET_LINK_LIBRARIES (load_brm ${ENGINE_LDFLAGS} ${ENGINE_OAM_LIBS} ${ENGINE_EXEC_LIBS} ${NETSNMP_LIBRARIES})

INSTALL (
  TARGETS load_brm
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
