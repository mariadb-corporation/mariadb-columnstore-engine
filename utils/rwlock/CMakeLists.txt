INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (rwlock_LIB_SRCS rwlock.cpp rwlock_local.cpp)

ADD_LIBRARY (rwlock SHARED ${rwlock_LIB_SRCS})
ADD_DEPENDENCIES (rwlock external_boost)

INSTALL (
  TARGETS rwlock
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
