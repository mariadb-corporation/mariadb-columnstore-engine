INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (rwlock_LIB_SRCS rwlock.cpp rwlock_local.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (rwlock STATIC ${rwlock_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (rwlock SHARED ${rwlock_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (rwlock external_boost)

INSTALL (
  TARGETS rwlock
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
