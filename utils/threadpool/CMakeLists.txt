INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp fair_threadpool.cpp)
ADD_LIBRARY (threadpool SHARED ${threadpool_LIB_SRCS})
ADD_DEPENDENCIES (threadpool loggingcpp external_boost)
TARGET_LINK_LIBRARIES (threadpool boost_chrono)
INSTALL (
  TARGETS threadpool
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
