INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  messageqcpp_LIB_SRCS
  messagequeue.cpp
  messagequeuepool.cpp
  bytestream.cpp
  socketparms.cpp
  inetstreamsocket.cpp
  samenodepseudosocket.cpp
  iosocket.cpp
  compressed_iss.cpp
  bytestreampool.cpp
)

ADD_LIBRARY (messageqcpp STATIC ${messageqcpp_LIB_SRCS})

ADD_DEPENDENCIES (messageqcpp loggingcpp)

# We don't isntall static library install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
