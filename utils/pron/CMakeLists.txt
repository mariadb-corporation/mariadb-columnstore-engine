INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})
SET (pron_LIB_SRCS pron.cpp)
ADD_LIBRARY (pron STATIC ${pron_LIB_SRCS})
TARGET_LINK_LIBRARIES (pron messageqcpp loggingcpp)

# We don't isntall static library install(TARGETS pron DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
