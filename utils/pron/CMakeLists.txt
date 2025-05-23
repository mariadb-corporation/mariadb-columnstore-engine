include_directories(${ENGINE_COMMON_INCLUDES})
set(pron_LIB_SRCS pron.cpp)
add_library(pron STATIC ${pron_LIB_SRCS})
target_link_libraries(pron messageqcpp loggingcpp)

# We don't isntall static library install(TARGETS pron DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
