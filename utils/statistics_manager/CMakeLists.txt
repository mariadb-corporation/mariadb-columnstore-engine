include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

columnstore_static_library(statistics_manager statistics.cpp)
columnstore_link(statistics_manager PRIVATE rowgroup)
