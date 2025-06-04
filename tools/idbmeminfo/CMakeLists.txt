include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(idbmeminfo_SRCS idbmeminfo.cpp)

columnstore_executable(idbmeminfo ${idbmeminfo_SRCS})
columnstore_link(idbmeminfo ${ENGINE_LDFLAGS})
