include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(ddlcleanup_SRCS ddlcleanup.cpp)

columnstore_executable(ddlcleanup ${ddlcleanup_SRCS})
columnstore_link(ddlcleanup ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ddlcleanuputil)
