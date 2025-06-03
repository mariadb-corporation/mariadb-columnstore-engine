include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(columnstoreDBWrite_SRCS columnstoreDB.cpp)

columnstore_executable(columnstoreDBWrite ${columnstoreDBWrite_SRCS})

columnstore_link(columnstoreDBWrite ${ENGINE_LDFLAGS} ncurses ${ENGINE_EXEC_LIBS})
