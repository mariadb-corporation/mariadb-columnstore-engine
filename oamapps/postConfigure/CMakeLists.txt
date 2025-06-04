include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(mycnfUpgrade_SRCS mycnfUpgrade.cpp)

columnstore_executable(mycnfUpgrade ${mycnfUpgrade_SRCS})

columnstore_link(mycnfUpgrade ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${ENGINE_EXEC_LIBS})
