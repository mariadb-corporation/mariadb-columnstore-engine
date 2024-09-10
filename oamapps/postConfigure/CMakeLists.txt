INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (mycnfUpgrade_SRCS mycnfUpgrade.cpp)

ADD_EXECUTABLE (mycnfUpgrade ${mycnfUpgrade_SRCS})

TARGET_COMPILE_OPTIONS (mycnfUpgrade PRIVATE -Wno-unused-result)

TARGET_LINK_LIBRARIES (mycnfUpgrade ${ENGINE_LDFLAGS} ${ENGINE_READLINE_LIBRARY} ncurses ${ENGINE_EXEC_LIBS})

INSTALL (
  TARGETS mycnfUpgrade
  DESTINATION ${ENGINE_BINDIR}
  COMPONENT columnstore-engine
)
