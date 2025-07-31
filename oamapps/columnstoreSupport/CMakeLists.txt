include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(columnstoreSupport_SRCS columnstoreSupport.cpp mcsSupportUtil.cpp)

columnstore_executable(columnstoreSupport ${columnstoreSupport_SRCS})
target_compile_options(columnstoreSupport PRIVATE -Wno-unused-result)
columnstore_link(columnstoreSupport ${ENGINE_LDFLAGS} ncurses ${ENGINE_EXEC_LIBS})

columnstore_install_program(dbmsReport.sh, ${ENGINE_BINDIR})
columnstore_install_program(bulklogReport.sh, ${ENGINE_BINDIR})
columnstore_install_program(configReport.sh, ${ENGINE_BINDIR})
columnstore_install_program(hardwareReport.sh, ${ENGINE_BINDIR})
columnstore_install_program(logReport.sh, ${ENGINE_BINDIR})
columnstore_install_program(resourceReport.sh, ${ENGINE_BINDIR})
