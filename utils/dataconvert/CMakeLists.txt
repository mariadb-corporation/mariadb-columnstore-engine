
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

add_library(dataconvert SHARED ${dataconvert_LIB_SRCS})

add_dependencies(dataconvert loggingcpp)

install(TARGETS dataconvert DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

if (WITH_DATACONVERT_UT)
    add_executable(dataconvert_tests dataconvert-tests.cpp)
    target_link_libraries(dataconvert_tests ${ENGINE_LDFLAGS} ${GTEST_LIBRARIES} ${ENGINE_EXEC_LIBS} ${MARIADB_CLIENT_LIBS}) 
    install(TARGETS dataconvert_tests DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
endif()
