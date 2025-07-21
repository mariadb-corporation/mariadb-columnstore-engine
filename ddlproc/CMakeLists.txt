include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(DDLProc_SRCS ddlproc.cpp ddlprocessor.cpp ../utils/common/crashtrace.cpp)

columnstore_executable(DDLProc ${DDLProc_SRCS})

columnstore_link(DDLProc ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} threadpool loggingcpp)
