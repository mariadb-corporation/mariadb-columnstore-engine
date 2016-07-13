
include_directories( ${ENGINE_COMMON_INCLUDES} )



########### next target ###############

set(loggingcpp_LIB_SRCS
    message.cpp
    messagelog.cpp
    logger.cpp
    errorcodes.cpp
    sqllogger.cpp
    stopwatch.cpp
    idberrorinfo.cpp)

add_library(loggingcpp SHARED ${loggingcpp_LIB_SRCS})

set_target_properties(loggingcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS loggingcpp DESTINATION ${ENGINE_LIBDIR})

install(FILES MessageFile.txt ErrorMessage.txt DESTINATION ${ENGINE_ETCDIR})


#
#
# TODO BEN FIX ME
#
#
execute_process(
  COMMAND ./genMsgId.pl > messageids-temp.h
  COMMAND diff -abBq messageids-temp.h messageids.h >/dev/null 2>&1;
   if [ $$? -ne 0 ]; then
       mv -f messageids-temp.h messageids.h;
   else
       touch messageids.h;
   fi;
   rm -f messageids-temp.h
)

execute_process(
  COMMAND ./genErrId.pl > errorids-temp.h
  COMMAND diff -abBq errorids-temp.h errorids.h >/dev/null 2>&1;
   if [ $$? -ne 0 ]; then
       mv -f errorids-temp.h errorids.h;
   else
       touch errorids.h;
   fi;
   rm -f errorids-temp.h
)


########### install files ###############

#install(FILES  loggingid.h messageobj.h messagelog.h messageids.h logger.h errorcodes.h exceptclasses.h sqllogger.h stopwatch.h idberrorinfo.h errorids.h DESTINATION include)
#install(FILES  MessageFile.txt ErrorMessage.txt DESTINATION )
