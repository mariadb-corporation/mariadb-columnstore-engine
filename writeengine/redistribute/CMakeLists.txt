
add_library(writeengineredistribute SHARED 
    we_redistribute.cpp
    we_redistributecontrol.cpp
    we_redistributecontrolthread.cpp
    we_redistributeworkerthread.cpp)


target_link_libraries(writeengineredistribute 
  PRIVATE messageqcpp_h
          loggingcpp_h
          oamcpp_h
          configcpp_h
          alarmmanager_h
          brm_h
          idbboot_h
          joblist_h
          common_h
          we_server_h
          idbdatafile_h
          we_shared_h
          compress_h)

set_target_properties(writeengineredistribute PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_compile_definitions(writeengineredistribute PRIVATE BOOST_NO_CXX11_SCOPED_ENUMS)


install(TARGETS writeengineredistribute DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

