
cs_add_library(writeengineredistribute SHARED 
    we_redistribute.cpp
    we_redistributecontrol.cpp
    we_redistributecontrolthread.cpp
    we_redistributeworkerthread.cpp)


target_link_libraries(writeengineredistribute 
  PUBLIC 
  PRIVATE
    alarmmanager_h
    brm_h
    common_h
    compress_h
    configcpp_h
    execplan_h
    idbboot_h
    idbdatafile_h
    joblist_h
    loggingcpp_h
    messageqcpp_h
    oamcpp_h
    rwlock_h
    we_server_h
    we_shared_h
    )

set_target_properties(writeengineredistribute PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_compile_definitions(writeengineredistribute PRIVATE BOOST_NO_CXX11_SCOPED_ENUMS)


install(TARGETS writeengineredistribute DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)

