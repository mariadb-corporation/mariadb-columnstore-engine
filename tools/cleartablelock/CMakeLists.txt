
# cleartablelock 

add_executable(cleartablelock cleartablelock.cpp cleartablelockthread.cpp)

target_link_libraries(cleartablelock 
    PRIVATE alarmmanager_h
            brm_h 
            common_h
            compress_h
            configcpp_h
            execplan_h
            idbdatafile_h 
            joblist_h
            loggingcpp_h
            messageqcpp_h
            oamcpp_h 
            we_wrapper_h 
            we_shared_h 
            we_server_h
            Boost::system
            )

target_pch(cleartablelock)

install(TARGETS cleartablelock DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

