
# cleartablelock 

add_executable(cleartablelock cleartablelock.cpp cleartablelockthread.cpp)

target_link_libraries(cleartablelock 
    PRIVATE alarmmanager_h
            Boost::system
            brm
            common_h
            compress_h
            configcpp_h
            execplan
            idbdatafile_h 
            joblist_h
            loggingcpp
            messageqcpp
            oamcpp
            rwlock_h
            we_server_h
            writeengine_h
            we_shared_h
            )

target_pch(cleartablelock)

install(TARGETS cleartablelock DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

