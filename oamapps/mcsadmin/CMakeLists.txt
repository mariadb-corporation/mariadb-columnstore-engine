

add_executable(mcsadmin mcsadmin.cpp)

target_link_libraries(mcsadmin 
  PRIVATE alarmmanager_h
          Boost::system
          Boost::thread
          brm_h
          columnstore_h
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
          we_server_h
          we_shared_h)

install(TARGETS mcsadmin DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

