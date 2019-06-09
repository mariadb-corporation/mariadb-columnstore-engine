

add_executable(mcsadmin mcsadmin.cpp)

target_link_libraries(mcsadmin 
  PRIVATE alarmmanager_h
          Boost::system
          Boost::thread
          brm
          columnstore_h
          common_h
          idbboot_h
          joblist_h
          we_server_h
          we_shared_h
          writeengine
          Readline::Readline
          )

install(TARGETS mcsadmin DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

