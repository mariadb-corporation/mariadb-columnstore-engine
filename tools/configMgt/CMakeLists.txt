
# autoInstaller 

add_executable(autoInstaller autoInstaller.cpp)

target_link_libraries(autoInstaller 
  PRIVATE alarmmanager_h
          Boost::system
          brm_h
          common_h
          configcpp
          execplan_h
          idbboot
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp
          Readline::Readline
          rwlock_h
          Boost::filesystem
          )

install(TARGETS autoInstaller DESTINATION ${ENGINE_BINDIR})



# autoConfigure

add_executable(autoConfigure autoConfigure.cpp)

target_link_libraries(autoConfigure 
  PRIVATE alarmmanager_h
          brm
          common_h
          idbboot
          joblist_h
          Boost::system
          Boost::filesystem
          )

install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
                                                                                                                 