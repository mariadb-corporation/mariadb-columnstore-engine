
# autoInstaller 

add_executable(autoInstaller autoInstaller.cpp)

target_link_libraries(autoInstaller 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          Boost::system
          )

install(TARGETS autoInstaller DESTINATION ${ENGINE_BINDIR})



# autoConfigure

add_executable(autoConfigure autoConfigure.cpp)

target_link_libraries(autoConfigure 
  PRIVATE alarmmanager_h
          brm_h
          common_h
          configcpp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h
          Boost::system)

install(TARGETS autoConfigure DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
