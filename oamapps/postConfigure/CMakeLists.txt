# postConfigure
add_executable(postConfigure postConfigure.cpp helpers.cpp)

target_link_libraries(postConfigure 
  PRIVATE alarmmanager_h
          brm_h 
          columnstore_h
          common_h
          configcpp_h         
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h          
          oamcpp_h
          Readline::Readline)



# installer
add_executable(installer installer.cpp helpers.cpp)

target_link_libraries(installer 
  PRIVATE alarmmanager_h
          brm_h 
          common_h
          configcpp_h         
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h          
          oamcpp_h
          Readline::Readline)



# getMySQLpw 
add_executable(getMySQLpw getMySQLpw.cpp)

target_link_libraries(getMySQLpw 
  PRIVATE brm
          oamcpp
          alarmmanager_h
          common_h
          configcpp_h
          idbboot_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          )



#[[

add_executable(amazonInstaller amazonInstaller.cpp helpers.cpp)

target_link_libraries(amazonInstaller ${ENGINE_LDFLAGS} readline ncurses ${SNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS amazonInstaller DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
]]


# mycnfUpgrade
add_executable(mycnfUpgrade mycnfUpgrade.cpp)

target_link_libraries(mycnfUpgrade 
  PRIVATE alarmmanager_h
          brm_h
          configcpp_h
          joblist_h
          loggingcpp_h
          messageqcpp_h
          oamcpp_h )



# install
install(
  TARGETS installer 
          getMySQLpw 
          mycnfUpgrade 
          postConfigure 
  DESTINATION ${ENGINE_BINDIR} 
  COMPONENT platform)


install(
  PROGRAMS quick_installer_single_server.sh 
           quick_installer_multi_server.sh 
           quick_installer_amazon.sh
  DESTINATION ${ENGINE_BINDIR} 
  COMPONENT platform)               
