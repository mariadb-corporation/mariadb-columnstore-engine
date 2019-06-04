# postConfigure
add_executable(postConfigure postConfigure.cpp helpers.cpp)

target_link_libraries(postConfigure 
  PRIVATE 
    brm
    Readline::Readline
    )



# installer
add_executable(installer installer.cpp helpers.cpp)

target_link_libraries(installer 
  PRIVATE 
    alarmmanager_h
    brm
    common_h
    configcpp
    execplan_h
    idbboot_h
    joblist_h
    loggingcpp_h
    messageqcpp
    oamcpp
    Readline::Readline
    rwlock_h
    Boost::system
    )



# getMySQLpw 
add_executable(getMySQLpw getMySQLpw.cpp)

target_link_libraries(getMySQLpw 
  PRIVATE
    oamcpp
    messageqcpp_h
    configcpp
    alarmmanager_h
    brm_h
    execplan_h
    joblist_h
    loggingcpp_h
    rwlock_h
    common_h
    idbboot
    Boost::system
    Boost::filesystem
    )



#[[

add_executable(amazonInstaller amazonInstaller.cpp helpers.cpp)

target_link_libraries(amazonInstaller ${ENGINE_LDFLAGS} readline ncurses ${SNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS amazonInstaller DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
]]


# mycnfUpgrade
add_executable(mycnfUpgrade mycnfUpgrade.cpp)

target_link_libraries(mycnfUpgrade 
  PRIVATE 
    alarmmanager_h
    brm_h
    common_h
    execplan_h
    idbboot
    configcpp
    joblist_h
    loggingcpp_h
    messageqcpp_h
    oamcpp
    rwlock_h
    Boost::system
    Boost::regex
    Boost::filesystem
    )



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
