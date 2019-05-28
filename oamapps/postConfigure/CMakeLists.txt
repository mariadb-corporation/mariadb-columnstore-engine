# postConfigure
cs_add_executable(postConfigure postConfigure.cpp helpers.cpp)

target_link_libraries(postConfigure 
  PRIVATE 
    brm
    Readline::Readline
    )



# installer
cs_add_executable(installer installer.cpp helpers.cpp)

target_link_libraries(installer 
  PRIVATE 
    brm
    Readline::Readline
  )


# getMySQLpw 
cs_add_executable(getMySQLpw getMySQLpw.cpp)

target_link_libraries(getMySQLpw 
  PRIVATE
    brm
  )



#[[

cs_add_executable(amazonInstaller amazonInstaller.cpp helpers.cpp)

target_link_libraries(amazonInstaller ${ENGINE_LDFLAGS} readline ncurses ${SNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS amazonInstaller DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT platform)
]]


# mycnfUpgrade
cs_add_executable(mycnfUpgrade mycnfUpgrade.cpp)

target_link_libraries(mycnfUpgrade 
  PRIVATE 
    brm
    common_h
    idbboot
    joblist_h
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
  DESTINATION ${CMAKE_INSTALL_BINDIR} 
  COMPONENT platform)


install(
  PROGRAMS quick_installer_single_server.sh 
           quick_installer_multi_server.sh 
           quick_installer_amazon.sh
  DESTINATION ${CMAKE_INSTALL_BINDIR} 
  COMPONENT platform)               
