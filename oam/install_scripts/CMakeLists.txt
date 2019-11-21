configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslogSetup.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreSyslogSetup.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstore-post-install.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-post-install" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstore.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstore-pre-uninstall.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore-pre-uninstall" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstoreLogRotate.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstoreLogRotate" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/postInstall.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/postInstall.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/startupTests.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/startupTests.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstore.service.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore.service" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/columnstore_module_installer.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/columnstore_module_installer.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/master-rep-columnstore.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/master-rep-columnstore.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/disable-rep-columnstore.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/disable-rep-columnstore.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mariadb-command-line.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mariadb-command-line.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/slave-rep-columnstore.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/slave-rep-columnstore.sh" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/mcs_module_installer.sh.in" "${CMAKE_CURRENT_SOURCE_DIR}/mcs_module_installer.sh" @ONLY)

install(PROGRAMS columnstore-post-install 
                columnstore-pre-uninstall 
                remote_command.sh  
                columnstore_module_installer.sh 
                startupTests.sh  
                remote_scp_get.sh 
                columnstore_run.sh 
                post-mysql-install 
                post-mysqld-install 
                columnstore_os_check.sh 
                columnstore 
                columnstoreSyslogSetup.sh  
                remote_scp_put.sh 
                remotessh.exp 
                rsync.sh 
                remote_command_verify.sh  
                master-rep-columnstore.sh  
                slave-rep-columnstore.sh 
                disable-rep-columnstore.sh  
                mariadb-command-line.sh 
                mcs_module_installer.sh
                DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(FILES columnstore.service
              columnstoreAlias
              columnstoreSyslog
              columnstoreSyslog7
              columnstoreSyslog-ng
              columnstoreLogRotate
              myCnf-include-args.text
              myCnf-exclude-args.text
              DESTINATION ${ENGINE_SUPPORTDIR} COMPONENT platform)

          install(FILES  module DESTINATION ${ENGINE_DATADIR}/local COMPONENT platform)

