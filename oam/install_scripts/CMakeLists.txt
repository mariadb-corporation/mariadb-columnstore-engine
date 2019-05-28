


set(_scripts  post-install 
              pre-uninstall 
              remote_command.sh  
              columnstoreLogRotate 
              transactionLog 
              transactionLogArchiver.sh  
              module_installer.sh 
              startupTests.sh  
              package_installer.sh   
              remote_scp_get.sh 
              columnstoreAlias 
              run.sh 
              post-mysql-install 
              post-mysqld-install 
              binary_installer.sh  
              os_check.sh 
              columnstore 
              columnstoreSyslog 
              columnstoreSyslog-ng 
              syslogSetup.sh  
              remote_scp_put.sh 
              columnstore.def  
              remotessh.exp 
              rsync.sh 
              remote_command_verify.sh  
              columnstore.conf 
              columnstoreSyslog7 
              master-rep-columnstore.sh  
              slave-rep-columnstore.sh 
              disable-rep-columnstore.sh  
              myCnf-include-args.text 
              myCnf-exclude-args.text
              columnstore.service
              mariadb-command-line.sh )

foreach(_script ${_scripts})
  configure_file(${_script}.in  ${columnstore_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}/${_script} @ONLY)
  install(
    PROGRAMS ${columnstore_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}/${_script} 
    DESTINATION ${CMAKE_INSTALL_BINDIR} 
    COMPONENT platform
  )
endforeach()

                
install(
  FILES module 
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR} 
  COMPONENT platform)

