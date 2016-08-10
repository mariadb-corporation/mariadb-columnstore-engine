
install(PROGRAMS post-install 
                pre-uninstall 
                remote_command.sh  
                columnstoreLogRotate 
                transactionLog 
                transactionLogArchiver.sh  
                module_installer.sh 
                startupTests.sh  
                user_installer.sh 
                performance_installer.sh  
                remote_scp_get.sh 
                columnstoreAlias 
                run.sh 
                upgrade-columnstore.sh  
                post-mysql-install 
                post-mysqld-install 
                binary_installer.sh  
                os_check.sh 
                columnstore 
                columnstoreSyslog 
                columnstoreSyslog-ng 
                syslogSetup.sh  
                remote_scp_put.sh 
                columnstoreUninstall.sh 
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
                DESTINATION ${ENGINE_BINDIR})
                
install(FILES  module DESTINATION ${ENGINE_LOCALDIR})

