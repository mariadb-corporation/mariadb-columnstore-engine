IF(RPM)

SET(CMAKE_INSTALL_PREFIX ${INSTALL_ENGINE})

SET(CPACK_GENERATOR "RPM")
SET(CPACK_RPM_PACKAGE_DEBUG 1)
SET(CPACK_PACKAGING_INSTALL_PREFIX ${INSTALL_ENGINE})
CMAKE_MINIMUM_REQUIRED(VERSION 2.8.7)

SET(CPACK_RPM_COMPONENT_INSTALL ON)

SET(CPACK_COMPONENTS_ALL platform libs storage-engine)

SET(CPACK_PACKAGE_NAME "mariadb-columnstore")
SET(ENGINE_ARCH "x86_64")

IF (NOT CPACK_RPM_PACKAGE_VERSION)
SET (CPACK_RPM_PACKAGE_VERSION "1.0.0")
ENDIF()
IF (NOT CPACK_RPM_PACKAGE_RELEASE)
SET (CPACK_RPM_PACKAGE_RELEASE "0")
ENDIF()

SET(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_RPM_PACKAGE_VERSION}-${CPACK_RPM_PACKAGE_RELEASE}-${ENGINE_ARCH}-${RPM}")

SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "MariaDB Columnstore: a very fast and robust SQL database server")
SET(CPACK_PACKAGE_URL "http://mariadb.org")

SET(CPACK_PACKAGE_SUMMARY "MariaDB-Columnstore software")
SET(CPACK_PACKAGE_VENDOR "MariaDB Corporation Ab")
SET(CPACK_PACKAGE_LICENSE "Copyright (c) 2016 MariaDB Corporation Ab., all rights reserved; redistributable under the terms of the GPL, see the file COPYING for details.")


SET(CPACK_RPM_PACKAGE_LICENSE "GPLv2")
SET(CPACK_RPM_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_PACKAGE_RELOCATABLE FALSE)
SET(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
SET(CPACK_RPM_PACKAGE_URL ${CPACK_PACKAGE_URL})
SET(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_SUMMARY})
SET(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
SET(CPACK_RPM_PACKAGE_LICENSE ${CPACK_PACKAGE_LICENSE})

SET(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}

It is GPL v2 licensed, which means you can use the it free of charge under the
conditions of the GNU General Public License Version 2 (http://www.gnu.org/licenses/).

MariaDB documentation can be found at https://mariadb.com/kb
MariaDB bug reports should be submitted through https://jira.mariadb.org 

")

SET(CPACK_RPM_platform_PACKAGE_DESCRIPTION "MariaDB-Columnstore binary files")
SET(CPACK_RPM_platform_PACKAGE_SUMMARY "MariaDB-Columnstore software binaries")
SET(CPACK_RPM_platform_PACKAGE_GROUP "Applications")

SET(CPACK_RPM_libs_PACKAGE_DESCRIPTION "MariaDB-Columnstore libraries")
SET(CPACK_RPM_libs_PACKAGE_SUMMARY "MariaDB-Columnstore software libraries")

SET(CPACK_RPM_storage-engine_PACKAGE_DESCRIPTION "MariaDB Columnstore connector binary files")
SET(CPACK_RPM_storage-engine_PACKAGE_SUMMARY "MariaDB-Columnstore software MariaDB connector")
SET(CPACK_RPM_storage-engine_PACKAGE_GROUP "Applications")

# "set/append array" - append a set of strings, separated by a space
MACRO(SETA var)
  FOREACH(v ${ARGN})
    SET(${var} "${${var}} ${v}")
  ENDFOREACH()
ENDMACRO(SETA)

SETA(CPACK_RPM_libs_PACKAGE_PROVIDES "mariadb-columnstore-libs")
SETA(CPACK_RPM_platform_PACKAGE_PROVIDES "mariadb-columnstore-platform")
SETA(CPACK_RPM_storage-engine_PACKAGE_PROVIDES "mariadb-columnstore-storage-engine")

# Boost is a source build in CentOS 6 so don't require it as a package
SET(REDHAT_VERSION_NUMBER OFF)
IF (EXISTS "/etc/redhat-release")
    file (READ "/etc/redhat-release" REDHAT_VERSION)
    string(REGEX MATCH "release ([0-9]+)" CENTOS "${REDHAT_VERSION}")
    set(REDHAT_VERSION_NUMBER "${CMAKE_MATCH_1}")
ENDIF ()
if (${REDHAT_VERSION_NUMBER} EQUAL 6)
    SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "mariadb-columnstore-libs" "net-snmp-libs")
    # Disable auto require as this will also try to pull Boost via RPM
    SET(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
else ()
    SETA(CPACK_RPM_platform_PACKAGE_REQUIRES "expect" "boost >= 1.53.0" "mariadb-columnstore-libs" "net-snmp-libs")
endif()

SETA(CPACK_RPM_storage-engine_PACKAGE_REQUIRES "mariadb-columnstore-libs")

SET(CPACK_RPM_platform_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_platform.sh)
SET(CPACK_RPM_libs_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_libs.sh)
SET(CPACK_RPM_storage-engine_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/postInstall_storage_engine.sh)

SET(CPACK_RPM_platform_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_platform.sh)
SET(CPACK_RPM_libs_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_libs.sh)
SET(CPACK_RPM_storage-engine_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/build/preUn_storage_engine.sh)

SET(CPACK_RPM_SPEC_MORE_DEFINE "${CPACK_RPM_SPEC_MORE_DEFINE}
%define ignore \#
")

SET(ignored
    "%ignore /usr"
    "%ignore /usr/local"
)

#SET(CPACK_RPM_SPEC_MORE_DEFINE "
#%define _prefix ${CMAKE_INSTALL_PREFIX}
#")

SET(CPACK_RPM_platform_USER_FILELIST
"/usr/local/mariadb/columnstore/bin/DDLProc"
"/usr/local/mariadb/columnstore/bin/ExeMgr"
"/usr/local/mariadb/columnstore/bin/ProcMgr"
"/usr/local/mariadb/columnstore/bin/ProcMon"
"/usr/local/mariadb/columnstore/bin/DMLProc"
"/usr/local/mariadb/columnstore/bin/WriteEngineServer"
"/usr/local/mariadb/columnstore/bin/cpimport"
"/usr/local/mariadb/columnstore/bin/post-install"
"/usr/local/mariadb/columnstore/bin/post-mysql-install"
"/usr/local/mariadb/columnstore/bin/post-mysqld-install"
"/usr/local/mariadb/columnstore/bin/pre-uninstall"
"/usr/local/mariadb/columnstore/bin/PrimProc"
"/usr/local/mariadb/columnstore/bin/DecomSvr"
"/usr/local/mariadb/columnstore/bin/upgrade-columnstore.sh"
"/usr/local/mariadb/columnstore/bin/run.sh"
"/usr/local/mariadb/columnstore/bin/columnstore"
"/usr/local/mariadb/columnstore/bin/columnstoreSyslog"
"/usr/local/mariadb/columnstore/bin/columnstoreSyslog7"
"/usr/local/mariadb/columnstore/bin/columnstoreSyslog-ng" 
"/usr/local/mariadb/columnstore/bin/syslogSetup.sh"
"/usr/local/mariadb/columnstore/bin/cplogger"
"/usr/local/mariadb/columnstore/bin/columnstore.def"
"/usr/local/mariadb/columnstore/bin/dbbuilder"
"/usr/local/mariadb/columnstore/bin/cpimport.bin"
"/usr/local/mariadb/columnstore/bin/load_brm"
"/usr/local/mariadb/columnstore/bin/save_brm"
"/usr/local/mariadb/columnstore/bin/dbrmctl"
"/usr/local/mariadb/columnstore/bin/controllernode"
"/usr/local/mariadb/columnstore/bin/reset_locks"
"/usr/local/mariadb/columnstore/bin/workernode"
"/usr/local/mariadb/columnstore/bin/colxml"
"/usr/local/mariadb/columnstore/bin/clearShm"
"/usr/local/mariadb/columnstore/bin/viewtablelock"
"/usr/local/mariadb/columnstore/bin/cleartablelock"
"/usr/local/mariadb/columnstore/bin/mcsadmin"
"/usr/local/mariadb/columnstore/bin/remote_command.sh"
"/usr/local/mariadb/columnstore/bin/postConfigure"
"/usr/local/mariadb/columnstore/bin/columnstoreLogRotate"
"/usr/local/mariadb/columnstore/bin/transactionLog"
"/usr/local/mariadb/columnstore/bin/columnstoreDBWrite"
"/usr/local/mariadb/columnstore/bin/transactionLogArchiver.sh"
"/usr/local/mariadb/columnstore/bin/installer"
"/usr/local/mariadb/columnstore/bin/module_installer.sh"
"/usr/local/mariadb/columnstore/bin/user_installer.sh"
"/usr/local/mariadb/columnstore/bin/performance_installer.sh"
"/usr/local/mariadb/columnstore/bin/startupTests.sh"
"/usr/local/mariadb/columnstore/bin/os_check.sh"
"/usr/local/mariadb/columnstore/bin/remote_scp_put.sh"
"/usr/local/mariadb/columnstore/bin/remotessh.exp"
"/usr/local/mariadb/columnstore/bin/ServerMonitor"
"/usr/local/mariadb/columnstore/bin/master-rep-columnstore.sh" 
"/usr/local/mariadb/columnstore/bin/slave-rep-columnstore.sh"
"/usr/local/mariadb/columnstore/bin/rsync.sh"
"/usr/local/mariadb/columnstore/bin/columnstoreSupport"
"/usr/local/mariadb/columnstore/bin/hardwareReport.sh"
"/usr/local/mariadb/columnstore/bin/softwareReport.sh"
"/usr/local/mariadb/columnstore/bin/configReport.sh"
"/usr/local/mariadb/columnstore/bin/logReport.sh"
"/usr/local/mariadb/columnstore/bin/bulklogReport.sh"
"/usr/local/mariadb/columnstore/bin/resourceReport.sh"
"/usr/local/mariadb/columnstore/bin/hadoopReport.sh"
"/usr/local/mariadb/columnstore/bin/alarmReport.sh"
"/usr/local/mariadb/columnstore/bin/amazonInstaller"
"/usr/local/mariadb/columnstore/bin/remote_command_verify.sh"
"/usr/local/mariadb/columnstore/bin/disable-rep-columnstore.sh"
"/usr/local/mariadb/columnstore/bin/columnstore.service"
"/usr/local/mariadb/columnstore/etc/MessageFile.txt"
"/usr/local/mariadb/columnstore/etc/ErrorMessage.txt"
"/usr/local/mariadb/columnstore/local/module"
"/usr/local/mariadb/columnstore/releasenum"
"/usr/local/mariadb/columnstore/bin/rollback"
"/usr/local/mariadb/columnstore/bin/editem"
"/usr/local/mariadb/columnstore/bin/getConfig"
"/usr/local/mariadb/columnstore/bin/setConfig"
"/usr/local/mariadb/columnstore/bin/setenv-hdfs-12"
"/usr/local/mariadb/columnstore/bin/setenv-hdfs-20"
"/usr/local/mariadb/columnstore/bin/configxml.sh"
"/usr/local/mariadb/columnstore/bin/remote_scp_get.sh"
"/usr/local/mariadb/columnstore/bin/columnstoreAlias"
"/usr/local/mariadb/columnstore/bin/autoConfigure"
"/usr/local/mariadb/columnstore/bin/ddlcleanup"
"/usr/local/mariadb/columnstore/bin/idbmeminfo"
"/usr/local/mariadb/columnstore/bin/IDBInstanceCmds.sh"
"/usr/local/mariadb/columnstore/bin/IDBVolumeCmds.sh"
"/usr/local/mariadb/columnstore/bin/binary_installer.sh" 
"/usr/local/mariadb/columnstore/bin/myCnf-include-args.text" 
"/usr/local/mariadb/columnstore/bin/myCnf-exclude-args.text"
"/usr/local/mariadb/columnstore/bin/mycnfUpgrade"
"/usr/local/mariadb/columnstore/bin/getMySQLpw"
"/usr/local/mariadb/columnstore/bin/columnstore.conf"
"/usr/local/mariadb/columnstore/share/snmp/mibs/AGENTX-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/CALPONT-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/DISMAN-EVENT-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/DISMAN-SCHEDULE-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/DISMAN-SCRIPT-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/EtherLike-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/HCNUM-TC.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/HOST-RESOURCES-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/HOST-RESOURCES-TYPES.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IANA-ADDRESS-FAMILY-NUMBERS-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IANAifType-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IANA-LANGUAGE-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IANA-RTPROTO-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IF-INVERTED-STACK-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IF-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/INET-ADDRESS-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IP-FORWARD-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IPV6-ICMP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IPV6-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IPV6-TCP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IPV6-TC.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/IPV6-UDP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NET-SNMP-AGENT-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NET-SNMP-EXAMPLES-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NET-SNMP-EXTEND-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NET-SNMP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NET-SNMP-TC.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/NOTIFICATION-LOG-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/RFC1155-SMI.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/RFC1213-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/RFC-1215.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/RMON-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SMUX-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-COMMUNITY-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-FRAMEWORK-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-MPD-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-NOTIFICATION-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-PROXY-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-TARGET-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-USER-BASED-SM-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-USM-AES-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-USM-DH-OBJECTS-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMPv2-CONF.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMPv2-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMPv2-SMI.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMPv2-TC.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMPv2-TM.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/SNMP-VIEW-BASED-ACM-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/TCP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/TRANSPORT-ADDRESS-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UCD-DEMO-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UCD-DISKIO-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UCD-DLMOD-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UCD-IPFWACC-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UCD-SNMP-MIB.txt"
"/usr/local/mariadb/columnstore/share/snmp/mibs/UDP-MIB.txt"
"/usr/local/mariadb/columnstore/post/functions"
"/usr/local/mariadb/columnstore/post/test-001.sh"
"/usr/local/mariadb/columnstore/post/test-002.sh"
"/usr/local/mariadb/columnstore/post/test-003.sh"
"/usr/local/mariadb/columnstore/post/test-004.sh"
${ignored})

SET(CPACK_RPM_libs_USER_FILELIST 
"/usr/local/mariadb/columnstore/lib/libconfigcpp.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libconfigcpp.so.1"
"/usr/local/mariadb/columnstore/lib/libconfigcpp.so"
"/usr/local/mariadb/columnstore/lib/libddlpackageproc.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libddlpackageproc.so.1"
"/usr/local/mariadb/columnstore/lib/libddlpackageproc.so"
"/usr/local/mariadb/columnstore/lib/libddlpackage.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libddlpackage.so.1"
"/usr/local/mariadb/columnstore/lib/libddlpackage.so"
"/usr/local/mariadb/columnstore/lib/libdmlpackageproc.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libdmlpackageproc.so.1"
"/usr/local/mariadb/columnstore/lib/libdmlpackageproc.so"
"/usr/local/mariadb/columnstore/lib/libdmlpackage.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libdmlpackage.so.1"
"/usr/local/mariadb/columnstore/lib/libdmlpackage.so"
"/usr/local/mariadb/columnstore/lib/libexecplan.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libexecplan.so.1"
"/usr/local/mariadb/columnstore/lib/libexecplan.so"
"/usr/local/mariadb/columnstore/lib/libfuncexp.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libfuncexp.so.1"
"/usr/local/mariadb/columnstore/lib/libfuncexp.so"
"/usr/local/mariadb/columnstore/lib/libudfsdk.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libudfsdk.so.1"
"/usr/local/mariadb/columnstore/lib/libudfsdk.so"
"/usr/local/mariadb/columnstore/lib/libjoblist.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libjoblist.so.1"
"/usr/local/mariadb/columnstore/lib/libjoblist.so"
"/usr/local/mariadb/columnstore/lib/libjoiner.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libjoiner.so.1"
"/usr/local/mariadb/columnstore/lib/libjoiner.so"
"/usr/local/mariadb/columnstore/lib/libloggingcpp.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libloggingcpp.so.1"
"/usr/local/mariadb/columnstore/lib/libloggingcpp.so"
"/usr/local/mariadb/columnstore/lib/libmessageqcpp.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libmessageqcpp.so.1"
"/usr/local/mariadb/columnstore/lib/libmessageqcpp.so"
"/usr/local/mariadb/columnstore/lib/liboamcpp.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/liboamcpp.so.1"
"/usr/local/mariadb/columnstore/lib/liboamcpp.so"
"/usr/local/mariadb/columnstore/lib/libsnmpmanager.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libsnmpmanager.so.1"
"/usr/local/mariadb/columnstore/lib/libsnmpmanager.so"
"/usr/local/mariadb/columnstore/lib/libthreadpool.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libthreadpool.so.1"
"/usr/local/mariadb/columnstore/lib/libthreadpool.so"
"/usr/local/mariadb/columnstore/lib/libwindowfunction.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libwindowfunction.so.1"
"/usr/local/mariadb/columnstore/lib/libwindowfunction.so"
"/usr/local/mariadb/columnstore/lib/libwriteengine.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libwriteengine.so.1"
"/usr/local/mariadb/columnstore/lib/libwriteengine.so"
"/usr/local/mariadb/columnstore/lib/libwriteengineclient.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libwriteengineclient.so.1"
"/usr/local/mariadb/columnstore/lib/libwriteengineclient.so"
"/usr/local/mariadb/columnstore/lib/libbrm.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libbrm.so.1"
"/usr/local/mariadb/columnstore/lib/libbrm.so"
"/usr/local/mariadb/columnstore/lib/librwlock.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/librwlock.so.1"
"/usr/local/mariadb/columnstore/lib/librwlock.so"
"/usr/local/mariadb/columnstore/lib/libdataconvert.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libdataconvert.so.1"
"/usr/local/mariadb/columnstore/lib/libdataconvert.so"
"/usr/local/mariadb/columnstore/lib/librowgroup.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/librowgroup.so.1"
"/usr/local/mariadb/columnstore/lib/librowgroup.so"
"/usr/local/mariadb/columnstore/lib/libcacheutils.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libcacheutils.so.1"
"/usr/local/mariadb/columnstore/lib/libcacheutils.so"
"/usr/local/mariadb/columnstore/lib/libcommon.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libcommon.so.1"
"/usr/local/mariadb/columnstore/lib/libcommon.so"
"/usr/local/mariadb/columnstore/lib/libcompress.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libcompress.so.1"
"/usr/local/mariadb/columnstore/lib/libcompress.so"
"/usr/local/mariadb/columnstore/lib/libddlcleanuputil.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libddlcleanuputil.so.1"
"/usr/local/mariadb/columnstore/lib/libddlcleanuputil.so"
"/usr/local/mariadb/columnstore/lib/libbatchloader.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libbatchloader.so.1"
"/usr/local/mariadb/columnstore/lib/libbatchloader.so"
"/usr/local/mariadb/columnstore/lib/libmysqlcl_idb.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libmysqlcl_idb.so.1"
"/usr/local/mariadb/columnstore/lib/libmysqlcl_idb.so"
"/usr/local/mariadb/columnstore/lib/libquerystats.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libquerystats.so.1"
"/usr/local/mariadb/columnstore/lib/libquerystats.so"
"/usr/local/mariadb/columnstore/lib/libwriteengineredistribute.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libwriteengineredistribute.so.1"
"/usr/local/mariadb/columnstore/lib/libwriteengineredistribute.so"
"/usr/local/mariadb/columnstore/lib/libidbdatafile.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libidbdatafile.so.1"
"/usr/local/mariadb/columnstore/lib/libidbdatafile.so"
"/usr/local/mariadb/columnstore/lib/libthrift.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libthrift.so.1"
"/usr/local/mariadb/columnstore/lib/libthrift.so"
"/usr/local/mariadb/columnstore/lib/libquerytele.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libquerytele.so.1"
"/usr/local/mariadb/columnstore/lib/libquerytele.so"
${ignored})

SET(CPACK_RPM_storage-engine_USER_FILELIST
"/usr/local/mariadb/columnstore/lib/libcalmysql.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libcalmysql.so.1"
"/usr/local/mariadb/columnstore/lib/libcalmysql.so"
"/usr/local/mariadb/columnstore/lib/libudf_mysql.so.1.0.0"
"/usr/local/mariadb/columnstore/lib/libudf_mysql.so.1"
"/usr/local/mariadb/columnstore/lib/libudf_mysql.so"
"/usr/local/mariadb/columnstore/mysql/mysql-Columnstore"
"/usr/local/mariadb/columnstore/mysql/install_calpont_mysql.sh"
"/usr/local/mariadb/columnstore/mysql/syscatalog_mysql.sql"
"/usr/local/mariadb/columnstore/mysql/dumpcat_mysql.sql"
"/usr/local/mariadb/columnstore/mysql/dumpcat.pl"
"/usr/local/mariadb/columnstore/mysql/calsetuserpriority.sql"
"/usr/local/mariadb/columnstore/mysql/calremoveuserpriority.sql"
"/usr/local/mariadb/columnstore/mysql/calshowprocesslist.sql"
${ignored})


INCLUDE (CPack)

ENDIF()
