
########### install files ###############

install(FILES  snmpd.conf 
                snmptrapd.conf 
                snmpd.conf.singleserver 
                snmptrapd.conf.singleserver 
                DESTINATION ${ENGINE_ETCDIR})
                
install(FILES  CALPONT-MIB.txt  	
                AGENTX-MIB.txt  	
                DISMAN-EVENT-MIB.txt  	
                DISMAN-SCHEDULE-MIB.txt  	
                DISMAN-SCRIPT-MIB.txt  	
                EtherLike-MIB.txt  	
                HCNUM-TC.txt  	
                HOST-RESOURCES-MIB.txt  	
                HOST-RESOURCES-TYPES.txt  	
                IANA-ADDRESS-FAMILY-NUMBERS-MIB.txt  	
                IANAifType-MIB.txt  	
                IANA-LANGUAGE-MIB.txt  	
                IANA-RTPROTO-MIB.txt  	
                IF-INVERTED-STACK-MIB.txt  	
                IF-MIB.txt  	
                INET-ADDRESS-MIB.txt  	
                IP-FORWARD-MIB.txt  	
                IP-MIB.txt  	
                IPV6-ICMP-MIB.txt  	
                IPV6-MIB.txt  	
                IPV6-TCP-MIB.txt  	
                IPV6-TC.txt  	
                IPV6-UDP-MIB.txt  	
                NET-SNMP-AGENT-MIB.txt  	
                NET-SNMP-EXAMPLES-MIB.txt  	
                NET-SNMP-EXTEND-MIB.txt  	
                NET-SNMP-MIB.txt  	
                NET-SNMP-TC.txt  	
                NOTIFICATION-LOG-MIB.txt  	
                RFC1155-SMI.txt  	
                RFC1213-MIB.txt  	
                RFC-1215.txt  	
                RMON-MIB.txt  	
                SMUX-MIB.txt  	
                SNMP-COMMUNITY-MIB.txt  	
                SNMP-FRAMEWORK-MIB.txt  	
                SNMP-MPD-MIB.txt  	
                SNMP-NOTIFICATION-MIB.txt  	
                SNMP-PROXY-MIB.txt  	
                SNMP-TARGET-MIB.txt  	
                SNMP-USER-BASED-SM-MIB.txt  	
                SNMP-USM-AES-MIB.txt  	
                SNMP-USM-DH-OBJECTS-MIB.txt  	
                SNMPv2-CONF.txt  	
                SNMPv2-MIB.txt  	
                SNMPv2-SMI.txt  	
                SNMPv2-TC.txt  	
                SNMPv2-TM.txt  	
                SNMP-VIEW-BASED-ACM-MIB.txt  	
                TCP-MIB.txt  	
                TRANSPORT-ADDRESS-MIB.txt  	
                UCD-DEMO-MIB.txt  	
                UCD-DISKIO-MIB.txt  	
                UCD-DLMOD-MIB.txt  	
                UCD-IPFWACC-MIB.txt  	
                UCD-SNMP-MIB.txt  	
                UDP-MIB.txt DESTINATION ${ENGINE_MIBDIR} )
                
install(FILES  snmpdx.conf snmpdx.conf.singleserver DESTINATION ${ENGINE_LOCALDIR})
