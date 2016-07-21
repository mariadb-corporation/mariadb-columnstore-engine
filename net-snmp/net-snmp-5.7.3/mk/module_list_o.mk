# contents below built automatically by configure; do not edit by hand
module_list_o= \
	snmpv3/snmpMPDStats_5_5.o \
	snmpv3/usmStats_5_5.o \
	snmpv3/snmpEngine.o \
	snmpv3/usmConf.o \
	snmpv3/usmUser.o \
	mibII/snmp_mib_5_5.o \
	mibII/system_mib.o \
	mibII/sysORTable.o \
	mibII/vacm_vars.o \
	mibII/setSerialNo.o \
	mibII/at.o \
	mibII/ip.o \
	mibII/tcp.o \
	mibII/icmp.o \
	mibII/udp.o \
	ucd-snmp/disk_hw.o \
	ucd-snmp/proc.o \
	ucd-snmp/versioninfo.o \
	ucd-snmp/pass.o \
	ucd-snmp/pass_persist.o \
	ucd-snmp/loadave.o \
	agent/extend.o \
	ucd-snmp/errormib.o \
	ucd-snmp/file.o \
	ucd-snmp/dlmod.o \
	ucd-snmp/proxy.o \
	ucd-snmp/logmatch.o \
	ucd-snmp/memory.o \
	ucd-snmp/vmstat.o \
	notification/snmpNotifyTable.o \
	notification/snmpNotifyFilterProfileTable.o \
	notification-log-mib/notification_log.o \
	target/target_counters_5_5.o \
	target/snmpTargetAddrEntry.o \
	target/snmpTargetParamsEntry.o \
	target/target.o \
	agent/nsTransactionTable.o \
	agent/nsModuleTable.o \
	agent/nsDebug.o \
	agent/nsCache.o \
	agent/nsLogging.o \
	agent/nsVacmAccessTable.o \
	agentx/master.o \
	agentx/subagent.o \
	disman/event/mteScalars.o \
	disman/event/mteTrigger.o \
	disman/event/mteTriggerTable.o \
	disman/event/mteTriggerDeltaTable.o \
	disman/event/mteTriggerExistenceTable.o \
	disman/event/mteTriggerBooleanTable.o \
	disman/event/mteTriggerThresholdTable.o \
	disman/event/mteTriggerConf.o \
	disman/event/mteEvent.o \
	disman/event/mteEventTable.o \
	disman/event/mteEventSetTable.o \
	disman/event/mteEventNotificationTable.o \
	disman/event/mteEventConf.o \
	disman/event/mteObjects.o \
	disman/event/mteObjectsTable.o \
	disman/event/mteObjectsConf.o \
	disman/schedule/schedCore.o \
	disman/schedule/schedConf.o \
	disman/schedule/schedTable.o \
	utilities/override.o \
	utilities/execute.o \
	utilities/iquery.o \
	host/hrh_storage.o \
	host/hrh_filesys.o \
	host/hrSWInstalledTable.o \
	host/hrSWRunTable.o \
	host/hr_system.o \
	host/hr_device.o \
	host/hr_other.o \
	host/hr_proc.o \
	host/hr_network.o \
	host/hr_print.o \
	host/hr_disk.o \
	host/hr_partition.o \
	util_funcs/header_generic.o \
	mibII/updates.o \
	util_funcs.o \
	mibII/vacm_context.o \
	mibII/vacm_conf.o \
	mibII/kernel_linux.o \
	mibII/ipAddr.o \
	mibII/var_route.o \
	mibII/route_write.o \
	mibII/tcpTable.o \
	mibII/udpTable.o \
	ip-mib/ip_scalars.o \
	util_funcs/header_simple_table.o \
	util_funcs/restart.o \
	ucd-snmp/pass_common.o \
	header_complex.o \
	snmp-notification-mib/snmpNotifyFilterTable/snmpNotifyFilterTable.o \
	agentx/protocol.o \
	agentx/client.o \
	agentx/master_admin.o \
	agentx/agentx_config.o \
	host/data_access/swinst.o \
	host/data_access/swrun.o \
	host/hrSWRunPerfTable.o \
	if-mib/ifTable/ifTable.o \
	if-mib/ifXTable/ifXTable.o \
	ip-mib/ipAddressTable/ipAddressTable.o \
	ip-mib/ipAddressPrefixTable/ipAddressPrefixTable.o \
	ip-mib/ipDefaultRouterTable/ipDefaultRouterTable.o \
	ip-mib/inetNetToMediaTable/inetNetToMediaTable.o \
	ip-mib/inetNetToMediaTable/inetNetToMediaTable_interface.o \
	ip-mib/inetNetToMediaTable/inetNetToMediaTable_data_access.o \
	ip-mib/ipSystemStatsTable/ipSystemStatsTable.o \
	ip-mib/ipSystemStatsTable/ipSystemStatsTable_interface.o \
	ip-mib/ipSystemStatsTable/ipSystemStatsTable_data_access.o \
	ip-mib/ipv6ScopeZoneIndexTable/ipv6ScopeZoneIndexTable.o \
	ip-mib/ipIfStatsTable/ipIfStatsTable.o \
	ip-mib/ipIfStatsTable/ipIfStatsTable_interface.o \
	ip-mib/ipIfStatsTable/ipIfStatsTable_data_access.o \
	ip-forward-mib/ipCidrRouteTable/ipCidrRouteTable.o \
	ip-forward-mib/inetCidrRouteTable/inetCidrRouteTable.o \
	tcp-mib/tcpConnectionTable/tcpConnectionTable.o \
	tcp-mib/tcpListenerTable/tcpListenerTable.o \
	udp-mib/udpEndpointTable/udpEndpointTable.o \
	hardware/fsys/hw_fsys.o \
	hardware/fsys/fsys_mntent.o \
	hardware/memory/hw_mem.o \
	hardware/memory/memory_linux.o \
	hardware/cpu/cpu.o \
	hardware/cpu/cpu_linux.o \
	snmp-notification-mib/snmpNotifyFilterTable/snmpNotifyFilterTable_interface.o \
	snmp-notification-mib/snmpNotifyFilterTable/snmpNotifyFilterTable_data_access.o \
	host/data_access/swinst_apt.o \
	host/data_access/swrun_procfs_status.o \
	if-mib/data_access/interface.o \
	if-mib/ifTable/ifTable_interface.o \
	if-mib/ifTable/ifTable_data_access.o \
	if-mib/ifXTable/ifXTable_interface.o \
	if-mib/ifXTable/ifXTable_data_access.o \
	ip-mib/ipAddressTable/ipAddressTable_interface.o \
	ip-mib/ipAddressTable/ipAddressTable_data_access.o \
	ip-mib/ipAddressPrefixTable/ipAddressPrefixTable_interface.o \
	ip-mib/ipAddressPrefixTable/ipAddressPrefixTable_data_access.o \
	ip-mib/ipDefaultRouterTable/ipDefaultRouterTable_interface.o \
	ip-mib/ipDefaultRouterTable/ipDefaultRouterTable_data_access.o \
	ip-mib/ipDefaultRouterTable/ipDefaultRouterTable_data_get.o \
	ip-mib/data_access/arp_common.o \
	ip-mib/data_access/arp_netlink.o \
	ip-mib/data_access/systemstats_common.o \
	ip-mib/data_access/systemstats_linux.o \
	ip-mib/data_access/scalars_linux.o \
	ip-mib/ipv6ScopeZoneIndexTable/ipv6ScopeZoneIndexTable_interface.o \
	ip-mib/ipv6ScopeZoneIndexTable/ipv6ScopeZoneIndexTable_data_access.o \
	ip-mib/ipIfStatsTable/ipIfStatsTable_data_get.o \
	ip-forward-mib/ipCidrRouteTable/ipCidrRouteTable_interface.o \
	ip-forward-mib/ipCidrRouteTable/ipCidrRouteTable_data_access.o \
	ip-forward-mib/inetCidrRouteTable/inetCidrRouteTable_interface.o \
	ip-forward-mib/inetCidrRouteTable/inetCidrRouteTable_data_access.o \
	tcp-mib/data_access/tcpConn_common.o \
	tcp-mib/data_access/tcpConn_linux.o \
	util_funcs/get_pid_from_inode.o \
	tcp-mib/tcpConnectionTable/tcpConnectionTable_interface.o \
	tcp-mib/tcpConnectionTable/tcpConnectionTable_data_access.o \
	tcp-mib/tcpListenerTable/tcpListenerTable_interface.o \
	tcp-mib/tcpListenerTable/tcpListenerTable_data_access.o \
	udp-mib/udpEndpointTable/udpEndpointTable_interface.o \
	udp-mib/udpEndpointTable/udpEndpointTable_data_access.o \
	if-mib/data_access/interface_linux.o \
	if-mib/data_access/interface_ioctl.o \
	ip-mib/data_access/ipaddress_common.o \
	ip-mib/data_access/ipaddress_linux.o \
	ip-mib/data_access/defaultrouter_common.o \
	ip-mib/data_access/defaultrouter_linux.o \
	ip-mib/data_access/ipv6scopezone_common.o \
	ip-mib/data_access/ipv6scopezone_linux.o \
	ip-forward-mib/data_access/route_common.o \
	ip-forward-mib/data_access/route_linux.o \
	ip-forward-mib/data_access/route_ioctl.o \
	udp-mib/data_access/udp_endpoint_common.o \
	udp-mib/data_access/udp_endpoint_linux.o \
	ip-mib/data_access/ipaddress_ioctl.o

# end configure generated code
