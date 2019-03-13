/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporaton

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/******************************************************************************************
* $Id: processmanager.h 2163 2013-04-04 18:40:54Z rdempsey $
*
******************************************************************************************/


#ifndef _PROCESSMANAGER_H_
#define _PROCESSMANAGER_H_

#include <string>

#include "liboamcpp.h"
#include "threadpool.h"
#include "socketclosed.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <errno.h>
#include <netinet/in.h>
#if defined(__GLIBC__) && __GLIBC__ >=2 && __GLIBC_MINOR__ >= 1
#include <netpacket/packet.h>
#include <net/ethernet.h>
#else
#include <sys/types.h>
#include <netinet/if_ether.h>
#endif

#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <errno.h>
#include <netinet/in.h>
#if defined(__GLIBC__) && __GLIBC__ >=2 && __GLIBC_MINOR__ >= 1
#include <netpacket/packet.h>
#include <net/ethernet.h>
#else
#include <sys/types.h>
#include <netinet/if_ether.h>
#endif

#include <arpa/inet.h>
#include <ifaddrs.h>

void pingDeviceThread();

namespace processmanager
{

void startSystemThread(oam::DeviceNetworkList devicenetworklist);
void stopSystemThread(oam::DeviceNetworkList devicenetworklist);
void startModuleThread(std::string moduleName);
void stopModuleThread(std::string moduleName);
void processMSG(messageqcpp::IOSocket* fIos);

void sendUpgradeRequest();

/** @brief Timeset for Milleseconds
*/
#define TS_MS(x) ((x) * 1000000)

struct HeartBeatProc
{
    std::string ModuleName;                 //!< Module Name
    std::string ProcessName;                //!< Process Name
    int ID;									//!< Heartbeat ID
    bool receiveFlag;						//!< Heartbeat indication flag
};

typedef   std::list<HeartBeatProc> HeartBeatProcList;

typedef   std::map<std::string, std::string>	srvStateList;

const int MAX_ARGUMENTS = 10;
const std::string DEFAULT_LOG_FILE = "/var/log/mariadb/columnstore/ProcessManager.log";


/**
  * parentFlag accessor
  */

class Configuration
{
public:
    /**
     * @brief Constructor
     */
    Configuration();
    /**
     * Destructor
     */
    ~Configuration();

    /**
     * @brief Return the module opstate tag
     */
    std::string	getstateInfo(std::string moduleName);

    /**
     * moduleName accessor
     */
    const std::string&	moduleName() const
    {
        return flocalModuleName;
    }

    /**
     * moduleType accessor
     */
    const std::string&	moduleType() const
    {
        return flocalModuleType;
    }

    /**
    * moduleName accessor
    */
    const uint16_t&	moduleID() const
    {
        return flocalModuleID;
    }

    /**
    * parentName accessor
    */
    const std::string&	OAMParentName() const
    {
        return fOAMParentModuleName;
    }

    /**
      * parentFlag accessor
      */
    const bool&	OAMParentFlag() const
    {
        return fOAMParentModuleFlag;
    }

    /**
      * ServerInstallType accessor
      */
    const uint16_t&	ServerInstallType() const
    {
        return fserverInstallType;
    }

    /**
      * standbyName accessor
      */
    const std::string&	OAMStandbyName() const
    {
        return fOAMStandbyModuleName;
    }

    /**
      * standbyParentFlag accessor
      */
    const bool&	OAMStandbyParentFlag() const
    {
        return fOAMStandbyModuleFlag;
    }

    /**
      * parentFlag accessor
      */

private:
    srvStateList	stateInfoList;
    std::string 	flocalModuleName;
    std::string 	flocalModuleType;
    uint16_t		flocalModuleID;
    std::string 	fOAMParentModuleName;
    bool 			fOAMParentModuleFlag;
    uint16_t		fserverInstallType;
    std::string 	fOAMStandbyModuleName;
    bool			fOAMStandbyModuleFlag;
};

class ProcessLog
{
public:
    /**
     * @brief Constructor
     */

    ProcessLog();

    /**
     * @brief Destructor
     */

    ~ProcessLog();

    /**
     * @brief Write the message to the log
     */
    void writeLog(const int lineNumber, const std::string logContent, const logging::LOG_TYPE logType = logging::LOG_TYPE_INFO);

    /**
     * @brief Write the message to the log
     */
    void writeLog(const int lineNumber, const int logContent, const logging::LOG_TYPE logType = logging::LOG_TYPE_INFO);

    /**
     * @brief Compose a log data in the required format
     */
    void setSysLogData();

    /**
     * @brief return the sysLogData
     */
    std::string getSysLogData();

    /**
     * @brief log process status change into system log
     */

    void writeSystemLog();

private:
    std::ofstream PMLog;
    std::string   sysLogData;
};

class ProcessManager
{
public:

    /**
     * @brief Constructor
     */
    ProcessManager(Configuration& config, ProcessLog& log);

    /**
     * @brief Default Destructor
     */
    ~ProcessManager();

    /**
    * @brief Process the received message
    */

//    void	processMSG(messageqcpp::IOSocket fIos, messageqcpp::ByteStream msg);

    /**
    *@brief send a request to the associated Process Monitor
    */
    // void		sendRequestToMonitor(ByteStream::byte target, ByteStream request);

    /**
    *@brief Build a request message
    */
    messageqcpp::ByteStream  buildRequestMessage(messageqcpp::ByteStream::byte requestID, messageqcpp::ByteStream::byte actionIndicator, std::string processName, bool manualFlag = true);

    /**
    *@brief Start all processes on the specified module
    */

    int startModule(std::string target, messageqcpp::ByteStream::byte actionIndicator, uint16_t startType, bool systemStart = false);

    /**
    *@brief Stop all processes on the specified module
    */
    int stopModule(std::string target, messageqcpp::ByteStream::byte actionIndicator, bool manualFlag, int timeout = 60 );

    /**
     *@brief power off the specified module
     */
    int shutdownModule(std::string target, messageqcpp::ByteStream::byte actionIndicator, bool manualFlag, int timeout = 10 );

    /**
    *@brief Disable a specified module
    */
    int disableModule(std::string target, bool manualFlag);

    /**
    *@brief recycle Processes
    */
    void recycleProcess(std::string module, bool enableModule = false);

    /**
    *@brief Enable a specified module
    */
    int enableModule(std::string target, int state, bool failover = false);

    /**
    *@brief Enable a specified module
    */
    int enableModuleStatus(std::string target);

    void dbrmctl(std::string command);

    /**
     *@brief start all Mgr Controlled processes for a module
     */
    void startMgrProcesses(std::string moduleName);

    /**
     *@brief stop process on a specific module
     */
    int stopProcess(std::string moduleName, std::string processName, messageqcpp::ByteStream::byte actionIndicator, bool manualFlag, int timeout = 10);

    /**
     *@brief start process on a specific module
     */
    int startProcess(std::string moduleName, std::string processName, messageqcpp::ByteStream::byte actionIndicator);

    /**
     *@brief restart process on a specific module
     */
    int restartProcess(std::string moduleName, std::string processName, messageqcpp::ByteStream::byte actionIndicator, bool manualFlag);

    /**
     *@brief reinit process on a specific module
     */
    int reinitProcess(std::string moduleName, std::string processName);

    /**
     *@brief set the state of the specified module
     */
    void setModuleState(std::string moduleName, uint16_t state);

    /**
     *@brief set the state of the specified Ext device
     */
    void setExtdeviceState(std::string extDeviceName, uint16_t state);

    /**
     *@brief set the state of the specified NIC
     */
    void setNICState(std::string hostName, uint16_t state);

    /**
     *@brief set the state of the system
     */
    void setSystemState(uint16_t state);

    /**
     *@brief set all processes running on module auto or manual offline
     */
    void setProcessStates(std::string moduleName, uint16_t state, std::string processNameSkip = "");

    /**
     *@brief set the state of the specified process
     */
    int setProcessState (std::string moduleName, std::string processName, uint16_t state, pid_t PID);

    /**
     *@brief updatelog on a specific module
     */
    int updateLog (std::string action, std::string moduleName, std::string level);

    /**
     *@brief get log configuration on a specific module
     */
    int getConfigLog (std::string moduleName);

    /**
     *@brief update process configuration
     */
    int updateConfig (std::string moduleName);

    /**
     *@brief Build System Tables request
     */
    int buildSystemTables(std::string moduleName);

    /**
     *@brief Stop a Process Type
     */
    int stopProcessType(std::string processName, bool manualFlag = true );

    /**
     *@brief Start a Process Type
     */
    int startProcessType(std::string processName);

    /**
     *@brief Restart a Process Type
     */
    int restartProcessType(std::string processName, std::string skipModule = "none", bool manualFlag = true);

    /**
     *@brief ReInit a Process Type
     */
    int reinitProcessType(std::string processName);

    /**
     *@brief Add Module
     */
    int addModule(oam::DeviceNetworkList devicenetworklist, std::string password, bool manualFlag = true);

    /**
     *@brief Configure Module
     */
    int configureModule(std::string moduleName);

    /**
     *@brief Reconfigure Module
     */
    int reconfigureModule(oam::DeviceNetworkList devicenetworklist);

    /**
     *@brief Remove Module
     */
    int removeModule(oam::DeviceNetworkList devicenetworklist, bool manualFlag = true);

    /**
     *@brief Check for simplex module run-type and start mate processes if needed
     */
    void checkSimplexModule(std::string moduleName);

    /**
     *@brief update core on a specific module
     */
    int updateCore (std::string action, std::string moduleName);

    /**
     *@brief update PMS Configuration
     */
    int updatePMSconfig(bool check = false);

    /**
     *@brief update WorkerNode Configuration
     */
    int updateWorkerNodeconfig();

    /**
     *@brief Clears all alarms related to a module
     */
    void clearModuleAlarms(std::string moduleName);

    /**
     *@brief Clears all alarms related to a NIC hostName
     */
    void clearNICAlarms(std::string hostName);

    /**
     *@brief Send Msg to Process Monitor
     */
    std::string sendMsgProcMon1( std::string module, messageqcpp::ByteStream msg, int requestID );

    /*
    * Updates the Columnstore.xml file for DDL/DMLProc IPs during PM switchover
    */
    int setPMProcIPs( std::string moduleName, std::string processName = oam::UnassignedName);

    /*
    * OAM Parent Module change-over
    */
    int OAMParentModuleChange();

    /** @brief find a new hot-standby module based on Process-Manager status, if one exist
    	*/
    std::string getStandbyModule();

    /** @brief set Standby Module info in Columnstore.xml
    	*/
    bool setStandbyModule(std::string newStandbyModule, bool send = true);

    /** @brief clear Standby Module info in Columnstore.xml
    	*/
    bool clearStandbyModule();

    int setEnableState(std::string target, std::string state);

    /** @brief Distribute Calpont Config File to system modules
    	*/
    int distributeConfigFile(std::string name, std::string file = "Columnstore.xml");

    /** @brief Switch OAM Parent Module
    	*/
    int switchParentOAMModule(std::string moduleName);

    /** @brief get DBRM Data and send to requester
    	*/
    int getDBRMData(messageqcpp::IOSocket fIos, std::string moduleName);

    /** @brief remount the dbroot disk
    	*/
//	int remountDbroots(std::string option);

    /**
    *@brief Send Msg to Process Monitor
    */
    int sendMsgProcMon( std::string module, messageqcpp::ByteStream msg, int requestID, int timeout = 240 );

    /** @brief get Alarm Data and send to requester
    	*/
    int getAlarmData(messageqcpp::IOSocket fIos, int type, std::string date);

    /**
    *@brief Save BRM database
    */

    void saveBRM(bool skipSession = false, bool clearshm = true);

    /**
    *@brief set query system state not ready
    */

    void setQuerySystemState(bool set);

    /** @brief stop by process type
    	*/
    void stopProcessTypes(bool manualFlag = true);

    /** @brief unmount a dbroot
    	*/
    int unmountDBRoot(std::string dbrootID);

    /** @brief mount a dbroot
    	*/
    int mountDBRoot(std::string dbrootID);

    /** @brief distribute fstab update message
    	*/
    int updateFstab(std::string moduleName, std::string entry);

    /** @brief Set MySQL Replication
    	*/
    int setMySQLReplication(oam::DeviceNetworkList devicenetworklist, std::string masterModule = oam::UnassignedName, bool distributeDB = false, std::string password = oam::UnassignedName, bool enable = true, bool addModule = false);

    /** @brief Gluster Assign dbroot to a module
    	*/
    int glusterAssign(std::string moduleName, std::string dbroot);

    /** @brief Gluster Unassign dbroot to a module
    	*/
    int glusterUnassign(std::string moduleName, std::string dbroot);


private:

    Configuration&	config;
    ProcessLog&		log;

    /**
     *@brief Create a /ect/module file for remote server
     */

    bool createModuleFile(std::string remoteModuleName);

    /**
     *@brief pdate Extent Map section in Columnstore.xml
     */
    bool updateExtentMap();

    /*
    * Make inittab to auto-launch ProcMon
    */
    bool makeXMInittab(std::string moduleName, std::string systemID, std::string parentOAMModuleHostName);

    /*
    * setup External Module mount file
    */
    bool setXMmount(std::string moduleName, std::string parentOAMModuleHostName, std::string parentOAMModuleIPAddr);

    /** @brief send status updates to process monitor
    	*/
    void sendStatusUpdate(messageqcpp::ByteStream obs, messageqcpp::ByteStream::byte returnRequestType);

    /** @brief flush inode cache
    	*/
    void flushInodeCache();

};

} //end of namespace
#endif // _PROCESSMANAGER_H_


