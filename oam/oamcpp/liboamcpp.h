/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
 ******************************************************************************************/
/**
 * @file
 */
#ifndef LIBOAMCPP_H
#define LIBOAMCPP_H

#include <exception>
#include <stdexcept>
#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <limits.h>
#include <sstream>
#include <vector>
#include <algorithm>
#ifdef __linux__
#include <sys/sysinfo.h>
#include <netdb.h>
#endif
#include <fcntl.h>
#include <sys/file.h>

#include "bytestream.h"
#include "configcpp.h"
#include "boost/tuple/tuple.hpp"
#include "dbrm.h"

#include "messagequeue.h"

#if defined(_MSC_VER) && defined(xxxLIBOAM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace oam
{
/*
 * 	Global OAM parmaters
 */

/** @brief Maximum Number of Modules within the Calpont System
 */
// const int MAX_MODULE = 1024;

/** @brief Maximum Number of DBRoots within the Calpont System
 */
const int MAX_DBROOT = 10240;
// const int MAX_DBROOT_AMAZON = 190;	//DUE TO DEVICE NAME LIMIT

/** @brief Maximum Number of Modules Types within the Calpont System
 */
const int MAX_MODULE_TYPE = 3;

/** @brief Maximum Number of External Devices within the Calpont System
 */
// const int MAX_EXT_DEVICE = 20;

/** @brief Maximum Number of Arguments per process
 */
// const int MAX_ARGUMENTS = 15;

/** @brief Maximum Number of Dependancy processes per process
 */
// const int MAX_DEPENDANCY = 6;

/** @brief Maximum Number of processes within the Calpont System
 */
// const int MAX_PROCESS_PER_MODULE = 15;
// const int MAX_PROCESS = MAX_MODULE * MAX_PROCESS_PER_MODULE;

/** @brief Maximum Number of Parameters per process
 */
// const int MAX_PARAMS = 13;

/** @brief Maximum Module Type Size
 */
const int MAX_MODULE_TYPE_SIZE = 2;

/** @brief Maximum Module ID Size
 */
const int MAX_MODULE_ID_SIZE = 4;

/** @brief Maximum Number of NICs per Module
 */
const int MAX_NIC = 4;

/** @brief Unassigned Name and IP Address Value
 */
const std::string UnassignedIpAddr = "0.0.0.0";
const std::string UnassignedName = "unassigned";

/** @brief Calpont System Configuration file sections
 */
const std::string configSections[] = {
    "SystemConfig", "SystemModuleConfig", "SystemModuleConfig", "SessionManager", "VersionBuffer",
    "OIDManager",   "PrimitiveServers",   "Installation",       "ExtentMap",      ""};

// const uint32_t NOTIFICATIONKEY = 0x49444231;

/** @brief Server Type Installs
 */

enum INSTALLTYPE
{
  RESERVED,                  // 0 = not used
  INSTALL_NORMAL,            // 1 = Normal - dm/um/pm on a seperate servers
  INSTALL_COMBINE_DM_UM_PM,  // 2 = dm/um/pm on a single server
  INSTALL_COMBINE_DM_UM,     // 3 = dm/um on a same server
  INSTALL_COMBINE_PM_UM      // 4 = pm/um on a same server
};

/** @brief OAM API Return values
 */

enum API_STATUS
{
  API_SUCCESS,
  API_FAILURE,
  API_INVALID_PARAMETER,
  API_FILE_OPEN_ERROR,
  API_TIMEOUT,
  API_DISABLED,
  API_FILE_ALREADY_EXIST,
  API_ALREADY_IN_PROGRESS,
  API_MINOR_FAILURE,
  API_FAILURE_DB_ERROR,
  API_INVALID_STATE,
  API_READONLY_PARAMETER,
  API_TRANSACTIONS_COMPLETE,
  API_CONN_REFUSED,
  API_CANCELLED,
  API_STILL_WORKING,
  API_DETACH_FAILURE,
  API_MAX
};

/** @brief Host/IP Address Config Structure
 *
 */

struct HostConfig_s
{
  std::string HostName;  //!< Host Name
  std::string IPAddr;    //!< IP address
  uint16_t NicID;        //!< NIC ID
};
typedef struct HostConfig_s HostConfig;

/** @brief Host/IP Address Config List
 *
 */

typedef std::vector<HostConfig> HostConfigList;

/** @brief Device Network Config Structure
 *
 */

struct DeviceNetworkConfig_s
{
  std::string DeviceName;          //!< Device Name
  std::string UserTempDeviceName;  //!< User Temp Device Name
  std::string DisableState;        //!< Disabled State
  HostConfigList hostConfigList;   //!< IP Address and Hostname List
};

typedef struct DeviceNetworkConfig_s DeviceNetworkConfig;

/** @brief Device Network Config List
 *
 */

typedef std::vector<DeviceNetworkConfig> DeviceNetworkList;

/** @brief Disk Monitor File System List
 *
 */

typedef std::vector<std::string> DiskMonitorFileSystems;

/** @brief DBRoot Config List
 *
 */

typedef std::vector<uint16_t> DBRootConfigList;

/** @brief Device DBRoot Config Structure
 *
 */

struct DeviceDBRootConfig_s
{
  uint16_t DeviceID;                  //!< Device ID
  DBRootConfigList dbrootConfigList;  //!< DBRoot List
};

typedef struct DeviceDBRootConfig_s DeviceDBRootConfig;

/** @brief Device DBRoot Config List
 *
 */

typedef std::vector<DeviceDBRootConfig> DeviceDBRootList;

struct ModuleTypeConfig_s
{
  std::string ModuleType;                 //!< Module Type
  std::string ModuleDesc;                 //!< Module Description
  uint16_t ModuleCount;                   //!< Module Equipage Count
  uint16_t ModuleCPUCriticalThreshold;    //!< CPU Critical Threahold %
  uint16_t ModuleCPUMajorThreshold;       //!< CPU Major Threahold %
  uint16_t ModuleCPUMinorThreshold;       //!< CPU Minor Threahold %
  uint16_t ModuleCPUMinorClearThreshold;  //!< CPU Minor Clear Threahold %
  uint16_t ModuleMemCriticalThreshold;    //!< Mem Critical Threahold %
  uint16_t ModuleMemMajorThreshold;       //!< Mem Major Threahold %
  uint16_t ModuleMemMinorThreshold;       //!< Mem Minor Threahold %
  uint16_t ModuleDiskCriticalThreshold;   //!< Disk Critical Threahold %
  uint16_t ModuleDiskMajorThreshold;      //!< Disk Major Threahold %
  uint16_t ModuleDiskMinorThreshold;      //!< Disk Minor Threahold %
  uint16_t ModuleSwapCriticalThreshold;   //!< Swap Critical Threahold %
  uint16_t ModuleSwapMajorThreshold;      //!< Swap Major Threahold %
  uint16_t ModuleSwapMinorThreshold;      //!< Swap Minor Threahold %
  DeviceNetworkList ModuleNetworkList;    //!< Module IP Address and Hostname List
  DiskMonitorFileSystems FileSystems;     //!< Module Disk File System list
  DeviceDBRootList ModuleDBRootList;      //!< Module DBRoot
};
typedef struct ModuleTypeConfig_s ModuleTypeConfig;

/** @brief System Module Type Configuration Structure
 *
 *   Structure that is returned by the getSystemConfigFile API for the
 *   System Module Configuration data stored in the System Configuration file
 */

struct SystemModuleTypeConfig_s
{
  std::vector<ModuleTypeConfig> moduletypeconfig;  //!< Module Type Configuration Structure
};
typedef struct SystemModuleTypeConfig_s SystemModuleTypeConfig;

/** @brief Module Name Configuration Structure
 *
 *   Structure that is returned by the getSystemConfigFile API for the
 *   Module Name Configuration data stored in the System Configuration file
 */

struct ModuleConfig_s
{
  std::string ModuleName;             //!< Module Name
  std::string ModuleType;             //!< Module Type
  std::string ModuleDesc;             //!< Module Description
  std::string DisableState;           //!< Disabled State
  HostConfigList hostConfigList;      //!< IP Address and Hostname List
  DBRootConfigList dbrootConfigList;  //!< DBRoot ID list
};
typedef struct ModuleConfig_s ModuleConfig;

/** @brief Local Module OAM Configuration StructureLOG_
 *
 *   Structure that is returned by the getModuleInfo API for the
 *   Local Module OAM Configuration data stored in the Local Module
 *   Configuration file
 *   Returns: Local Module Name, Local Module Type, Local Module ID,
 *					OAM Parent Module Name, OAM Parent Flag,
 *					Server Type Install ID, OAM Standby Parent Module Name,
 *					OAM Standby Parent Flag,
 */

typedef boost::tuple<std::string, std::string, uint16_t, std::string, bool, uint16_t, std::string, bool>
    oamModuleInfo_t;

/** @brief System Storage Configuration Structure
 *
 *   Structure that is returned by the getStorageConfig API
 *   Returns: Storage Type, System DBRoot count, PM dbroot info,
 */

typedef boost::tuple<std::string, uint16_t, DeviceDBRootList, std::string> systemStorageInfo_t;

/** @brief OAM API I/F class
 *
 *  Operations, Administration, and Maintenance C++ APIs. These APIS are utilized
 *  to Configure the Hardware and the Software on the Calpont Database Appliance.
 *  They are also used to retrieve the configuration, hardware and process status,
 *  alarms, logs, and performance data.
 *
 */

class Oam
{
 public:
  /** @brief OAM C++ API Class constructor
   */
  EXPORT Oam();

  /** @brief OAM C++ API Class destructor
   */
  EXPORT virtual ~Oam();

  /** @brief get System Module Configuration information
   *
   * get System Module Configuration information value from the system config file.
   * @param systemmoduletypeconfig Returned System Module Configuration Structure
   */
  EXPORT void getSystemConfig(SystemModuleTypeConfig& systemmoduletypeconfig);

  /** @brief get System Module Type Configuration information for a Module
   *
   * get System Module Type Configuration information for a Module from the system config file.
   * @param moduletype the Module Type to get information
   * @param moduletypeconfig Returned System Module Configuration Structure
   */
  EXPORT void getSystemConfig(const std::string& moduletype, ModuleTypeConfig& moduletypeconfig);

  /** @brief get System Module Name Configuration information for a Module
   *
   * get System Module Name Configuration information for a Module from the system config file.
   * @param moduleName the Module Name to get information
   * @param moduleconfig Returned System Module Configuration Structure
   */
  EXPORT void getSystemConfig(const std::string& moduleName, ModuleConfig& moduleconfig);

  /** @brief get System Configuration String Parameter
   *
   * get System Configuration String Parameter from the system config file.
   * @param name the Parameter Name to get value
   * @param value Returned Parameter Value
   */
  EXPORT void getSystemConfig(const std::string& name, std::string& value);

  /** @brief get System Configuration Integer Parameter
   *
   * get System Configuration Integer Parameter from the system config file.
   * @param name the Parameter Name to get value
   * @param value Returned Parameter Value
   */
  EXPORT void getSystemConfig(const std::string& name, int& value);

  /** @brief get Local Module Configuration Data
   *
   * get Local Module Name, OAM Parent Flag, and Realtime Linux OS Flag from
   * local config file.
   * @return oamModuleInfo_t structure, which contains the local Module OAM
   *         Configuration Data
   */
  EXPORT oamModuleInfo_t getModuleInfo();

  // Integer to ASCII convertor

  EXPORT std::string itoa(const int);

  /**
   *@brief Get Storage Config Data
   */
  EXPORT systemStorageInfo_t getStorageConfig();

  /**
   *@brief Get PM - DBRoot Config data
   */
  EXPORT void getPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist);

  /**
   *@brief Get DBRoot - PM Config data
   */
  EXPORT void getDbrootPmConfig(const int dbrootid, int& pmid);

  /**
   *@brief Get System DBRoot Config data
   */
  EXPORT void getSystemDbrootConfig(DBRootConfigList& dbrootconfiglist);

  /** @brief validate Module name
   */
  EXPORT int validateModule(const std::string name);

  /**
   * @brief	changeMyCnf
   *
   * purpose:	change my.cnf
   *
   **/
  EXPORT bool changeMyCnf(std::string paramater, std::string value);

 private:
  /** @brief validate Process name
   */
  int validateProcess(const std::string moduleName, std::string processName);

  /** @brief local exception control function
   * @param function Function throwing the exception
   * @param returnStatus
   * @param msg A message to be included
   */
  EXPORT void exceptionControl(std::string function, int returnStatus, const char* extraMsg = NULL);

  std::string tmpdir;
  std::string CalpontConfigFile;
  std::string userDir;

};  // end of class

}  // namespace oam

#undef EXPORT

#endif
// vim:ts=4 sw=4:
