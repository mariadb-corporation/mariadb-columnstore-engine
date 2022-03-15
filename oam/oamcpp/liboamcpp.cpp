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
#include <unistd.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#if defined(__linux__)
#include <sys/statfs.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>

#elif defined(_MSC_VER)
#elif defined(__FreeBSD__)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/mount.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <stdexcept>
#include <csignal>
#include <sstream>

#include "columnstoreversion.h"
#include "ddlpkg.h"
#include "../../dbcon/dmlpackage/dmlpkg.h"
#define LIBOAM_DLLEXPORT
#include "liboamcpp.h"
#undef LIBOAM_DLLEXPORT

#ifdef _MSC_VER
#include "idbregistry.h"
#endif
#include "mcsconfig.h"
#include "installdir.h"
#include "dbrm.h"
#include "sessionmanager.h"
#include "IDBPolicy.h"
#include "IDBDataFile.h"
#include "vlarray.h"

#if defined(__GNUC__)
#include <string>
static const std::string optim(
    "Build is "
#if !defined(__OPTIMIZE__)
    "NOT "
#endif
    "optimized");
#endif

namespace fs = boost::filesystem;

using namespace config;
using namespace std;
using namespace messageqcpp;
using namespace oam;
using namespace logging;
using namespace BRM;

namespace oam
{
// flag to tell us ctrl-c was hit
uint32_t ctrlc = 0;

//------------------------------------------------------------------------------
// Signal handler to catch Control-C signal to terminate the process
// while waiting for a shutdown or suspend action
//------------------------------------------------------------------------------
void handleControlC(int i)
{
  std::cout << "Received Control-C to terminate the command..." << std::endl;
  ctrlc = 1;
}

Oam::Oam()
{
  CalpontConfigFile = std::string(MCSSYSCONFDIR) + "/columnstore/Columnstore.xml";

  // get user
  string USER = "root";
  char* p = getenv("USER");

  if (p && *p)
    USER = p;

  userDir = USER;

  if (USER != "root")
  {
    userDir = "home/" + USER;
  }

  tmpdir = startup::StartUp::tmpDir();
}

Oam::~Oam()
{
}

/********************************************************************
 *
 * get System Module Type Configuration Information
 *
 ********************************************************************/

void Oam::getSystemConfig(SystemModuleTypeConfig& systemmoduletypeconfig)
{
  const string Section = "SystemModuleConfig";
  const string MODULE_TYPE = "ModuleType";
  systemmoduletypeconfig.moduletypeconfig.clear();

  Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

  for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE + 1; moduleTypeID++)
  {
    ModuleTypeConfig moduletypeconfig;

    // get Module info

    string moduleType = MODULE_TYPE + itoa(moduleTypeID);

    Oam::getSystemConfig(sysConfig->getConfig(Section, moduleType), moduletypeconfig);

    if (moduletypeconfig.ModuleType.empty())
      continue;

    systemmoduletypeconfig.moduletypeconfig.push_back(moduletypeconfig);
  }
}

/********************************************************************
 *
 * get System Module Configuration Information by Module Type
 *
 ********************************************************************/

void Oam::getSystemConfig(const std::string& moduletype, ModuleTypeConfig& moduletypeconfig)
{
  Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
  const string Section = "SystemModuleConfig";
  const string MODULE_TYPE = "ModuleType";
  const string MODULE_DESC = "ModuleDesc";
  const string MODULE_COUNT = "ModuleCount";
  const string MODULE_DISABLE_STATE = "ModuleDisableState";
  const string MODULE_CPU_CRITICAL = "ModuleCPUCriticalThreshold";
  const string MODULE_CPU_MAJOR = "ModuleCPUMajorThreshold";
  const string MODULE_CPU_MINOR = "ModuleCPUMinorThreshold";
  const string MODULE_CPU_MINOR_CLEAR = "ModuleCPUMinorClearThreshold";
  const string MODULE_DISK_CRITICAL = "ModuleDiskCriticalThreshold";
  const string MODULE_DISK_MAJOR = "ModuleDiskMajorThreshold";
  const string MODULE_DISK_MINOR = "ModuleDiskMinorThreshold";
  const string MODULE_MEM_CRITICAL = "ModuleMemCriticalThreshold";
  const string MODULE_MEM_MAJOR = "ModuleMemMajorThreshold";
  const string MODULE_MEM_MINOR = "ModuleMemMinorThreshold";
  const string MODULE_SWAP_CRITICAL = "ModuleSwapCriticalThreshold";
  const string MODULE_SWAP_MAJOR = "ModuleSwapMajorThreshold";
  const string MODULE_SWAP_MINOR = "ModuleSwapMinorThreshold";
  const string MODULE_IP_ADDR = "ModuleIPAddr";
  const string MODULE_SERVER_NAME = "ModuleHostName";
  const string MODULE_DISK_MONITOR_FS = "ModuleDiskMonitorFileSystem";
  const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";
  const string MODULE_DBROOT_ID = "ModuleDBRootID";

  for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE + 1; moduleTypeID++)
  {
    string moduleType = MODULE_TYPE + itoa(moduleTypeID);

    if (sysConfig->getConfig(Section, moduleType) == moduletype)
    {
      string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);
      string ModuleType = MODULE_TYPE + itoa(moduleTypeID);
      string ModuleDesc = MODULE_DESC + itoa(moduleTypeID);
      string ModuleCPUCriticalThreshold = MODULE_CPU_CRITICAL + itoa(moduleTypeID);
      string ModuleCPUMajorThreshold = MODULE_CPU_MAJOR + itoa(moduleTypeID);
      string ModuleCPUMinorThreshold = MODULE_CPU_MINOR + itoa(moduleTypeID);
      string ModuleCPUMinorClearThreshold = MODULE_CPU_MINOR_CLEAR + itoa(moduleTypeID);
      string ModuleDiskCriticalThreshold = MODULE_DISK_CRITICAL + itoa(moduleTypeID);
      string ModuleDiskMajorThreshold = MODULE_DISK_MAJOR + itoa(moduleTypeID);
      string ModuleDiskMinorThreshold = MODULE_DISK_MINOR + itoa(moduleTypeID);
      string ModuleMemCriticalThreshold = MODULE_MEM_CRITICAL + itoa(moduleTypeID);
      string ModuleMemMajorThreshold = MODULE_MEM_MAJOR + itoa(moduleTypeID);
      string ModuleMemMinorThreshold = MODULE_MEM_MINOR + itoa(moduleTypeID);
      string ModuleSwapCriticalThreshold = MODULE_SWAP_CRITICAL + itoa(moduleTypeID);
      string ModuleSwapMajorThreshold = MODULE_SWAP_MAJOR + itoa(moduleTypeID);
      string ModuleSwapMinorThreshold = MODULE_SWAP_MINOR + itoa(moduleTypeID);

      moduletypeconfig.ModuleCount = strtol(sysConfig->getConfig(Section, ModuleCount).c_str(), 0, 0);
      moduletypeconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
      moduletypeconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc);
      moduletypeconfig.ModuleCPUCriticalThreshold =
          strtol(sysConfig->getConfig(Section, ModuleCPUCriticalThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleCPUMajorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleCPUMajorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleCPUMinorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleCPUMinorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleCPUMinorClearThreshold =
          strtol(sysConfig->getConfig(Section, ModuleCPUMinorClearThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleDiskCriticalThreshold =
          strtol(sysConfig->getConfig(Section, ModuleDiskCriticalThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleDiskMajorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleDiskMajorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleDiskMinorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleDiskMinorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleMemCriticalThreshold =
          strtol(sysConfig->getConfig(Section, ModuleMemCriticalThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleMemMajorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleMemMajorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleMemMinorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleMemMinorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleSwapCriticalThreshold =
          strtol(sysConfig->getConfig(Section, ModuleSwapCriticalThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleSwapMajorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleSwapMajorThreshold).c_str(), 0, 0);
      moduletypeconfig.ModuleSwapMinorThreshold =
          strtol(sysConfig->getConfig(Section, ModuleSwapMinorThreshold).c_str(), 0, 0);

      int moduleFound = 0;

      // get NIC IP address/hostnames
      for (int moduleID = 1; moduleID <= moduletypeconfig.ModuleCount; moduleID++)
      {
        DeviceNetworkConfig devicenetworkconfig;
        HostConfig hostconfig;

        for (int nicID = 1; nicID < MAX_NIC + 1; nicID++)
        {
          string ModuleIpAddr =
              MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);

          string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);

          if (ipAddr.empty())
            break;
          else if (ipAddr == UnassignedIpAddr)
            continue;

          string ModuleHostName =
              MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
          string serverName = sysConfig->getConfig(Section, ModuleHostName);

          hostconfig.IPAddr = ipAddr;
          hostconfig.HostName = serverName;
          hostconfig.NicID = nicID;

          devicenetworkconfig.hostConfigList.push_back(hostconfig);
        }

        if (!devicenetworkconfig.hostConfigList.empty())
        {
          string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);
          devicenetworkconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

          devicenetworkconfig.DeviceName = moduletypeconfig.ModuleType + itoa(moduleID);
          moduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
          devicenetworkconfig.hostConfigList.clear();

          moduleFound++;

          if (moduleFound >= moduletypeconfig.ModuleCount)
            break;
        }
      }

      // get filesystems
      DiskMonitorFileSystems fs;

      for (int fsID = 1;; fsID++)
      {
        string ModuleDiskMonitorFS = MODULE_DISK_MONITOR_FS + itoa(fsID) + "-" + itoa(moduleTypeID);

        string fsName = sysConfig->getConfig(Section, ModuleDiskMonitorFS);

        if (fsName.empty())
          break;

        fs.push_back(fsName);
      }

      moduletypeconfig.FileSystems = fs;

      // get dbroot IDs
      moduleFound = 0;

      for (int moduleID = 1; moduleID <= moduletypeconfig.ModuleCount; moduleID++)
      {
        string ModuleDBRootCount = MODULE_DBROOT_COUNT + itoa(moduleID) + "-" + itoa(moduleTypeID);
        string temp = sysConfig->getConfig(Section, ModuleDBRootCount).c_str();

        if (temp.empty() || temp == oam::UnassignedName)
          continue;

        int moduledbrootcount = strtol(temp.c_str(), 0, 0);

        DeviceDBRootConfig devicedbrootconfig;
        DBRootConfigList dbrootconfiglist;

        if (moduledbrootcount < 1)
        {
          dbrootconfiglist.clear();
        }
        else
        {
          int foundIDs = 0;

          for (int dbrootID = 1; dbrootID < moduledbrootcount + 1; dbrootID++)
          {
            string DBRootID =
                MODULE_DBROOT_ID + itoa(moduleID) + "-" + itoa(dbrootID) + "-" + itoa(moduleTypeID);

            string dbrootid = sysConfig->getConfig(Section, DBRootID);

            if (dbrootid.empty() || dbrootid == oam::UnassignedName || dbrootid == "0")
              continue;

            dbrootconfiglist.push_back(atoi(dbrootid.c_str()));
            foundIDs++;

            if (moduledbrootcount == foundIDs)
              break;
          }
        }

        sort(dbrootconfiglist.begin(), dbrootconfiglist.end());
        devicedbrootconfig.DeviceID = moduleID;
        devicedbrootconfig.dbrootConfigList = dbrootconfiglist;
        moduletypeconfig.ModuleDBRootList.push_back(devicedbrootconfig);
        devicedbrootconfig.dbrootConfigList.clear();

        moduleFound++;

        if (moduleFound >= moduletypeconfig.ModuleCount)
          break;
      }

      return;
    }
  }

  // Module Not found
  exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
}

/********************************************************************
 *
 * get System Module Configuration Information by Module Name
 *
 ********************************************************************/

void Oam::getSystemConfig(const std::string& module, ModuleConfig& moduleconfig)
{
  Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
  const string Section = "SystemModuleConfig";
  const string MODULE_TYPE = "ModuleType";
  const string MODULE_DESC = "ModuleDesc";
  const string MODULE_COUNT = "ModuleCount";
  const string MODULE_IP_ADDR = "ModuleIPAddr";
  const string MODULE_SERVER_NAME = "ModuleHostName";
  const string MODULE_DISABLE_STATE = "ModuleDisableState";
  const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";
  const string MODULE_DBROOT_ID = "ModuleDBRootID";

  string moduletype = module.substr(0, MAX_MODULE_TYPE_SIZE);
  int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE).c_str());

  if (moduleID < 1)
    // invalid ID
    exceptionControl("getSystemConfig", API_INVALID_PARAMETER);

  for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE + 1; moduleTypeID++)
  {
    string moduleType = MODULE_TYPE + itoa(moduleTypeID);
    string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);

    if (sysConfig->getConfig(Section, moduleType) == moduletype)
    {
      string ModuleType = MODULE_TYPE + itoa(moduleTypeID);
      string ModuleDesc = MODULE_DESC + itoa(moduleTypeID);
      string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);

      moduleconfig.ModuleName = module;
      moduleconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
      moduleconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc) + " #" + itoa(moduleID);
      moduleconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

      string ModuleDBRootCount = MODULE_DBROOT_COUNT + itoa(moduleID) + "-" + itoa(moduleTypeID);
      string temp = sysConfig->getConfig(Section, ModuleDBRootCount).c_str();

      int moduledbrootcount = 0;

      if (temp.empty() || temp != oam::UnassignedName)
        moduledbrootcount = strtol(temp.c_str(), 0, 0);

      HostConfig hostconfig;

      // get NIC IP address/hostnames
      moduleconfig.hostConfigList.clear();

      for (int nicID = 1; nicID < MAX_NIC + 1; nicID++)
      {
        string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);

        string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);

        if (ipAddr.empty() || ipAddr == UnassignedIpAddr)
          continue;

        string ModuleHostName =
            MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
        string serverName = sysConfig->getConfig(Section, ModuleHostName);

        hostconfig.IPAddr = ipAddr;
        hostconfig.HostName = serverName;
        hostconfig.NicID = nicID;

        moduleconfig.hostConfigList.push_back(hostconfig);
      }

      // get DBroot IDs
      moduleconfig.dbrootConfigList.clear();

      for (int dbrootID = 1; dbrootID < moduledbrootcount + 1; dbrootID++)
      {
        string ModuleDBRootID =
            MODULE_DBROOT_ID + itoa(moduleID) + "-" + itoa(dbrootID) + "-" + itoa(moduleTypeID);

        string moduleDBRootID = sysConfig->getConfig(Section, ModuleDBRootID);

        if (moduleDBRootID.empty() || moduleDBRootID == oam::UnassignedName || moduleDBRootID == "0")
          continue;

        moduleconfig.dbrootConfigList.push_back(atoi(moduleDBRootID.c_str()));
      }

      sort(moduleconfig.dbrootConfigList.begin(), moduleconfig.dbrootConfigList.end());

      return;
    }
  }

  // Module Not found
  exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
}

/********************************************************************
 *
 * get System Configuration String Parameter value
 *
 ********************************************************************/

void Oam::getSystemConfig(const std::string& name, std::string& value)
{
  Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

  // get string variables

  for (int i = 0;; i++)
  {
    if (configSections[i] == "")
      // end of section list
      break;

    value = sysConfig->getConfig(configSections[i], name);

    if (!(value.empty()))
    {
      // match found
      return;
    }
  }

  // no match found
  exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
}

/********************************************************************
 *
 * get System Configuration Integer Parameter value
 *
 ********************************************************************/

void Oam::getSystemConfig(const std::string& name, int& value)
{
  string returnValue;

  // get string variables

  Oam::getSystemConfig(name, returnValue);

  // covert returned Parameter value to Interger

  value = atoi(returnValue.c_str());
}

/********************************************************************
 *
 * get Local Module Information from Local Module Configuration file
 *
 *   Returns: Local Module Name, Local Module Type, Local Module ID,
 *                  OAM Parent Module Name, and OAM Parent Flag
 *
 ********************************************************************/

oamModuleInfo_t Oam::getModuleInfo()
{
  string localModule;
  string localModuleType;
  int localModuleID;

  // Get Module Name from module-file
  string fileName = "/var/lib/columnstore/local/module";

  ifstream oldFile(fileName.c_str());

  char line[400];

  while (oldFile.getline(line, 400))
  {
    localModule = line;
    break;
  }

  oldFile.close();

  if (localModule.empty())
  {
    // not found
    // system("touch /var/log/mariadb/columnstore/test8");
    exceptionControl("getModuleInfo", API_FAILURE);
  }

  localModuleType = localModule.substr(0, MAX_MODULE_TYPE_SIZE);
  localModuleID = atoi(localModule.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE).c_str());

  // Get Parent OAM Module name

  string ParentOAMModule = oam::UnassignedName;
  string StandbyOAMModule = oam::UnassignedName;
  bool parentOAMModuleFlag = false;
  bool standbyOAMModuleFlag = false;
  int serverTypeInstall = 1;

  try
  {
    Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
    string Section = "SystemConfig";
    ParentOAMModule = sysConfig->getConfig(Section, "ParentOAMModuleName");
    StandbyOAMModule = sysConfig->getConfig(Section, "StandbyOAMModuleName");

    if (localModule == ParentOAMModule)
      parentOAMModuleFlag = true;

    if (localModule == StandbyOAMModule)
      standbyOAMModuleFlag = true;

    // Get Server Type Install ID

    serverTypeInstall = atoi(sysConfig->getConfig("Installation", "ServerTypeInstall").c_str());
  }
  catch (...)
  {
  }

  return boost::make_tuple(localModule, localModuleType, localModuleID, ParentOAMModule, parentOAMModuleFlag,
                           serverTypeInstall, StandbyOAMModule, standbyOAMModuleFlag);
}

/********************************************************************
 *
 * Get Storage Config Data
 *
 ********************************************************************/
systemStorageInfo_t Oam::getStorageConfig()
{
  DeviceDBRootList deviceDBRootList;
  std::string storageType = "";
  std::string UMstorageType = "";
  int SystemDBRootCount = 0;

  try
  {
    getSystemConfig("DBRootStorageType", storageType);
  }
  catch (...)
  {
    exceptionControl("getStorageConfig", oam::API_FAILURE);
  }

  try
  {
    getSystemConfig("UMStorageType", UMstorageType);
  }
  catch (...)
  {
    exceptionControl("getStorageConfig", oam::API_FAILURE);
  }

  try
  {
    getSystemConfig("DBRootCount", SystemDBRootCount);
  }
  catch (...)
  {
    exceptionControl("getStorageConfig", oam::API_FAILURE);
  }

  try
  {
    SystemModuleTypeConfig systemmoduletypeconfig;
    getSystemConfig(systemmoduletypeconfig);

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

      if (moduleCount > 0 && moduletype == "pm")
      {
        deviceDBRootList = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList;
        return boost::make_tuple(storageType, SystemDBRootCount, deviceDBRootList, UMstorageType);
      }
    }
  }
  catch (...)
  {
    exceptionControl("getStorageConfig", oam::API_FAILURE);
  }

  return boost::make_tuple(storageType, SystemDBRootCount, deviceDBRootList, UMstorageType);
}

/********************************************************************
 *
 * Get PM - DBRoot Config data
 *
 ********************************************************************/
void Oam::getPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist)
{
  string module = "pm" + itoa(pmid);
  // validate Module name
  int returnStatus = validateModule(module);

  if (returnStatus != API_SUCCESS)
    exceptionControl("getPmDbrootConfig", returnStatus);

  try
  {
    ModuleConfig moduleconfig;
    getSystemConfig(module, moduleconfig);

    DBRootConfigList::iterator pt1 = moduleconfig.dbrootConfigList.begin();

    for (; pt1 != moduleconfig.dbrootConfigList.end(); pt1++)
    {
      dbrootconfiglist.push_back((*pt1));
    }
  }
  catch (...)
  {
    // dbrootid not found, return with error
    exceptionControl("getPmDbrootConfig", API_INVALID_PARAMETER);
  }
}

/********************************************************************
 *
 * Get DBRoot - PM Config data
 *
 ********************************************************************/
void Oam::getDbrootPmConfig(const int dbrootid, int& pmid)
{
  SystemModuleTypeConfig systemmoduletypeconfig;
  ModuleTypeConfig moduletypeconfig;
  ModuleConfig moduleconfig;

  try
  {
    getSystemConfig(systemmoduletypeconfig);

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

      if (moduleCount > 0 && moduletype == "pm")
      {
        DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();

        for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end(); pt++)
        {
          DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

          for (; pt1 != (*pt).dbrootConfigList.end(); pt1++)
          {
            if (*pt1 == dbrootid)
            {
              pmid = (*pt).DeviceID;
              return;
            }
          }
        }
      }
    }

    // dbrootid not found, return with error
    exceptionControl("getDbrootPmConfig", API_INVALID_PARAMETER);
  }
  catch (exception&)
  {
  }

  // dbrootid not found, return with error
  exceptionControl("getDbrootPmConfig", API_INVALID_PARAMETER);
}

/********************************************************************
 *
 * Get System DBRoot Config data
 *
 ********************************************************************/
void Oam::getSystemDbrootConfig(DBRootConfigList& dbrootconfiglist)
{
  SystemModuleTypeConfig systemmoduletypeconfig;
  ModuleTypeConfig moduletypeconfig;
  ModuleConfig moduleconfig;

  try
  {
    getSystemConfig(systemmoduletypeconfig);

    for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty())
        // end of list
        break;

      int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

      string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

      if (moduleCount > 0 && moduletype == "pm")
      {
        DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();

        for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end(); pt++)
        {
          DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

          for (; pt1 != (*pt).dbrootConfigList.end(); pt1++)
          {
            dbrootconfiglist.push_back(*pt1);
          }
        }
      }
    }

    sort(dbrootconfiglist.begin(), dbrootconfiglist.end());
  }
  catch (...)
  {
    // dbrootid not found, return with error
    exceptionControl("getSystemDbrootConfig", API_INVALID_PARAMETER);
  }

  return;
}

/***************************************************************************
 *
 * Function:  validateModule
 *
 * Purpose:   Validate Module Name
 *
 ****************************************************************************/

int Oam::validateModule(const std::string name)
{
  if (name.size() < 3)
    // invalid ID
    return API_INVALID_PARAMETER;

  string moduletype = name.substr(0, MAX_MODULE_TYPE_SIZE);
  int moduleID = atoi(name.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE).c_str());

  if (moduleID < 1)
    // invalid ID
    return API_INVALID_PARAMETER;

  SystemModuleTypeConfig systemmoduletypeconfig;

  try
  {
    getSystemConfig(systemmoduletypeconfig);
  }
  catch (...)
  {
    return API_INVALID_PARAMETER;
  }

  for (unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
  {
    if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType == moduletype)
    {
      if (systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0)
        return API_INVALID_PARAMETER;

      DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();

      for (; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
      {
        if (name == (*pt).DeviceName)
          return API_SUCCESS;
      }
    }
  }

  return API_INVALID_PARAMETER;
}

/******************************************************************************************
 * @brief	changeMyCnf
 *
 * purpose:	change my.cnf and add if value is not present to mysqld section
 *
 ******************************************************************************************/
bool Oam::changeMyCnf(std::string paramater, std::string value)
{
  string mycnfFile = std::string(MCSMYCNFDIR) + "/columnstore.cnf";
  ifstream file(mycnfFile.c_str());

  if (!file)
  {
    cout << "File not found: " << mycnfFile << endl;
    return false;
  }

  vector<string> lines;
  char line[200];
  string buf;
  bool foundIt = false;

  while (file.getline(line, 200))
  {
    buf = line;
    string::size_type pos = buf.find(paramater, 0);

    if (pos == 0)
    {
      string::size_type pos = buf.find("=", 0);

      if (pos != string::npos)
      {
        buf = paramater + " = " + value;
        foundIt = true;
      }
    }

    // output to temp file
    lines.push_back(buf);
  }

  if (!foundIt)
  {
    // Its not in the file go back to beginning of file
    file.clear();
    file.seekg(0, ios::beg);
    lines.clear();

    while (file.getline(line, 200))
    {
      buf = line;
      string::size_type pos = buf.find("[mysqld]", 0);

      if (pos != string::npos)
      {
        lines.push_back(buf);
        buf = "loose-" + paramater + " = " + value;
      }

      // output to temp file
      lines.push_back(buf);
    }
  }

  file.close();
  unlink(mycnfFile.c_str());
  ofstream newFile(mycnfFile.c_str());

  // create new file
  int fd = open(mycnfFile.c_str(), O_RDWR | O_CREAT, 0664);

  copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
  newFile.close();

  close(fd);

  return true;
}

/***************************************************************************
 * PRIVATE FUNCTIONS
 ***************************************************************************/

/***************************************************************************
 *
 * Function:  exceptionControl
 *
 * Purpose:   exception control function
 *
 ****************************************************************************/

void Oam::exceptionControl(std::string function, int returnStatus, const char* extraMsg)
{
  std::string msg;

  switch (returnStatus)
  {
    case API_INVALID_PARAMETER:
    {
      msg = "Invalid Parameter passed in ";
      msg.append(function);
      msg.append(" API");
    }
    break;

    case API_FILE_OPEN_ERROR:
    {
      msg = "File Open error from ";
      msg.append(function);
      msg.append(" API");
    }
    break;

    case API_TIMEOUT:
    {
      msg = "Timeout error from ";
      msg.append(function);
      msg.append(" API");
    }
    break;

    case API_DISABLED:
    {
      msg = "API Disabled: ";
      msg.append(function);
    }
    break;

    case API_FILE_ALREADY_EXIST:
    {
      msg = "File Already Exist";
    }
    break;

    case API_ALREADY_IN_PROGRESS:
    {
      msg = "Already In Process";
    }
    break;

    case API_FAILURE_DB_ERROR:
    {
      msg = "Database Test Error";
    }
    break;

    case API_INVALID_STATE:
    {
      msg = "Target in an invalid state";
    }
    break;

    case API_READONLY_PARAMETER:
    {
      msg = "Parameter is Read-Only, can't update";
    }
    break;

    case API_TRANSACTIONS_COMPLETE:
    {
      msg = "Finished waiting for transactions";
    }
    break;

    case API_CONN_REFUSED:
    {
      msg = "Connection refused";
    }
    break;

    case API_CANCELLED:
    {
      msg = "Operation Cancelled";
    }
    break;

    default:
    {
      msg = "API Failure return in ";
      msg.append(function);
      msg.append(" API");
    }
    break;
  }  // end of switch

  if (extraMsg)
  {
    msg.append(":\n    ");
    msg.append(extraMsg);
  }

  throw runtime_error(msg);
}

/***************************************************************************
 *
 * Function:  itoa
 *
 * Purpose:   Integer to ASCII convertor
 *
 ****************************************************************************/

std::string Oam::itoa(const int i)
{
  stringstream ss;
  ss << i;
  return ss.str();
}
}  // namespace oam
// vim:ts=4 sw=4:
