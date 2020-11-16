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


#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/version.hpp>
namespace bi = boost::interprocess;

#include "processmonitor.h"
#include "installdir.h"

#include "IDBPolicy.h"
#include "utils_utf8.h"
#include "crashtrace.h"
#include "checks.h"

using namespace std;
using namespace messageqcpp;
using namespace processmonitor;
using namespace oam;
using namespace logging;
using namespace alarmmanager;
using namespace config;
using namespace idbdatafile;

//using namespace procheartbeat;

static void* messageThread(MonitorConfig* config);
static void* statusControlThread(void*);
static void* sigchldHandleThread(void*);
static void	SIGCHLDHandler(int signal_number);
static void* chldHandleThread(MonitorConfig* config);
static void sigHupHandler(int sig);
static void* mysqlMonitorThread(MonitorConfig* config);
string systemOAM;
string dm_server;
string cloud;
string DataRedundancyConfig = "n";
bool HDFS = false;

void updateShareMemory(processStatusList* aPtr);

bool runStandby = false;
bool processInitComplete = false;
bool rootUser = true;
bool mainResumeFlag;
string USER = "root";
string PMwithUM = "n";
bool startProcMon = false;
string tmpLogDir;
string SUDO = "";

//extern std::string gOAMParentModuleName;
extern bool gOAMParentModuleFlag;

pthread_mutex_t STATUS_LOCK;

bool getshm(const string &name, int size, bi::shared_memory_object &target) {
    MonitorLog log;
    bool created = false;
    try
    {
        bi::permissions perms;
        perms.set_unrestricted();
        bi::shared_memory_object shm(bi::create_only, name.c_str(), bi::read_write, perms);
        created = true;
        shm.truncate(size);
        target.swap(shm);
    }
    catch (bi::interprocess_exception& biex)
    {
        if (biex.get_error_code() == bi::already_exists_error) {
            try {
                bi::shared_memory_object shm(bi::open_only, name.c_str(), bi::read_write);
                target.swap(shm);
            }
            catch (exception &e) {
                ostringstream os;
                os << "ProcMon failed to attach to the " << name << " shared mem segment, got " << e.what();
                log.writeLog(__LINE__, os.str(), LOG_TYPE_CRITICAL);
                exit(1);
            }
        }
        else {
            ostringstream os;
            os << "ProcMon failed to create the '" << name << "' shared mem segment, got " << biex.what() << ".";
            os << "  Check the permissions on /dev/shm; should be 1777";
            log.writeLog(__LINE__, os.str(), LOG_TYPE_CRITICAL);
            exit(1);
        }
    }
    return created;
}


/******************************************************************************************
* @brief	main
*
* purpose:	Launch boot child processes and sit on read for incoming messages
*
******************************************************************************************/
int main(int argc, char** argv)
{
#ifndef _MSC_VER
    setuid(0); // set effective ID to root; ignore return status
#endif

    struct sigaction ign;

    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = fatalHandler;
    sigaction(SIGSEGV, &ign, 0);
    sigaction(SIGABRT, &ign, 0);
    sigaction(SIGFPE, &ign, 0);

    if (argc > 1 && string(argv[1]) == "--daemon")
    {
        if (fork() != 0) return 0;

        umask(0);
        setsid();
        chdir("/");
        close(0);
        close(1);
        close(2);
        open("/dev/null", O_RDONLY);
        open("/dev/null", O_WRONLY);
        open("/dev/null", O_WRONLY);
    }

    // setup environment for using HDFS.
    idbdatafile::IDBPolicy::configIDBPolicy();

    Oam oam;
    MonitorLog log;
    MonitorConfig config;
    ProcessMonitor aMonitor(config, log);

    log.writeLog(__LINE__, " ");
    log.writeLog(__LINE__, "**********Process Monitor Started**********");
    log.writeLog(__LINE__, " ", LOG_TYPE_DEBUG);
    log.writeLog(__LINE__, "**********Process Monitor Started**********", LOG_TYPE_DEBUG);

    //Ignore SIGPIPE signals
    signal(SIGPIPE, SIG_IGN);

    //create SIGHUP handler to get configuration updates
    signal(SIGHUP, sigHupHandler);

    //check if root-user
    int user;
    user = getuid();

    if (user != 0) 
	{
        rootUser = false;
		SUDO = "sudo ";
	}
	
    char* p = getenv("USER");

    if (p && *p)
        USER = p;

    // Set locale language
    setlocale(LC_ALL, "");
    setlocale(LC_NUMERIC, "C");

    //get tmp log directory
    tmpLogDir = startup::StartUp::tmpDir();
    
    string cmd = "mkdir -p " + tmpLogDir;
    system(cmd.c_str());

    // create message thread
    pthread_t MessageThread;
    int ret = pthread_create (&MessageThread, NULL, (void*(*)(void*))&messageThread, &config);

    if ( ret != 0 )
    {
        log.writeLog(__LINE__, "pthread_create failed, exiting..., return code = " + oam.itoa(ret), LOG_TYPE_CRITICAL);
        string cmd = "columnstore stop > /dev/null 2>&1";
        system(cmd.c_str());
        exit(1);
    }

    //check if this is a fresh install, meaning the Columnstore.xml file is not setup
    //if so, wait for messages from Procmgr to start us up
    Config* sysConfig = Config::makeConfig();
    string exemgrIpadd = sysConfig->getConfig("ExeMgr1", "IPAddr");

    if ( exemgrIpadd == "0.0.0.0" )
    {
        int count = 0;

        while (true)
        {
            if ( startProcMon )
                break;
            else
            {
                count++;

                if (count > 10 )
                {
                    count = 0;
                    log.writeLog(__LINE__, "Waiting for ProcMgr to start up", LOG_TYPE_DEBUG);
                }

                sleep(1);
            }
        }

        //re-read local system info with updated Columnstore.xml
        sleep(1);
//	      Config* sysConfig = Config::makeConfig();
        MonitorConfig config;

        //PMwithUM config
        try
        {
            oam.getSystemConfig( "PMwithUM", PMwithUM);
        }
        catch (...)
        {
            PMwithUM = "n";
        }

        string modType = config.moduleType();
        
        string mysqlpw = oam.getMySQLPassword();

		string passwordOption = "";
		if ( mysqlpw != oam::UnassignedName )
			passwordOption = " --password=" + mysqlpw;


        //run the module install script
        string cmd = "columnstore_module_installer.sh --module=" + modType + " " + passwordOption + " > " + tmpLogDir + "/module_installer.log 2>&1";
        log.writeLog(__LINE__, "run columnstore_module_installer.sh", LOG_TYPE_DEBUG);
        log.writeLog(__LINE__, cmd, LOG_TYPE_DEBUG);

        int ret = system(cmd.c_str());

        if ( ret != 0 )
        {
            log.writeLog(__LINE__, "columnstore_module_installer.sh error, exiting..., return code = " + oam.itoa(ret), LOG_TYPE_CRITICAL);
            string cmd = "columnstore stop > /dev/null 2>&1";
            system(cmd.c_str());
            exit(1);
        }

        //exit to allow ProcMon to restart in a setup state
        log.writeLog(__LINE__, "restarting for a initial setup", LOG_TYPE_DEBUG);

        exit (0);
    }

    // if amazon cloud, check and update Instance IP Addresses and volumes
    try
    {
        oam.getSystemConfig( "Cloud", cloud);
        log.writeLog(__LINE__, "Cloud setting = " + cloud, LOG_TYPE_DEBUG);
    }
    catch (...) {}

	if ( cloud == "amazon-ec2" || cloud == "amazon-vpc" )
    {
        if (!aMonitor.amazonIPCheck())
        {
            log.writeLog(__LINE__, "ERROR: amazonIPCheck failed, exiting", LOG_TYPE_CRITICAL);
            sleep(2);
            string cmd = "columnstore stop > /dev/null 2>&1";
            system(cmd.c_str());
            exit(1);
        }
    }

    //get gluster config
    try
    {
        oam.getSystemConfig( "DataRedundancyConfig", DataRedundancyConfig);
    }
    catch (...)
    {
        DataRedundancyConfig = "n";
    }

    if ( DataRedundancyConfig == "y" )
    {
        system("mount -a > /dev/null 2>&1");
    }

    //hdfs / hadoop config
    string DBRootStorageType;

    try
    {
        oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
    }
    catch (...) {}

    if ( DBRootStorageType == "hdfs" )
        HDFS = true;

    //PMwithUM config
    try
    {
        oam.getSystemConfig( "PMwithUM", PMwithUM);
    }
    catch (...)
    {
        PMwithUM = "n";
    }

    //define entry if missing
    if ( gOAMParentModuleFlag )
    {
        string PrimaryUMModuleName;

        try
        {
            oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
        }
        catch (...)
        {
            sysConfig->setConfig("SystemConfig", "PrimaryUMModuleName", oam::UnassignedName);
            sysConfig->write();
        }
    }

    if ( config.moduleType() == "pm" )
    {
        if ( gOAMParentModuleFlag )
            log.writeLog(__LINE__, "ProcMon: Starting as ACTIVE Parent", LOG_TYPE_DEBUG);
        else
            log.writeLog(__LINE__, "ProcMon: Starting as NON-ACTIVE Parent", LOG_TYPE_DEBUG);
    }

    //create and mount data directories
    aMonitor.createDataDirs(cloud);

    //check if this module is recovering after a reboot for an active OAM parent state
    ByteStream msg;
    ByteStream::byte requestID = GETPARENTOAMMODULE;
    msg << requestID;

    int moduleStatus = oam::ACTIVE;

    //check if currently configured as Parent OAM Module on startup
    if ( gOAMParentModuleFlag )
    {
        try
        {
            oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
        }
        catch (...) {}

        if ( ( config.OAMStandbyName() != oam::UnassignedName ) &&
                DBRootStorageType != "internal" )
        {
            //try for 20 minutes checking if the standby node is up
            string parentOAMModule;
            log.writeLog(__LINE__, "starting has parent, double check. checking with old Standby Module", LOG_TYPE_DEBUG);
            int count = 0;

            for (; count < 120 ; count++)
            {
                parentOAMModule = aMonitor.sendMsgProcMon1( config.OAMStandbyName(), msg, requestID );

                if (  parentOAMModule != "FAILED" )
                    break;

                log.writeLog(__LINE__, "Standby PM not responding, retrying", LOG_TYPE_WARNING);
                sleep(10);
            }

            // check if standby never replied, if so, shutdown
            if ( count >= 120 )
            {
                log.writeLog(__LINE__, "Standby PM not responding, ColumnStore shutting down", LOG_TYPE_CRITICAL);
                //Set the alarm
		//		aMonitor.sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
		//		sleep (1);

                string cmd = "columnstore stop > /dev/null 2>&1";

                system(cmd.c_str());
            }

            log.writeLog(__LINE__, "Old Standby has moduleparentOAMModule = " + parentOAMModule, LOG_TYPE_DEBUG);

            if ( parentOAMModule != config.moduleName() )
            {
                gOAMParentModuleFlag = false;

                log.writeLog(__LINE__, "NOT Parent OAM Module", LOG_TYPE_DEBUG);
                log.writeLog(__LINE__, "NOT Parent OAM Module");

                try
                {
                    Config* sysConfig = Config::makeConfig();

                    // get Standby IP address
                    ModuleConfig moduleconfig;
                    oam.getSystemConfig(config.OAMStandbyName(), moduleconfig);
                    HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
                    string IPaddr = (*pt1).IPAddr;

                    sysConfig->setConfig("ProcMgr", "IPAddr", IPaddr);
                    sysConfig->setConfig("ProcMgr_Alarm", "IPAddr", IPaddr);

                    log.writeLog(__LINE__, "set ProcMgr IPaddr to Old Standby Module: " + IPaddr, LOG_TYPE_DEBUG);
					//update MariaDB ColumnStore Config table
                    try
                    {
                        sysConfig->write();
                        sleep(1);
                    }
                    catch (...)
                    {
                        log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
                    }
                }
                catch (...)
                {
                    log.writeLog(__LINE__, "ERROR: makeConfig failed", LOG_TYPE_ERROR);
                }

                // get updated Columnstore.xml and ProcessConfig.xml, retry in case ProcMgr isn't up yet
                if (!HDFS)
                {
                    int count = 0;

                    while (true)
                    {
                        try
                        {
                            oam.distributeConfigFile(config.moduleName());
                            log.writeLog(__LINE__, "Successfull return from distributeConfigFile", LOG_TYPE_DEBUG);

                            oam.distributeConfigFile(config.moduleName(), "ProcessConfig.xml");
                            log.writeLog(__LINE__, "Successfull return from distributeProcessFile", LOG_TYPE_DEBUG);
                            break;
                        }
                        catch (...)
                        {
                            count++;

                            if (count > 10 )
                            {
                                count = 0;
                                log.writeLog(__LINE__, "error return from distributeConfigFile, waiting for Active ProcMgr to start", LOG_TYPE_DEBUG);
                            }

                            sleep(1);
                        }
                    }
                }

                // not OAM parent module, delay starting until a successful get status is performed
                // makes sure the Parent OAM ProcMon is fully ready
                while (true)
                {
                    try
                    {
                        bool degraded;
                        oam.getModuleStatus(config.moduleName(), moduleStatus, degraded);

                        // if HDFS, wait until module state is MAN_INIT before continuing
                        if (HDFS)
                        {
                            if ( moduleStatus == oam::MAN_INIT)
                                break;
                        }

                        break;
                    }
                    catch (...)
                    {
                        log.writeLog(__LINE__, "waiting for good return from getModuleStatus", LOG_TYPE_DEBUG);
                        sleep (1);
                    }
                }
            }
        }
    }
    else
    {
        // not active Parent, get updated Columnstore.xml, retry in case ProcMgr isn't up yet
        if (!HDFS)
        {
            int count = 0;

            while (true)
            {
                try
                {
                    oam.distributeConfigFile(config.moduleName());
                    log.writeLog(__LINE__, "Successfull return from distributeConfigFile", LOG_TYPE_DEBUG);

                    oam.distributeConfigFile(config.moduleName(), "ProcessConfig.xml");
                    log.writeLog(__LINE__, "Successfull return from distributeProcessFile", LOG_TYPE_DEBUG);

                    break;
                }
                catch (...)
                {
                    count++;

                    if (count > 10 )
                    {
                        count = 0;
                        log.writeLog(__LINE__, "error return from distributeConfigFile, waiting for Active ProcMgr to start", LOG_TYPE_DEBUG);
                    }

                    sleep(1);
                }
            }
        }

        // not OAM parent module, delay starting until a successful get status is performed
        // makes sure the Parent OAM ProcMon is fully ready
        while (true)
        {
            try
            {
                bool degraded;
                oam.getModuleStatus(config.moduleName(), moduleStatus, degraded);

                // if HDFS, wait until module state is MAN_INIT before continuing
                if (HDFS)
                {
                    if ( moduleStatus == oam::MAN_INIT)
                        break;
                }

                break;
            }
            catch (...)
            {
                log.writeLog(__LINE__, "waiting for good return from getModuleStatus", LOG_TYPE_DEBUG);
                sleep (1);
            }
        }
    }

    // this will occur on non-distributed installs the first time ProcMon runs
    if ( config.OAMParentName() == oam::UnassignedName )
    {
        cerr << endl << "OAMParentModuleName == oam::UnassignedName, exiting " << endl;
        log.writeLog(__LINE__, "OAMParentModuleName == oam::UnassignedName, restarting");
        exit (1);
    }

    //check if module is in a DISABLED state
    bool DISABLED = false;

    if ( moduleStatus == oam::MAN_DISABLED ||
            moduleStatus == oam::AUTO_DISABLED )
        DISABLED = true;

    if ( config.moduleType() == "pm" )
    {
        int retry = 0;

        for (  ; retry < 20 ; retry++ )
        {
            int ret = aMonitor.checkDataMount();

            if ( ret == oam::API_SUCCESS)
                break;

            if (ret == API_INVALID_PARAMETER)
            {
                //no dbroots assigned, treat as disabled
                if ( !DISABLED )
                    DISABLED = true;
            }

            if ( DISABLED )
            {
                log.writeLog(__LINE__, "ERROR: checkDataMount to failed, module is disabled, continuing", LOG_TYPE_WARNING);
                break;
            }
            else
                log.writeLog(__LINE__, "ERROR: checkDataMount to failed, retrying", LOG_TYPE_WARNING);

            //send notification about the mount setup failure
            oam.sendDeviceNotification(config.moduleName(), DBROOT_MOUNT_FAILURE);
            sleep(30);
        }

        if ( retry == 20 )
        {
            log.writeLog(__LINE__, "Check DB mounts failed, shutting down", LOG_TYPE_CRITICAL);

            //Set the alarm
		//	aMonitor.sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
		//	sleep (1);
            string cmd = "columnstore stop > /dev/null 2>&1";
            system(cmd.c_str());
        }

        if ( !gOAMParentModuleFlag )
        {
            runStandby = true;
            // delete any old active alarm log file
            unlink ("/var/log/mariadb/columnstore/activeAlarms");
        }

        //Clear mainResumeFlag

        mainResumeFlag = false;

        //launch Status table control thread on 'pm' modules
        pthread_t statusThread;
        int ret = pthread_create (&statusThread, NULL, &statusControlThread, NULL);

        if ( ret != 0 )
            log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);

        //wait for flag to be set

        while (!mainResumeFlag)
        {
            log.writeLog(__LINE__, "WAITING FOR mainResumeFlag to be set", LOG_TYPE_DEBUG);

            sleep(1);
        }
    }

    SystemStatus systemstatus;

    try
    {
        oam.getSystemStatus(systemstatus, false);
    }
    catch (...)
    {
    }

    // determine Standby OAM Module, if needed
    if ( gOAMParentModuleFlag &&
            config.OAMStandbyName() == oam::UnassignedName &&
            config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM )
    {


        string standbyPM = "";

        //check if gluster, if so then find PMs that have copies of DBROOT #1
        string pmList = "";

        if (DataRedundancyConfig == "y")
        {

            try
            {
                string errmsg;
                oam.glusterctl(oam::GLUSTER_WHOHAS, "1", pmList, errmsg);

                log.writeLog(__LINE__, "glusterctl called :" + pmList, LOG_TYPE_DEBUG);

                boost::char_separator<char> sep(" ");
                boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);

                for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
                        it != tokens.end();
                        ++it)
                {
                    string pm = "pm" + *it;

                    // skip if current module
                    if ( pm == config.moduleName() )
                        continue;

                    int opState;
                    bool degraded;

                    try
                    {
                        oam.getModuleStatus(pm, opState, degraded);
                    }
                    catch (...)
                    {}

                    if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
                    {
                        continue;
                    }
                    else
                    {
                        standbyPM = pm;
                        break;
                    }
                }

            }
            catch (...)
            {}
        }
        else
        {
            for ( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
            {
                string moduleName = systemstatus.systemmodulestatus.modulestatus[i].Module;

                if ( moduleName.substr(0, MAX_MODULE_TYPE_SIZE) == "pm" &&
                        moduleName != config.moduleName() )
                {
                    // multi pm system
                    int moduleStatus = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;

                    if ( moduleStatus != oam::MAN_DISABLED &&
                            moduleStatus != oam::AUTO_DISABLED )
                    {

                        standbyPM = moduleName;
                        break;
                    }
                }
            }
        }

        if ( standbyPM != "" )
        {
            // found a standby candidate
            oam.setSystemConfig("StandbyOAMModuleName", standbyPM);

            // update Standby IP Address
            ModuleConfig moduleconfig;
            oam.getSystemConfig(standbyPM, moduleconfig);
            HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
            string standbyIPaddr = (*pt1).IPAddr;

            Config* sysConfig2 = Config::makeConfig();

            sysConfig2->setConfig("ProcStatusControlStandby", "IPAddr", standbyIPaddr);
            sysConfig2->write();

            oam.setHotStandbyPM(standbyIPaddr);

            log.writeLog(__LINE__, "Columnstore.xml Standby OAM updated : " + standbyPM + ":" + standbyIPaddr, LOG_TYPE_DEBUG);
            log.writeLog(__LINE__, "Set Standby Module = " + standbyPM, LOG_TYPE_DEBUG);

            try
            {
                oam.distributeConfigFile(config.moduleName());
                log.writeLog(__LINE__, "successfull return from distributeConfigFile", LOG_TYPE_DEBUG);
            }
            catch (...)
            {}
        }
    }

    // non Parent Module, don't start until process-manager is up on parent module
    // away to control starting mutliple Active Process-Managers
    if ( !gOAMParentModuleFlag && config.moduleType() == "pm" )
    {
        string parentOAMModuleName;

        while (true)
        {
            try
            {
                Config* sysConfig = Config::makeConfig();
                parentOAMModuleName = sysConfig->getConfig("SystemConfig", "ParentOAMModuleName");

                if ( parentOAMModuleName != oam::UnassignedName )
                    break;

                sleep(1);
                log.writeLog(__LINE__, "Waiting for process-manager on parent module", LOG_TYPE_ERROR);
            }
            catch (...)
            {
                log.writeLog(__LINE__, "Problem getting the ParentOAMModuleName key from the Columnstore System Configuration file", LOG_TYPE_CRITICAL);
                exit(1);
            }
        }

        while (true)
        {
            try
            {
                Oam oam;
                ProcessStatus procstat;
                oam.getProcessStatus("ProcessManager", parentOAMModuleName, procstat);

                if ( procstat.ProcessOpState == oam::ACTIVE )
                    break;

                sleep(1);
                log.writeLog(__LINE__, "Waiting for process-manager to go ACTIVE", LOG_TYPE_DEBUG);
            }
            catch (exception& ex)
            {
//				string error = ex.what();
//				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
            }
            catch (...)
            {
//				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
            }
        }
    }

    //Mark this process AUTO-OFFLINE
    aMonitor.updateProcessInfo("ProcessMonitor", oam::AUTO_OFFLINE, getpid());

    //handle SIGCHLD signal
    pthread_t signalThread;
    ret = pthread_create (&signalThread, NULL, &sigchldHandleThread, NULL);

    if ( ret != 0 )
        log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);

    //mysqld status monitor thread
    if ( config.moduleType() == "um" ||
            ( config.moduleType() == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ||
            ( config.moduleType() == "pm" && PMwithUM == "y") )
    {
        pthread_t mysqlThread;
        ret = pthread_create (&mysqlThread, NULL, (void*(*)(void*))&mysqlMonitorThread, NULL);

        if ( ret != 0 )
            log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);
    }

    //update syslog file priviledges
    aMonitor.changeModLog();

    //Read ProcessConfig file to get process list belong to this process monitor
    SystemProcessConfig systemprocessconfig;

    try
    {
        oam.getProcessConfig(systemprocessconfig);
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    string OAMParentModuleType = config.OAMParentName().substr(0, 2);

    //Build a map for application name tag and launch ID for this Process-Monitor
    for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
    {
        //skip if both BootLaunch and LaunchID are 0
        if ( systemprocessconfig.processconfig[i].BootLaunch == 0 &&
                systemprocessconfig.processconfig[i].LaunchID == 0 )
            continue;

        if ( (systemprocessconfig.processconfig[i].ModuleType == config.moduleType() ) ||
                ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                  config.moduleType() == "pm" && PMwithUM == "y") ||
                ( systemprocessconfig.processconfig[i].ModuleType == "ChildExtOAMModule") ||
                ( systemprocessconfig.processconfig[i].ModuleType == "ChildOAMModule" ) ||
                ( systemprocessconfig.processconfig[i].ModuleType == "ParentOAMModule" &&
                  config.moduleType() == OAMParentModuleType ) )
        {
            // If Process Monitor, update local state
            if ( systemprocessconfig.processconfig[i].ProcessName == "ProcessMonitor")
            {
                config.buildList(systemprocessconfig.processconfig[i].ModuleType,
                                 systemprocessconfig.processconfig[i].ProcessName,
                                 systemprocessconfig.processconfig[i].ProcessLocation,
                                 systemprocessconfig.processconfig[i].ProcessArgs,
                                 systemprocessconfig.processconfig[i].LaunchID,
                                 getpid(),
                                 oam::AUTO_OFFLINE,
                                 systemprocessconfig.processconfig[i].BootLaunch,
                                 systemprocessconfig.processconfig[i].RunType,
                                 systemprocessconfig.processconfig[i].DepProcessName,
                                 systemprocessconfig.processconfig[i].DepModuleName,
                                 systemprocessconfig.processconfig[i].LogFile);
            }
            else
            {
                if ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                        config.moduleType() == "pm" && PMwithUM == "y" &&
                        systemprocessconfig.processconfig[i].ProcessName == "DMLProc" )
                    continue;


                if ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                        config.moduleType() == "pm" && PMwithUM == "y" &&
                        systemprocessconfig.processconfig[i].ProcessName == "DDLProc" )
                    continue;

                // Get Last Known Process Status and PID
                int state = oam::AUTO_OFFLINE;
                int PID = 0;

                try
                {
                    Oam oam;
                    ProcessStatus procstat;
                    oam.getProcessStatus(systemprocessconfig.processconfig[i].ProcessName, config.moduleName(), procstat);
                    state = procstat.ProcessOpState;
                    PID = procstat.ProcessID;
                }
                catch (exception& ex)
                {
//					string error = ex.what();
//					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                }
                catch (...)
                {
//					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                }

                config.buildList(systemprocessconfig.processconfig[i].ModuleType,
                                 systemprocessconfig.processconfig[i].ProcessName,
                                 systemprocessconfig.processconfig[i].ProcessLocation,
                                 systemprocessconfig.processconfig[i].ProcessArgs,
                                 systemprocessconfig.processconfig[i].LaunchID,
                                 PID,
                                 state,
                                 systemprocessconfig.processconfig[i].BootLaunch,
                                 systemprocessconfig.processconfig[i].RunType,
                                 systemprocessconfig.processconfig[i].DepProcessName,
                                 systemprocessconfig.processconfig[i].DepModuleName,
                                 systemprocessconfig.processconfig[i].LogFile);
            }
        }
    }

    log.writeLog(__LINE__, "SYSTEM STATUS = " + oam.itoa(systemstatus.SystemOpState), LOG_TYPE_DEBUG);

    if ( systemstatus.SystemOpState != MAN_OFFLINE && !DISABLED)
    {

        // Loop through the process list to check the process current state
        // Launch the Processes controlled by the Process-Monitor
        processList::iterator listPtr;
        processList* aPtr = config.monitoredListPtr();
        listPtr = aPtr->begin();

        for (; listPtr != aPtr->end(); ++listPtr)
        {
            // If Process Monitor, skip
            if ( (*listPtr).ProcessName == "ProcessMonitor")
                continue;

            if ((*listPtr).processID != 0)
            {
                if ((*listPtr).BootLaunch == BOOT_LAUNCH)
                {
                    //Check for SIMPLEX runtype processes
                    int initType = aMonitor.checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

                    if ( initType == oam::COLD_STANDBY )
                    {
                        //there is a mate active, skip
                        (*listPtr).state = oam::COLD_STANDBY;
                        //	sleep(1);
                        continue;
                    }
                    else if ( initType == oam::MAN_INIT )
                        initType = oam::AUTO_INIT;

                    //Check the process current state
                    if ((kill((*listPtr).processID, 0)) != 0
                            && (*listPtr).state != oam::MAN_OFFLINE)
                    {
                        //The process died, start the process, reset the pid and time

                        //Set the alarm
                        aMonitor.sendAlarm((*listPtr).ProcessName.c_str(), PROCESS_DOWN_AUTO, SET);

                        //stop the process first to make sure it's gone
                        aMonitor.stopProcess((*listPtr).processID,
                                             (*listPtr).ProcessName,
                                             (*listPtr).ProcessLocation,
                                             oam::FORCEFUL,
                                             false);

                        //Start the process
                        (*listPtr).processID = aMonitor.startProcess( (*listPtr).ProcessModuleType,
                                               (*listPtr).ProcessName,
                                               (*listPtr).ProcessLocation,
                                               (*listPtr).ProcessArgs,
                                               (*listPtr).launchID,
                                               (*listPtr).BootLaunch,
                                               (*listPtr).RunType,
                                               (*listPtr).DepProcessName,
                                               (*listPtr).DepModuleName,
                                               (*listPtr).LogFile,
                                               initType);

                        // StorageManager doesn't send the "I'm online" msg to Proc*.
                        // Just mark it active for now.  TODO: make it use the ping fcn in IDB* instead.
                        if (listPtr->ProcessName == "StorageManager")
                            oam.setProcessStatus("StorageManager", boost::get<0>(oam.getModuleInfo()), 
                              oam::ACTIVE, listPtr->processID);
                                               
                        string restartStatus;

                        if ( (*listPtr).processID == oam::API_MINOR_FAILURE ||
                                (*listPtr).processID == oam::API_FAILURE )
                            // restart failed
                            string restartStatus = " restart failed!!";
                        else
                            string restartStatus = " restarted successfully!!";

                        log.writeLog(__LINE__, restartStatus, LOG_TYPE_INFO);
                    }
                }
            }
            else if ((*listPtr).BootLaunch == BOOT_LAUNCH)
            {
                //Check for SIMPLEX runtype processes
                int initType = aMonitor.checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

                if ( initType == oam::COLD_STANDBY )
                {
                    //there is a mate active, skip
                    (*listPtr).state = oam::COLD_STANDBY;
                    sleep(1);
                    continue;
                }
                else if ( initType == oam::MAN_INIT )
                    initType = oam::AUTO_INIT;

                if ((*listPtr).state == oam::MAN_OFFLINE)
                    continue;

                //stop the process first to make sure it's gone
                aMonitor.stopProcess((*listPtr).processID,
                                     (*listPtr).ProcessName,
                                     (*listPtr).ProcessLocation,
                                     oam::FORCEFUL,
                                     false);

                //Start the boot time processes, set its state, ProcessID
                (*listPtr).processID = aMonitor.startProcess((*listPtr).ProcessModuleType,
                                       (*listPtr).ProcessName,
                                       (*listPtr).ProcessLocation,
                                       (*listPtr).ProcessArgs,
                                       (*listPtr).launchID,
                                       (*listPtr).BootLaunch,
                                       (*listPtr).RunType,
                                       (*listPtr).DepProcessName,
                                       (*listPtr).DepModuleName,
                                       (*listPtr).LogFile,
                                       initType);

                // StorageManager doesn't send the "I'm online" msg to Proc*.
                // Just mark it active for now.  TODO: make it use the ping fcn in IDB* instead.
                if (listPtr->ProcessName == "StorageManager")
                    oam.setProcessStatus("StorageManager", boost::get<0>(oam.getModuleInfo()), 
                    oam::ACTIVE, listPtr->processID);
                                       
                string restartStatus;

                if ( (*listPtr).processID == oam::API_MINOR_FAILURE ||
                        (*listPtr).processID == oam::API_FAILURE )
                    // restart failed
                    string restartStatus = " restart failed!!";
                else
                    string restartStatus = " restarted successfully!!";

                log.writeLog(__LINE__, restartStatus, LOG_TYPE_INFO);
            }
        } //end of for loop
    }

    // create process health (monitor) thread
    pthread_t processHealthThread;
    ret = pthread_create (&processHealthThread, NULL, (void*(*)(void*))&chldHandleThread, &config);

    if ( ret != 0 )
        log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);

    //Mark this process Init Complete
    while (true)
    {
        try
        {
            oam.processInitComplete("ProcessMonitor");
            log.writeLog(__LINE__, "processInitComplete Successfully Called", LOG_TYPE_DEBUG);
        }
        catch (exception& ex)
        {
            string error = ex.what();
            log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: " + error, LOG_TYPE_ERROR);
            // this would fail if Parent OAM Node is down
            sleep(1);
            continue;
        }
        catch (...)
        {
            log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: Caught unknown exception!", LOG_TYPE_ERROR);
            // this would fail if Parent OAM Node is down
            sleep(1);
            continue;
        }

        for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
        {
            // If Process Monitor, update local state
            if ( systemprocessconfig.processconfig[i].ProcessName == "ProcessMonitor")
            {
                config.buildList(systemprocessconfig.processconfig[i].ModuleType,
                                 systemprocessconfig.processconfig[i].ProcessName,
                                 systemprocessconfig.processconfig[i].ProcessLocation,
                                 systemprocessconfig.processconfig[i].ProcessArgs,
                                 systemprocessconfig.processconfig[i].LaunchID,
                                 getpid(),
                                 oam::ACTIVE,
                                 systemprocessconfig.processconfig[i].BootLaunch,
                                 systemprocessconfig.processconfig[i].RunType,
                                 systemprocessconfig.processconfig[i].DepProcessName,
                                 systemprocessconfig.processconfig[i].DepModuleName,
                                 systemprocessconfig.processconfig[i].LogFile);
                break;
            }
        }

        break;

        //Clear the alarms
        aMonitor.sendAlarm("ProcessMonitor", PROCESS_DOWN_MANUAL, CLEAR);
        aMonitor.sendAlarm("ProcessMonitor", PROCESS_DOWN_AUTO, CLEAR);
    }

    //set process init complete and ready to process message request
    processInitComplete = true;

    // suspend forever
    while (true)
    {
        sleep(1000);
    }
}

/******************************************************************************************
* @brief	messageThread
*
* purpose:	Read incoming messages
*
******************************************************************************************/
static void* messageThread(MonitorConfig* config)
{
    //ProcMon log file
    MonitorLog log;
    assert(config);
    ProcessMonitor aMonitor(*config, log);
    log.writeLog(__LINE__, "Message Thread started ..", LOG_TYPE_DEBUG);
    Oam oam;

    string msgPort = config->moduleName() + "_ProcessMonitor";
    string port = "";

    //ProcMon will wait for request
    IOSocket fIos;
    Config* sysConfig = Config::makeConfig();

    //read and cleanup port before trying to use
    try
    {
        port = sysConfig->getConfig(msgPort, "Port");
    }
    catch (...)
    {}

    //check if enter doesnt exist, if not use pm1's
    if (port.empty() or port == "" )
    {
        msgPort = "pm1_ProcessMonitor";
        port = sysConfig->getConfig(msgPort, "Port");
    }

    log.writeLog(__LINE__, "PORTS: " + msgPort + "/" + port, LOG_TYPE_DEBUG);

    string cmd = "fuser -k " + port + "/tcp >/dev/null 2>&1";

    system(cmd.c_str());

    for (;;)
    {
        try
        {
            ByteStream msg;
            MessageQueueServer mqs(msgPort);

            for (;;)
            {
                try
                {
                    fIos = mqs.accept();

                    try
                    {
                        msg = fIos.read();

                        if (msg.length() > 0)
                        {
                            aMonitor.processMessage(msg, fIos);
                        }
                    }
                    catch (exception& ex)
                    {
                        string error = ex.what();
//						log.writeLog(__LINE__, "EXCEPTION ERROR on fIos.read() for " + msgPort + ", error: " + error, LOG_TYPE_ERROR);
                    }
                    catch (...)
                    {
//						log.writeLog(__LINE__, "EXCEPTION ERROR on fIos.read() for " + msgPort + ", Caught unknown exception!", LOG_TYPE_ERROR);
                    }
                }
                catch (exception& ex)
                {
                    string error = ex.what();
//					log.writeLog(__LINE__, "EXCEPTION ERROR on mqs.accept() for " + msgPort + ", error: " + error, LOG_TYPE_ERROR);
                }
                catch (...)
                {
//					log.writeLog(__LINE__, "EXCEPTION ERROR on mqs.accept() for " + msgPort + ", Caught unknown exception!", LOG_TYPE_ERROR);
                }

                // give time to allow Mgr to read any acks before closing
                sleep(1);
                fIos.close();
            }
        }
        catch (exception& ex)
        {
            string error = ex.what();
            log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for " + msgPort + ": " + error, LOG_TYPE_ERROR);

            // takes 2 - 4 minites to free sockets, sleep and retry
            sleep(1);
        }
        catch (...)
        {
            log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for " + msgPort + ": Caught unknown exception!", LOG_TYPE_ERROR);

            // takes 2 - 4 minites to free sockets, sleep and retry
            sleep(1);
        }
    }

    return NULL;
}

/******************************************************************************************
* @brief	mysqlMonitorThread
*
* purpose:	monitor mysqld by getting status
*
******************************************************************************************/
static void* mysqlMonitorThread(MonitorConfig* config)
{
    MonitorLog log;
    assert(config);
    ProcessMonitor aMonitor(*config, log);
    log.writeLog(__LINE__, "mysqld Monitoring Thread started ..", LOG_TYPE_DEBUG);
    Oam oam;

    while (true)
    {
        //read status, whichs set process status
        try
        {
            oam.actionMysqlCalpont(MYSQL_STATUS);
        }
        catch (...)
        {}

        sleep(5);
    }

    return NULL;
}

/******************************************************************************************
* @brief	sigchldHandleThread / SIGCHLDHandler
*
* purpose:	Catch and process dieing child processes
*
******************************************************************************************/
static void* sigchldHandleThread(void*)
{
    struct sigaction sigchld_action;
    memset (&sigchld_action, 0, sizeof (sigchld_action));
    sigchld_action.sa_handler = &SIGCHLDHandler;
    sigaction(SIGCHLD, &sigchld_action, NULL);
    return NULL;
}

static void	SIGCHLDHandler(int signal_number)
{
    int status;

    waitpid(-1, &status, WNOHANG);

    return;
}

/******************************************************************************************
* @brief	chldHandleThread
*
* purpose:	Monitor and process dieing Non SIGCHILD SNMP child processes
*			Also validate the internal Process status with the Process-Status disk file
*
******************************************************************************************/
static void* chldHandleThread(MonitorConfig* config)
{
    //ProcMon log file
    MonitorLog log;
    assert(config);
    ProcessMonitor aMonitor(*config, log);
    log.writeLog(__LINE__, "Child Process Monitoring Thread started ..", LOG_TYPE_DEBUG);
    Oam oam;
    SystemProcessStatus systemprocessstatus;

    //Loop through the process list to check the process current state
    processList::iterator listPtr;
    processList* aPtr = config->monitoredListPtr();

    //get dbhealth flag
    string DBFunctionalMonitorFlag;

    try
    {
        oam.getSystemConfig( "DBFunctionalMonitorFlag", DBFunctionalMonitorFlag);
    }
    catch (...) {}

    int delayCount = 0;

    while (true)
    {
        //get process restart configured settings
        int processRestartCount = 10;
        int processRestartPeriod = 120;

        try
        {
            oam.getSystemConfig("ProcessRestartCount", processRestartCount);
            oam.getSystemConfig("ProcessRestartPeriod", processRestartPeriod);
        }
        catch (...)
        {
            processRestartCount = 10;
            processRestartPeriod = 120;
        }

        listPtr = aPtr->begin();

        for (; listPtr != aPtr->end(); ++listPtr)
        {
            // compare internal process state and PID with system process status
            // Issue alarm if system state is INIT for longer than 1 minute
            // Update internal process state when in INIT and System is ACTIVE/FAILED
            // Updated System process state when AOS and different from internal
            int outOfSyncCount = 0;

            if ( delayCount == 2 )
            {
                while (true)
                {
                    int state = (*listPtr).state;	//set as default
                    int PID = (*listPtr).processID;	//set as default

                    try
                    {
                        ProcessStatus procstat;
                        oam.getProcessStatus((*listPtr).ProcessName, config->moduleName(), procstat);
                        state = procstat.ProcessOpState;
                        PID = procstat.ProcessID;

                        if (state == oam::BUSY_INIT )
                        {
                            // updated local state ot BUSY_INIT
                            (*listPtr).state = state;
                            break;
                        }

                        if ( (state == oam::AUTO_INIT && (*listPtr).state == oam::AUTO_INIT) ||
                                (state == oam::MAN_INIT && (*listPtr).state == oam::MAN_INIT) )
                        {
                            // get current time in seconds
                            time_t cal;
                            time (&cal);

                            if ( (cal - (*listPtr).currentTime) > 20 )
                            {
                                // issue ALARM and update status to FAILED
                                aMonitor.sendAlarm((*listPtr).ProcessName, PROCESS_INIT_FAILURE, SET);
//								(*listPtr).state = oam::FAILED;
//								aMonitor.updateProcessInfo((*listPtr).ProcessName, oam::FAILED, (*listPtr).processID);

                                //force restart the un-initted process
                                log.writeLog(__LINE__, (*listPtr).ProcessName + "/" + oam.itoa((*listPtr).processID) + " failed to init in 20 seconds, force killing it so it can restart", LOG_TYPE_CRITICAL);

                                //skip killing 0 or 1
                                if ( (*listPtr).processID > 1 )
                                    kill((*listPtr).processID, SIGKILL);

                                break;
                            }

                            break;
                        }
                    }
                    catch (exception& ex)
                    {
                        string error = ex.what();
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                        break;
                    }
                    catch (...)
                    {
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                        break;
                    }

                    if (state != (*listPtr).state || PID != (*listPtr).processID)
                    {
                        if ( state == oam::STANDBY && (*listPtr).state == oam::ACTIVE )
                            break;
                        else
                        {
                            if ( (state == oam::ACTIVE && (*listPtr).state == oam::AUTO_INIT) ||
                                    (state == oam::ACTIVE && (*listPtr).state == oam::MAN_INIT) ||
                                    (state == oam::ACTIVE && (*listPtr).state == oam::STANDBY) ||
                                    (state == oam::ACTIVE && (*listPtr).state == oam::INITIAL) ||
                                    (state == oam::ACTIVE && (*listPtr).state == oam::STANDBY_INIT) ||
                                    (state == oam::ACTIVE && (*listPtr).state == oam::BUSY_INIT) ||
                                    (state == oam::STANDBY && (*listPtr).state == oam::AUTO_INIT) ||
                                    (state == oam::STANDBY && (*listPtr).state == oam::MAN_INIT) ||
                                    (state == oam::STANDBY && (*listPtr).state == oam::INITIAL) ||
                                    (state == oam::STANDBY && (*listPtr).state == oam::BUSY_INIT) ||
                                    (state == oam::STANDBY && (*listPtr).state == oam::STANDBY_INIT) )
                            {
                                // updated local state to ACTIVE
                                (*listPtr).state = state;
                                break;
                            }

                            if ( (state == oam::FAILED && (*listPtr).state == oam::AUTO_INIT) ||
                                    (state == oam::FAILED && (*listPtr).state == oam::BUSY_INIT) ||
                                    (state == oam::FAILED && (*listPtr).state == oam::MAN_INIT) )
                            {
                                // issue ALARM and update local status to FAILED
                                log.writeLog(__LINE__, (*listPtr).ProcessName + " failed initialization", LOG_TYPE_WARNING);
                                aMonitor.sendAlarm((*listPtr).ProcessName, PROCESS_INIT_FAILURE, SET);
                                (*listPtr).state = state;

                                //setModule status to failed
                                try
                                {
                                    oam.setModuleStatus(config->moduleName(), oam::FAILED);
                                }
                                catch (exception& ex)
                                {
                                    string error = ex.what();
//									log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                                }
                                catch (...)
                                {
//									log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                                }

                                break;
                            }

                            if (state == oam::AUTO_OFFLINE || state == oam::INITIAL ||
                                    PID != (*listPtr).processID)
                            {
                                //due to a small window, only process if out-of-sync for more than 1 second
                                outOfSyncCount++;

                                if ( outOfSyncCount == 2 )
                                {
                                    // out of sync, update with internal state/PID
                                    log.writeLog(__LINE__, "State out-of-sync, update on " + (*listPtr).ProcessName + "/" + oam.itoa((*listPtr).state) + "/" + oam.itoa((*listPtr).processID), LOG_TYPE_DEBUG);

                                    aMonitor.updateProcessInfo((*listPtr).ProcessName, (*listPtr).state, (*listPtr).processID);
                                    break;
                                }

                                sleep(1);
                            }
                            else
                                break;
                        }
                    }
                    else
                        break;
                }
            }

            //Handle died or out of sync process if in the right state
            if ( (*listPtr).state == oam::MAN_OFFLINE )
                //skip
                continue;

            //log.writeLog(__LINE__, "check status " + (*listPtr).ProcessName + "/" + oam.itoa((*listPtr).processID) + " " +  oam.itoa(kill((*listPtr).processID, 0)) + " " + oam.itoa((*listPtr).state) , LOG_TYPE_CRITICAL);
            if (  ( (kill((*listPtr).processID, 0)) != 0 && (*listPtr).state == oam::ACTIVE ) ||
                    ( (kill((*listPtr).processID, 0)) != 0 && (*listPtr).state == oam::STANDBY ) ||
                    ( (kill((*listPtr).processID, 0)) != 0 && (*listPtr).state == oam::MAN_INIT ) ||
                    ( (kill((*listPtr).processID, 0)) != 0 && (*listPtr).state == oam::BUSY_INIT ) ||
                    ( (kill((*listPtr).processID, 0)) != 0 && (*listPtr).state == oam::AUTO_INIT &&
                      (*listPtr).processID != 0 ) ||
                    ( (*listPtr).state == oam::ACTIVE && (*listPtr).processID == 0 ) )
            {
				log.writeLog(__LINE__, "*****MariaDB ColumnStore Process Restarting: " + (*listPtr).ProcessName + ", old PID = " + oam.itoa((*listPtr).processID), LOG_TYPE_CRITICAL);

                if ( (*listPtr).dieCounter >= processRestartCount ||
                        processRestartCount == 0)
                {
                    // don't restart it
                    config->buildList((*listPtr).ProcessModuleType,
                                      (*listPtr).ProcessName,
                                      (*listPtr).ProcessLocation,
                                      (*listPtr).ProcessArgs,
                                      (*listPtr).launchID,
                                      0,
                                      oam::AUTO_OFFLINE,
                                      (*listPtr).BootLaunch,
                                      (*listPtr).RunType,
                                      (*listPtr).DepProcessName,
                                      (*listPtr).DepModuleName,
                                      (*listPtr).LogFile);

                    //Set the alarm
                    aMonitor.sendAlarm((*listPtr).ProcessName, PROCESS_DOWN_AUTO, SET);

                    //Update ProcessConfig file
                    aMonitor.updateProcessInfo((*listPtr).ProcessName, oam::AUTO_OFFLINE, 0);

                    //Log this event
                    if ( processRestartCount == 0)
                        log.writeLog(__LINE__, "*****Process not restarted, restart count set to 0: " + (*listPtr).ProcessName, LOG_TYPE_CRITICAL);
                    else
                        log.writeLog(__LINE__, "*****Process continually dying, stopped trying to restore it: " + (*listPtr).ProcessName, LOG_TYPE_CRITICAL);

                    //setModule status to degraded
                    try
                    {
                        bool degraded;
                        int moduleStatus;
                        oam.getModuleStatus(config->moduleName(), moduleStatus, degraded);

                        if ( moduleStatus == oam::ACTIVE)
                        {
                            try
                            {
                                oam.setModuleStatus(config->moduleName(), oam::DEGRADED);
                            }
                            catch (exception& ex)
                            {
                                string error = ex.what();
                                log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                            }
                            catch (...)
                            {
                                log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                            }

                        }
                    }
                    catch (...)
                    {}

					// check if Mdoule failover is needed due to process outage
					aMonitor.checkModuleFailover((*listPtr).ProcessName);

                    //check the db health
                    if (DBFunctionalMonitorFlag == "y" )
                    {
                        log.writeLog(__LINE__, "Call the check DB Functional API", LOG_TYPE_DEBUG);

                        try
                        {
                            oam.checkDBFunctional();
                            log.writeLog(__LINE__, "check DB Functional passed", LOG_TYPE_DEBUG);
                        }
                        catch (...)
                        {
                            log.writeLog(__LINE__, "check DB Functional FAILED", LOG_TYPE_ERROR);
                        }
                    }
                }
                else
                {
                    time_t cal;
                    time (&cal);

                    if (  (cal - (*listPtr).currentTime) > (int) processRestartPeriod )
                        (*listPtr).dieCounter = 0;
                    else
                        ++(*listPtr).dieCounter;

                    int initStatus = oam::AUTO_INIT;

                    if ( (*listPtr).RunType == oam::ACTIVE_STANDBY && runStandby)
                        initStatus = oam::STANDBY;

                    //record the process information into processList
                    config->buildList((*listPtr).ProcessModuleType,
                                      (*listPtr).ProcessName,
                                      (*listPtr).ProcessLocation,
                                      (*listPtr).ProcessArgs,
                                      (*listPtr).launchID,
                                      0,
                                      oam::AUTO_OFFLINE,
                                      (*listPtr).BootLaunch,
                                      (*listPtr).RunType,
                                      (*listPtr).DepProcessName,
                                      (*listPtr).DepModuleName,
                                      (*listPtr).LogFile);

                    //Set the alarm
                    aMonitor.sendAlarm((*listPtr).ProcessName, PROCESS_DOWN_AUTO, SET);

                    int i = 0;
                    string restartStatus;

                    for (  ; i < 10 ; i++ )
                    {
                        //stop the process first to make sure it's gone
                        aMonitor.stopProcess((*listPtr).processID,
                                             (*listPtr).ProcessName,
                                             (*listPtr).ProcessLocation,
                                             oam::FORCEFUL,
                                             false);

                        //Start the process
                        (*listPtr).processID = aMonitor.startProcess( (*listPtr).ProcessModuleType,
                                               (*listPtr).ProcessName,
                                               (*listPtr).ProcessLocation,
                                               (*listPtr).ProcessArgs,
                                               (*listPtr).launchID,
                                               (*listPtr).BootLaunch,
                                               (*listPtr).RunType,
                                               (*listPtr).DepProcessName,
                                               (*listPtr).DepModuleName,
                                               (*listPtr).LogFile,
                                               initStatus);

                        // StorageManager doesn't send the "I'm online" msg to Proc*.
                        // Just mark it active for now.  TODO: make it use the ping fcn in IDB* instead.
                        if (listPtr->ProcessName == "StorageManager")
                            oam.setProcessStatus("StorageManager", boost::get<0>(oam.getModuleInfo()), 
                              oam::ACTIVE, listPtr->processID);
              
                        if ( (*listPtr).processID == oam::API_FAILURE )
                        {
                            // restart hard failure
                            restartStatus = " restart failed with hard failure, don't retry!!";
                            (*listPtr).processID = 0;

							// check if Module failover is needed due to process outage
							aMonitor.checkModuleFailover((*listPtr).ProcessName);
                            break;
                        }
                        else
                        {
                            if ( (*listPtr).processID != oam::API_MINOR_FAILURE )
							{
                                //restarted successful
                        		//Inform Process Manager that Process restart
                        		aMonitor.processRestarted( (*listPtr).ProcessName, false);
                                break;
							}
						}
                        // restart failed with minor error, sleep and try
                        sleep(5);
                    }

                    if ( i == 10 || (*listPtr).processID == oam::API_FAILURE)
                    {
                        //setModule status to degraded
                        try
                        {
                            bool degraded;
                            int moduleStatus;
                            oam.getModuleStatus(config->moduleName(), moduleStatus, degraded);

                            if ( moduleStatus == oam::ACTIVE)
                            {
                                try
                                {
                                    oam.setModuleStatus(config->moduleName(), oam::DEGRADED);
                                }
                                catch (exception& ex)
                                {
                                    string error = ex.what();
                                    log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                                }
                                catch (...)
                                {
                                    log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                                }

                            }
                        }
                        catch (...)
                        {}

                        //check the db health
                        if (DBFunctionalMonitorFlag == "y" )
                        {
                            log.writeLog(__LINE__, "Call the check DB Functional API", LOG_TYPE_DEBUG);

                            try
                            {
                                oam.checkDBFunctional();
                                log.writeLog(__LINE__, "check DB Functional passed", LOG_TYPE_DEBUG);
                            }
                            catch (...)
                            {
                                log.writeLog(__LINE__, "check DB Functional FAILED", LOG_TYPE_ERROR);
                            }
                        }
                    }

                    if ( i == 10 )
                    {
                        // restart timeout
                        restartStatus = " restart failed after 10 retries";
                        (*listPtr).processID = 0;
                    }
                    else
                    {
                        restartStatus = " restarted successfully!!";

                        //Inform Process Manager that Process restart
                        aMonitor.processRestarted( (*listPtr).ProcessName, false);
                    }

                    //Log this event
					log.writeLog(__LINE__, "MariaDB ColumnStore Process " + (*listPtr).ProcessName + restartStatus, LOG_TYPE_INFO);
                }
            }
        }

        delayCount++;

        if ( delayCount > 2 )
            delayCount = 0;

        sleep(5);
    }
    return NULL;
}

/******************************************************************************************
* @brief	sigHupHandler
*
* purpose:	Hanlder SIGHUP signal and update internal DB
*
******************************************************************************************/
static void sigHupHandler(int sig)
{
    MonitorLog log;
    MonitorConfig config;
    ProcessMonitor aMonitor(config, log);
    log.writeLog(__LINE__, "SIGHUP Thread started ..", LOG_TYPE_DEBUG);

    aMonitor.updateConfig();

}

static int PROCSTATshmsize = 0;
shmProcessStatus* fShmProcessStatus = 0;
boost::interprocess::shared_memory_object fProcStatShmobj;
boost::interprocess::mapped_region fProcStatMapreg;

int fmoduleNumber = 0;
int extDeviceNumber = 0;
int NICNumber = 0;
int dbrootNumber = 0;
int processNumber = 0;

boost::interprocess::shared_memory_object fSysStatShmobj;
boost::interprocess::mapped_region fSysStatMapreg;

void* processStatusMSG(messageqcpp::IOSocket* fIos);

processStatusList* aPtr;
SystemProcessConfig systemprocessconfig;
ModuleTypeConfig moduletypeconfig;
SystemModuleTypeConfig systemModuleTypeConfig;
SystemExtDeviceConfig systemextdeviceconfig;

std::vector<string>	moduleDisableStateList;
std::vector<string>	hostNameList;
std::vector<string>	ipaddrNameList;
std::vector<string>	moduleNameList;
std::vector<string>	extDeviceNameList;

shmDeviceStatus* fShmNICStatus = 0;
shmDeviceStatus* fShmDbrootStatus = 0;
shmDeviceStatus* fShmExtDeviceStatus = 0;
shmDeviceStatus* fShmSystemStatus = 0;

processStatusList fstatusListPtr;

processStatusList*	statusListPtr()
{
    return &fstatusListPtr;
}


/******************************************************************************************
* @brief	statusControlThread
*
* purpose:	Setup Status Shared-Memory table and process request to get and set
*			into the Status Shared-Memory table
*
******************************************************************************************/
static void* statusControlThread(void*)
{
    MonitorLog log;
    MonitorConfig config;
    ProcessMonitor aMonitor(config, log);
    Oam oam;
    BRM::ShmKeys fShmKeys;

    log.writeLog(__LINE__, "statusControlThread Thread started ..", LOG_TYPE_DEBUG);

    //
    //Read ProcessConfig file to get process list and build Status List
    //
    try
    {
        oam.getProcessConfig(systemprocessconfig);
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    try
    {
        oam.getSystemConfig(systemModuleTypeConfig);
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    // build status list

    for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
    {
        int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

        if ( moduleCount == 0 )
            // skip of no modules configured
            continue;

        // dm/um/pm
        string systemModuleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;

        fmoduleNumber = fmoduleNumber + moduleCount;

        // store ModuleNames / HostNames and IP Addresses (NIC)
        DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
        {
            moduleNameList.push_back((*pt).DeviceName);
            moduleDisableStateList.push_back((*pt).DisableState);

            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
            {
                ipaddrNameList.push_back((*pt1).IPAddr);
                hostNameList.push_back((*pt1).HostName);
            }
        }

        NICNumber = hostNameList.size();
        string OAMParentModuleType = config.OAMParentName().substr(0, 2);

        pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
        {
            for ( unsigned int j = 0; j < systemprocessconfig.processconfig.size(); j++)
            {
                //skip if both BootLaunch and LaunchID are 0
                if ( systemprocessconfig.processconfig[j].BootLaunch == 0 &&
                        systemprocessconfig.processconfig[j].LaunchID == 0 )
                    continue;

                // "ChildOAMModule" "ParentOAMModule" dm/um/pm
                string processModuleType = systemprocessconfig.processconfig[j].ModuleType;

                if (processModuleType == systemModuleType
                        || ( processModuleType == "um" &&
                             systemModuleType == "pm" && PMwithUM == "y")
                        || processModuleType == "ChildExtOAMModule"
                        || (processModuleType == "ChildOAMModule" )
                        || (processModuleType == "ParentOAMModule" && systemModuleType == OAMParentModuleType) )
                {
                    if ( processModuleType == "um" &&
                            systemModuleType == "pm" && PMwithUM == "y" &&
                            systemprocessconfig.processconfig[j].ProcessName == "DMLProc" )
                        continue;


                    if ( processModuleType == "um" &&
                            systemModuleType == "pm" && PMwithUM == "y" &&
                            systemprocessconfig.processconfig[j].ProcessName == "DDLProc" )
                        continue;

                    processstatus procstat;
                    procstat.ProcessName = systemprocessconfig.processconfig[j].ProcessName;
                    procstat.ModuleName = (*pt).DeviceName;
                    procstat.tableIndex = processNumber;
                    fstatusListPtr.push_back(procstat);
                    processNumber++;
                }
            }
        }
    }

    aPtr = statusListPtr();

    //
    //Allocate Shared Memory for storing Process Status Data
    //

    string shmLocation = "/dev/shm/";

    PROCSTATshmsize = MAX_PROCESS * sizeof(shmProcessStatus);
    bool memInit = true;
#if 0
    int shmid = shmget(fShmKeys.PROCESSSTATUS_SYSVKEY, PROCSTATshmsize, IPC_EXCL | IPC_CREAT | 0666);

    if (shmid == -1)
    {
        // table already exist
        memInit = false;
        shmid = shmget(fShmKeys.PROCESSSTATUS_SYSVKEY, PROCSTATshmsize, 0666);

        if (shmid == -1)
        {
            log.writeLog(__LINE__, "*****ProcessStatusTable shmget failed.", LOG_TYPE_ERROR);
            exit(1);
        }
    }

    fShmProcessStatus = static_cast<struct shmProcessStatus*>(shmat(shmid, NULL, 0));
#endif
    string keyName = BRM::ShmKeys::keyToName(fShmKeys.PROCESSSTATUS_SYSVKEY);
    memInit = getshm(keyName, PROCSTATshmsize, fProcStatShmobj);

    bi::mapped_region region(fProcStatShmobj, bi::read_write);
    fProcStatMapreg.swap(region);
    fShmProcessStatus = static_cast<shmProcessStatus*>(fProcStatMapreg.get_address());

    if (fShmProcessStatus == 0)
    {
        log.writeLog(__LINE__, "*****ProcessStatusTable shmat failed.", LOG_TYPE_CRITICAL);
        exit(1);
    }

    //Initialize Shared memory
    if (memInit)
    {
        memset(fShmProcessStatus, 0, PROCSTATshmsize);

        for ( int i = 0; i < processNumber ; ++i)
        {
            fShmProcessStatus[i].ProcessOpState = oam::INITIAL;
        }

        log.writeLog(__LINE__, "Process Status shared Memory allocated and Initialized", LOG_TYPE_DEBUG);
    }

    //
    //Allocate Shared Memory for storing System/Module Status Data
    //
    fmoduleNumber++;		//add 1 to cover system status entry

    static const int SYSTEMSTATshmsize = MAX_MODULE * sizeof(shmDeviceStatus);
    memInit = true;
#if 0
    shmid = shmget(fShmKeys.SYSTEMSTATUS_SYSVKEY, SYSTEMSTATshmsize, IPC_EXCL | IPC_CREAT | 0666);

    if (shmid == -1)
    {
        // table already exist
        memInit = false;
        shmid = shmget(fShmKeys.SYSTEMSTATUS_SYSVKEY, SYSTEMSTATshmsize, 0666);

        if (shmid == -1)
        {
            log.writeLog(__LINE__, "*****SystemStatusTable shmget failed.", LOG_TYPE_ERROR);
            exit(1);
        }
    }

    fShmSystemStatus = static_cast<struct shmDeviceStatus*>(shmat(shmid, NULL, 0));
#endif
    keyName = BRM::ShmKeys::keyToName(fShmKeys.SYSTEMSTATUS_SYSVKEY);
    memInit = getshm(keyName, SYSTEMSTATshmsize, fSysStatShmobj);

    bi::mapped_region region2(fSysStatShmobj, bi::read_write);
    fSysStatMapreg.swap(region2);
    fShmSystemStatus = static_cast<shmDeviceStatus*>(fSysStatMapreg.get_address());

    if (fShmSystemStatus == 0)
    {
        log.writeLog(__LINE__, "*****SystemStatusTable shmat failed.", LOG_TYPE_CRITICAL);
        exit(1);
    }

    //Initialize Shared memory
    if (memInit)
    {
        // Init System/Module Status Memory
        memset(fShmSystemStatus, 0, SYSTEMSTATshmsize);

        //set system status
        memcpy(fShmSystemStatus[0].Name, "system", sizeof("system"));

        if (runStandby)
        {
            try
            {
                SystemStatus systemstatus;
                oam.getSystemStatus(systemstatus);
                fShmSystemStatus[0].OpState = systemstatus.SystemOpState;
                memcpy(fShmSystemStatus[0].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
            }
            catch (...)
            {
                fShmSystemStatus[0].OpState = oam::DOWN;
            }
        }
        else
            fShmSystemStatus[0].OpState = oam::DOWN;

        //set module status
        for ( int i = 1; i < fmoduleNumber ; ++i)
        {
            memcpy(fShmSystemStatus[i].Name, moduleNameList[i - 1].c_str(), NAMESIZE);

            if (runStandby)
            {
                try
                {
                    int opState;
                    bool degraded;
                    oam.getModuleStatus(moduleNameList[i - 1], opState, degraded);
                    fShmSystemStatus[i].OpState = opState;
                    memcpy(fShmSystemStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                }
                catch (...)
                {
                    fShmSystemStatus[i].OpState = oam::INITIAL;
                }
            }
            else
            {
                if ( moduleDisableStateList[i - 1] == oam::MANDISABLEDSTATE )
                    fShmSystemStatus[i].OpState = oam::MAN_DISABLED;
                else if ( moduleDisableStateList[i - 1] == oam::AUTODISABLEDSTATE )
                    fShmSystemStatus[i].OpState = oam::AUTO_DISABLED;
                else
                    fShmSystemStatus[i].OpState = oam::INITIAL;
            }
        }

        log.writeLog(__LINE__, "System/Module Status shared Memory allocated and Initialized", LOG_TYPE_DEBUG);
    }

    //
    //Allocate Shared Memory for storing NIC Status Data
    //
    boost::interprocess::shared_memory_object fNICStatShmobj;
    static const int NICSTATshmsize = (MAX_MODULE * MAX_NIC) * sizeof(shmDeviceStatus);

    keyName = BRM::ShmKeys::keyToName(fShmKeys.NICSTATUS_SYSVKEY);
    memInit = getshm(keyName, NICSTATshmsize, fNICStatShmobj);

    bi::mapped_region fNICStatMapreg(fNICStatShmobj, bi::read_write);
    fShmNICStatus = static_cast<shmDeviceStatus*>(fNICStatMapreg.get_address());

    if (fShmNICStatus == 0)
    {
        log.writeLog(__LINE__, "*****NICStatusTable shmat failed.", LOG_TYPE_CRITICAL);
        exit(1);
    }

    //Initialize Shared memory
    if (memInit)
    {
        // Init NIC Status Memory
        memset(fShmNICStatus, 0, NICSTATshmsize);

        for ( int i = 0; i < NICNumber ; ++i)
        {
            fShmNICStatus[i].OpState = oam::INITIAL;
            memcpy(fShmNICStatus[i].Name, hostNameList[i].c_str(), NAMESIZE);
        }

        log.writeLog(__LINE__, "NIC Status shared Memory allocated and Initialized", LOG_TYPE_DEBUG);
    }

    //
    //Allocate Shared Memory for storing External Device Status Data
    //

    try
    {
        oam.getSystemConfig(systemextdeviceconfig);
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count; i++)
    {
        if ( systemextdeviceconfig.extdeviceconfig[i].Name == oam::UnassignedName ||
                systemextdeviceconfig.extdeviceconfig[i].Name.empty() )
            continue;

        extDeviceNameList.push_back(systemextdeviceconfig.extdeviceconfig[i].Name);
        extDeviceNumber++;
    }

    boost::interprocess::shared_memory_object fExtStatShmobj;
    static const int EXTDEVICESTATshmsize = MAX_EXT_DEVICE * sizeof(shmDeviceStatus);
    keyName = BRM::ShmKeys::keyToName(fShmKeys.SWITCHSTATUS_SYSVKEY);
    memInit = getshm(keyName, EXTDEVICESTATshmsize, fExtStatShmobj);

    bi::mapped_region fExtStatMapreg(fExtStatShmobj, bi::read_write);
    fShmExtDeviceStatus = static_cast<shmDeviceStatus*>(fExtStatMapreg.get_address());

    if (fShmExtDeviceStatus == 0)
    {
        log.writeLog(__LINE__, "*****ExtDeviceStatusTable shmat failed.", LOG_TYPE_CRITICAL);
        exit(1);
    }

    //Initialize Shared memory
    if (memInit)
    {
        // Init Ext Device Status Memory
        memset(fShmExtDeviceStatus, 0, EXTDEVICESTATshmsize);

        for ( int i = 0; i < extDeviceNumber ; ++i)
        {
            fShmExtDeviceStatus[i].OpState = oam::INITIAL;
            memcpy(fShmExtDeviceStatus[i].Name, extDeviceNameList[i].c_str(), NAMESIZE);
        }

        log.writeLog(__LINE__, "Ext Device Status shared Memory allocated and Initialized", LOG_TYPE_DEBUG);
    }

    //
    //Allocate Shared Memory for storing DBRoot Status Data
    //

    string DBRootStorageType;

    try
    {
        oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
    }
    catch (...) {}

    std::vector<string>dbrootList;

    if ( DBRootStorageType == "external" ||
            DataRedundancyConfig == "y")
    {
        //get system dbroots
        DBRootConfigList dbrootConfigList;

        try
        {
            oam.getSystemDbrootConfig(dbrootConfigList);
        }
        catch (exception& e)
        {
            log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemDbrootConfig: Caught unknown exception!", LOG_TYPE_ERROR);
        }

        DBRootConfigList::iterator pt = dbrootConfigList.begin();

        for ( ; pt != dbrootConfigList.end() ; pt++)
        {
            dbrootList.push_back(oam.itoa(*pt));
            dbrootNumber++;
        }
    }

    boost::interprocess::shared_memory_object fDbrootShmobj;
    static const int DBROOTSTATshmsize = MAX_DBROOT * sizeof(shmDeviceStatus);
    keyName = BRM::ShmKeys::keyToName(fShmKeys.DBROOTSTATUS_SYSVKEY);
    memInit = getshm(keyName, DBROOTSTATshmsize, fDbrootShmobj);

    bi::mapped_region fdDbrootStatMapreg(fDbrootShmobj, bi::read_write);
    fShmDbrootStatus = static_cast<shmDeviceStatus*>(fdDbrootStatMapreg.get_address());

    if (fShmDbrootStatus == 0)
    {
        log.writeLog(__LINE__, "*****DbrootStatusTable shmat failed.", LOG_TYPE_CRITICAL);
        exit(1);
    }

    //Initialize Shared memory
    if (memInit)
    {
        // Init DBRoot Status Memory
        memset(fShmDbrootStatus, 0, DBROOTSTATshmsize);

        for ( int i = 0; i < dbrootNumber ; ++i)
        {
            fShmDbrootStatus[i].OpState = oam::INITIAL;
            memcpy(fShmDbrootStatus[i].Name, dbrootList[i].c_str(), NAMESIZE);
        }

        log.writeLog(__LINE__, "Dbroot Status shared Memory allocated and Initialized", LOG_TYPE_DEBUG);
    }

    //Set mainResumeFlag, to start up main thread

    mainResumeFlag = true;

    string portName = "ProcStatusControl";

    if (runStandby)
    {
        portName = "ProcStatusControlStandby";
        processStatusList* aPtr = statusListPtr();
        updateShareMemory(aPtr);
    }

    //
    //Now wait for Process Status Get and Set request
    //

    //read and cleanup port before trying to use
    try
    {
        Config* sysConfig = Config::makeConfig();
        string port = sysConfig->getConfig(portName, "Port");
        string cmd = "fuser -k " + port + "/tcp >/dev/null 2>&1";

	system(cmd.c_str());
    }
    catch (...)
    {
    }

    log.writeLog(__LINE__, "statusControlThread Thread reading " + portName + " port", LOG_TYPE_DEBUG);

    IOSocket* fIos;
    MessageQueueServer* mqs;
    int standbyUpdateCount = 0;

    mqs = new MessageQueueServer(portName);
    struct timespec ts = { 1, 0 };

    for (;;)
    {
        if (!runStandby && portName == "ProcStatusControlStandby")
        {
            portName = "ProcStatusControl";
            delete mqs;
            mqs = new MessageQueueServer(portName);

            log.writeLog(__LINE__, "statusControlThread Thread reading " + portName + " port", LOG_TYPE_DEBUG);

            processStatusList* aPtr = statusListPtr();
            updateShareMemory(aPtr);
        }

        if (runStandby && portName == "ProcStatusControl")
        {
            portName = "ProcStatusControlStandby";
            delete mqs;
            mqs = new MessageQueueServer(portName);

            log.writeLog(__LINE__, "statusControlThread Thread reading " + portName + " port", LOG_TYPE_DEBUG);
        }

        fIos = NULL;
        try
        {
            //log.writeLog(__LINE__, "***before accept", LOG_TYPE_DEBUG);
            fIos = new IOSocket();
            *fIos = mqs->accept(&ts);

            if ( fIos->isOpen() )
            {
                //log.writeLog(__LINE__, "***before create thread", LOG_TYPE_DEBUG);
                pthread_t messagethread;
                int status = pthread_create (&messagethread, NULL, (void*(*)(void*))&processStatusMSG, fIos);

                //log.writeLog(__LINE__, "***after create thread", LOG_TYPE_DEBUG);

                if ( status != 0 )
                {
                    log.writeLog(__LINE__, "messagethread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
                    delete fIos;
                }
            }
            else
                delete fIos;

        }
        catch (...)
        {
            if (fIos)
                delete fIos;
        }

        if ( runStandby )
        {
            standbyUpdateCount++;

            if ( standbyUpdateCount >= 3 )
            {
                //processStatusList* aPtr = statusListPtr();
                updateShareMemory(aPtr);
                standbyUpdateCount = 0;
            }
        }
    } // end of for loop
    return NULL;
}

/******************************************************************************************
* @brief	processStatusMSG
*
* purpose:	Process the status message
*
******************************************************************************************/
void* processStatusMSG(messageqcpp::IOSocket* cfIos)
{
    messageqcpp::IOSocket* fIos = cfIos;

    pthread_t ThreadId;
    ThreadId = pthread_self();

    MonitorLog log;
    MonitorConfig config;
    ProcessMonitor aMonitor(config, log);
    Oam oam;

    ByteStream* msg;
    msg = new ByteStream();

    //log.writeLog(__LINE__, "***start create thread", LOG_TYPE_DEBUG);
    struct timespec ts = { 20, 0 };

    try
    {
        *msg = fIos->read(&ts);
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "***read error, close create thread: " + error, LOG_TYPE_DEBUG);
        fIos->close();
        delete fIos;
        delete msg;
        pthread_detach (ThreadId);
        pthread_exit(0);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "***read error, close create thread", LOG_TYPE_DEBUG);
        fIos->close();
        delete fIos;
        delete msg;
        pthread_detach (ThreadId);
        pthread_exit(0);
    }

    if (msg->length() <= 0)
    {
//		log.writeLog(__LINE__, "***0 bytes, close create thread", LOG_TYPE_DEBUG);
        fIos->close();
        delete fIos;
        delete msg;
        pthread_detach (ThreadId);
        pthread_exit(0);
    }

    ByteStream::byte requestType;
    *msg >> requestType;
    //log.writeLog(__LINE__, "statusControl: Msg received, requestType = " + oam.itoa(requestType), LOG_TYPE_DEBUG);

    switch (requestType)
    {
        case GET_PROC_STATUS:
        {
            std::string moduleName;
            std::string processName;

            ByteStream::byte state;
            ByteStream::quadbyte PID;
            std::string changeDate;
            ByteStream ackmsg;

            *msg >> moduleName;
            *msg >> processName;

            processStatusList::iterator listPtr;
            //processStatusList* aPtr = statusListPtr();
            listPtr = aPtr->begin();

            int shmIndex = 0;

            for (; listPtr != aPtr->end(); ++listPtr)
            {
                if ((*listPtr).ProcessName == processName &&
                        (*listPtr).ModuleName == moduleName)
                {
                    shmIndex = (*listPtr).tableIndex;
                    break;
                }
            }

            if (listPtr == aPtr->end())
            {
                // not in list
//									log.writeLog(__LINE__, "statusControl: GET_PROC_STATUS: Process not valid: " + processName + " / " + moduleName, LOG_TYPE_DEBUG);
                ackmsg << (ByteStream::byte) API_FAILURE;
                fIos->write(ackmsg);
                break;
            }

            //get table info
            state = fShmProcessStatus[shmIndex].ProcessOpState;
            PID = fShmProcessStatus[shmIndex].ProcessID;
            changeDate = fShmProcessStatus[shmIndex].StateChangeDate;

            ackmsg << (ByteStream::byte) API_SUCCESS;
            ackmsg << state;
            ackmsg << PID;
            ackmsg << changeDate;
            fIos->write(ackmsg);
        }
        break;

        case SET_PROC_STATUS:
        {
            std::string moduleName;
            std::string processName;
            ByteStream::byte state;
            ByteStream::quadbyte PID;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> moduleName;
            *msg >> processName;
            *msg >> state;
            *msg >> PID;

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set Process " + moduleName + "/" + processName + " State = " + oamState[state], LOG_TYPE_DEBUG);

            processStatusList::iterator listPtr;
            //processStatusList* aPtr = statusListPtr();
            listPtr = aPtr->begin();

            int shmIndex = 0;

            for (; listPtr != aPtr->end(); ++listPtr)
            {
                if ((*listPtr).ProcessName == processName &&
                        (*listPtr).ModuleName == moduleName)
                {
                    shmIndex = (*listPtr).tableIndex;
                    break;
                }
            }

            if (listPtr == aPtr->end())
            {
                // not in list
                log.writeLog(__LINE__, "statusControl: SET_PROC_STATUS: Process not valid: " + moduleName + "/" + processName, LOG_TYPE_DEBUG);
                break;
            }

            //check and process for Active/Standby process run-type
            if ( state == oam::ACTIVE )
            {

                std::string moduleType = moduleName.substr(0, 2);

                for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
                {
                    if ( systemprocessconfig.processconfig[i].ModuleType == moduleType &&
                            systemprocessconfig.processconfig[i].ProcessName == processName	)
                    {
                        if ( systemprocessconfig.processconfig[i].RunType == oam::ACTIVE_STANDBY )
                        {
                            // process is ACTIVE_STANDBY run-state, get Module run-type and state
                            try
                            {
                                oam.getSystemConfig(moduleType, moduletypeconfig);

                                if ( moduletypeconfig.RunType == oam::ACTIVE_STANDBY )
                                {
                                    for ( int i = 1; i < fmoduleNumber; ++i)
                                    {
                                        memcpy(charName, fShmSystemStatus[i].Name, NAMESIZE);
                                        shmName = charName;

                                        if ( moduleName == shmName )
                                        {
                                            if ( fShmSystemStatus[i].OpState == oam::STANDBY )
                                            {
                                                //set current state to STANDBY
                                                state = oam::STANDBY;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            catch (exception& ex)
                            {
                                string error = ex.what();
//													log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
                                break;
                            }
                            catch (...)
                            {
//													log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
                                break;
                            }
                        }
                        else
                            // not oam::ACTIVE/STANDBY
                            break;
                    }
                }
            }

            // invalid state change ACTIVE TO MAN_INIT / AUTO_INIT
            if ( fShmProcessStatus[shmIndex].ProcessOpState == oam::ACTIVE )
            {
                if ( state == oam::MAN_INIT || state == oam::AUTO_INIT )
                {
                    log.writeLog(__LINE__, "statusControl: " + moduleName + "/" + processName + " Current State = ACTIVE, invalid update request to " + oamState[state], LOG_TYPE_DEBUG);
                    break;
                }
            }

            if (!utils::is_nonnegative(PID))
                PID = 0;

            log.writeLog(__LINE__, "statusControl: Set Process " + moduleName + "/" + processName +  + " State = " + oamState[state] + " PID = " + oam.itoa(PID), LOG_TYPE_DEBUG);

            //update table
            if (  state < PID_UPDATE )
                fShmProcessStatus[shmIndex].ProcessOpState = state;

            if (  PID != 1 )
                fShmProcessStatus[shmIndex].ProcessID = PID;

            memcpy(fShmProcessStatus[shmIndex].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);

            //if DMLProc set to BUSY_INIT, set system state to BUSY_INIT
            if ( processName == "DMLProc" && state == oam::BUSY_INIT )
            {
                fShmSystemStatus[0].OpState = state;
                memcpy(fShmSystemStatus[0].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set System State = " + oamState[state], LOG_TYPE_DEBUG);
            }

            //if DMLProc set to ACTIVE, set system state to ACTIVE if in an INIT state
            if ( processName == "DMLProc" && state == oam::ACTIVE )
            {
                if ( fShmSystemStatus[0].OpState == oam::BUSY_INIT ||
                        fShmSystemStatus[0].OpState == oam::MAN_INIT ||
                        fShmSystemStatus[0].OpState == oam::AUTO_INIT )
                {
                    fShmSystemStatus[0].OpState = state;
                    memcpy(fShmSystemStatus[0].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                    log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set System State = " + oamState[state], LOG_TYPE_DEBUG);
                }

				BRM::DBRM dbrm;
				dbrm.setSystemQueryReady(true);
            }

        }
        break;

        case GET_ALL_PROC_STATUS:
        {
            ByteStream ackmsg;
            ByteStream::byte state;
            ByteStream::quadbyte PID;
            std::string changeDate;
            std::string processName;
            std::string moduleName;

            processStatusList::iterator listPtr;
            ////processStatusList* aPtr = statusListPtr();

            ackmsg << (ByteStream::quadbyte) aPtr->size();

            for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
            {
                int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

                if ( moduleCount == 0 )
                    // skip of no modules configured
                    continue;

                string moduleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;

                listPtr = aPtr->begin();

                for (; listPtr != aPtr->end(); ++listPtr)
                {
                    moduleName = (*listPtr).ModuleName;

                    if ( moduleName.find(moduleType) != string::npos )
                    {
                        processName = (*listPtr).ProcessName;
                        int shmIndex = (*listPtr).tableIndex;
                        state = fShmProcessStatus[shmIndex].ProcessOpState;
                        PID = fShmProcessStatus[shmIndex].ProcessID;
                        changeDate = fShmProcessStatus[shmIndex].StateChangeDate;

                        ackmsg << processName;
                        ackmsg << moduleName;
                        ackmsg << state;
                        ackmsg << PID;
                        ackmsg << changeDate;
                    }
                }
            }

            fIos->write(ackmsg);
        }
        break;

        case GET_PROC_STATUS_BY_PID:
        {
            std::string moduleName;
            std::string processName;

            ByteStream ackmsg;
            ByteStream::byte state;
            ByteStream::quadbyte PID;

            *msg >> moduleName;
            *msg >> PID;

            processStatusList::iterator listPtr;
            //processStatusList* aPtr = statusListPtr();
            listPtr = aPtr->begin();

            int shmIndex = 0;

            for (; listPtr != aPtr->end(); ++listPtr)
            {
                if ((*listPtr).ModuleName == moduleName)
                {
                    shmIndex = (*listPtr).tableIndex;

                    //get PID
                    if ( PID == (ByteStream::quadbyte) fShmProcessStatus[shmIndex].ProcessID)
                    {
                        // match found, get state
                        state = fShmProcessStatus[shmIndex].ProcessOpState;
                        //get process name
                        processName = (*listPtr).ProcessName;

                        ackmsg << (ByteStream::byte) API_SUCCESS;
                        ackmsg << state;
                        ackmsg << processName;
                        fIos->write(ackmsg);
                        break;
                    }
                }
            }

            if (listPtr == aPtr->end())
            {
                // not in list
                ackmsg << (ByteStream::byte) API_FAILURE;
                fIos->write(ackmsg);
//									log.writeLog(__LINE__, "statusControl: GET_PROC_STATUS_BY_PID: PID not valid: " + oam.itoa(PID) + " / " + moduleName);
                break;
            }
        }
        break;

        case GET_SYSTEM_STATUS:
        {
            ByteStream ackmsg;
            ByteStream::byte state;
            std::string name;
            std::string changeDate;
            ByteStream::byte systemStatusOnly;

            *msg >> systemStatusOnly;

            if ( systemStatusOnly == 1 )
            {
                for (int j = 0 ; j < fmoduleNumber; ++j)
                {
                    name = fShmSystemStatus[j].Name;

                    if ( name.find("system") != string::npos )
                    {
                        state = fShmSystemStatus[j].OpState;
                        changeDate = fShmSystemStatus[j].StateChangeDate;

                        ackmsg << name;
                        ackmsg << state;
                        ackmsg << changeDate;
                        break;
                    }
                }
            }
            else
            {
                ackmsg << (ByteStream::byte) fmoduleNumber;

                for (int j = 0 ; j < fmoduleNumber; ++j)
                {
                    name = fShmSystemStatus[j].Name;

                    if ( name.find("system") != string::npos )
                    {
                        state = fShmSystemStatus[j].OpState;
                        changeDate = fShmSystemStatus[j].StateChangeDate;

                        ackmsg << name;
                        ackmsg << state;
                        ackmsg << changeDate;
                    }
                }

                for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
                {
                    int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

                    if ( moduleCount == 0 )
                        // skip of no modules configured
                        continue;

                    string moduleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;

                    for (int j = 0 ; j < fmoduleNumber; ++j)
                    {
                        name = fShmSystemStatus[j].Name;

                        if ( name.find(moduleType) != string::npos )
                        {
                            state = fShmSystemStatus[j].OpState;
                            changeDate = fShmSystemStatus[j].StateChangeDate;

                            ackmsg << name;
                            ackmsg << state;
                            ackmsg << changeDate;
                        }
                    }
                }

                ackmsg << (ByteStream::byte) extDeviceNumber;

                for (int i = 0 ; i < extDeviceNumber; ++i)
                {
                    name = fShmExtDeviceStatus[i].Name;
                    state = fShmExtDeviceStatus[i].OpState;
                    changeDate = fShmExtDeviceStatus[i].StateChangeDate;

                    ackmsg << name;
                    ackmsg << state;
                    ackmsg << changeDate;
                }

                ackmsg << (ByteStream::byte) NICNumber;

                for (int i = 0 ; i < NICNumber; ++i)
                {
                    name = fShmNICStatus[i].Name;
                    state = fShmNICStatus[i].OpState;
                    changeDate = fShmNICStatus[i].StateChangeDate;

                    ackmsg << name;
                    ackmsg << state;
                    ackmsg << changeDate;
                }

                ackmsg << (ByteStream::byte) dbrootNumber;

                for (int i = 0 ; i < dbrootNumber; ++i)
                {
                    name = fShmDbrootStatus[i].Name;
                    state = fShmDbrootStatus[i].OpState;
                    changeDate = fShmDbrootStatus[i].StateChangeDate;

                    ackmsg << name;
                    ackmsg << state;
                    ackmsg << changeDate;
                }
            }

            fIos->write(ackmsg);
        }
        break;

        case SET_SYSTEM_STATUS:
        {
            ByteStream::byte state;
            *msg >> state;
            fShmSystemStatus[0].OpState = state;
            memcpy(fShmSystemStatus[0].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set System State = " + oamState[state], LOG_TYPE_DEBUG);

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }
        }
        break;

        case SET_MODULE_STATUS:
        {
            ByteStream::byte state;
            std::string moduleName;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> moduleName;
            *msg >> state;

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set Module " + moduleName + " State = " + oamState[state], LOG_TYPE_DEBUG);

            //Handle Module RunType of ACTIVE_STANDBY
            string moduletype = moduleName.substr(0, MAX_MODULE_TYPE_SIZE);
            string moduleID = moduleName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

            try
            {
                oam.getSystemConfig(moduletype, moduletypeconfig);
            }
            catch (exception& ex)
            {
                string error = ex.what();
                log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
                break;
            }
            catch (...)
            {
//									log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
                break;
            }

            if ( moduletypeconfig.RunType == oam::ACTIVE_STANDBY )
            {
                if ( state == oam::ACTIVE )
                {
                    for ( int i = 1; i < fmoduleNumber; ++i)
                    {
                        memcpy(charName, fShmSystemStatus[i].Name, NAMESIZE);
                        shmName = charName;
                        string othermoduletype = shmName.substr(0, MAX_MODULE_TYPE_SIZE);
                        string othermoduleID = shmName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                        if ( moduletype == othermoduletype &&
                                moduleID != othermoduleID )
                        {
                            if ( fShmSystemStatus[i].OpState == oam::ACTIVE )
                            {
                                //found one, set current state to STANDBY
                                state = oam::STANDBY;

                                //set ACTIVE_STANDBY process to STANDBY state
                                try
                                {
                                    oam.getProcessConfig(systemprocessconfig);

                                    for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
                                    {
                                        if ( systemprocessconfig.processconfig[i].ModuleType == moduletype &&
                                                systemprocessconfig.processconfig[i].RunType == oam::ACTIVE_STANDBY )
                                        {

                                            processStatusList::iterator listPtr;
                                            //processStatusList* aPtr = statusListPtr();
                                            listPtr = aPtr->begin();

                                            for (; listPtr != aPtr->end(); ++listPtr)
                                            {
                                                if ( systemprocessconfig.processconfig[i].ProcessName 	== (*listPtr).ProcessName &&
                                                        moduleName == (*listPtr).ModuleName )
                                                {
                                                    int shmIndex = (*listPtr).tableIndex;
                                                    fShmProcessStatus[shmIndex].ProcessOpState = oam::STANDBY;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (exception& ex)
                                {
                                    string error = ex.what();
//														log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
                                }
                                catch (...)
                                {
//														log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
                                }

                                break;
                            }
                        }
                    }
                }
                else
                {
                    //check to see if a STANDBY Mate needs to go ACTIVE
                    if ( state == oam::DOWN || state == oam::MAN_OFFLINE
                            || state == oam::FAILED)
                    {
                        for ( int i = 1; i < fmoduleNumber; ++i)
                        {
                            memcpy(charName, fShmSystemStatus[i].Name, NAMESIZE);
                            shmName = charName;
                            string othermoduletype = shmName.substr(0, MAX_MODULE_TYPE_SIZE);
                            string othermoduleID = shmName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE);

                            if ( moduletype == othermoduletype &&
                                    moduleID != othermoduleID )
                            {
                                if ( fShmSystemStatus[i].OpState == oam::STANDBY )
                                {
                                    //found one, set it to ACTIVE
                                    fShmSystemStatus[i].OpState = oam::ACTIVE;
                                    memcpy(fShmSystemStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);

                                    //set ACTIVE_STANDBY process to ACTIVE state
                                    try
                                    {
                                        oam.getProcessConfig(systemprocessconfig);

                                        for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
                                        {
                                            if ( systemprocessconfig.processconfig[i].ModuleType == moduletype &&
                                                    systemprocessconfig.processconfig[i].RunType == oam::ACTIVE_STANDBY )
                                            {

                                                processStatusList::iterator listPtr;
                                                //processStatusList* aPtr = statusListPtr();
                                                listPtr = aPtr->begin();

                                                for (; listPtr != aPtr->end(); ++listPtr)
                                                {
                                                    if ( systemprocessconfig.processconfig[i].ProcessName 	== (*listPtr).ProcessName &&
                                                            shmName == (*listPtr).ModuleName )
                                                    {
                                                        int shmIndex = (*listPtr).tableIndex;
                                                        fShmProcessStatus[shmIndex].ProcessOpState = oam::ACTIVE;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    catch (exception& ex)
                                    {
                                        string error = ex.what();
//															log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
                                    }
                                    catch (...)
                                    {
//															log.writeLog(__LINE__, "statusControl: EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
                                    }

                                    break;
                                }
                            }
                        }
                    }
                }
            }

            //set current Module state
            int i = 1;

            for ( ; i < fmoduleNumber; ++i)
            {
                memcpy(charName, fShmSystemStatus[i].Name, NAMESIZE);
                shmName = charName;

                if ( moduleName == shmName )
                {
                    fShmSystemStatus[i].OpState = state;
                    memcpy(fShmSystemStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                    break;
                }
            }

            if ( i == fmoduleNumber)
            {
                // not in list
                log.writeLog(__LINE__, "statusControl: SET_MODULE_STATUS: Module not valid: " + moduleName, LOG_TYPE_ERROR);
                break;
            }
        }
        break;

        case SET_EXT_DEVICE_STATUS:
        {
            ByteStream::byte state;
            std::string name;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> name;
            *msg >> state;

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set Ext Device " + name + " State = " + oamState[state], LOG_TYPE_DEBUG);

            int i = 0;

            for ( ; i < extDeviceNumber; ++i)
            {
                memcpy(charName, fShmExtDeviceStatus[i].Name, NAMESIZE);
                shmName = charName;

                if ( name == shmName )
                {
                    fShmExtDeviceStatus[i].OpState = state;
                    memcpy(fShmExtDeviceStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                    break;
                }
            }

            if ( i == extDeviceNumber)
            {
                // not in list
                log.writeLog(__LINE__, "statusControl: SET_SWITCH_STATUS: Switch not valid: " + name, LOG_TYPE_ERROR);
                break;
            }
        }
        break;

        case SET_DBROOT_STATUS:
        {
            ByteStream::byte state;
            std::string name;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> name;
            *msg >> state;

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set DBroot " + name + " State = " + oamState[state], LOG_TYPE_DEBUG);

            if ( dbrootNumber == 0 )
            {
                // no dbroots setup in shared memory, must be internal
                log.writeLog(__LINE__, "statusControl: SET_DBROOT_STATUS: DBroot not valid: " + name, LOG_TYPE_ERROR);
                break;
            }

            int i = 0;

            for ( ; i < dbrootNumber; ++i)
            {
                memcpy(charName, fShmDbrootStatus[i].Name, NAMESIZE);
                shmName = charName;

                if ( name == shmName )
                {
                    fShmDbrootStatus[i].OpState = state;
                    memcpy(fShmDbrootStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                    break;
                }
            }

            if ( i == dbrootNumber)
            {
                // not in list
                log.writeLog(__LINE__, "statusControl: SET_DBROOT_STATUS: DBroot not valid: " + name, LOG_TYPE_ERROR);
                break;
            }
        }
        break;

        case SET_NIC_STATUS:
        {
            ByteStream::byte state;
            std::string hostName;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> hostName;
            *msg >> state;

            if (!runStandby)
            {
                ByteStream ackmsg;
                ackmsg << (ByteStream::byte) requestType;
                fIos->write(ackmsg);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Set NIC " + hostName + " State = " + oamState[state], LOG_TYPE_DEBUG);

            int i = 0;

            for ( ; i < NICNumber; ++i)
            {
                memcpy(charName, fShmNICStatus[i].Name, NAMESIZE);
                shmName = charName;

                if ( hostName == shmName )
                {
                    fShmNICStatus[i].OpState = state;
                    memcpy(fShmNICStatus[i].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                    break;
                }
            }

            if ( i == NICNumber)
            {
                // not in list
                log.writeLog(__LINE__, "statusControl: SET_NIC_STATUS: NIC not valid: " + hostName, LOG_TYPE_ERROR);
                break;
            }
        }
        break;

        case ADD_MODULE:
        {
            ByteStream ackmsg;
            ByteStream::byte moduleCount, nicCount;
            oam::DeviceNetworkConfig devicenetworkconfig;
            oam::DeviceNetworkList devicenetworklist;
            string value;
            MonitorConfig currentConfig;

            *msg >> moduleCount;

            for (int i = 0; i < moduleCount; i++)
            {
                *msg >> value;
                devicenetworkconfig.DeviceName = value;
                devicenetworklist.push_back(devicenetworkconfig);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Add Module");

            string moduleType = devicenetworkconfig.DeviceName.substr(0, MAX_MODULE_TYPE_SIZE);
            string OAMParentModuleType = currentConfig.OAMParentName().substr(0, 2);

            // add to module status shared memory
            DeviceNetworkList::iterator pt = devicenetworklist.begin();

            for ( ; pt != devicenetworklist.end() ; pt++)
            {
                moduleNameList.push_back((*pt).DeviceName);

                string moduleName = (*pt).DeviceName;
                memcpy(fShmSystemStatus[fmoduleNumber].Name, moduleName.c_str(), NAMESIZE);
                fShmSystemStatus[fmoduleNumber].OpState = oam::MAN_DISABLED;
                memcpy(fShmSystemStatus[fmoduleNumber].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                fmoduleNumber++;
            }

            // add to NIC status shared memory
            *msg >> nicCount;

            for (int i = 0; i < nicCount; i++)
            {
                *msg >> value;
                memcpy(fShmNICStatus[NICNumber].Name, value.c_str(), NAMESIZE);
                fShmNICStatus[NICNumber].OpState = oam::INITIAL;
                memcpy(fShmNICStatus[NICNumber].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                NICNumber++;
            }

            processStatusList::iterator listPtr;
            listPtr = aPtr->begin();

            // add to process status shared memory
            pt = devicenetworklist.begin();

            for ( ; pt != devicenetworklist.end() ; pt++)
            {
                for ( unsigned int j = 0; j < systemprocessconfig.processconfig.size(); j++)
                {
                    //skip if both BootLaunch and LaunchID are 0
                    if ( systemprocessconfig.processconfig[j].BootLaunch == 0 &&
                            systemprocessconfig.processconfig[j].LaunchID == 0 )
                        continue;

                    // "ChildOAMModule" "ParentOAMModule" dm/um/pm
                    string processModuleType = systemprocessconfig.processconfig[j].ModuleType;

                    if (processModuleType == moduleType
                            || ( processModuleType == "um" &&
                                 moduleType == "pm" && PMwithUM == "y")
                            || processModuleType == "ChildExtOAMModule"
                            || (processModuleType == "ChildOAMModule" )
                            || (processModuleType == "ParentOAMModule" && moduleType == OAMParentModuleType) )
                    {
                        if ( processModuleType == "um" &&
                                moduleType == "pm" && PMwithUM == "y" &&
                                systemprocessconfig.processconfig[j].ProcessName == "DMLProc" )
                            continue;

                        if ( processModuleType == "um" &&
                                moduleType == "pm" && PMwithUM == "y" &&
                                systemprocessconfig.processconfig[j].ProcessName == "DDLProc" )
                            continue;

                        processstatus procstat;
                        procstat.ProcessName = systemprocessconfig.processconfig[j].ProcessName;
                        procstat.ModuleName = (*pt).DeviceName;
                        procstat.tableIndex = processNumber;
                        fstatusListPtr.push_back(procstat);

                        fShmProcessStatus[processNumber].ProcessOpState = oam::MAN_OFFLINE;
                        fShmProcessStatus[processNumber].ProcessID = 0;
                        memcpy(fShmProcessStatus[processNumber].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);
                        processNumber++;
                    }
                }
            }

            ackmsg << (ByteStream::byte) API_SUCCESS;
            fIos->write(ackmsg);

            try
            {
                oam.getSystemConfig(systemModuleTypeConfig);
            }
            catch (exception& ex)
            {
                string error = ex.what();
                log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
            }
            catch (...)
            {
                log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
            }
        }
        break;

        case REMOVE_MODULE:
        {
            ByteStream ackmsg;
            ByteStream::byte moduleCount;
            oam::DeviceNetworkConfig devicenetworkconfig;
            oam::DeviceNetworkList devicenetworklist;
            string value;
            std::string shmName;
            char charName[NAMESIZE];

            *msg >> moduleCount;

            for (int i = 0; i < moduleCount; i++)
            {
                *msg >> value;
                devicenetworkconfig.DeviceName = value;
                devicenetworklist.push_back(devicenetworkconfig);
            }

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Remove Module");

            // remove from module status shared memory
            DeviceNetworkList::iterator pt = devicenetworklist.begin();

            for ( ; pt != devicenetworklist.end() ; pt++)
            {
                string moduleName = (*pt).DeviceName;

                for ( int j = 0 ; j < fmoduleNumber ; j++ )
                {
                    memcpy(charName, fShmSystemStatus[j].Name, NAMESIZE);
                    shmName = charName;

                    if ( moduleName == shmName )
                    {
                        for ( int k = j + 1 ; k < fmoduleNumber ; k++)
                        {
                            string name = fShmSystemStatus[k].Name;
                            int state = fShmSystemStatus[k].OpState;
                            string changeDate = fShmSystemStatus[k].StateChangeDate;

                            memcpy(fShmSystemStatus[j].Name, name.c_str(), NAMESIZE);
                            fShmSystemStatus[j].OpState = state;
                            memcpy(fShmSystemStatus[j].StateChangeDate, changeDate.c_str(), DATESIZE);
                        }

                        fmoduleNumber--;
                    }
                }
            }

            // remove from process status shared memory
            pt = devicenetworklist.begin();

            for ( ; pt != devicenetworklist.end() ; pt++)
            {
                string moduleName = (*pt).DeviceName;

                processStatusList::iterator listPtr;
                //processStatusList* aPtr = statusListPtr();
                listPtr = aPtr->begin();

                for (; listPtr != aPtr->end(); )
                {
                    if ( moduleName == (*listPtr).ModuleName )
                        aPtr->erase(listPtr);
                    else
                        ++listPtr;
                }
            }

            ackmsg << (ByteStream::byte) API_SUCCESS;
            fIos->write(ackmsg);

            try
            {
                oam.getSystemConfig(systemModuleTypeConfig);
            }
            catch (exception& ex)
            {
                string error = ex.what();
                log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
            }
            catch (...)
            {
                log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
            }
        }
        break;

        case ADD_EXT_DEVICE:
        {
            ByteStream ackmsg;
            string device;

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Add External Device");

            *msg >> device;

            fShmExtDeviceStatus[extDeviceNumber].OpState = oam::INITIAL;
            memcpy(fShmExtDeviceStatus[extDeviceNumber].Name, device.c_str(), NAMESIZE);
            extDeviceNumber++;

            if (!runStandby)
            {
                ackmsg << (ByteStream::byte) ADD_EXT_DEVICE;
                fIos->write(ackmsg);
            }
        }
        break;

        case REMOVE_EXT_DEVICE:
        {
            ByteStream ackmsg;
            string device;
            std::string shmName;
            char charName[NAMESIZE];

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Remove External Device");

            *msg >> device;

            for ( int j = 0 ; j < extDeviceNumber ; j++ )
            {
                memcpy(charName, fShmExtDeviceStatus[j].Name, NAMESIZE);
                shmName = charName;

                if ( device == shmName )
                {
                    for ( int k = j + 1 ; k < extDeviceNumber ; k++)
                    {
                        string name = fShmExtDeviceStatus[k].Name;
                        int state = fShmExtDeviceStatus[k].OpState;
                        string changeDate = fShmExtDeviceStatus[k].StateChangeDate;

                        memcpy(fShmExtDeviceStatus[j].Name, name.c_str(), NAMESIZE);
                        fShmExtDeviceStatus[j].OpState = state;
                        memcpy(fShmExtDeviceStatus[j].StateChangeDate, changeDate.c_str(), DATESIZE);
                    }

                    extDeviceNumber--;
                }
            }

            if (!runStandby)
            {
                ackmsg << (ByteStream::byte) REMOVE_EXT_DEVICE;
                fIos->write(ackmsg);
            }
        }
        break;

        case ADD_DBROOT:
        {
            ByteStream ackmsg;
            string device;

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Add DBRoot");

            *msg >> device;

            fShmDbrootStatus[dbrootNumber].OpState = oam::INITIAL;
            memcpy(fShmDbrootStatus[dbrootNumber].Name, device.c_str(), NAMESIZE);
            memcpy(fShmDbrootStatus[dbrootNumber].StateChangeDate, oam.getCurrentTime().c_str(), DATESIZE);

            dbrootNumber++;

            if (!runStandby)
            {
                ackmsg << (ByteStream::byte) ADD_DBROOT;
                fIos->write(ackmsg);
            }
        }
        break;

        case REMOVE_DBROOT:
        {
            ByteStream ackmsg;
            string device;
            std::string shmName;
            char charName[NAMESIZE];

            log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: Remove DBRoot");

            *msg >> device;

            for ( int j = 0 ; j < dbrootNumber ; j++ )
            {
                memcpy(charName, fShmDbrootStatus[j].Name, NAMESIZE);
                shmName = charName;

                if ( device == shmName )
                {
                    for ( int k = j + 1 ; k < dbrootNumber ; k++)
                    {
                        string name = fShmDbrootStatus[k].Name;
                        int state = fShmDbrootStatus[k].OpState;
                        string changeDate = fShmDbrootStatus[k].StateChangeDate;

                        memcpy(fShmDbrootStatus[j].Name, name.c_str(), NAMESIZE);
                        fShmDbrootStatus[j].OpState = state;
                        memcpy(fShmDbrootStatus[j].StateChangeDate, changeDate.c_str(), DATESIZE);
                    }

                    dbrootNumber--;
                }
            }

            if (!runStandby)
            {
                ackmsg << (ByteStream::byte) REMOVE_DBROOT;
                fIos->write(ackmsg);
            }
        }
        break;

        case GET_SHARED_MEM:
        {
            ByteStream ackmsg;
            ByteStream::byte type;

            *msg >> type;

            switch (type)
            {
                case 1:
                {
                    log.writeLog(__LINE__, "statusControl: REQUEST RECEIVED: GET_SHARED_MEM for process");

                    ByteStream::byte processNumber;

                    *msg >> processNumber;

                    ackmsg << (ByteStream::byte) GET_SHARED_MEM;

                    for ( int i = 0 ; i < processNumber ; i++ )
                    {
                        ackmsg << (ByteStream::quadbyte) fShmProcessStatus[i].ProcessID;
                        ackmsg << fShmProcessStatus[i].ProcessOpState;
                    }

                    fIos->write(ackmsg);

                    break;
                }

                default:
                    break;
            }
        }
        break;

        default:
            break;

    } // end of switch

    //log.writeLog(__LINE__, "***end, close create thread", LOG_TYPE_DEBUG);
    fIos->close();
    delete fIos;
    delete msg;
    pthread_detach (ThreadId);
    pthread_exit(0);
    return NULL;
}

/******************************************************************************************
* @brief	updateShareMemory
*
* purpose:	Get and update shared memory from Parent OAM module
*
******************************************************************************************/
void updateShareMemory(processStatusList* aPtr)
{
    MonitorLog log;
    MonitorConfig config;
    ProcessMonitor aMonitor(config, log);
    Oam oam;

//	log.writeLog(__LINE__, "Get Process Status shared Memory from Active OAM", LOG_TYPE_DEBUG);

    SystemProcessStatus systemprocessstatus;
    ProcessStatus processstatus;

    processStatusList::iterator listPtr;
    listPtr = aPtr->begin();

    try
    {
        oam.getProcessStatus(systemprocessstatus);

        for ( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
        {
            int shmIndex = 0;

            for (; listPtr != aPtr->end(); ++listPtr)
            {
                if ((*listPtr).ProcessName == systemprocessstatus.processstatus[i].ProcessName &&
                        (*listPtr).ModuleName == systemprocessstatus.processstatus[i].Module)
                {
                    shmIndex = (*listPtr).tableIndex;
                    break;
                }
            }

            if (listPtr == aPtr->end())
                continue;

            //update table
            fShmProcessStatus[shmIndex].ProcessOpState = systemprocessstatus.processstatus[i].ProcessOpState;
            fShmProcessStatus[shmIndex].ProcessID = systemprocessstatus.processstatus[i].ProcessID;
            string stime = systemprocessstatus.processstatus[i].StateChangeDate ;
            memcpy(fShmProcessStatus[shmIndex].StateChangeDate, stime.c_str(), DATESIZE);
        }

//		log.writeLog(__LINE__, "Process Status shared Memory Initialized from Active OAM Module", LOG_TYPE_DEBUG);
    }
    catch (...)
    {
        return;
    }

//	log.writeLog(__LINE__, "Get System Status shared Memory from Active OAM", LOG_TYPE_DEBUG);

    SystemStatus systemstatus;

    try
    {
        oam.getSystemStatus(systemstatus, false);
        fShmSystemStatus[0].OpState = systemstatus.SystemOpState;
        string stime = systemstatus.systemmodulestatus.modulestatus[0].StateChangeDate ;
        memcpy(fShmSystemStatus[0].StateChangeDate, stime.c_str(), DATESIZE);
    }
    catch (...)
    {
        return;
    }

//	log.writeLog(__LINE__, "Get Module Status shared Memory from Active OAM", LOG_TYPE_DEBUG);

    std::string shmName;
    char charName[NAMESIZE];

    for ( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
    {
        if ( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
            // end of list
            break;

        int j = 1;

        for ( ; j < fmoduleNumber; ++j)
        {
            memcpy(charName, fShmSystemStatus[j].Name, NAMESIZE);
            shmName = charName;

            if ( systemstatus.systemmodulestatus.modulestatus[i].Module == shmName )
            {
                fShmSystemStatus[j].OpState = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;
                string stime = systemstatus.systemmodulestatus.modulestatus[i].StateChangeDate ;
                memcpy(fShmSystemStatus[j].StateChangeDate, stime.c_str(), DATESIZE);
                break;
            }
        }
    }
}
// vim:ts=4 sw=4:

