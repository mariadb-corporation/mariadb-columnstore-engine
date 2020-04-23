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

/***************************************************************************
* $Id: processmonitor.cpp 2044 2013-08-07 19:47:37Z dhill $
*
 ***************************************************************************/

#include <boost/scoped_ptr.hpp>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "columnstoreversion.h"
#include "mcsconfig.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
#include "processmonitor.h"
#include "installdir.h"
#include "cacheutils.h"
#include "ddlcleanuputil.h"
using namespace cacheutils;

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace alarmmanager;
using namespace logging;
using namespace config;

using namespace idbdatafile;
namespace bf = boost::filesystem;

extern string systemOAM;
extern string dm_server;
extern bool runStandby;
extern bool processInitComplete;
extern int fmoduleNumber;
extern string cloud;
extern string DataRedundancyConfig;
extern bool rootUser;
extern string USER;
extern bool HDFS;
extern string PMwithUM;
extern bool startProcMon;
extern string tmpLogDir;
extern string SUDO;

//std::string gOAMParentModuleName;
bool gOAMParentModuleFlag;
bool gOAMStandbyModuleFlag;

typedef boost::tuple<std::string, ALARMS, int> sendAlarmInfo_t;
typedef boost::tuple<std::string, int, pid_t> sendProcessInfo_t;

pthread_mutex_t ALARM_LOCK;
pthread_mutex_t LIST_LOCK;
pthread_mutex_t PROCESS_LOCK;

namespace processmonitor
{

void sendAlarmThread (sendAlarmInfo_t* t);
void sendProcessThread (sendProcessInfo_t* t);

using namespace oam;

/******************************************************************************************
* @brief	MonitorConfig
*
* purpose:	MonitorConfig constructor
*
******************************************************************************************/

MonitorConfig::MonitorConfig()
{
    Oam oam;
    oamModuleInfo_t t;

    //get local module info
    try
    {
        t = oam.getModuleInfo();
        flocalModuleName = boost::get<0>(t);
        flocalModuleType = boost::get<1>(t);
        flocalModuleID = boost::get<2>(t);
        fOAMParentModuleName = boost::get<3>(t);
        fOAMParentModuleFlag = boost::get<4>(t);
        fserverInstallType = boost::get<5>(t);
        fOAMStandbyModuleName = boost::get<6>(t);
        fOAMStandbyModuleFlag = boost::get<7>(t);

        gOAMStandbyModuleFlag = boost::get<7>(t);
        gOAMParentModuleFlag = boost::get<4>(t);
    }
    catch (exception& e)
    {
        cout << endl << "ProcMon Construct Error reading getModuleInfo = " << e.what() << endl;
    }

//	cout << "OAMParentModuleName = " << fOAMParentModuleName << endl;

//	if ( fOAMParentModuleName == oam::UnassignedName ) {
//		cout << endl << "OAMParentModuleName == oam::UnassignedName, exiting " << endl;
//		exit (-1);
//	}

    //get calpont software version and release
    fsoftwareVersion = columnstore_version;
    fsoftwareRelease = columnstore_release;

}


/******************************************************************************************
* @brief	MonitorConfig destructor
*
* purpose:	MonitorConfig destructor
*
******************************************************************************************/
MonitorConfig::~MonitorConfig()
{
}

/******************************************************************************************
* @brief	MonitorLog Constructor
*
* purpose:	Constructor:open the log file for writing
*
******************************************************************************************/
MonitorLog::MonitorLog()
{
}

/******************************************************************************************
* @brief	MonitorLog Destructor
*
* purpose:	Destructor:close the log file
*
******************************************************************************************/
MonitorLog::~MonitorLog()
{
}

/******************************************************************************************
* @brief	writeLog for string
*
* purpose:	write string message to the log file
*
******************************************************************************************/
void	MonitorLog::writeLog(const int lineNumber, const string logContent, const LOG_TYPE logType)
{
    //Log this event
    LoggingID lid(18);
    MessageLog ml(lid);
    Message msg;
    Message::Args args;
    args.add(logContent);
    msg.format(args);

    switch (logType)
    {
        case LOG_TYPE_DEBUG:
            ml.logDebugMessage(msg);
            break;

        case LOG_TYPE_INFO:
            ml.logInfoMessage(msg);
            break;

        case LOG_TYPE_WARNING:
            ml.logWarningMessage(msg);
            break;

        case LOG_TYPE_ERROR:
            args.add("line:");
            args.add(lineNumber);
            ml.logErrorMessage(msg);
            break;

        case LOG_TYPE_CRITICAL:
            ml.logCriticalMessage(msg);
            break;
    }

    return;
}

/******************************************************************************************
* @brief	writeLog for integer
*
* purpose:	write integer information to the log file
*
******************************************************************************************/
void	MonitorLog::writeLog(const int lineNumber, const int logContent, const LOG_TYPE logType)
{
    //Log this event
    LoggingID lid(18);
    MessageLog ml(lid);
    Message msg;
    Message::Args args;
    args.add(logContent);
    msg.format(args);

    switch (logType)
    {
        case LOG_TYPE_DEBUG:
            ml.logDebugMessage(msg);
            break;

        case LOG_TYPE_INFO:
            ml.logInfoMessage(msg);
            break;

        case LOG_TYPE_WARNING:
            ml.logWarningMessage(msg);
            break;

        case LOG_TYPE_ERROR:
            args.add("line:");
            args.add(lineNumber);
            ml.logErrorMessage(msg);
            break;

        case LOG_TYPE_CRITICAL:
            ml.logCriticalMessage(msg);
            break;
    }

    return;
}

/******************************************************************************************
* @brief	ProcessMonitor Constructor
*
* purpose:	ProcessMonitor Constructor
*
******************************************************************************************/
ProcessMonitor::ProcessMonitor(MonitorConfig& aconfig, MonitorLog& alog):
    config(aconfig), log(alog)
{
//	log.writeLog(__LINE__, "Process Monitor starts");
}

/******************************************************************************************
* @brief	ProcessMonitor Default Destructor
*
* purpose:	ProcessMonitor Default Destructor
*
******************************************************************************************/
ProcessMonitor::~ProcessMonitor()
{
}

/******************************************************************************************
* @brief	statusListPtr
*
* purpose:	return the process status list
*
******************************************************************************************/
//processStatusList*	ProcessMonitor::statusListPtr()
//{
//	return &fstatusListPtr;
//}

/******************************************************************************************
* @brief	buildList
*
* purpose:	Build a list of processes the monitor started
*
******************************************************************************************/
void  MonitorConfig::buildList(string ProcessModuleType, string processName, string ProcessLocation,
                               string arg_list[MAXARGUMENTS], uint16_t launchID, pid_t processID,
                               uint16_t state, uint16_t BootLaunch, string RunType,
                               string DepProcessName[MAXDEPENDANCY], string DepModuleName[MAXDEPENDANCY],
                               string LogFile)
{
    //check if the process is already in the list
    MonitorLog log;
    Oam oam;

    if ( processName == "mysqld" )
        return;
        
    // Might need to add a similar do-nothing clause for StorageManager?

    pthread_mutex_lock(&LIST_LOCK);

    // get current time in seconds
    time_t cal;
    time (&cal);

    processList::iterator listPtr;
    processList* aPtr = monitoredListPtr();

    //Log the current list
    /*	log.writeLog(__LINE__, "");
    	log.writeLog(__LINE__, "BEGIN: The current list in this monitor is");

    	for (listPtr=aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
    	{
    		log.writeLog(__LINE__, (*listPtr).ProcessModuleType);
    		log.writeLog(__LINE__, (*listPtr).ProcessName);
    		log.writeLog(__LINE__, (*listPtr).ProcessLocation);
    		log.writeLog(__LINE__, (*listPtr).currentTime);
    		log.writeLog(__LINE__, (*listPtr).processID);
    		log.writeLog(__LINE__, (*listPtr).state);
    	}
    */

    listPtr = aPtr->begin();

    for (; listPtr != aPtr->end(); ++listPtr)
    {
        if ((*listPtr).ProcessName == processName)
            break;
    }

    if (listPtr == aPtr->end())
    {
        // not in list, add it
        processInfo proInfo;
        proInfo.ProcessModuleType = ProcessModuleType;
        proInfo.ProcessName = processName;
        proInfo.ProcessLocation = ProcessLocation;

        for (unsigned int i = 0; i < MAXARGUMENTS; i++)
        {
            if (arg_list[i].length() == 0)
                break;

            proInfo.ProcessArgs[i] = arg_list[i];
        }

        proInfo.launchID = launchID;
        proInfo.currentTime = cal;
        proInfo.processID = processID;
        proInfo.state = state;
        proInfo.BootLaunch = BootLaunch;
        proInfo.RunType = RunType;
        proInfo.LogFile = LogFile;
        proInfo.dieCounter = 0;

        for (unsigned int i = 0; i < MAXDEPENDANCY; i++)
        {
            if (DepProcessName[i].length() == 0)
                break;

            proInfo.DepProcessName[i] = DepProcessName[i];
        }

        for (unsigned int i = 0; i < MAXDEPENDANCY; i++)
        {
            if (DepModuleName[i].length() == 0)
                break;

            proInfo.DepModuleName[i] = DepModuleName[i];
        }

        listPtr = aPtr->begin();

        if ( listPtr == aPtr->end())
        {
            // list empty, add first one
            fmonitoredListPtr.push_back(proInfo);
        }
        else
        {
            for (; listPtr != aPtr->end(); ++listPtr)
            {
                if ((*listPtr).launchID > launchID)
                {
                    fmonitoredListPtr.insert(listPtr, proInfo);
                    break;
                }
            }

            if ( listPtr == aPtr->end())
                fmonitoredListPtr.push_back(proInfo);
        }
    }
    else
    {
        // in list, just update the information

        if ( ProcessLocation.empty() )
            //status update only
            (*listPtr).state = state;
        else
        {
            (*listPtr).processID = processID;
            (*listPtr).currentTime = cal;
            (*listPtr).state = state;
            (*listPtr).launchID = launchID;
            (*listPtr).BootLaunch = BootLaunch;
            (*listPtr).RunType = RunType;
            (*listPtr).LogFile = LogFile;

            for (unsigned int i = 0; i < MAXARGUMENTS; i++)
            {
                (*listPtr).ProcessArgs[i] = arg_list[i];
            }
        }
    }

    //Log the current list
    /*	log.writeLog(__LINE__, "");
    	log.writeLog(__LINE__, "END: The current list in this monitor is");

    	for (listPtr=aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
    	{
    		log.writeLog(__LINE__, (*listPtr).ProcessModuleType);
    		log.writeLog(__LINE__, (*listPtr).ProcessName);
    		log.writeLog(__LINE__, (*listPtr).ProcessLocation);
    		log.writeLog(__LINE__, (*listPtr).currentTime);
    		log.writeLog(__LINE__, (*listPtr).processID);
    		log.writeLog(__LINE__, (*listPtr).state);
    	}
    */
    pthread_mutex_unlock(&LIST_LOCK);

    return;
}

/******************************************************************************************
* @brief	monitoredListPtr
*
* purpose:	return the process list
*
******************************************************************************************/
processList*	MonitorConfig::monitoredListPtr()
{
    return &fmonitoredListPtr;
}

/******************************************************************************************
* @brief	processMessage
*
* purpose:	receive and process message
*
******************************************************************************************/
void ProcessMonitor::processMessage(messageqcpp::ByteStream msg, messageqcpp::IOSocket mq)

{
    Oam oam;
    ByteStream	ackMsg;
    MonitorConfig currentConfig;

    ByteStream::byte messageType;
    ByteStream::byte requestID;
    ByteStream::byte actIndicator;
    ByteStream::byte manualFlag;
    string processName;

    msg >> messageType;

    switch (messageType)
    {
        case REQUEST:
        {
            msg >> requestID;
            msg >> actIndicator;

            if (!processInitComplete)
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) requestID;
                ackMsg << (ByteStream::byte) oam::API_FAILURE;
                mq.write(ackMsg);
                break;
            }

            switch (requestID)
            {
                case STOP:
                {
                    msg >> processName;
                    msg >> manualFlag;
                    log.writeLog(__LINE__,  "MSG RECEIVED: Stop process request on " + processName);
                    int requestStatus = API_SUCCESS;

                    // check for mysqld
                    if ( processName == "mysqld" )
                    {
                        try
                        {
                            oam.actionMysqlCalpont(MYSQL_STOP);
                        }
                        catch (...)
                        {}

                        ackMsg << (ByteStream::byte) ACK;
                        ackMsg << (ByteStream::byte) STOP;
                        ackMsg << (ByteStream::byte) API_SUCCESS;
                        mq.write(ackMsg);

                        log.writeLog(__LINE__, "STOP: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

                        break;
                    }
                    processList::iterator listPtr;
                    processList* aPtr = config.monitoredListPtr();
                    listPtr = aPtr->begin();

                    for (; listPtr != aPtr->end(); ++listPtr)
                    {
                        if ((*listPtr).ProcessName == processName)
                        {
                            // update local process state
                            if ( manualFlag )
                            {
                                (*listPtr).state = oam::MAN_OFFLINE;
                                (*listPtr).dieCounter = 0;
                            }
                            else
                                (*listPtr).state = oam::AUTO_OFFLINE;

                            //stop the process first
                            if (stopProcess((*listPtr).processID, (*listPtr).ProcessName, (*listPtr).ProcessLocation, actIndicator, manualFlag))
                                requestStatus = API_FAILURE;
                            else
                                (*listPtr).processID = 0;

                            break;
                        }
                    }

                    if (listPtr == aPtr->end())
                    {
                        log.writeLog(__LINE__,  "ERROR: No such process: " + processName);
                        requestStatus = API_FAILURE;
                    }

                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) STOP;
                    ackMsg << (ByteStream::byte) requestStatus;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "STOP: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

                    break;
                }

                case START:
                {
                    msg >> processName;
                    msg >> manualFlag;
                    log.writeLog(__LINE__, "MSG RECEIVED: Start process request on: " + processName);

                    // check for mysqld
                    if ( processName == "mysqld" )
                    {
                        try
                        {
                            oam.actionMysqlCalpont(MYSQL_START);
                        }
                        catch (...)
                        {}

                        ackMsg << (ByteStream::byte) ACK;
                        ackMsg << (ByteStream::byte) START;
                        ackMsg << (ByteStream::byte) API_SUCCESS;
                        mq.write(ackMsg);

                        log.writeLog(__LINE__, "START: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

                        break;
                    }

                    ProcessConfig processconfig;
                    ProcessStatus processstatus;

                    try
                    {
                        //Get the process information
                        Oam oam;
                        oam.getProcessConfig(processName, config.moduleName(), processconfig);

                        oam.getProcessStatus(processName, config.moduleName(), processstatus);
                    }
                    catch (exception& ex)
                    {
//						string error = ex.what();
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR );
                    }
                    catch (...)
                    {
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR );
                    }

                    int requestStatus = API_SUCCESS;

                    //check the process current status & start the requested process
                    if (processstatus.ProcessOpState != oam::ACTIVE)
                    {

                        int initType = oam::STANDBY_INIT;

                        if ( actIndicator == oam::GRACEFUL_STANDBY)
                        {
                            //this module running Parent OAM Standby
                            runStandby = true;
                            log.writeLog(__LINE__,  "ProcMon Running Hot-Standby");

                            // delete any old active alarm log file
                            unlink ("/var/log/mariadb/columnstore/activeAlarms");
                        }

                        //Check for SIMPLEX runtype processes
                        initType = checkSpecialProcessState( processconfig.ProcessName, processconfig.RunType, processconfig.ModuleType );

                        if ( initType == oam::COLD_STANDBY)
                        {
                            //there is a mate active, skip
                            config.buildList(processconfig.ModuleType,
                                             processconfig.ProcessName,
                                             processconfig.ProcessLocation,
                                             processconfig.ProcessArgs,
                                             processconfig.LaunchID,
                                             0,
                                             oam::COLD_STANDBY,
                                             processconfig.BootLaunch,
                                             processconfig.RunType,
                                             processconfig.DepProcessName,
                                             processconfig.DepModuleName,
                                             processconfig.LogFile);

                            requestStatus = API_SUCCESS;
                            ackMsg << (ByteStream::byte) ACK;
                            ackMsg << (ByteStream::byte) START;
                            ackMsg << (ByteStream::byte) requestStatus;
                            mq.write(ackMsg);
                            //sleep(1);

                            log.writeLog(__LINE__, "START: process left STANDBY " + processName);
                            log.writeLog(__LINE__, "START: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));
                            break;
                        }

                        pid_t processID = startProcess(processconfig.ModuleType,
                                                       processconfig.ProcessName,
                                                       processconfig.ProcessLocation,
                                                       processconfig.ProcessArgs,
                                                       processconfig.LaunchID,
                                                       processconfig.BootLaunch,
                                                       processconfig.RunType,
                                                       processconfig.DepProcessName,
                                                       processconfig.DepModuleName,
                                                       processconfig.LogFile,
                                                       initType,
                                                       actIndicator);

                        // StorageManager doesn't send the "I'm online" msg to Proc*.
                        // Just mark it active for now.  TODO: make it use the ping fcn in IDB* instead.
                        if (processconfig.ProcessName == "StorageManager")
                            oam.setProcessStatus("StorageManager", boost::get<0>(oam.getModuleInfo()), 
                              oam::ACTIVE, processID);
                        
                        if ( processID > oam::API_MAX )
                            processID = oam::API_SUCCESS;

                        requestStatus = processID;
                    }
                    else
                        log.writeLog(__LINE__, "START: process already active " + processName);

                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) START;
                    ackMsg << (ByteStream::byte) requestStatus;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "START: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

                    break;
                }

                case RESTART:
                {
                    msg >> processName;
                    msg >> manualFlag;
                    log.writeLog(__LINE__,  "MSG RECEIVED: Restart process request on " + processName);
                    int requestStatus = API_SUCCESS;

                    // check for mysqld restart
                    if ( processName == "mysqld" )
                    {
                        try
                        {
                            oam.actionMysqlCalpont(MYSQL_RESTART);
                        }
                        catch (...)
                        {}

                        ackMsg << (ByteStream::byte) ACK;
                        ackMsg << (ByteStream::byte) RESTART;
                        ackMsg << (ByteStream::byte) API_SUCCESS;
                        mq.write(ackMsg);

                        log.writeLog(__LINE__, "RESTART: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

                        break;
                    }

                    processList::iterator listPtr;
                    processList* aPtr = config.monitoredListPtr();
                    listPtr = aPtr->begin();

                    for (; listPtr != aPtr->end(); ++listPtr)
                    {
                        if ((*listPtr).ProcessName == processName)
                        {
                            // update local process state
                            if ( manualFlag )
                            {
                                (*listPtr).state = oam::MAN_OFFLINE;
                                (*listPtr).dieCounter = 0;
                            }
                            else
                                (*listPtr).state = oam::AUTO_OFFLINE;

                            //stop the process first
                            if (stopProcess((*listPtr).processID, (*listPtr).ProcessName, (*listPtr).ProcessLocation, actIndicator, manualFlag))
                                requestStatus = API_FAILURE;
                            else
                            {
//								sleep(1);
                                (*listPtr).processID = 0;

                                //Check for SIMPLEX runtype processes
                                int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

                                if ( initType == oam::COLD_STANDBY )
                                {
                                    //there is a mate active, skip
                                    (*listPtr).state = oam::COLD_STANDBY;
                                    requestStatus = API_SUCCESS;
                                    //sleep(1);
                                    break;
                                }

                                //start the process again
                                pid_t processID = startProcess((*listPtr).ProcessModuleType,
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
                      
                                if ( processID > oam::API_MAX )
                                    processID = oam::API_SUCCESS;

                                requestStatus = processID;

                            }

                            break;
                        }
                    }

                    if (listPtr == aPtr->end())
                    {
                        log.writeLog(__LINE__,  "ERROR: No such process: " + processName, LOG_TYPE_ERROR );
                        requestStatus = API_FAILURE;
                    }

                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) RESTART;
                    ackMsg << (ByteStream::byte) requestStatus;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "RESTART: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

                    break;
                }

                case PROCREINITPROCESS:
                {
                    msg >> processName;
                    msg >> manualFlag;

                    log.writeLog(__LINE__, "MSG RECEIVED: Re-Init process request on: " + processName);

                    if ( processName == "cpimport" )
                    {
                        system("pkill -sighup cpimport");
                        for (int i=0; i < 10; i++)
                        {
                            //get pid
                            char buf[512];
                            FILE *cmd_pipe = popen("pidof -s cpimport", "r");

                            fgets(buf, 512, cmd_pipe);
                            pid_t pid = strtoul(buf, NULL, 10);

                            pclose( cmd_pipe );

                            if (pid)
                                sleep(2);
                            else
                                break;
                        }
                        // kill other processes
                        system("pkill -9 cpimport.bin");
                    }
                    else
                    {
                        processList::iterator listPtr;
                        processList* aPtr = config.monitoredListPtr();
                        listPtr = aPtr->begin();

                        for (; listPtr != aPtr->end(); ++listPtr)
                        {
                            if ((*listPtr).ProcessName == processName)
                            {
                                if ( (*listPtr).processID <= 1 )
                                {
                                    log.writeLog(__LINE__,  "ERROR: process not active", LOG_TYPE_DEBUG );
                                    break;
                                }

                                reinitProcess((*listPtr).processID, (*listPtr).ProcessName, actIndicator);
                                break;
                            }
                        }

                        if (listPtr == aPtr->end())
                        {
                            log.writeLog(__LINE__,  "ERROR: No such process: " + processName, LOG_TYPE_ERROR );
                        }
                    }

                    log.writeLog(__LINE__, "PROCREINITPROCESS: completed, no ack to ProcMgr");
                    break;
                }

                case STOPALL:
                {
                    msg >> manualFlag;
                    log.writeLog(__LINE__,  "MSG RECEIVED: Stop All process request...");

                    if ( actIndicator == STATUS_UPDATE )
                    {
                        //check and send notification
                        MonitorConfig config;

                        if ( config.moduleType() == "um" )
                            oam.sendDeviceNotification(config.moduleName(), START_UM_DOWN);
                        else if ( gOAMParentModuleFlag )
                            oam.sendDeviceNotification(config.moduleName(), START_PM_MASTER_DOWN);
                        else if (gOAMStandbyModuleFlag)
                            oam.sendDeviceNotification(config.moduleName(), START_PM_STANDBY_DOWN);
                        else
                            oam.sendDeviceNotification(config.moduleName(), START_PM_COLD_DOWN);

                        ackMsg << (ByteStream::byte) ACK;
                        ackMsg << (ByteStream::byte) STOPALL;
                        ackMsg << (ByteStream::byte) API_SUCCESS;
                        mq.write(ackMsg);

                        log.writeLog(__LINE__, "STOPALL: ACK back to ProcMgr, STATUS_UPDATE only performed");
                        break;
                    }

                    //get local module run-type
                    string runType = oam::LOADSHARE;	//default

                    try
                    {
                        ModuleTypeConfig moduletypeconfig;
                        oam.getSystemConfig(config.moduleType(), moduletypeconfig);
                        runType = moduletypeconfig.RunType;
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

                    //Loop reversely through the process list, stop all processes
                    processList* aPtr = config.monitoredListPtr();
                    processList::reverse_iterator rPtr;
                    uint16_t rtnCode;
                    int requestStatus = API_SUCCESS;

                    for (rPtr = aPtr->rbegin(); rPtr != aPtr->rend(); ++rPtr)
                    {
                        if ( (*rPtr).BootLaunch == INIT_LAUNCH)
                            //skip
                            continue;

                        if ( (*rPtr).BootLaunch == BOOT_LAUNCH &&
                                gOAMParentModuleFlag )
                            if ( actIndicator != INSTALL )
                                //skip
                                continue;

                        // update local process state here so monitor thread doesn't jump on it
                        if ( manualFlag )
                        {
                            (*rPtr).state = oam::MAN_OFFLINE;
                            (*rPtr).dieCounter = 0;
                        }
                        else
                            (*rPtr).state = oam::AUTO_OFFLINE;

                        rtnCode = stopProcess((*rPtr).processID, (*rPtr).ProcessName, (*rPtr).ProcessLocation, actIndicator, manualFlag);

                        if (rtnCode)
                            // error in stopping a process
                            requestStatus = API_FAILURE;
                        else
                            (*rPtr).processID = 0;
                    }

                    //reset BRM locks and clearShm
                    if ( requestStatus == oam::API_SUCCESS )
                    {
                        string logdir("/var/log/mariadb/columnstore");

                        if (access(logdir.c_str(), W_OK) != 0) logdir = tmpLogDir;

                        string cmd = "reset_locks > " + logdir + "/reset_locks.log1 2>&1";
                        system(cmd.c_str());
                        log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);

                        if ( !gOAMParentModuleFlag )
                        {
                            cmd = "clearShm -c > /dev/null 2>&1";
                            rtnCode = system(cmd.c_str());

                            if (WEXITSTATUS(rtnCode) != 1)
                            {
                                log.writeLog(__LINE__, "Successfully ran DBRM clearShm", LOG_TYPE_DEBUG);
                            }
                            else
                                log.writeLog(__LINE__, "Error running DBRM clearShm", LOG_TYPE_ERROR);
                        }

                        //stop the mysqld daemon
                        try
                        {
                            oam.actionMysqlCalpont(MYSQL_STOP);
                            log.writeLog(__LINE__, "Stop MySQL Process", LOG_TYPE_DEBUG);
                        }
                        catch (...)
                        {}

                        //send down notification
                        oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);

                        //setModule status to offline
                        if ( manualFlag )
                        {
                            try
                            {
                                oam.setModuleStatus(config.moduleName(), oam::MAN_OFFLINE);
                            }
                            catch (exception& ex)
                            {
                                string error = ex.what();
//								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                            }
                            catch (...)
                            {
//								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                            }
                        }
                        else
                        {
                            try
                            {
                                oam.setModuleStatus(config.moduleName(), oam::AUTO_OFFLINE);
                            }
                            catch (exception& ex)
                            {
                                string error = ex.what();
//								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                            }
                            catch (...)
                            {
//								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                            }
                        }
                    }
                    else
                    {
                        //setModule status to failed
                        try
                        {
                            oam.setModuleStatus(config.moduleName(), oam::FAILED);
                        }
                        catch (exception& ex)
                        {
                            string error = ex.what();
//							log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
                        }
                        catch (...)
                        {
//							log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                        }
                    }


                    if ( config.moduleType() == "pm" )
                    {
                        //go unmount disk NOT assigned to this pm
                        unmountExtraDBroots();
                    }

                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) STOPALL;
                    ackMsg << (ByteStream::byte) requestStatus;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "STOPALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

                    break;
                }

                case STARTALL:
                {
                    msg >> manualFlag;
                    int requestStatus = oam::API_SUCCESS;
                    log.writeLog(__LINE__,  "MSG RECEIVED: Start All process request...");

                    //start the mysqld daemon

                    try
                    {
                        oam.actionMysqlCalpont(MYSQL_START);
                    }
                    catch (...)
                    {
                        // mysqld didn't start, return with error
                        // mysql didn't start, return with error
                        log.writeLog(__LINE__, "STARTALL: MySQL failed to start, start-module failure", LOG_TYPE_CRITICAL);

                        ackMsg << (ByteStream::byte) ACK;
                        ackMsg << (ByteStream::byte) STARTALL;
                        ackMsg << (ByteStream::byte) oam::API_FAILURE;
                        mq.write(ackMsg);

                        try
                        {
                            oam.setProcessStatus("mysqld", config.moduleName(), oam::FAILED, 0);
                        }
                        catch (...)
                        {}

                        log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) oam::API_FAILURE));

                        break;
                    }

                    if ( config.moduleType() == "pm" )
                    {
                        //setup DBRoot mounts
                        createDataDirs(cloud);
                        int ret = checkDataMount();

                        if (ret != oam::API_SUCCESS)
                        {
                            int ret_status = oam::API_FAILURE;

                            log.writeLog(__LINE__, "checkDataMount error, startmodule failed", LOG_TYPE_CRITICAL);

                            ackMsg << (ByteStream::byte) ACK;
                            ackMsg << (ByteStream::byte) STARTALL;
                            ackMsg << (ByteStream::byte) ret_status;
                            mq.write(ackMsg);

                            log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) oam::API_FAILURE));

                            break;
                        }
                    }

                    //Loop through all Process belong to this module
                    processList::iterator listPtr;
                    processList* aPtr = config.monitoredListPtr();
                    listPtr = aPtr->begin();
                    requestStatus = API_SUCCESS;

                    //launched any processes controlled by ProcMon that aren't active
                    for (; listPtr != aPtr->end(); ++listPtr)
                    {
                        if ( (*listPtr).BootLaunch != BOOT_LAUNCH)
                            continue;

                        int opState = oam::ACTIVE;
                        bool degraded;
                        oam.getModuleStatus(config.moduleName(), opState, degraded);

                        if (opState == oam::FAILED)
                        {
                            requestStatus = oam::API_FAILURE;
                            break;
                        }

                        //check the process current status & start the requested process
                        if ((*listPtr).state == oam::MAN_OFFLINE ||
                                (*listPtr).state == oam::AUTO_OFFLINE ||
                                (*listPtr).state == oam::COLD_STANDBY ||
                                (*listPtr).state == oam::INITIAL)
                        {

                            //Check for SIMPLEX runtype processes
                            int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

                            if ( initType == oam::COLD_STANDBY )
                            {
                                //there is a mate active, skip
                                (*listPtr).state = oam::COLD_STANDBY;
                                requestStatus = API_SUCCESS;
                                //sleep(1);
                                continue;
                            }

                            pid_t processID = startProcess((*listPtr).ProcessModuleType,
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
                                  oam::ACTIVE, processID);
                          
                            if ( processID > oam::API_MAX )
                            {
                                processID = oam::API_SUCCESS;
                            }

                            requestStatus = processID;

                            if ( requestStatus != oam::API_SUCCESS )
                            {
                                // error in launching a process
                                break;
                            }

//							sleep(1);
                        }
                    }

                    if ( requestStatus == oam::API_SUCCESS )
                    {
                        //launched any processes controlled by ProcMgr
                        for (listPtr = aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
                        {
                            if ((*listPtr).BootLaunch != MGR_LAUNCH)
                                continue;

                            int opState = oam::ACTIVE;
                            bool degraded;
                            oam.getModuleStatus(config.moduleName(), opState, degraded);

                            if (opState == oam::FAILED)
                            {
                                requestStatus = oam::API_FAILURE;
                                break;
                            }

                            //check the process current status & start the requested process
                            if ((*listPtr).state == oam::MAN_OFFLINE ||
                                    (*listPtr).state == oam::AUTO_OFFLINE ||
                                    (*listPtr).state == oam::COLD_STANDBY ||
                                    (*listPtr).state == oam::INITIAL)
                            {

                                //Check for SIMPLEX runtype processes
                                int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

                                if ( initType == oam::COLD_STANDBY )
                                {
                                    //there is a mate active, skip
                                    (*listPtr).state = oam::COLD_STANDBY;
                                    requestStatus = API_SUCCESS;
                                    //sleep(1);
                                    continue;
                                }

                                pid_t processID = startProcess((*listPtr).ProcessModuleType,
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
                                      oam::ACTIVE, processID);
                         
                                if ( processID > oam::API_MAX )
                                    processID = oam::API_SUCCESS;

                                requestStatus = processID;

                                if ( requestStatus != oam::API_SUCCESS )
                                {
                                    // error in launching a process
                                    if ( requestStatus == oam::API_FAILURE &&
                                            (*listPtr).RunType == SIMPLEX)
										checkModuleFailover((*listPtr).ProcessName);
                                    else
                                        break;
                                }
                                else
                                {
                                    //run startup test script to perform basic DB sanity testing
                                    if ( gOAMParentModuleFlag
                                            && (*listPtr).ProcessName == "DBRMWorkerNode"
                                            && opState != oam::MAN_INIT )
                                    {
                                        if ( runStartupTest() != oam::API_SUCCESS )
                                        {
                                            requestStatus = oam::API_FAILURE_DB_ERROR;
                                            break;
                                        }
                                    }
                                }

//								sleep(2);
                            }
                            else
                            {
                                // if DBRMWorkerNode and ACTIVE, run runStartupTest
                                if ( gOAMParentModuleFlag
                                        && (*listPtr).ProcessName == "DBRMWorkerNode"
                                        && (*listPtr).state == oam::ACTIVE
                                        && opState != oam::MAN_INIT )
                                {
                                    if ( runStartupTest() != oam::API_SUCCESS )
                                    {
                                        requestStatus = oam::API_FAILURE_DB_ERROR;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if ( requestStatus == oam::API_SUCCESS )
                    {

                        //check and send noitification
                        MonitorConfig config;

                        if ( config.moduleType() == "um" )
                            oam.sendDeviceNotification(config.moduleName(), UM_ACTIVE);
                        else if ( gOAMParentModuleFlag )
                            oam.sendDeviceNotification(config.moduleName(), PM_MASTER_ACTIVE);
                        else if (gOAMStandbyModuleFlag)
                            oam.sendDeviceNotification(config.moduleName(), PM_STANDBY_ACTIVE);
                        else
                            oam.sendDeviceNotification(config.moduleName(), PM_COLD_ACTIVE);
                    }

                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) STARTALL;
                    ackMsg << (ByteStream::byte) requestStatus;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

                    break;
                }

                case SHUTDOWNMODULE:
                {
                    msg >> manualFlag;
                    log.writeLog(__LINE__,  "MSG RECEIVED: Shutdown Module request...");

                    //Loop reversely thorugh the process list

                    /*					processList* aPtr = config.monitoredListPtr();
                    					processList::reverse_iterator rPtr;
                    					uint16_t rtnCode;

                    					for (rPtr = aPtr->rbegin(); rPtr != aPtr->rend(); ++rPtr)
                    					{
                    						// don't shut yourself or ProcessManager down"
                    						if ((*rPtr).ProcessName == "ProcessMonitor" || (*rPtr).ProcessName == "ProcessManager")
                    							continue;

                    						// update local process state
                    						if ( manualFlag )
                    							(*rPtr).state = oam::MAN_OFFLINE;
                    						else
                    							(*rPtr).state = oam::AUTO_OFFLINE;

                    						rtnCode = stopProcess((*rPtr).processID, (*rPtr).ProcessName, (*rPtr).ProcessLocation, actIndicator, manualFlag);
                    						if (rtnCode)
                    							log.writeLog(__LINE__,  "Process cannot be stopped:" + (*rPtr).ProcessName, LOG_TYPE_DEBUG);
                    						else
                    							(*rPtr).processID = 0;
                    					}

                    					//send down notification
                    					oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);

                    //stop the mysqld daemon and then columnstore
                    					try {
                    						oam.actionMysqlCalpont(MYSQL_STOP);
                    					}
                    					catch(...)
                    					{}
                    */
                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) SHUTDOWNMODULE;
                    ackMsg << (ByteStream::byte) API_SUCCESS;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "SHUTDOWNMODULE: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

                    //sleep to give time for process-manager to finish up
                    sleep(5);
                    string cmd = "columnstore stop > /dev/null 2>&1";
                    system(cmd.c_str());
                    exit (0);

                    break;
                }


                default:
                    break;
            } //end of switch

            break;
        }

        case PROCUPDATELOG:
        {
            string action;
            string level;

            msg >> action;
            msg >> level;

            log.writeLog(__LINE__,  "MSG RECEIVED: " + action + " logging at level " + level);

            uint16_t rtnCode;
            int requestStatus = API_SUCCESS;

            rtnCode = updateLog(action, level);

            if (rtnCode)
                // error in updating log
                requestStatus = API_FAILURE;

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCUPDATELOG;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCUPDATELOG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            break;
        }

        case PROCGETCONFIGLOG:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Get Module Log Configuration data");

            int16_t requestStatus = getConfigLog();

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCGETCONFIGLOG;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCGETCONFIGLOG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            break;
        }

        case CHECKPOWERON:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Check Power-On Test results log file");
            checkPowerOnResults();

            break;
        }

        case PROCUPDATECONFIG:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Update Process Configuration");

            int16_t requestStatus = updateConfig();

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCUPDATECONFIG;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCUPDATECONFIG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            break;
        }

        case PROCBUILDSYSTEMTABLES:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Check and Build System Tables");

            int16_t requestStatus = buildSystemTables();

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCBUILDSYSTEMTABLES;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCBUILDSYSTEMTABLES: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            break;
        }

        case LOCALHEARTBEAT:
        {
            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) LOCALHEARTBEAT;
            ackMsg << (ByteStream::byte) API_SUCCESS;
            mq.write(ackMsg);
            break;
        }

        case CONFIGURE:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Configure Module");
            string configureModuleName;
            msg >> configureModuleName;

            int requestStatus = API_SUCCESS;

            configureModule(configureModuleName);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) CONFIGURE;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "CONFIGURE: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            //set startmodule flag
            startProcMon = true;
            break;
        }

        case RECONFIGURE:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Reconfigure Module");
            string reconfigureModuleName;
            msg >> reconfigureModuleName;

            uint16_t rtnCode;
            int requestStatus = API_SUCCESS;

            //validate that I should be receiving this message
            if ( config.moduleType() != "um" &&
                    config.moduleType() != "pm" )
                requestStatus = oam::API_FAILURE;
            else
            {
                if ( config.moduleType() == "um" &&
                        reconfigureModuleName.find("pm") == string::npos )
                    requestStatus = oam::API_FAILURE;
                else
                {
                    if ( config.moduleType() == "pm" &&
                            reconfigureModuleName.find("um") == string::npos )
                        requestStatus = oam::API_FAILURE;
                    else
                    {
                        rtnCode = reconfigureModule(reconfigureModuleName);

                        if (rtnCode)
                            // error in updating log
                            requestStatus = rtnCode;
                    }
                }
            }

            // install mysqld rpms if being reconfigured as a um
            if ( reconfigureModuleName.find("um") != string::npos )
            {
                string cmd = "post-mysqld-install >> " + tmpLogDir + "/rpminstall";
                system(cmd.c_str());
                cmd = "post-mysql-install >> " + tmpLogDir + "/rpminstall";
                system(cmd.c_str());
                int ret = system("systemctl cat mariadb.service > /dev/null 2>&1");
                if (!ret)
                {
                    cmd = "systemctl start mariadb.service > " + tmpLogDir + "/mysqldstart";
                    system(cmd.c_str());
                }
                else
                {
                    cmd = "/usr/bin/mysqld_safe & > " + tmpLogDir + "/mysqldstart";
                    system(cmd.c_str());
                }

				string tmpFile = tmpLogDir + "/mysqldstart";
                ifstream file (tmpFile.c_str());

                if (!file)
                {
                    requestStatus = oam::API_FAILURE;
                    log.writeLog(__LINE__, "RECONFIGURE: mysqld failed to start", LOG_TYPE_ERROR);
                }
                else
                {
                    char line[200];
                    string buf;
                    int count = 0;

                    while (file.getline(line, 200))
                    {
                        buf = line;

                        if ( buf.find("OK", 0) != string::npos )
                            count++;
                    }

                    file.close();

                    if (count == 0)
                    {
                        requestStatus = oam::API_FAILURE;
                        log.writeLog(__LINE__, "RECONFIGURE: mysqld failed to start", LOG_TYPE_ERROR);
                    }
                    else
                        log.writeLog(__LINE__, "RECONFIGURE: install started mysqld");
                }
            }

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) RECONFIGURE;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "RECONFIGURE: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

            //now exit so Process Monitor can restart and reinitialzation as the New module type
            if ( requestStatus != oam::API_FAILURE )
            {
                log.writeLog(__LINE__, "RECONFIGURE: ProcMon exiting so it can restart as new module type", LOG_TYPE_DEBUG);
//				sleep(1);

                exit(1);
            }

            break;
        }

        case GETSOFTWAREINFO:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Get Calpont Software Info");

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) GETSOFTWAREINFO;
            ackMsg << config.SoftwareVersion() + config.SoftwareRelease();
            mq.write(ackMsg);

            log.writeLog(__LINE__, "GETSOFTWAREINFO: ACK back to ProcMgr with " + config.SoftwareVersion() + config.SoftwareRelease());

            break;
        }

        case UPDATEPARENTNFS:
        {
            string IPAddress;

            msg >> IPAddress;

            log.writeLog(__LINE__,  "MSG RECEIVED: Update fstab file with new Parent OAM IP of " + IPAddress);

            int requestStatus = API_SUCCESS;

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) UPDATEPARENTNFS;
            ackMsg << (ByteStream::byte) requestStatus;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "UPDATEPARENTNFS: ACK back to ProcMgr");

            break;
        }

        case OAMPARENTACTIVE:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: OAM Parent Activate");

            runStandby = false;

            log.writeLog(__LINE__, "Running Active", LOG_TYPE_INFO);

            //give time for Status Control thread to start reading incoming messages
            sleep(3);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) OAMPARENTACTIVE;
            ackMsg << (ByteStream::byte) API_SUCCESS;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "OAMPARENTACTIVE: ACK back to ProcMgr");

            MonitorConfig config;
            break;
        }

        case UPDATECONFIGFILE:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Update Calpont Config file");

            (void)updateConfigFile(msg);

            log.writeLog(__LINE__, "UPDATECONFIGFILE: Completed");

            MonitorConfig config;
            break;
        }

        case GETPARENTOAMMODULE:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Get Parent OAM Module");

            MonitorConfig config;
            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) GETPARENTOAMMODULE;
            ackMsg << config.OAMParentName();
            mq.write(ackMsg);

            log.writeLog(__LINE__, "GETPARENTOAMMODULE: ACK back with " + config.OAMParentName());

            break;
        }

        case OAMPARENTCOLD:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: OAM Parent Standby ");

            runStandby = true;

            // delete any old active alarm log file
            unlink ("/var/log/mariadb/columnstore/activeAlarms");

            log.writeLog(__LINE__, "Running Standby", LOG_TYPE_INFO);
            //give time for Status Control thread to start reading incoming messages
            sleep(3);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) OAMPARENTCOLD;
            ackMsg << (ByteStream::byte) API_SUCCESS;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "OAMPARENTCOLD: ACK back to ProcMgr");

            MonitorConfig config;
            break;
        }

        case PROCUNMOUNT:
        {
            string dbrootID;
            msg >> dbrootID;

            log.writeLog(__LINE__,  "MSG RECEIVED: Unmount DBRoot: " + dbrootID);

            //Flush the cache
            cacheutils::flushPrimProcCache();
            cacheutils::dropPrimProcFdCache();
            flushInodeCache();

            int return_status = API_SUCCESS;

            if (DataRedundancyConfig == "n")
            {
                int retry = 1;

				string tmpUmount = tmpLogDir + "/umount.log";
                for ( ; retry < 5 ; retry++)
                {
                    string cmd = "export LC_ALL=C;" + SUDO + "umount /var/lib/columnstore/data" + dbrootID + " > " + tmpUmount + " 2>&1";

                    system(cmd.c_str());

                    return_status = API_SUCCESS;

                    if (!oam.checkLogStatus(tmpUmount, "busy"))
                        break;

					cmd = "lsof /var/lib/columnstore/data" + dbrootID + " >> " + tmpUmount + " 2>&1";
					system(cmd.c_str());
					cmd = "fuser -muvf /var/lib/columnstore/data" + dbrootID + " >> " + tmpUmount + " 2>&1";
					system(cmd.c_str());

                    sleep(2);
                    //Flush the cache
                    cacheutils::flushPrimProcCache();
                    cacheutils::dropPrimProcFdCache();
                    flushInodeCache();
                }

                if ( retry >= 5 )
                {
                    log.writeLog(__LINE__, "unmount failed, device busy, DBRoot: " + dbrootID, LOG_TYPE_ERROR);
                    return_status = API_FAILURE;
                    string cmd = "mv -f " + tmpUmount + " " + tmpUmount + "failed";
                    system(cmd.c_str());
                }
            }
            else
            {
                try
                {
                    int ret = glusterUnassign(dbrootID);

                    if ( ret != 0 )
                        log.writeLog(__LINE__, "Error unassigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
                    else
                        log.writeLog(__LINE__, "Gluster unassign gluster dbroot# " + dbrootID);
                }
                catch (...)
                {
                    log.writeLog(__LINE__, "Exception unassigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
                }
            }

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCUNMOUNT;
            ackMsg << (ByteStream::byte) return_status;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCUNMOUNT: ACK back to ProcMgr, status: " + oam.itoa(return_status));

            break;
        }

        case PROCMOUNT:
        {
            string dbrootID;
            msg >> dbrootID;

            log.writeLog(__LINE__,  "MSG RECEIVED: Mount DBRoot: " + dbrootID);;

            int return_status = API_SUCCESS;

            if (DataRedundancyConfig == "n")
            {
				string tmpMount = tmpLogDir + "/mount.log";
                string cmd = SUDO + "export LC_ALL=C;" + SUDO + "mount /var/lib/columnstore/data" + dbrootID + " > " + tmpMount  + "2>&1";
                system(cmd.c_str());

                if ( !rootUser )
                {
                    cmd = SUDO + "chown -R " + USER + ":" + USER + " /var/lib/columnstore/data" + dbrootID + " > /dev/null 2>&1";
                    system(cmd.c_str());
                }

                return_status = API_SUCCESS;
                ifstream in(tmpMount.c_str());

                in.seekg(0, std::ios::end);
                int size = in.tellg();

                if ( size != 0 )
                {
                    if (!oam.checkLogStatus(tmpMount, "already"))
                    {
                        log.writeLog(__LINE__, "mount failed, DBRoot: " + dbrootID, LOG_TYPE_ERROR);
                        return_status = API_FAILURE;
						string cmd = "mv -f " + tmpMount + " " + tmpMount + "failed";
						system(cmd.c_str());
                    }
                }
            }

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCMOUNT;
            ackMsg << (ByteStream::byte) return_status;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCMOUNT: ACK back to ProcMgr, status: " + oam.itoa(return_status));

            break;
        }

        case PROCFSTABUPDATE:
        {
            string entry;
            msg >> entry;

            //check if entry already exist in /etc/fstab
            string cmd = "grep " + entry + " /etc/fstab /dev/null 2>&1";
            int status = system(cmd.c_str());

            if (WEXITSTATUS(status) != 0 )
            {
				//chmod before update, used on amazon ami EBS. not other systems
				system("sudo chmod 666 /etc/fstab");

                cmd = "echo " + entry + " >> /etc/fstab";
                system(cmd.c_str());

                log.writeLog(__LINE__, "Add line entry to /etc/fstab : " + entry);
            }

            //check if entry already exist in ../local/etc/pm1/fstab
            cmd = "grep " + entry + "/var/lib/columnstore/local/etc/pm1/fstab /dev/null 2>&1";
            status = system(cmd.c_str());

            if (WEXITSTATUS(status) != 0 )
            {
                cmd = "echo " + entry + " >> /var/lib/columnstore/local/etc/pm1/fstab";

                system(cmd.c_str());

                log.writeLog(__LINE__, "Add line entry to /var/lib/columnstore/local/etc/pm1/fstab : " + entry);
            }

            //mkdir on entry directory
            string::size_type pos = entry.find(" ", 0);
            string::size_type pos1 = entry.find(" ", pos + 1);
            string directory = entry.substr(pos + 1, pos1 - pos);

            cmd = "mkdir " + directory;

            system(cmd.c_str());
            log.writeLog(__LINE__, "create directory: " + directory);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCFSTABUPDATE;
            ackMsg << (ByteStream::byte) API_SUCCESS;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCFSTABUPDATE: ACK back to ProcMgr");

            break;
        }

        case MASTERREP:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Run Master Replication script ");

            string masterLogFile = oam::UnassignedName;
            string masterLogPos = oam::UnassignedName;

            if ( ( (PMwithUM == "n") && (config.moduleType() == "pm") ) &&
                    ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) MASTERREP;
                ackMsg << (ByteStream::byte) oam::API_FAILURE;
                ackMsg <<  masterLogFile;
                ackMsg <<  masterLogPos;
                mq.write(ackMsg);

                log.writeLog(__LINE__, "MASTERREP: Error PM invalid msg - ACK back to ProcMgr return status = " + oam.itoa((int) oam::API_FAILURE));
                break;
            }

            //change local my.cnf file
            int ret;
            int retry;

            for ( retry = 0 ; retry < 3 ; retry++ )
            {
                ret = changeMyCnf("master");

                if ( ret == oam::API_FAILURE )
                {
                    ackMsg << (ByteStream::byte) ACK;
                    ackMsg << (ByteStream::byte) MASTERREP;
                    ackMsg << (ByteStream::byte) ret;
                    ackMsg <<  masterLogFile;
                    ackMsg <<  masterLogPos;
                    mq.write(ackMsg);

                    log.writeLog(__LINE__, "MASTERREP: Error in changeMyCnf - ACK back to ProcMgr return status = " + oam.itoa((int) ret));
                    break;
                }

                // run Master Rep script
                ret = runMasterRep(masterLogFile, masterLogPos);

                if ( ret == API_FAILURE )
                {
                    log.writeLog(__LINE__, "MASTERREP: runMasterRep failure, retry", LOG_TYPE_ERROR);
                    sleep(5);
                    continue;
                }
                else
                    break;
            }

            if ( retry >= 3 )
                log.writeLog(__LINE__, "MASTERREP: runMasterRep failure", LOG_TYPE_CRITICAL);
            else
                runDisableRep();	//disable slave on new master

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) MASTERREP;
            ackMsg << (ByteStream::byte) ret;
            ackMsg <<  masterLogFile;
            ackMsg <<  masterLogPos;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "MASTERREP: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case SLAVEREP:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Run Slave Replication script ");

            string masterLogFile;
            msg >> masterLogFile;
            string masterLogPos;
            msg >> masterLogPos;

            if ( ( (PMwithUM == "n") && (config.moduleType() == "pm") ) &&
                    ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) SLAVEREP;
                ackMsg << (ByteStream::byte) oam::API_FAILURE;
                mq.write(ackMsg);

                log.writeLog(__LINE__, "SLAVEREP: Error PM invalid msg - ACK back to ProcMgr return status = " + oam.itoa((int) oam::API_FAILURE));
                break;
            }

            //change local my.cnf file
            int ret = changeMyCnf("slave");

            if ( ret == oam::API_FAILURE )
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) SLAVEREP;
                ackMsg << (ByteStream::byte) ret;
                mq.write(ackMsg);

                log.writeLog(__LINE__, "SLAVEREP: Error in changeMyCnf - ACK back to ProcMgr return status = " + oam.itoa((int) ret));
                break;
            }

            // run Slave Rep script
            ret = runSlaveRep(masterLogFile, masterLogPos);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) SLAVEREP;
            ackMsg << (ByteStream::byte) ret;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "SLAVEREP: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case MASTERDIST:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Run Master DB Distribute command ");

            string password;
            msg >> password;
            string module;
            msg >> module;

            if ( ( (PMwithUM == "n") && (config.moduleType() == "pm") ) &&
                    ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) MASTERDIST;
                ackMsg << (ByteStream::byte) oam::API_FAILURE;
                mq.write(ackMsg);

                log.writeLog(__LINE__, "MASTERDIST: runMasterRep - ACK back to ProcMgr return status = " + oam.itoa((int) oam::API_FAILURE));
            }

            if ( password == oam::UnassignedName )
                password = "ssh";

            // run Master Dist
            int ret = runMasterDist(password, module);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) MASTERDIST;
            ackMsg << (ByteStream::byte) ret;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "MASTERDIST: runMasterRep - ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case DISABLEREP:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: Disable MySQL Replication ");

            //change local my.cnf file
            int ret = changeMyCnf("disable");

            // run Disable rep
            ret = runDisableRep();

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) DISABLEREP;
            ackMsg << (ByteStream::byte) ret;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "DISABLEREP: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case PROCGLUSTERASSIGN:
        {
            string dbrootID;
            msg >> dbrootID;

            log.writeLog(__LINE__,  "MSG RECEIVED: Gluster Assign DBRoot: " + dbrootID);

            // run Master Dist
            int ret = glusterAssign(dbrootID);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCGLUSTERASSIGN;
            ackMsg << (ByteStream::byte) ret;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCGLUSTERASSIGN: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case PROCGLUSTERUNASSIGN:
        {
            string dbrootID;
            msg >> dbrootID;

            log.writeLog(__LINE__,  "MSG RECEIVED: Gluster Unassign DBRoot: " + dbrootID);

            // run Master Dist
            int ret = glusterUnassign(dbrootID);

            ackMsg << (ByteStream::byte) ACK;
            ackMsg << (ByteStream::byte) PROCGLUSTERUNASSIGN;
            ackMsg << (ByteStream::byte) ret;
            mq.write(ackMsg);

            log.writeLog(__LINE__, "PROCGLUSTERUNASSIGN: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

            break;
        }

        case SYNCFSALL:
        {
            log.writeLog(__LINE__,  "MSG RECEIVED: SYNC FileSystem...");
            int requestStatus = API_SUCCESS;
            requestStatus = syncFS();
            if (requestStatus == API_SUCCESS)
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) SYNCFSALL;
                ackMsg << (ByteStream::byte) API_SUCCESS;
            }
            else
            {
                ackMsg << (ByteStream::byte) ACK;
                ackMsg << (ByteStream::byte) SYNCFSALL;
                ackMsg << (ByteStream::byte) API_FAILURE;
            }
            mq.write(ackMsg);

            log.writeLog(__LINE__, "SYNCFSALL: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));
            break;
        }

        default:
            break;
    } //end of switch

    return;
}

/******************************************************************************************
* @brief	stopProcess
*
* purpose:	stop a process
*
******************************************************************************************/
int ProcessMonitor::stopProcess(pid_t processID, std::string processName, std::string processLocation, int actionIndicator, bool manualFlag)
{
    int status;
    MonitorLog log;
    Oam oam;

    log.writeLog(__LINE__, "STOPPING Process: " + processName, LOG_TYPE_DEBUG);

    sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);

    if ( manualFlag )
    {
        // Mark the process offline
        updateProcessInfo(processName, oam::MAN_OFFLINE, 0);

        // Set the alarm
        sendAlarm(processName, PROCESS_DOWN_MANUAL, SET);

    }
    else
    {
        // Mark the process offline
        updateProcessInfo(processName, oam::AUTO_OFFLINE, 0);

        // Set the alarm
        sendAlarm(processName, PROCESS_DOWN_AUTO, SET);
    }

    // bypass if pid = 0
    if ( processID == 0 )
        status = API_SUCCESS;
    else
    {
        // XXXPAT: StorageManager shouldn't be killed with KILL, or there's a chance of data corruption.
        // once we minimize that chance, we could allow KILL to be sent.
        if (actionIndicator == GRACEFUL || processName == "StorageManager")
        {
            status = kill(processID, SIGTERM);
        }
        else
        {
            status = kill(processID, SIGKILL);
        }

        // processID not found, set as success
        if ( errno == ESRCH)
            status = API_SUCCESS;

        if ( status != API_SUCCESS)
        {
            status = errno;
            log.writeLog(__LINE__, "Failure to stop Process: " + processName + ", error = " + oam.itoa(errno), LOG_TYPE_ERROR );
        }
    }

    // sending sigkill to StorageManager right after sigterm would not allow it to 
    // exit gracefully.  This will wait until StorageManager goes down to prevent
    // weirdness that I suspect will happen if we combine a slow connection with a restart
    // command.
    if (processName == "StorageManager" && processID != 0)
    {
        while (status == API_SUCCESS)
        {
            sleep(1);
            ostringstream os;
            os << "Waiting for StorageManager to exit gracefully... pid is " << processID;
            log.writeLog(__LINE__, os.str(), LOG_TYPE_DEBUG);
            status = kill(processID, SIGTERM);
            break;
        }
    
        return API_SUCCESS;
    }
    
    //now do a pkill on process just to make sure all is clean
    string::size_type pos = processLocation.find("bin/", 0);
    string procName = processLocation.substr(pos + 4, 15) + "\\*";
    string cmd = "pkill -9 " + procName;
    system(cmd.c_str());
    log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);

    return status;
}

/******************************************************************************************
* @brief	startProcess
*
* purpose:	Start a process
*
******************************************************************************************/
pid_t ProcessMonitor::startProcess(string processModuleType, string processName, string processLocation,
                                   string arg_list[MAXARGUMENTS], uint16_t launchID, uint16_t BootLaunch,
                                   string RunType, string DepProcessName[MAXDEPENDANCY],
                                   string DepModuleName[MAXDEPENDANCY], string LogFile, uint16_t initType,  uint16_t actIndicator)
{
    // Compiler complains about non-initialiased variable here.
    pid_t  newProcessID = 0;
    char* argList[MAXARGUMENTS];
    unsigned int i = 0;
    MonitorLog log;
    MonitorConfig currentConfig;
    unsigned int numAugs = 0;
    Oam oam;
    SystemProcessStatus systemprocessstatus;
    ProcessStatus processstatus;
    Config *cs_config = Config::makeConfig();
    string DBRootStorageType = cs_config->getConfig("Installation", "DBRootStorageType");

    log.writeLog(__LINE__, "STARTING Process: " + processName, LOG_TYPE_DEBUG);
    log.writeLog(__LINE__, "Process location: " + processLocation, LOG_TYPE_DEBUG);

    //check process location
    if (access(processLocation.c_str(), X_OK) != 0)
    {
        log.writeLog(__LINE__, "Process location: " + processLocation + " not found", LOG_TYPE_ERROR);

        //record the process information into processList
        config.buildList(processModuleType, processName, processLocation, arg_list,
                         launchID, newProcessID, FAILED, BootLaunch, RunType,
                         DepProcessName, DepModuleName, LogFile);

        //Update Process Status: Mark Process INIT state
        updateProcessInfo(processName, FAILED, newProcessID);

        return oam::API_FAILURE;
    }

    //check process dependency
    if (DepProcessName[i].length() != 0)
    {
        for (int i = 0; i < MAXDEPENDANCY; i++)
        {
            //Get dependent process status
            if (DepProcessName[i].length() == 0)
            {
                break;
            }

            //check for System wild card on Module Name
            if ( DepModuleName[i] == "*" )
            {
                //check for all accurrances of this Module Name
                try
                {
                    oam.getProcessStatus(systemprocessstatus);

                    for ( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
                    {
                        if ( systemprocessstatus.processstatus[j].ProcessName == DepProcessName[i] )
                        {

                            log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + systemprocessstatus.processstatus[j].Module + " is " + oam.itoa(systemprocessstatus.processstatus[j].ProcessOpState), LOG_TYPE_DEBUG);

                            int opState = oam::ACTIVE;
                            bool degraded;
                            oam.getModuleStatus(systemprocessstatus.processstatus[j].Module, opState, degraded);

                            if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED ||
                                    opState == oam::AUTO_OFFLINE)
                                continue;

                            if (systemprocessstatus.processstatus[j].ProcessOpState != oam::ACTIVE )
                            {
                                log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
                                return oam::API_MINOR_FAILURE;
                            }
                        }
                    }
                }
                catch (exception& ex)
                {
//					string error = ex.what();
//					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                    return oam::API_MINOR_FAILURE;
                }
                catch (...)
                {
//					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                    return oam::API_MINOR_FAILURE;
                }
            }
            else
            {
                //check for a Module Type wildcard
                if ( DepModuleName[i].find("*") != string::npos)
                {
                    string moduleName = DepModuleName[i].substr(0, MAX_MODULE_TYPE_SIZE);

                    try
                    {
                        oam.getProcessStatus(systemprocessstatus);

                        for ( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
                        {
                            if ( systemprocessstatus.processstatus[j].ProcessName == DepProcessName[i]
                                    &&  systemprocessstatus.processstatus[j].Module.find(moduleName, 0) != string::npos)
                            {

                                log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + systemprocessstatus.processstatus[j].Module + " is " + oam.itoa(systemprocessstatus.processstatus[j].ProcessOpState), LOG_TYPE_DEBUG);

                                int opState = oam::ACTIVE;
                                bool degraded;
                                oam.getModuleStatus(systemprocessstatus.processstatus[j].Module, opState, degraded);

                                if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED ||
                                        opState == oam::AUTO_OFFLINE)
                                    continue;

                                if (systemprocessstatus.processstatus[j].ProcessOpState != oam::ACTIVE )
                                {
                                    log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
                                    return oam::API_MINOR_FAILURE;
                                }
                            }
                        }
                    }
                    catch (exception& ex)
                    {
//						string error = ex.what();
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                        return oam::API_MINOR_FAILURE;
                    }
                    catch (...)
                    {
//						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                        return oam::API_MINOR_FAILURE;
                    }
                }
                else
                {
                    //check for a Current Module wildcard
                    if ( DepModuleName[i] == "@")
                    {
                        int state = oam::ACTIVE;;

                        try
                        {
                            ProcessStatus procstat;
                            oam.getProcessStatus(DepProcessName[i],
                                                 config.moduleName(),
                                                 procstat);
                            state = procstat.ProcessOpState;
                        }
                        catch (exception& ex)
                        {
//							string error = ex.what();
//							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                            return oam::API_MINOR_FAILURE;
                        }
                        catch (...)
                        {
//							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                            return oam::API_MINOR_FAILURE;
                        }

                        log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + config.moduleName() + " is " + oam.itoa(state), LOG_TYPE_DEBUG);

                        if (state == oam::FAILED)
                        {
                            log.writeLog(__LINE__, "Dependent Process is FAILED state, Hard Failed Restoral", LOG_TYPE_DEBUG);

                            //record the process information into processList
                            config.buildList(processModuleType, processName, processLocation, arg_list,
                                             launchID, newProcessID, FAILED, BootLaunch, RunType,
                                             DepProcessName, DepModuleName, LogFile);

                            //Update Process Status: Mark Process INIT state
                            updateProcessInfo(processName, FAILED, newProcessID);

                            return oam::API_FAILURE;
                        }

                        if (state != oam::ACTIVE)
                        {
                            log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
                            return oam::API_MINOR_FAILURE;
                        }
                    }
                    else
                    {
                        // specific module name and process dependency
                        int state  = oam::ACTIVE;

                        try
                        {
                            ProcessStatus procstat;
                            oam.getProcessStatus(DepProcessName[i],
                                                 DepModuleName[i],
                                                 procstat);
                            state = procstat.ProcessOpState;
                        }
                        catch (exception& ex)
                        {
//							string error = ex.what();
//							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
                            return oam::API_MINOR_FAILURE;
                        }
                        catch (...)
                        {
//							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
                            return oam::API_MINOR_FAILURE;
                        }

                        log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + DepModuleName[i] + " is " + oam.itoa(state), LOG_TYPE_DEBUG);

                        int opState = oam::ACTIVE;
                        bool degraded;
                        oam.getModuleStatus(DepModuleName[i], opState, degraded);

                        if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED ||
                                opState == oam::AUTO_OFFLINE)
                            continue;

                        if (state != oam::ACTIVE)
                        {
                            log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
                            return oam::API_MINOR_FAILURE;
                        }
                    }
                }
            }
        }//end of FOR
    }

    for (i = 0; i < MAXARGUMENTS - 1; i++)
    {
        if (arg_list[i].length() == 0)
            break;

        //check if workernode argument, if so setup argument #2 as the slave ID for this module
        string::size_type pos = arg_list[i].find("DBRM_Worker", 0);

        if (pos != string::npos)
        {
            try
            {
                int slavenodeID = oam.getLocalDBRMID(config.moduleName());
                arg_list[i] = "DBRM_Worker" + oam.itoa(slavenodeID);
                log.writeLog(__LINE__, "getLocalDBRMID Worker Node ID = " + oam.itoa(slavenodeID), LOG_TYPE_DEBUG);
            }
            catch (...)
            {
                log.writeLog(__LINE__, "EXCEPTION ERROR on getLocalDBRMID: no DBRM for module", LOG_TYPE_ERROR);

                //record the process information into processList
                config.buildList(processModuleType, processName, processLocation, arg_list,
                                 launchID, newProcessID, FAILED, BootLaunch, RunType,
                                 DepProcessName, DepModuleName, LogFile);

                //Update Process Status: Mark Process INIT state
                updateProcessInfo(processName, FAILED, newProcessID);

                return oam::API_FAILURE;
            }
        }


        argList[i] = new char[arg_list[i].length() + 1];

        strcpy(argList[i], arg_list[i].c_str()) ;
//		log.writeLog(__LINE__, "Arg list ");
//		log.writeLog(__LINE__, argList[i]);
        numAugs++;
    }

    argList[i] = NULL;

    //run load-brm script before brm processes started
    if ( actIndicator != oam::GRACEFUL)
    {
        if ( ( gOAMParentModuleFlag && processName == "DBRMControllerNode") ||
                ( !gOAMParentModuleFlag && processName == "DBRMWorkerNode") )
        {
            string DBRMDir;
//			string tempDBRMDir = startup::StartUp::installDir() + "/data/dbrm";

            // get DBRMroot config setting
            string DBRMroot;
            oam.getSystemConfig("DBRMRoot", DBRMroot);

            string::size_type pos = DBRMroot.find("/BRM_saves", 0);

            if (pos != string::npos)
                //get directory path
                DBRMDir = DBRMroot.substr(0, pos);
            else
            {
                log.writeLog(__LINE__, "Error: /BRM_saves not found in DBRMRoot config setting", LOG_TYPE_CRITICAL);

                //record the process information into processList
                config.buildList(processModuleType, processName, processLocation, arg_list,
                                 launchID, newProcessID, FAILED, BootLaunch, RunType,
                                 DepProcessName, DepModuleName, LogFile);

                //Update Process Status: Mark Process INIT state
                updateProcessInfo(processName, FAILED, newProcessID);

                return oam::API_FAILURE;
            }

            //create dbrm directory, just to make sure its there
            string cmd = "mkdir -p " + DBRMDir + " > /dev/null 2>&1";
            system(cmd.c_str());

            // if Non Parent OAM Module, get the dbmr data from Parent OAM Module
            if ( !gOAMParentModuleFlag && !HDFS)
            {

                //create temp dbrm directory
//				string cmd = "mkdir " + tempDBRMDir + " > /dev/null 2>&1";
//				system(cmd.c_str());

                //setup softlink for editem on the 'um' or shared-nothing non active pm
                /*				if( config.moduleType() == "um" ||
                					(config.moduleType() == "pm") ) {
                					cmd = "mv -f " + DBRMDir + " /root/ > /dev/null 2>&1";
                					system(cmd.c_str());

                					cmd = "ln -s " + tempDBRMDir + " " + DBRMDir + " > /dev/null 2>&1";
                					system(cmd.c_str());
                				}
                */
                //change DBRMDir to temp DBRMDir
//				DBRMDir = tempDBRMDir;

                // remove all files for temp directory
//				cmd = "rm -f " + DBRMDir + "/*";
//				system(cmd.c_str());

                // go request files from parent OAM module
                if ( getDBRMdata(&DBRMDir) != oam::API_SUCCESS )
                {
                    log.writeLog(__LINE__, "Error: getDBRMdata failed", LOG_TYPE_ERROR);
                    sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);
                    return oam::API_MINOR_FAILURE;
                }
                // DBRMDir might have changed, so need to change DBRMroot
                bf::path tmp(DBRMroot);
                tmp = tmp.filename();
                DBRMroot = (bf::path(DBRMDir) / tmp).string();
                
                sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, CLEAR);
                // change DBRMroot to temp DBRMDir path
//				DBRMroot = tempDBRMDir + "/BRM_saves";
            }
            
            
            //
            // run the 'load_brm' script first if files exist
            //
            string loadScript = "load_brm";

            string fileName = DBRMroot + "_current";

            ssize_t fileSize = IDBPolicy::size(fileName.c_str());
            boost::scoped_ptr<IDBDataFile> oldFile(IDBDataFile::open(
                    IDBPolicy::getType(fileName.c_str(),
                                       IDBPolicy::WRITEENG),
                    fileName.c_str(), "r", 0));

            if (oldFile && fileSize > 0)
            {
                char line[200] = {0};
                oldFile->pread(line, 0, fileSize - 1);  // skip the \n
                line[fileSize] = '\0';  // not necessary, but be sure.
                // MCOL-1558 - the _current file is now relative to DBRMRoot
                string dbrmFile;
                if (line[0] == '/')    // handle absolute paths (saved by an old version)
                    dbrmFile = line;
                else
                    dbrmFile = DBRMroot.substr(0, DBRMroot.find_last_of('/') + 1) + line;

//				if ( !gOAMParentModuleFlag ) {

//					string::size_type pos = dbrmFile.find("/BRM_saves",0);
//					if (pos != string::npos)
//						dbrmFile = tempDBRMDir + dbrmFile.substr(pos,80);;
//				}

                string logdir("/var/log/mariadb/columnstore");

                if (access(logdir.c_str(), W_OK) != 0) logdir = tmpLogDir;

                string cmd = "reset_locks > " + logdir + "/reset_locks.log1 2>&1";
                system(cmd.c_str());
                log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);

                cmd = "clearShm -c  > /dev/null 2>&1";
                system(cmd.c_str());
                log.writeLog(__LINE__, "Clear Shared Memory script run", LOG_TYPE_DEBUG);

                cmd = loadScript + " " + dbrmFile + " > " + logdir + "/load_brm.log1 2>&1";
                log.writeLog(__LINE__, loadScript + " cmd = " + cmd, LOG_TYPE_DEBUG);
                system(cmd.c_str());

                cmd = logdir + "/load_brm.log1";

                if (oam.checkLogStatus(cmd, "OK"))
                    log.writeLog(__LINE__, "Successfully return from " + loadScript, LOG_TYPE_DEBUG);
                else
                {
                    log.writeLog(__LINE__, "Error return DBRM " + loadScript, LOG_TYPE_ERROR);
                    sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);

                    //record the process information into processList
                    config.buildList(processModuleType, processName, processLocation, arg_list,
                                     launchID, 0, FAILED, BootLaunch, RunType,
                                     DepProcessName, DepModuleName, LogFile);

                    //Update Process Status: Mark Process FAILED state
                    updateProcessInfo(processName, FAILED, 0);

                    return oam::API_FAILURE;
                }

                // now delete the dbrm data from local disk
                if ( !gOAMParentModuleFlag && !HDFS && DataRedundancyConfig == "n")
                {
                    IDBFileSystem &fs = IDBPolicy::getFs(DBRMDir);
                    fs.remove(DBRMDir.c_str());
                    log.writeLog(__LINE__, "removed downloaded DBRM files at " + DBRMDir, LOG_TYPE_DEBUG);
                    
                    #if 0
                    string cmd = "rm -f " + DBRMDir + "/*";
                    system(cmd.c_str());
                    log.writeLog(__LINE__, "removed DBRM file with command: " + cmd, LOG_TYPE_DEBUG);
                    #endif
                }
            }
            else
                log.writeLog(__LINE__, "No DBRM files exist, must be a initial startup", LOG_TYPE_DEBUG);
        }

        sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, CLEAR);
    }

    //do a pkill on process just to make sure there is no rouge version running
    string::size_type pos = processLocation.find("bin/", 0);
    string procName = processLocation.substr(pos + 4, 15) + "\\*";
    string cmd = "pkill -9 " + procName;
    system(cmd.c_str());
    log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);

    //Update Process Status: Mark Process INIT state
    updateProcessInfo(processName, initType, 0);

    //sleep, give time for INIT state to be update, prevent race condition with ACTIVE
    sleep(1);

    //check and setup for logfile
    time_t now;
    now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    char timestamp[200];
    strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);

    string logdir("/var/log/mariadb/columnstore");

    if (access(logdir.c_str(), W_OK) != 0) logdir = tmpLogDir;

    string outFileName = logdir + "/" + processName + ".out";
    string errFileName = logdir + "/" + processName + ".err";

    string saveoutFileName = outFileName + "." + timestamp + ".log1";
    string saveerrFileName = errFileName + "." + timestamp + ".log1";

    if ( LogFile == "off" )
    {
        string cmd = "mv " + outFileName + " " + saveoutFileName + " > /dev/null 2>&1";
        system(cmd.c_str());
        cmd = "mv " + errFileName + " " + saveerrFileName + " > /dev/null 2>&1";
        system(cmd.c_str());
    }
    else
    {
        string cmd = "mv " + outFileName + " " + saveoutFileName + " > /dev/null 2>&1";
        system(cmd.c_str());
        cmd = "mv " + errFileName + " " + saveerrFileName + " > /dev/null 2>&1";
        system(cmd.c_str());
    }

    //fork and exec new process
    newProcessID = fork();

    if (newProcessID != 0)
    {
        //
        // parent processing
        //

        if ( newProcessID == -1)
        {
            log.writeLog(__LINE__, "New Process ID = -1, failed StartProcess", LOG_TYPE_DEBUG);
            return oam::API_MINOR_FAILURE;
        }

        //FYI - NEEDS TO STAY HERE TO HAVE newProcessID

        //record the process information into processList
        config.buildList(processModuleType, processName, processLocation, arg_list,
                         launchID, newProcessID, initType, BootLaunch, RunType,
                         DepProcessName, DepModuleName, LogFile);

        //Update Process Status: Update PID
        updateProcessInfo(processName, PID_UPDATE, newProcessID);

        log.writeLog(__LINE__, processName + " PID is " + oam.itoa(newProcessID), LOG_TYPE_DEBUG);

        sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
        sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
        sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);

        //give time to get status updates from process before starting next process
        if ( processName == "DBRMWorkerNode" || processName == "ExeMgr" || processName == "DDLProc")
            sleep(3);
        else
        {
            if ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM )
            {
                if ( processName == "PrimProc" || processName == "WriteEngineServer")
                    sleep(3);
            }
            else
            {
                if ( (PMwithUM == "y") && processName == "PrimProc" )
                    sleep(3);
            }
        }

        for (i = 0; i < numAugs; i++)
        {
            if (strlen(argList[i]) == 0)
                break;

            delete [] argList[i];
        }
    }
    else
    {
        //
        //child processing
        //

        //Close all files opened by parent process
        for (int i = 0; i < sysconf(_SC_OPEN_MAX); i++)
        {
            close(i);
        }

        {
            int fd;
            fd = open("/dev/null", O_RDONLY);

            if (fd != 0)
            {
                dup2(fd, 0);
                close(fd);
            }

            if ( LogFile == "off" )
            {
                fd = open("/dev/null", O_WRONLY); //Should be fd 1

                if ( fd != 1 )
                {
                    dup2(fd, 1);
                    close(fd);
                }

                fd = open("/dev/null", O_WRONLY); //Should be fd 2

                if ( fd != 2 )
                {
                    dup2(fd, 2);
                    close(fd);
                }
            }
            else
            {
                // open STDOUT & STDERR to log file
//				log.writeLog(__LINE__, "STDOUT  to " + outFileName, LOG_TYPE_DEBUG);
                fd = open(outFileName.c_str(), O_CREAT | O_WRONLY, 0644);

                if (fd != 1)
                {
                    dup2(fd, 1);
                    close(fd);
                }

//				log.writeLog(__LINE__, "STDERR  to " + errFileName, LOG_TYPE_DEBUG);
                fd = open(errFileName.c_str(), O_CREAT | O_WRONLY, 0644);

                if (fd != 2)
                {
                    dup2(fd, 2);
                    close(fd);
                }
            }
        }

        //give time to get INIT status updated in shared memory
        sleep(1);
        execv(processLocation.c_str(), argList);

        //record the process information into processList
        config.buildList(processModuleType, processName, processLocation, arg_list,
                         launchID, newProcessID, FAILED, BootLaunch, RunType,
                         DepProcessName, DepModuleName, LogFile);

        //Update Process Status: Mark Process INIT state
        updateProcessInfo(processName, FAILED, newProcessID);

        return (oam::API_FAILURE);
    }

    return newProcessID;
}

/******************************************************************************************
* @brief	reinitProcess
*
* purpose:	re-Init a process
*
******************************************************************************************/
int ProcessMonitor::reinitProcess(pid_t processID, std::string processName, int actionIndicator)
{
    MonitorLog log;

    log.writeLog(__LINE__, "REINITTING Process: " + processName, LOG_TYPE_DEBUG);

    kill(processID, SIGHUP);

    return API_SUCCESS;
}

/******************************************************************************************
* @brief	stopAllProcess
*
* purpose:	Stop all processes started by this monitor
*
******************************************************************************************/
int stopAllProcess(int actionIndicator)
{
    int i;

    if (actionIndicator == GRACEFUL)
    {
        i = kill(0, SIGTERM);

    }
    else
    {
        i = kill(0, SIGKILL);
    }

    if ( i != API_SUCCESS)
    {
        i = errno;
    }

    return i;
}

/******************************************************************************************
* @brief	sendMessage
*
* purpose:	send message to the monitored process or the process manager
*
******************************************************************************************/
int		ProcessMonitor::sendMessage(const string& toWho, const string& message)
{
    int i = 0;
    return i;
}

/******************************************************************************************
* @brief	checkHeartBeat
*
* purpose:	check child process heart beat
*
******************************************************************************************/
int		ProcessMonitor::checkHeartBeat(const string processName)
{
    int i = 0;
    return i;
}

/******************************************************************************************
* @brief	sendAlarm
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
void	ProcessMonitor::sendAlarm(string alarmItem, ALARMS alarmID, int action)
{
    MonitorLog log;
    Oam oam;

//        cout << "sendAlarm" << endl;
//       cout << alarmItem << endl;
//        cout << oam.itoa(alarmID) << endl;
//        cout << oam.itoa(action) << endl;


    sendAlarmInfo_t* t1 = new sendAlarmInfo_t;
    *t1 = boost::make_tuple(alarmItem, alarmID, action);

    pthread_t SendAlarmThread;
    int status = pthread_create (&SendAlarmThread, NULL, (void* (*)(void*)) &sendAlarmThread, t1);

    if ( status != 0 )
        log.writeLog(__LINE__, "SendAlarmThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

    return;
}



/******************************************************************************************
* @brief	sendAlarmThread
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
void 	sendAlarmThread(sendAlarmInfo_t* t)
{
    MonitorLog log;
    Oam oam;
    ALARMManager alarmMgr;

    pthread_mutex_lock(&ALARM_LOCK);

    string alarmItem = boost::get<0>(*t);
    ALARMS alarmID = boost::get<1>(*t);
    int action = boost::get<2>(*t);

    //valid alarmID
    if ( alarmID < 1 || alarmID > oam::MAX_ALARM_ID )
    {
        log.writeLog(__LINE__,  "sendAlarmThread error: Invalid alarm ID", LOG_TYPE_DEBUG);

        delete t;
        pthread_mutex_unlock(&ALARM_LOCK);

        pthread_exit(0);
    }

    if ( action == SET )
    {
        log.writeLog(__LINE__,  "Send SET Alarm ID " + oam.itoa(alarmID) + " on device " + alarmItem, LOG_TYPE_DEBUG);
    }
    else
    {
        log.writeLog(__LINE__,  "Send CLEAR Alarm ID " + oam.itoa(alarmID) + " on device " + alarmItem, LOG_TYPE_DEBUG);
    }

//        cout << "sendAlarmThread" << endl;
//        cout << alarmItem << endl;
//        cout << oam.itoa(alarmID) << endl;
//        cout << oam.itoa(action) << endl;

    try
    {
        alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, action);
    }
    catch (...)
    {
        log.writeLog(__LINE__, "EXCEPTION ERROR on sendAlarmReport", LOG_TYPE_ERROR );
    }

    delete t;
    pthread_mutex_unlock(&ALARM_LOCK);

    pthread_exit(0);
}

/******************************************************************************************
* @brief	updateProcessInfo
*
* purpose:	Send msg to update process state and status change time on disk
*
******************************************************************************************/
bool ProcessMonitor::updateProcessInfo(std::string processName, int state, pid_t PID)
{
    MonitorLog log;
    Oam oam;

    log.writeLog(__LINE__, "StatusUpdate of Process " + processName + " State = " + oam.itoa(state) + " PID = " + oam.itoa(PID), LOG_TYPE_DEBUG);

    sendProcessInfo_t* t1 = new sendProcessInfo_t;
    *t1 = boost::make_tuple(processName, state, PID);

    // if state is offline, use thread for faster results
    if ( state == oam::MAN_OFFLINE || state == oam::AUTO_OFFLINE )
    {
        pthread_t SendProcessThread;
        int status = pthread_create (&SendProcessThread, NULL, (void* (*)(void*)) &sendProcessThread, t1);

        if ( status != 0 )
            log.writeLog(__LINE__, "SendProcessThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
    }
    else
    {
        sendProcessThread(t1);
    }

    return true;
}

/******************************************************************************************
* @brief	sendProcessThread
*
* purpose:	Send msg to update process state and status change time on disk
*
******************************************************************************************/
void sendProcessThread(sendProcessInfo_t* t)
{
    MonitorLog log;
    MonitorConfig config;
    Oam oam;

//	pthread_mutex_lock(&PROCESS_LOCK);

    string processName = boost::get<0>(*t);
    int state = boost::get<1>(*t);
    pid_t PID = boost::get<2>(*t);

    try
    {
        oam.setProcessStatus(processName, config.moduleName(), state, PID);
    }
    catch (exception& ex)
    {
        string error = ex.what();
        log.writeLog(__LINE__, "EXCEPTION ERROR on setProcessStatus: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
        log.writeLog(__LINE__, "EXCEPTION ERROR on setProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR );
    }

    delete t;
//	pthread_mutex_unlock(&PROCESS_LOCK);

    return;
}

/******************************************************************************************
* @brief	updateLog
*
* purpose:	Enable/Disable Logging configuration within the syslog.conf file
*
*			action: LOG_ENABLE / LOG_DISABLE
*			level:  all, critical, error, warning, info, debug, data
******************************************************************************************/
int ProcessMonitor::updateLog(std::string action, std::string level)
{
    MonitorLog log;
    Oam oam;

    struct flock fl;
    int fd;

    string fileName;
    oam.getSystemConfig("SystemLogConfigFile", fileName);

    if (fileName == oam::UnassignedName )
    {
        // unassigned
        log.writeLog(__LINE__, "ERROR: syslog file not configured ", LOG_TYPE_ERROR );
        return -1;
    }

    string::size_type pos = fileName.find("syslog-ng", 0);

    if (pos != string::npos)
    {
        // not supported
        log.writeLog(__LINE__, "ERROR: config file not support, " + fileName, LOG_TYPE_ERROR );
        return -1;
    }

    bool syslog7 = false;
    pos = fileName.find("49", 0);

    if (pos != string::npos)
    {
        syslog7 = true;
    }

    vector <string> lines;

    if ( level == "data" )
        return 0;

    ifstream oldFile (fileName.c_str());

    if (!oldFile )
    {
        // no file found
        log.writeLog(__LINE__, "ERROR: syslog file not found at " + fileName, LOG_TYPE_ERROR );
        return -1;
    }

    //if non-root, change file permissions so we can update it
    if ( !rootUser)
    {
        string cmd = "chmod 666 " + fileName + " > /dev/null 2>&1";
        system(cmd.c_str());
    }

    char line[200];
    string buf;
    bool restart = true;

    if ( action == oam::ENABLEDSTATE )
    {
        // check if enabling all log levels
        if ( level == "all")
        {
            vector <int> fileIDs;

            while (oldFile.getline(line, 200))
            {
                buf = line;

                for ( int i = 0;; i++)
                {
                    string localLogFile = oam::LogFile[i];

                    if (syslog7)
                        localLogFile = oam::LogFile7[i];

                    if ( localLogFile == "" )
                    {
                        // end of list
                        break;
                    }

                    string logFile = localLogFile;

                    pos = buf.find(logFile, 0);

                    if (pos != string::npos)
                    {
                        // log file already there, save fileID
                        fileIDs.push_back(i);
                        break;
                    }
                }

                // output to temp file
                lines.push_back(buf);
            } //end of while

            // check which fileIDs aren't in the syslog.conf
            bool update = false;

            for ( int i = 0;; i++)
            {
                bool found = false;
                string localLogFile = oam::LogFile[i];

                if (syslog7)
                    localLogFile = oam::LogFile7[i];

                if ( localLogFile == "" )
                {
                    // end of list
                    break;
                }

                vector<int>::iterator p = fileIDs.begin();

                while ( p != fileIDs.end() )
                {
                    if ( i == *p )
                    {
                        //already there
                        found = true;
                        break;
                    }

                    p++;
                }

                if (!found)
                {
                    lines.push_back(localLogFile);
                    log.writeLog(__LINE__, "Add in syslog.conf log file " + localLogFile, LOG_TYPE_DEBUG);
                    update = true;
                }
            }

            if (!update)
            {
                log.writeLog(__LINE__, "Log level file's already in syslog.conf", LOG_TYPE_DEBUG);
                restart = false;
            }
        }
        else
        {
            // enable a specific level
            // get log file for level
            for ( int i = 0;; i++)
            {
                if ( oam::LogLevel[i] == "" )
                {
                    // end of list
                    log.writeLog(__LINE__, "ERROR: log level file not found for level " + level, LOG_TYPE_ERROR );
                    oldFile.close();
                    return -1;
                }

                if ( level == oam::LogLevel[i] )
                {
                    // match found
                    string localLogFile = oam::LogFile[i];

                    if (syslog7)
                        localLogFile = oam::LogFile7[i];

                    string logFile = localLogFile;

                    while (oldFile.getline(line, 200))
                    {
                        buf = line;
                        string::size_type pos = buf.find(logFile, 0);

                        if (pos != string::npos)
                        {
                            log.writeLog(__LINE__, "Log level file already in syslog.conf", LOG_TYPE_DEBUG);
                            restart = false;
                        }

                        // output to temp file
                        lines.push_back(buf);
                    }

                    // file not found, add at the bottom of syslog.conf
                    lines.push_back(logFile);
                    break;
                }
            }
        }
    }
    else
    {
        // DISABLE LOG
        // check if disabling all log levels
        if ( level == "all")
        {
            bool update = false;

            while (oldFile.getline(line, 200))
            {
                buf = line;
                bool found = false;

                for ( int i = 0;; i++)
                {
                    string localLogFile = oam::LogFile[i];

                    if (syslog7)
                        localLogFile = oam::LogFile7[i];

                    if ( localLogFile == "" )
                    {
                        // end of list
                        break;
                    }

                    string logFile = localLogFile;

                    pos = buf.find(logFile, 0);

                    if (pos != string::npos)
                    {
                        // log file found
                        found = true;
                        update = true;
                        break;
                    }
                }

                if (!found)
                    // output to temp file
                    lines.push_back(buf);
            } //end of while

            if (!update)
            {
                log.writeLog(__LINE__, "No Log level file's in syslog.conf", LOG_TYPE_DEBUG);
                restart = false;
            }
        }
        else
        {
            // disable a specific level
            // get log file for level
            for ( int i = 0;; i++)
            {
                if ( oam::LogLevel[i] == "" )
                {
                    // end of list
                    log.writeLog(__LINE__, "ERROR: log level file not found for level " + level, LOG_TYPE_ERROR );
                    oldFile.close();
                    return -1;
                }

                if ( level == oam::LogLevel[i] )
                {
                    // match found
                    string localLogFile = oam::LogFile[i];

                    if (syslog7)
                        localLogFile = oam::LogFile7[i];

                    string logFile = localLogFile;
                    bool found = false;

                    while (oldFile.getline(line, 200))
                    {
                        buf = line;
                        string::size_type pos = buf.find(logFile, 0);

                        if (pos != string::npos)
                        {
                            // file found, don't push into new file
                            log.writeLog(__LINE__, "Log level file to DISABLE found in syslog.conf", LOG_TYPE_DEBUG);
                            found = true;
                        }
                        else
                        {
                            // no match, push into new temp file
                            lines.push_back(buf);
                        }
                    }

                    if (found)
                        break;
                    else
                    {
                        log.writeLog(__LINE__, "Log level file not in syslog.conf", LOG_TYPE_DEBUG);
                        restart = false;
                    }
                }
            }
        }
    }

    oldFile.close();

    //
    //  go write out new file if required
    //

    if (restart)
    {
        unlink (fileName.c_str());
        ofstream newFile (fileName.c_str());

        memset(&fl, 0, sizeof(fl));
        fl.l_type   = F_RDLCK;  // read lock
        fl.l_whence = SEEK_SET;
        fl.l_start  = 0;
        fl.l_len    = 0;	//lock whole file

        // create new file
        if ((fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0644)) >= 0)
        {
            // lock file

            if (fcntl(fd, F_SETLKW, &fl) != 0)
            {
                ostringstream oss;
                oss << "ProcessMonitor::updateLog: error locking file " <<
                    fileName <<
                    ": " <<
                    strerror(errno) <<
                    ", proceding anyway.";
                cerr << oss.str() << endl;
            }

            copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
            newFile.close();

            fl.l_type   = F_UNLCK;	//unlock
            fcntl(fd, F_SETLK, &fl);

            close(fd);

            oam.syslogAction("restart");
        }
        else
        {
            ostringstream oss;
            oss << "ProcessMonitor::updateLog: error opening file " <<
                fileName <<
                ": " <<
                strerror(errno);
            throw runtime_error(oss.str());
        }
    }

    //update file priviledges
    changeModLog();

    return 0;
}

/******************************************************************************************
* @brief	changeModLog
*
* purpose:	Updates the file mods so files can be read/write
* 			from external modules
*
******************************************************************************************/
void ProcessMonitor::changeModLog()
{
    for ( int i = 0;; i++)
    {
        if ( oam::LogFile[i].empty() )
            //end of list
            break;

        string logFile = oam::LogFile[i];
        string::size_type pos = logFile.find('/', 0);
        logFile = logFile.substr(pos, 200);
        string cmd = "chmod 755 " + logFile + " > /dev/null 2>&1";

        system(cmd.c_str());
    }

    return;
}

/******************************************************************************************
* @brief	getConfigLog
*
* purpose:	Get Logging configuration within the syslog.conf file
*
******************************************************************************************/
int ProcessMonitor::getConfigLog()
{
    MonitorLog log;
    Oam oam;

    string fileName;
    oam.getSystemConfig("SystemLogConfigFile", fileName);

    if (fileName == oam::UnassignedName )
    {
        // unassigned
        log.writeLog(__LINE__, "ERROR: syslog file not configured ", LOG_TYPE_ERROR );
        return API_FAILURE;
    }

    string::size_type pos = fileName.find("syslog-ng", 0);

    if (pos != string::npos)
    {
        // not supported
        log.writeLog(__LINE__, "ERROR: config file not support, " + fileName, LOG_TYPE_ERROR );
        return API_FAILURE;
    }

    ifstream oldFile (fileName.c_str());

    if (!oldFile)
    {
        // no file found
        log.writeLog(__LINE__, "ERROR: syslog file not found at " + fileName, LOG_TYPE_ERROR );
        return API_FILE_OPEN_ERROR;
    }

    int configData = 0;
    char line[200];
    string buf;

    while (oldFile.getline(line, 200))
    {
        buf = line;

        for ( int i = 0;; i++)
        {
            if ( oam::LogFile[i] == "" )
            {
                // end of list
                break;
            }

            string logFile = oam::LogFile[i];
            logFile = logFile.substr(14, 80);

            string::size_type pos = buf.find(logFile, 0);

            if (pos != string::npos)
            {
                // match found
                switch (i + 1)
                {
                    case 1:
                        configData = configData | LEVEL_CRITICAL;
                        break;

                    case 2:
                        configData = configData | LEVEL_ERROR;
                        break;

                    case 3:
                        configData = configData | LEVEL_WARNING;
                        break;

                    case 4:
                        configData = configData | LEVEL_INFO;
                        break;

                    case 5:
                        configData = configData | LEVEL_DEBUG;
                        break;

                    default:
                        break;
                } //end of switch
            }
        }
    } //end of while

    oldFile.close();

    // adjust data to be different from API RETURN CODES
    configData = configData + API_MAX;

    return configData;
}

/******************************************************************************************
* @brief	checkPowerOnResults
*
* purpose:	Read Power-On TEst results log file and report issues via Alarms
*
******************************************************************************************/
void ProcessMonitor::checkPowerOnResults()
{
    string  POWERON_TEST_RESULTS_FILE = "/var/lib/columnstore/st_status";
    MonitorLog log;

    ifstream oldFile (POWERON_TEST_RESULTS_FILE.c_str());

    if (!oldFile)
    {
        // no file found
        log.writeLog(__LINE__, "ERROR: Power-On Test results file not found at " + POWERON_TEST_RESULTS_FILE, LOG_TYPE_ERROR );
        return;
    }

    int configData = 0;
    char line[200];
    string buf;

    while (oldFile.getline(line, 200))
    {
        buf = line;
        string name;
        string level;
        string info = "";

        // extract name key word
        string::size_type pos = buf.find("name:", 0);
        string::size_type pos1;

        if (pos != string::npos)
        {
            // match found
            pos1 = buf.find("|", pos);

            if (pos1 != string::npos)
                // end of name found
                name = buf.substr(pos + 5, pos1 - (pos + 5));
            else
            {
                // name not found, skip this line
                continue;
            }
        }
        else
        {
            // name not found, skip this line
            continue;
        }

        // extract level key word
        pos = buf.find("level:", pos1);

        if (pos != string::npos)
        {
            // match found
            pos1 = buf.find("|", pos);

            if (pos1 != string::npos)
                // end of level found
                level = buf.substr(pos + 6, pos1 - (pos + 6));
            else
            {
                // level not found, skip this line
                continue;
            }
        }
        else
        {
            // level not found, skip this line
            continue;
        }

        // extract info key word, if any exist
        pos = buf.find("info:", pos1);

        if (pos != string::npos)
            // match found
            info = buf.substr(pos + 5, 200);

        // log findings
        LoggingID lid(18);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("Power-On self test: name = ");
        args.add(name);
        args.add(", level = ");
        args.add(level);
        args.add(", info = ");
        args.add(info);
        msg.format(args);
        ml.logDebugMessage(msg);

        // Issue alarm based on level

        pos = level.find("3", 0);

        if (pos != string::npos)
        {
            // Severe Warning found, Issue alarm
            sendAlarm(name, POWERON_TEST_SEVERE, SET);

            //Log this event
            LoggingID lid(18);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Power-On self test Severe Warning on ");
            args.add(name);
            args.add(": ");
            args.add(info);
            msg.format(args);
            ml.logWarningMessage(msg);
            continue;
        }

        pos = level.find("2", 0);

        if (pos != string::npos)
        {
            // Warning found, Issue alarm
            sendAlarm(name, POWERON_TEST_WARNING, SET);

            //Log this event
            LoggingID lid(18);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Power-On self test Warning on ");
            args.add(name);
            args.add(": ");
            args.add(info);
            msg.format(args);
            ml.logWarningMessage(msg);
            continue;
        }

        pos = level.find("1", 0);

        if (pos != string::npos)
        {
            // Info found, Log this event

            LoggingID lid(18);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Power-On self test Info on ");
            args.add(name);
            args.add(": ");
            args.add(info);
            msg.format(args);
            ml.logInfoMessage(msg);
            continue;
        }

    } //end of while

    oldFile.close();

    // adjust data to be different from API RETURN CODES
    configData = configData + API_MAX;

    return;
}

/******************************************************************************************
* @brief	updateConfig
*
* purpose:	Update Config data from disk
*
******************************************************************************************/
int ProcessMonitor::updateConfig()
{
    //ProcMon log file
    MonitorLog log;
    MonitorConfig currentConfig;
//	ProcessMonitor aMonitor(config, log);
    Oam oam;

    //Read ProcessConfig file to get process list belong to this process monitor
    SystemProcessConfig systemprocessconfig;

    try
    {
        oam.getProcessConfig(systemprocessconfig);
    }
    catch (exception& ex)
    {
        string error = ex.what();
        log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR );
    }
    catch (...)
    {
        log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR );
    }

    //Update a map for application launch ID for this Process-Monitor
    string OAMParentModuleType = currentConfig.OAMParentName().substr(0, MAX_MODULE_TYPE_SIZE);
    string systemModuleType = config.moduleName().substr(0, MAX_MODULE_TYPE_SIZE);

    for ( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
    {
        if (systemprocessconfig.processconfig[i].ModuleType == systemModuleType
                || ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                     systemModuleType == "pm" && PMwithUM == "y" )
                || systemprocessconfig.processconfig[i].ModuleType == "ChildExtOAMModule"
                || ( systemprocessconfig.processconfig[i].ModuleType == "ChildOAMModule" )
                || (systemprocessconfig.processconfig[i].ModuleType == "ParentOAMModule" &&
                    systemModuleType == OAMParentModuleType) )
        {
            if ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                    systemModuleType == "pm" && PMwithUM == "y" &&
                    systemprocessconfig.processconfig[i].ProcessName == "DMLProc" )
                continue;


            if ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
                    systemModuleType == "pm" && PMwithUM == "y" &&
                    systemprocessconfig.processconfig[i].ProcessName == "DDLProc" )
                continue;

            ProcessStatus processstatus;

            try
            {
                //Get the process information
                oam.getProcessStatus(systemprocessconfig.processconfig[i].ProcessName, config.moduleName(), processstatus);
            }
            catch (exception& ex)
            {
//				string error = ex.what();
//				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR );
                return API_FAILURE;
            }
            catch (...)
            {
//				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR );
                return API_FAILURE;
            }

            config.buildList(systemprocessconfig.processconfig[i].ModuleType,
                             systemprocessconfig.processconfig[i].ProcessName,
                             systemprocessconfig.processconfig[i].ProcessLocation,
                             systemprocessconfig.processconfig[i].ProcessArgs,
                             systemprocessconfig.processconfig[i].LaunchID,
                             processstatus.ProcessID,
                             processstatus.ProcessOpState,
                             systemprocessconfig.processconfig[i].BootLaunch,
                             systemprocessconfig.processconfig[i].RunType,
                             systemprocessconfig.processconfig[i].DepProcessName,
                             systemprocessconfig.processconfig[i].DepModuleName,
                             systemprocessconfig.processconfig[i].LogFile);
        }
    }

    return API_SUCCESS;
}

/******************************************************************************************
* @brief	buildSystemTables
*
* purpose:	Check for and build System Tables if not there
*			Only will be run from 'pm1'
*
******************************************************************************************/
int ProcessMonitor::buildSystemTables()
{
    Oam oam;
    string DBdir;
    oam.getSystemConfig("DBRoot1", DBdir);

    string fileName = DBdir + "/000.dir";

    //check if postConfigure or dbbuilder is already running
    string cmd = "ps aux | grep postConfigure | grep -v grep";
    int rtnCode = system(cmd.c_str());

    if (WEXITSTATUS(rtnCode) == 0)
        return API_ALREADY_IN_PROGRESS;

    cmd = "ps aux | grep dbbuilder | grep -v grep";
    rtnCode = system(cmd.c_str());

    if (WEXITSTATUS(rtnCode) == 0)
        return API_ALREADY_IN_PROGRESS;

    if (!IDBPolicy::exists(fileName.c_str()))
    {
        string logdir("/var/log/mariadb/columnstore");

        if (access(logdir.c_str(), W_OK) != 0) logdir = tmpLogDir;

        string cmd = "dbbuilder 7 > " + logdir + "/dbbuilder.log &";
        system(cmd.c_str());

        log.writeLog(__LINE__, "buildSystemTables: dbbuilder 7 Successfully Launched", LOG_TYPE_DEBUG);
        return API_SUCCESS;
    }

    log.writeLog(__LINE__, "buildSystemTables: System Tables Already Exist", LOG_TYPE_DEBUG );
    return API_FILE_ALREADY_EXIST;
}

/******************************************************************************************
* @brief	reconfigureModule
*
* purpose:	reconfigure Module functionality
*			Edit the moduleFile file with new Module Name
*
******************************************************************************************/
int ProcessMonitor::reconfigureModule(std::string reconfigureModuleName)
{
    Oam oam;

    //create custom files
    string dir = "/var/lib/columnstore/local/etc/" + reconfigureModuleName;

    string cmd = "mkdir " + dir + " > /dev/null 2>&1";
    system(cmd.c_str());

    cmd = "rm -f " + dir + "/* > /dev/null 2>&1";
    system(cmd.c_str());

    if ( reconfigureModuleName.find("um") != string::npos)
    {

        cmd = "cp /var/lib/columnstore/local/etc/um1/* " + dir + "/.";
        system(cmd.c_str());
    }
    else
    {
        cmd = "cp /var/lib/columnstore/local/etc/pm1/* " + dir + "/.";
        system(cmd.c_str());
    }

    //update module file
    string fileName = "/var/lib/columnstore/local/module";

    unlink (fileName.c_str());
    ofstream newFile (fileName.c_str());

    cmd = "echo " + reconfigureModuleName + " > " + fileName;
    system(cmd.c_str());

    newFile.close();

    return API_SUCCESS;
}

/******************************************************************************************
* @brief	configureModule
*
* purpose:	configure Module functionality
*			Edit the moduleFile file with new Module Name
*
******************************************************************************************/
int ProcessMonitor::configureModule(std::string configureModuleName)
{
    Oam oam;

    //update module file
    string fileName = "/var/lib/columnstore/local/module";

    unlink (fileName.c_str());
    ofstream newFile (fileName.c_str());

    string cmd = "echo " + configureModuleName + " > " + fileName;
    system(cmd.c_str());

    newFile.close();

    return API_SUCCESS;
}


/******************************************************************************************
* @brief	checkSpecialProcessState
*
* purpose:	Check if a SIMPLEX runtype Process has mates already up
*
******************************************************************************************/
int ProcessMonitor::checkSpecialProcessState( std::string processName, std::string runType, string processModuleType )
{
    MonitorLog log;
    MonitorConfig config;
    Oam oam;
    SystemProcessStatus systemprocessstatus;
    ProcessStatus processstatus;
    int retStatus = oam::MAN_INIT;

    if ( runType == SIMPLEX || runType == ACTIVE_STANDBY)
    {

        log.writeLog(__LINE__, "checkSpecialProcessState on : " + processName, LOG_TYPE_DEBUG);

        if ( runType == SIMPLEX && PMwithUM == "y" && processModuleType == "um" && gOAMParentModuleFlag)
            retStatus = oam::COLD_STANDBY;
        else if ( runType == SIMPLEX && gOAMParentModuleFlag )
            retStatus = oam::MAN_INIT;
        else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" &&
                  ( gOAMParentModuleFlag || !runStandby ) )
            retStatus = oam::MAN_INIT;
        else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" && config.OAMStandbyParentFlag() )
            retStatus = oam::STANDBY;
        else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" )
            retStatus = oam::COLD_STANDBY;
        else if ( runType == SIMPLEX && processModuleType == "ParentOAMModule" && !gOAMParentModuleFlag)
            retStatus = oam::COLD_STANDBY;
        else
        {
            //simplex on a non um1 or non-parent-pm
            if ( processName == "DMLProc" || processName == "DDLProc" )
            {
                string PrimaryUMModuleName;

                try
                {
                    oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
                }
                catch (...) {}

                if ( PrimaryUMModuleName != config.moduleName() )
                {
                    retStatus = oam::COLD_STANDBY;
                }
            }

            if ( retStatus != oam::COLD_STANDBY )
            {
                try
                {
                    oam.getProcessStatus(systemprocessstatus);

                    for ( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
                    {
                        if ( systemprocessstatus.processstatus[i].ProcessName == processName  &&
                                systemprocessstatus.processstatus[i].Module != config.moduleName() )
                        {
                            if ( systemprocessstatus.processstatus[i].ProcessOpState == ACTIVE ||
                                    systemprocessstatus.processstatus[i].ProcessOpState == MAN_INIT ||
                                    systemprocessstatus.processstatus[i].ProcessOpState == AUTO_INIT ||
                                    //		systemprocessstatus.processstatus[i].ProcessOpState == MAN_OFFLINE ||
                                    //		systemprocessstatus.processstatus[i].ProcessOpState == INITIAL ||
                                    systemprocessstatus.processstatus[i].ProcessOpState == BUSY_INIT)
                            {

                                // found a ACTIVE or going ACTIVE mate
                                if ( runType == oam::SIMPLEX )
                                    // SIMPLEX
                                    retStatus = oam::COLD_STANDBY;
                                else
                                {
                                    // ACTIVE_STANDBY
                                    for ( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
                                    {
                                        if ( systemprocessstatus.processstatus[j].ProcessName == processName  &&
                                                systemprocessstatus.processstatus[j].Module != config.moduleName() )
                                        {
                                            if ( systemprocessstatus.processstatus[j].ProcessOpState == STANDBY ||
                                                    systemprocessstatus.processstatus[j].ProcessOpState == STANDBY_INIT)
                                                // FOUND ACTIVE AND STANDBY
                                                retStatus = oam::COLD_STANDBY;
                                        }
                                    }

                                    // FOUND AN ACTIVE, BUT NO STANDBY
                                    log.writeLog(__LINE__, "checkSpecialProcessState, set STANDBY on " + processName, LOG_TYPE_DEBUG);
                                    retStatus = oam::STANDBY;
                                }
                            }
                        }
                    }
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
            }
        }
    }

    if ( retStatus == oam::COLD_STANDBY || retStatus == oam::STANDBY )
    {
        updateProcessInfo(processName, retStatus, 0);

        sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
        sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
        sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);
    }

    log.writeLog(__LINE__, "checkSpecialProcessState status return : " + oam.itoa(retStatus), LOG_TYPE_DEBUG);

    return retStatus;
}

/******************************************************************************************
* @brief	checkMateModuleState
*
* purpose:	Check if Mate Module is Active
*
******************************************************************************************/
int ProcessMonitor::checkMateModuleState()
{
    MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
    Oam oam;
    SystemStatus systemstatus;

    try
    {
        oam.getSystemStatus(systemstatus, false);

        for ( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
        {
            string moduleName = systemstatus.systemmodulestatus.modulestatus[i].Module;
            string moduleType = moduleName.substr(0, MAX_MODULE_TYPE_SIZE);
            int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE, MAX_MODULE_ID_SIZE).c_str());

            if ( moduleType == config.moduleType() && moduleID != config.moduleID() )
                if ( systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState == oam::ACTIVE )
                    return API_SUCCESS;
        }
    }
    catch (exception& ex)
    {
//		string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
        return API_FAILURE;
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
        return API_FAILURE;
    }

    return API_FAILURE;
}

/******************************************************************************************
* @brief	createDataDirs
*
* purpose:	Create the Calpont Data directories
*
*
******************************************************************************************/
int ProcessMonitor::createDataDirs(std::string cloud)
{
    MonitorLog log;
    Oam oam;

    if ( config.moduleType() == "um" &&
            ( cloud == "amazon-ec2" || cloud == "amazon-vpc") )
    {
        string UMStorageType;

        try
        {
            oam.getSystemConfig( "UMStorageType", UMStorageType);
        }
        catch (...) {}

        if (UMStorageType == "external")
        {
            if (!amazonVolumeCheck())
            {
                return API_FAILURE;
            }
        }
    }

    if ( config.moduleType() == "pm" )
    {
        DBRootConfigList dbrootConfigList;

        string DBRootStorageType;

        try
        {
            oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
        }
        catch (...) {}

        try
        {
            systemStorageInfo_t t;
            t = oam.getStorageConfig();

            if ( boost::get<1>(t) == 0 )
            {
                log.writeLog(__LINE__, "No dbroots are configured in Columnstore.xml file at proc mon startup time", LOG_TYPE_WARNING);
                return API_INVALID_PARAMETER;
            }

            DeviceDBRootList moduledbrootlist = boost::get<2>(t);

            DeviceDBRootList::iterator pt = moduledbrootlist.begin();

            for ( ; pt != moduledbrootlist.end() ; pt++)
            {
                int moduleID = (*pt).DeviceID;

                DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                for ( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
                {
                    int id = *pt1;

                    string DBRootName = "/var/lib/columnstore/data" + oam.itoa(id);

                    string cmd = "mkdir " + DBRootName + " > /dev/null 2>&1";
                    int rtnCode = system(cmd.c_str());

                    if (WEXITSTATUS(rtnCode) == 0)
                        log.writeLog(__LINE__, "Successful created directory " + DBRootName, LOG_TYPE_DEBUG);

                    cmd = "chmod 755 " + DBRootName + " > /dev/null 2>&1";
                    system(cmd.c_str());

                    if ( id == 1 )
                    {
                        cmd = "mkdir -p /var/lib/columnstore/data1/systemFiles/dbrm > /dev/null 2>&1";
                        system(cmd.c_str());
                    }

                    if ( (cloud == "amazon-ec2" || cloud == "amazon-vpc") &&
                            DBRootStorageType == "external" &&
                            config.moduleID() == moduleID)
                    {
                        if (!amazonVolumeCheck(id))
                        {
                            return API_FAILURE;
                        }
                    }
                }
            }
        }
        catch (exception& ex)
        {
            string error = ex.what();
//			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: " + error, LOG_TYPE_ERROR);
        }
        catch (...)
        {
//			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: Caught unknown exception!", LOG_TYPE_ERROR);
        }
    }

    return API_SUCCESS;
}


/******************************************************************************************
* @brief	processRestarted
*
* purpose:	Process Restarted, inform Process Mgr
*
*
******************************************************************************************/
int ProcessMonitor::processRestarted( std::string processName, bool manual)
{
    MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
    Oam oam;
    ByteStream msg;

    log.writeLog(__LINE__, "Inform Process Mgr that process was restarted: " + processName, LOG_TYPE_DEBUG);

    int returnStatus = API_FAILURE;

    msg << (ByteStream::byte) PROCESSRESTART;
    msg << config.moduleName();
    msg << processName;
    msg << (ByteStream::byte) manual;

    try
    {
        MessageQueueClient mqRequest("ProcMgr");
        mqRequest.write(msg);
        mqRequest.shutdown();
        returnStatus = API_SUCCESS;
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    return returnStatus;
}

/******************************************************************************************
* @brief	getDBRMdata
*
* purpose:	get DBRM Data from Process Mgr
*
*
******************************************************************************************/
int ProcessMonitor::getDBRMdata(string *path)
{
    MonitorLog log;

    Oam oam;
    ByteStream msg;

    int returnStatus = API_FAILURE;

    msg << (ByteStream::byte) GETDBRMDATA;
    msg << config.moduleName();

    try
    {
        MessageQueueClient mqRequest("ProcMgr");
        mqRequest.write(msg);

        ByteStream receivedMSG;

        struct timespec ts = { 30, 0 };

        //read message type
        try
        {
            receivedMSG = mqRequest.read(&ts);
        }
        catch (SocketClosed& ex)
        {
            string error = ex.what();
//			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
            return returnStatus;
        }
        catch (...)
        {
//			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
            return returnStatus;
        }

        if (receivedMSG.length() > 0)
        {

            string type;

            receivedMSG >> type;

            log.writeLog(__LINE__, type, LOG_TYPE_DEBUG);

            if ( type == "initial" )
            {
                log.writeLog(__LINE__, "initial system, no dbrm files to send", LOG_TYPE_DEBUG);
                returnStatus = API_SUCCESS;
            }
            else
            {
                // files coming, read number of files
                try
                {
                    receivedMSG = mqRequest.read(&ts);
                }
                catch (SocketClosed& ex)
                {
                    string error = ex.what();
//					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
                    return returnStatus;
                }
                catch (...)
                {
//					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
                    return returnStatus;
                }

                if (receivedMSG.length() > 0)
                {

                    ByteStream::byte numFiles;

                    receivedMSG >> numFiles;
                    log.writeLog(__LINE__, oam.itoa(numFiles), LOG_TYPE_DEBUG);

                    bool journalFile = false;
                    boost::uuids::uuid u = boost::uuids::random_generator()();
                    bf::path pTmp = bf::path(*path) / boost::uuids::to_string(u);
                    if (config::Config::makeConfig()->getConfig("Installation", "DBRootStorageType") != "storagemanager")
                        bf::create_directories(pTmp);
                    *path = pTmp.string();
                    log.writeLog(__LINE__, "Downloading DBRM files to " + *path, LOG_TYPE_DEBUG);
                    for ( int i = 0 ; i < numFiles ; i ++ )
                    {
                        string fileName;

                        //read file name
                        try
                        {
                            receivedMSG = mqRequest.read(&ts);
                        }
                        catch (SocketClosed& ex)
                        {
                            string error = ex.what();
//							log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
                            return returnStatus;
                        }
                        catch (...)
                        {
//							log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
                            return returnStatus;
                        }

                        if (receivedMSG.length() > 0)
                        {
                            receivedMSG >> fileName;

                            //check for journal file coming across
                            string::size_type pos = fileName.find("journal", 0);

                            if (pos != string::npos)
                                journalFile = true;

                            //change file name location to temp file local
//							string::size_type pos1 = fileName.find("/dbrm",0);
//							pos = fileName.find("data1",0);
//							if (pos != string::npos)
//							{
//								string temp = fileName.substr(0,pos);
//								string temp1 = temp + "data" + fileName.substr(pos1,80);
//								fileName = temp1;
//							}
                            bf::path pFilename(fileName);
                            pFilename = pTmp / pFilename.filename();
                            const char *cFilename = pFilename.string().c_str();
                            
                            boost::scoped_ptr<IDBDataFile> out(IDBDataFile::open(
                                                                   IDBPolicy::getType(cFilename,
                                                                           IDBPolicy::WRITEENG),
                                                                   cFilename, "w", 0));

                            // read file data
                            try
                            {
                                receivedMSG = mqRequest.read(&ts);
                            }
                            catch (SocketClosed& ex)
                            {
                                string error = ex.what();
//								log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
                                return returnStatus;
                            }
                            catch (...)
                            {
//								log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
                                return returnStatus;
                            }

                            if (receivedMSG.length() > 0)
                            {
                                out->write(receivedMSG.buf(), receivedMSG.length());
                                log.writeLog(__LINE__, fileName, LOG_TYPE_DEBUG);
                                log.writeLog(__LINE__, oam.itoa(receivedMSG.length()), LOG_TYPE_DEBUG);
                            }
                            else
                                log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
                        }
                        else
                            log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
                    }

                    //create journal file if none come across
                    if ( !journalFile)
                    {
                        bf::path pJournalFilename(pTmp / "BRM_saves_journal");
                        IDBDataFile *idbJournalFile = IDBDataFile::open(IDBPolicy::getType(pJournalFilename.string().c_str(),
                            IDBPolicy::WRITEENG), pJournalFilename.string().c_str(), "w", 0);
                        delete idbJournalFile;
                        //string cmd = "touch " + startup::StartUp::installDir() + "/data1/systemFiles/dbrm/BRM_saves_journal";
                        //system(cmd.c_str());
                    }

                    returnStatus = oam::API_SUCCESS;
                }
                else
                    log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
            }
        }
        else
            log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);

        mqRequest.shutdown();
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    return returnStatus;
}

/******************************************************************************************
* @brief	runStartupTest
*
* purpose:	Runs DB sanity test
*
*
******************************************************************************************/
int ProcessMonitor::runStartupTest()
{
    //ProcMon log file
    MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
    Oam oam;

    //skip if module is DISABLED
    int opState = oam::ACTIVE;
    bool degraded;
    oam.getModuleStatus(config.moduleName(), opState, degraded);

    if (geteuid() != 0 || opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
        return oam::API_SUCCESS;

    //run startup test script
    string logdir("/var/log/mariadb/columnstore");

    if (access(logdir.c_str(), W_OK) != 0) logdir = tmpLogDir;

    string cmd = "startupTests.sh > " + logdir + "/startupTests.log1 2>&1";
    system(cmd.c_str());

    cmd = logdir + "/startupTests.log1";

    bool fail = false;

    if (oam.checkLogStatus(cmd, "OK"))
    {
        log.writeLog(__LINE__, "startupTests passed", LOG_TYPE_DEBUG);
    }
    else
    {
        log.writeLog(__LINE__, "startupTests failed", LOG_TYPE_CRITICAL);
        fail = true;
    }

    if (!fail)
    {
        log.writeLog(__LINE__, "runStartupTest passed", LOG_TYPE_DEBUG);
        //Clear the alarm
        sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, CLEAR);
        return oam::API_SUCCESS;
    }
    else
    {
        log.writeLog(__LINE__, "ERROR: runStartupTest failed", LOG_TYPE_CRITICAL);
        //Set the alarm
        sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);

        //setModule status to failed
        try
        {
            oam.setModuleStatus(config.moduleName(), oam::FAILED);
        }
        catch (exception& ex)
        {
            string error = ex.what();
//			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
        }
        catch (...)
        {
//			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
        }

        return oam::API_FAILURE;
    }
}


/******************************************************************************************
* @brief	updateConfigFile
*
* purpose:	Update local Calpont Config File
*
******************************************************************************************/
int ProcessMonitor::updateConfigFile(messageqcpp::ByteStream msg)
{
    Config* sysConfig = Config::makeConfig();
    sysConfig->writeConfigFile(msg);

    return oam::API_SUCCESS;
}



/******************************************************************************************
* @brief	sendMsgProcMon1
*
* purpose:	Sends a Msg to ProcMon
*
******************************************************************************************/
std::string ProcessMonitor::sendMsgProcMon1( std::string module, ByteStream msg, int requestID )
{
    string msgPort = module + "_ProcessMonitor";
    string returnStatus = "FAILED";

    // do a ping test to determine a quick failure
    Config* sysConfig = Config::makeConfig();

    string IPAddr = sysConfig->getConfig(msgPort, "IPAddr");

    string cmdLine = "ping ";
    string cmdOption = " -c 1 -w 5 >> /dev/null 2>&1";
    string cmd = cmdLine + IPAddr + cmdOption;

    if ( system(cmd.c_str()) != 0 )
    {
        //ping failure
        log.writeLog(__LINE__, "sendMsgProcMon ping failure", LOG_TYPE_ERROR);
        return returnStatus;
    }

    try
    {
        MessageQueueClient mqRequest(msgPort);
        mqRequest.write(msg);

        // wait 30 seconds for response
        ByteStream::byte returnACK;
        ByteStream::byte returnRequestID;
        string requestStatus;
        ByteStream receivedMSG;

        struct timespec ts = { 30, 0 };

        try
        {
            receivedMSG = mqRequest.read(&ts);
        }
        catch (SocketClosed& ex)
        {
            string error = ex.what();
//			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
            return returnStatus;
        }
        catch (...)
        {
//			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
            return returnStatus;
        }

        if (receivedMSG.length() > 0)
        {
            receivedMSG >> returnACK;
            receivedMSG >> returnRequestID;
            receivedMSG >> requestStatus;

            if ( returnACK == oam::ACK &&  returnRequestID == requestID)
            {
                // ACK for this request
                returnStatus = requestStatus;
            }
            else
                log.writeLog(__LINE__, "sendMsgProcMon: message mismatch ", LOG_TYPE_ERROR);
        }
        else
            log.writeLog(__LINE__, "sendMsgProcMon: ProcMon Msg timeout on module " + module, LOG_TYPE_ERROR);

        mqRequest.shutdown();
    }
    catch (exception& ex)
    {
        string error = ex.what();
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
    }
    catch (...)
    {
//		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
    }

    return returnStatus;
}

/******************************************************************************************
* @brief	checkModuleFailover
*
* purpose:	check if module failover is needed due to a process outage
*
******************************************************************************************/
void ProcessMonitor::checkModuleFailover( std::string processName)
{
    Oam oam;

    //force failover on certain processes
    if ( processName == "DDLProc" ||
            processName == "DMLProc" ) {
			log.writeLog(__LINE__, "checkModuleFailover: process failover, process outage of " + processName, LOG_TYPE_CRITICAL);

        try
        {
            SystemProcessStatus systemprocessstatus;
            oam.getProcessStatus(systemprocessstatus);

            for ( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
            {
                if ( systemprocessstatus.processstatus[i].ProcessName == processName  &&
                        systemprocessstatus.processstatus[i].Module != config.moduleName() )
                {
                    //make sure it matches module type
                    string procModuleType = systemprocessstatus.processstatus[i].Module.substr(0, MAX_MODULE_TYPE_SIZE);

                    if ( config.moduleType() != procModuleType )
                        continue;

                    if ( systemprocessstatus.processstatus[i].ProcessOpState == oam::COLD_STANDBY ||
                            systemprocessstatus.processstatus[i].ProcessOpState == oam::AUTO_OFFLINE ||
                            systemprocessstatus.processstatus[i].ProcessOpState == oam::FAILED )
                    {
                        // found a AVAILABLE mate, start it
						log.writeLog(__LINE__, "Change UM Master to module " + systemprocessstatus.processstatus[i].Module, LOG_TYPE_DEBUG);
						log.writeLog(__LINE__, "Stop local UM module " + config.moduleName(), LOG_TYPE_DEBUG);
						log.writeLog(__LINE__, "Disable Local will Enable UM module " + systemprocessstatus.processstatus[i].Module, LOG_TYPE_DEBUG);

						oam::DeviceNetworkConfig devicenetworkconfig;
						oam::DeviceNetworkList devicenetworklist;

						devicenetworkconfig.DeviceName = config.moduleName();
						devicenetworklist.push_back(devicenetworkconfig);

                        try
                        {
							oam.stopModule(devicenetworklist, oam::FORCEFUL, oam::ACK_YES);
							log.writeLog(__LINE__, "success stopModule on module " + config.moduleName(), LOG_TYPE_DEBUG);

							try
							{
								oam.disableModule(devicenetworklist);
								log.writeLog(__LINE__, "success disableModule on module " + config.moduleName(), LOG_TYPE_DEBUG);
							}
							catch (exception& e)
							{
								log.writeLog(__LINE__, "failed disableModule on module " + config.moduleName(), LOG_TYPE_ERROR);
							}
                        }
						catch (exception& e)
						{
							log.writeLog(__LINE__, "failed stopModule on module " + config.moduleName(), LOG_TYPE_ERROR);
						}

                        break;
                    }
                }
            }
        }
        catch (exception& ex)
        {
//			string error = ex.what();
//			log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
        }
        catch (...)
        {
//			log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
        }
    }
}

/******************************************************************************************
* @brief	changeMyCnf
*
* purpose:	Change my.cnf
*
******************************************************************************************/
int ProcessMonitor::changeMyCnf(std::string type)
{
    Oam oam;

    log.writeLog(__LINE__, "changeMyCnf function called for " + type, LOG_TYPE_DEBUG);

    string mycnfFile = string(MCSMYCNFDIR) + "/columnstore.cnf";
    ifstream file (mycnfFile.c_str());

    if (!file)
    {
        log.writeLog(__LINE__, "changeMyCnf - my.cnf file not found: " + mycnfFile, LOG_TYPE_CRITICAL);
        return oam::API_FAILURE;
    }

    //get server-id based on ExeMgrx setup
    string serverID = "0";
    string localModuleName = config.moduleName();

    for ( int id = 1 ; ; id++ )
    {
        string Section = "ExeMgr" + oam.itoa(id);

        string moduleName;

        try
        {
            Config* sysConfig = Config::makeConfig();
            moduleName = sysConfig->getConfig(Section, "Module");

            if ( moduleName == localModuleName )
            {
                serverID = oam.itoa(id);
                break;
            }
        }
        catch (...) {}
    }

    if ( serverID == "0" )
    {
        log.writeLog(__LINE__, "changeMyCnf: ExeMgr for local module doesn't exist", LOG_TYPE_ERROR);
        return oam::API_FAILURE;
    }

    // set server-id and other options in my.cnf
    vector <string> lines;
    char line[200];
    string buf;

    while (file.getline(line, 200))
    {
        buf = line;
        string::size_type pos = buf.find("server-id", 0);

        if ( pos != string::npos )
        {
            buf = "server-id = " + serverID;

            string command = "SET GLOBAL server_id=" + serverID + ";";
            int ret = runMariaDBCommandLine(command);

            if (ret != 0)
            {
                log.writeLog(__LINE__, "changeMyCnf: runMariaDBCommandLine Error", LOG_TYPE_ERROR);
                return oam::API_FAILURE;
            }
        }

        pos = buf.find("log_bin", 0);
        if ( pos != string::npos )
        {
            buf = "log_bin";
        }

        // set local query flag if on pm
        if ( (PMwithUM == "y") && config.moduleType() == "pm" )
        {
            pos = buf.find("columnstore_local_query", 0);

            if ( pos != string::npos )
            {
                buf = "columnstore_local_query=1";

                string command = "SET GLOBAL " + buf + ";";

                int ret = runMariaDBCommandLine(command);

                if (ret != 0)
                {
                    log.writeLog(__LINE__, "changeMyCnf: runMariaDBCommandLine Error", LOG_TYPE_ERROR);
                    return oam::API_FAILURE;
                }
            }
        }
        else
        {
            // disable, if needed
            pos = buf.find("columnstore_local_query", 0);

            if ( pos != string::npos )
            {
                buf = "columnstore_local_query=0";

                string command = "SET GLOBAL " + buf + ";";
                int ret = runMariaDBCommandLine(command);

                if (ret != 0)
                {
                    log.writeLog(__LINE__, "changeMyCnf: runMariaDBCommandLine Error", LOG_TYPE_ERROR);
                    return oam::API_FAILURE;
                }
            }
        }

        //output to temp file
        lines.push_back(buf);
    }

    file.close();
    unlink (mycnfFile.c_str());
    ofstream newFile (mycnfFile.c_str());

    //create new file
    int fd = open(mycnfFile.c_str(), O_RDWR | O_CREAT, 0664);

    copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
    newFile.close();

    close(fd);

    // set owner and permission
    string cmd = "chmod 664 " + mycnfFile + " >/dev/null 2>&1";

    system(cmd.c_str());

    cmd = "chown " + USER + ":" + USER + " " + mycnfFile + " >/dev/null 2>&1";

    system(cmd.c_str());

    // restart mysql
	try {
    		oam.actionMysqlCalpont(MYSQL_RESTART);
    		sleep(5);	// give after mysql restart
    	}
    	catch(...)
    	{}
    log.writeLog(__LINE__, "changeMyCnf function successfully completed", LOG_TYPE_DEBUG);

    return oam::API_SUCCESS;
}

/******************************************************************************************
* @brief	runMariaDBCommandLine
*
* purpose:	run MariaDB Command Line script
*
******************************************************************************************/
int ProcessMonitor::runMariaDBCommandLine(std::string command)
{
    Oam oam;

    log.writeLog(__LINE__, "runMariaDBCommandLine function called: cmd = " + command, LOG_TYPE_DEBUG);

    // mysql port number
    string MySQLPort;

    try
    {
        oam.getSystemConfig("MySQLPort", MySQLPort);
    }
    catch (...)
    {
        MySQLPort = "3306";
    }

    if ( MySQLPort.empty() )
        MySQLPort = "3306";

    string cmd = "mariadb-command-line.sh --command='" + command + "' --port=" + MySQLPort + " --tmpdir=" + tmpLogDir + " > " + tmpLogDir + "/mariadb-command-line.sh.log 2>&1";

    log.writeLog(__LINE__, "cmd = " + cmd, LOG_TYPE_DEBUG);

    system(cmd.c_str());

    string logFile = tmpLogDir + "/mariadb-command-line.sh.log";

    if (oam.checkLogStatus(logFile, "ERROR 1045") )
    {
        log.writeLog(__LINE__, "mariadb-command-line.sh:  MySQL Password Error, check .my.cnf", LOG_TYPE_ERROR);
        return oam::API_FAILURE;
    }
    else
    {
        if (oam.checkLogStatus(logFile, "OK"))
        {
            log.writeLog(__LINE__, "mariadb-command-line.sh: Successful return", LOG_TYPE_DEBUG);
            return oam::API_SUCCESS;
        }
        else
        {
            log.writeLog(__LINE__, "mariadb-command-line.sh: Error return, check log " + tmpLogDir + "/mariadb-command-line.sh.log", LOG_TYPE_ERROR);
            return oam::API_FAILURE;
        }
    }

    return oam::API_FAILURE;
}


/******************************************************************************************
* @brief	runMasterRep
*
* purpose:	run Master Replication script
*
******************************************************************************************/
int ProcessMonitor::runMasterRep(std::string& masterLogFile, std::string& masterLogPos)
{
    Oam oam;

    SystemModuleTypeConfig systemModuleTypeConfig;

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

    // mysql port number
    string MySQLPort;

    try
    {
        oam.getSystemConfig("MySQLPort", MySQLPort);
    }
    catch (...)
    {
        MySQLPort = "3306";
    }

    if ( MySQLPort.empty() )
        MySQLPort = "3306";

    // create user for each module by ip address
    for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
    {
        int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

        if ( moduleCount == 0)
            continue;

        DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
        {
            string moduleName =  (*pt).DeviceName;

            //skip if module is not ACTIVE

//			int opState = oam::ACTIVE;
//			bool degraded;
//			oam.getModuleStatus(moduleName, opState, degraded);
//			if (opState != oam::ACTIVE)
//				continue;

            bool passwordError = false;

            string moduleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;

            if ( ( (PMwithUM == "n") && (moduleType == "pm") ) &&
                    ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
                continue;

            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
            {
                string ipAddr = (*pt1).IPAddr;

                string logFile = tmpLogDir + "/master-rep-columnstore-" + moduleName + ".log";
                string cmd = "master-rep-columnstore.sh --hostIP=" + ipAddr + " --port=" + MySQLPort + " --tmpdir=" + tmpLogDir + "  > " + logFile + " 2>&1";
                log.writeLog(__LINE__, "cmd = " + cmd, LOG_TYPE_DEBUG);

                system(cmd.c_str());

                if (oam.checkLogStatus(logFile, "ERROR 1045") )
                {
                    if ( passwordError )
                    {
                        log.writeLog(__LINE__, "master-rep-columnstore.sh:  MySQL Password Error", LOG_TYPE_ERROR);
                        return oam::API_FAILURE;
                    }

                    log.writeLog(__LINE__, "master-rep-columnstore.sh: Missing Password error, go check for a password and retry", LOG_TYPE_DEBUG);
                    passwordError = true;
                    break;
                }
                else
                {
                    if (oam.checkLogStatus(logFile, "OK"))
                        log.writeLog(__LINE__, "master-rep-columnstore.sh: Successful return for node " + moduleName, LOG_TYPE_DEBUG);
                    else
                    {
                        log.writeLog(__LINE__, "master-rep-columnstore.sh: Error return, check log " + logFile, LOG_TYPE_ERROR);
                        return oam::API_FAILURE;
                    }
                }
            }
        }
    }

    // go parse out the MASTER_LOG_FILE and MASTER_LOG_POS
    // this is what the output will look like
    //
    //	SHOW MASTER STATUS
    //	File	Position	Binlog_Do_DB	Binlog_Ignore_DB
    //	mysql-bin.000006	2921
    //
    // in log - show-master-status.log

	string masterLog = tmpLogDir + "/show-master-status.log";
    ifstream file (masterLog.c_str());

    if (!file)
    {
        log.writeLog(__LINE__, "runMasterRep - show master status log file doesn't exist - " + masterLog, LOG_TYPE_ERROR);
        return oam::API_FAILURE;
    }
    else
    {
        char line[200];
        string buf;

        while (file.getline(line, 200))
        {
            buf = line;
            string::size_type pos = buf.find("000", 0);

            if ( pos != string::npos )
            {
                pos = 0;
                string::size_type pos1 = buf.find("\t", pos);

                if ( pos1 != string::npos )
                {
                    string masterlogfile = buf.substr(pos, pos1 - pos);

                    //strip trailing spaces
                    string::size_type lead = masterlogfile.find_first_of(" ");
                    masterLogFile = masterlogfile.substr( 0, lead);

                    string masterlogpos = buf.substr(pos1, 80);

                    //strip off leading tab masterlogpos
                    lead = masterlogpos.find_first_not_of("\t");
                    masterlogpos = masterlogpos.substr( lead, masterlogpos.length() - lead);

                    //string trailing spaces
                    lead = masterlogpos.find_first_of(" ");
                    masterLogPos = masterlogpos.substr( 0, lead);

                    log.writeLog(__LINE__, "runMasterRep: masterlogfile=" + masterLogFile + ", masterlogpos=" + masterLogPos, LOG_TYPE_DEBUG);
                    file.close();
                    return oam::API_SUCCESS;
                }
            }
        }

        file.close();
    }

    log.writeLog(__LINE__, "runMasterRep - 'mysql-bin not found in log file - " + masterLog, LOG_TYPE_ERROR);

    return oam::API_FAILURE;
}

/******************************************************************************************
* @brief	runSlaveRep
*
* purpose:	run Slave Replication script
*
******************************************************************************************/
int ProcessMonitor::runSlaveRep(std::string& masterLogFile, std::string& masterLogPos)
{
    Oam oam;

    // get master replicaion module IP Address
    string PrimaryUMModuleName;
    oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);

    string masterIPAddress;

    try
    {
        ModuleConfig moduleconfig;
        oam.getSystemConfig(PrimaryUMModuleName, moduleconfig);
        HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
        masterIPAddress = (*pt1).IPAddr;
    }
    catch (...)
    {}

    // mysql port number
    string MySQLPort;

    try
    {
        oam.getSystemConfig("MySQLPort", MySQLPort);
    }
    catch (...)
    {
        MySQLPort = "3306";
    }

    if ( MySQLPort.empty() )
        MySQLPort = "3306";

    bool passwordError = false;

    while (true)
    {
        string logFile = tmpLogDir + "/slave-rep-columnstore.log";

        string cmd = "slave-rep-columnstore.sh --masteripaddr=" + masterIPAddress + " --masterlogfile=" + masterLogFile  + " --masterlogpos=" + masterLogPos + " --port=" + MySQLPort + " --tmpdir=" + tmpLogDir + "  > " + logFile + " 2>&1";

        log.writeLog(__LINE__, "cmd = " + cmd, LOG_TYPE_DEBUG);

        system(cmd.c_str());

        if (oam.checkLogStatus(logFile, "ERROR 1045") )
        {
            if ( passwordError )
            {
                log.writeLog(__LINE__, "slave-rep-columnstore.sh:  MySQL Password Error", LOG_TYPE_ERROR);
                return oam::API_FAILURE;
            }

            log.writeLog(__LINE__, "slave-rep-columnstore.sh: Missing Password error, go check for a password and retry", LOG_TYPE_DEBUG);
            passwordError = true;
        }
        else
        {
            if (oam.checkLogStatus(logFile, "OK"))
            {
                log.writeLog(__LINE__, "slave-rep-columnstore.sh: Successful return", LOG_TYPE_DEBUG);
                return oam::API_SUCCESS;
            }
            else
            {
                log.writeLog(__LINE__, "slave-rep-columnstore.sh: Error return, check log " + logFile, LOG_TYPE_ERROR);
                return oam::API_FAILURE;
            }
        }
    }

    return oam::API_FAILURE;
}

/******************************************************************************************
* @brief	runDisableRep
*
* purpose:	run Disable Replication script
*
******************************************************************************************/
int ProcessMonitor::runDisableRep()
{
    Oam oam;

    // mysql port number
    string MySQLPort;

    try
    {
        oam.getSystemConfig("MySQLPort", MySQLPort);
    }
    catch (...)
    {
        MySQLPort = "3306";
    }

    if ( MySQLPort.empty() )
        MySQLPort = "3306";

    string logFile = tmpLogDir + "/disable-rep-columnstore.log";

    string cmd = "disable-rep-columnstore.sh --tmpdir=" + tmpLogDir + "  >  " + logFile + " 2>&1";

    log.writeLog(__LINE__, "cmd = " + cmd, LOG_TYPE_DEBUG);

    system(cmd.c_str());

    if (oam.checkLogStatus(logFile, "OK"))
    {
        log.writeLog(__LINE__, "disable-rep-columnstore.sh: Successful return", LOG_TYPE_DEBUG);
        return oam::API_SUCCESS;
    }
    else
    {
        if (oam.checkLogStatus(cmd, "ERROR 1045") )
        {
            log.writeLog(__LINE__, "disable-rep-columnstore.sh: Missing Password error, return success", LOG_TYPE_DEBUG);
            return oam::API_SUCCESS;
        }

        log.writeLog(__LINE__, "disable-rep-columnstore.sh: Error return, check log " + logFile, LOG_TYPE_ERROR);
        return oam::API_FAILURE;
    }

    return oam::API_FAILURE;
}

/******************************************************************************************
* @brief	runMasterDist
*
* purpose:	run Master DB Distribution
*
******************************************************************************************/
int ProcessMonitor::runMasterDist(std::string& password, std::string& slaveModule)
{
    Oam oam;

    SystemModuleTypeConfig systemModuleTypeConfig;

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

    int slave = 0;

    if ( slaveModule == "all" )
    {
        // Distrubuted MySQL Front-end DB to Slave Modules
        for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
        {
            int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

            if ( moduleCount == 0)
                continue;

            string moduleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;

            if ( ( (PMwithUM == "n") && (moduleType == "pm") ) &&
                    ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
                continue;

            DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

            for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
            {
                string moduleName =  (*pt).DeviceName;

                //skip if local master mode
                if ( moduleName == config.moduleName() )
                    continue;

                slave++;

                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
                {
                    string ipAddr = (*pt1).IPAddr;

                    string logFile = tmpLogDir + "/master-dist_" + moduleName + ".log";
                    string cmd = "rsync.sh " + ipAddr + " " + password + " 1 > " + logFile;

                    log.writeLog(__LINE__, "cmd = " + cmd, LOG_TYPE_DEBUG);
                    system(cmd.c_str());


                    if (!oam.checkLogStatus(logFile, "FAILED"))
                    {
                        log.writeLog(__LINE__, "runMasterDist: Success rsync to module: " + moduleName, LOG_TYPE_DEBUG);
                        break;
                    }
                    else
                    {
                        log.writeLog(__LINE__, "runMasterDist: Failure rsync to module: " + moduleName, LOG_TYPE_ERROR);
                        return oam::API_FAILURE;
                    }
                }
            }
        }
    }
    else
    {
        // don't do PMs unless PMwithUM flag is set

        string moduleType = slaveModule.substr(0, MAX_MODULE_TYPE_SIZE);

        if ( (moduleType == "um") ||
                ( (PMwithUM == "y") && (moduleType == "pm") ) ||
                ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM) )
        {
            slave++;

            // get slave IP address
            ModuleConfig moduleconfig;
            oam.getSystemConfig(slaveModule, moduleconfig);
            HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
            string ipAddr = (*pt1).IPAddr;

            string logFile = tmpLogDir + "/master-dist_" + slaveModule + ".log";

            string cmd = "rsync.sh " + ipAddr + " " + password + " 1 > " + logFile;
            system(cmd.c_str());

            if (!oam.checkLogStatus(logFile, "FAILED"))
                log.writeLog(__LINE__, "runMasterDist: Success rsync to module: " + slaveModule, LOG_TYPE_DEBUG);
            else
            {
                log.writeLog(__LINE__, "runMasterDist: Failure rsync to module: " + slaveModule, LOG_TYPE_ERROR);
                return oam::API_FAILURE;
            }
        }
    }

    if (slave == 0 )
        log.writeLog(__LINE__, "runMasterDist: No configured slave nodes", LOG_TYPE_DEBUG);

    return oam::API_SUCCESS;
}


/******************************************************************************************
* @brief	amazonIPCheck
*
* purpose:	check and setups Amazon EC2 IP Addresses
*
******************************************************************************************/
bool ProcessMonitor::amazonIPCheck()
{
    MonitorLog log;
    Oam oam;

    // delete description file so it will create a new one
    string tmpLog = tmpLogDir + "/describeInstance.log";
    unlink(tmpLog.c_str());

    //
    // Get Module Info
    //
    SystemModuleTypeConfig systemModuleTypeConfig;

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

    //get Elastic IP Address count
    int AmazonElasticIPCount = 0;

    try
    {
        oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
    }
    catch (...)
    {
        AmazonElasticIPCount = 0;
    }

    ModuleTypeConfig moduletypeconfig;

    //get module/instance IDs
    for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
    {
        int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;

        if ( moduleCount == 0 )
            // skip of no modules configured
            continue;

        DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();

        for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
        {
            DeviceNetworkConfig devicenetworkconfig;
            HostConfig hostconfig;

            devicenetworkconfig.DeviceName = (*pt).DeviceName;

            HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

            for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
            {
                hostconfig.IPAddr = (*pt1).IPAddr;
                hostconfig.HostName = (*pt1).HostName;
                devicenetworkconfig.hostConfigList.push_back(hostconfig);
            }

            moduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
        }
    }

    // now loop and wait for 5 minutes for all configured Instances to be running
    // like after a reboot
    bool startFail = false;

    for ( int time = 0 ; time < 30 ; time++ )
    {
        startFail = false;
        DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

        for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
        {
            string moduleName = (*pt).DeviceName;

            // get all ips if parent oam
            // get just parent and local if not parent oam
            MonitorConfig currentConfig;

            if ( config.moduleName() == currentConfig.OAMParentName() ||
                    moduleName == config.moduleName() ||
                    moduleName == currentConfig.OAMParentName() )
            {
                HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

                for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
                {
                    string IPAddr = (*pt1).IPAddr;
                    string instanceID = (*pt1).HostName;

                    log.writeLog(__LINE__, "getEC2InstanceIpAddress called to get status for Module '" + moduleName + "' / Instance " + instanceID, LOG_TYPE_DEBUG);
                    string currentIPAddr = oam.getEC2InstanceIpAddress(instanceID);

                    if (currentIPAddr == "stopped")
                    {
                        log.writeLog(__LINE__, "Module '" + moduleName + "' / Instance '" + instanceID + "' not running", LOG_TYPE_WARNING);
                        startFail = true;
                    }
                    else
                    {
                        if (currentIPAddr == "terminated")
                        {
                            log.writeLog(__LINE__, "Module '" + moduleName + "' / Instance '" + instanceID + "' has no Private IP Address Assigned, system failed to start", LOG_TYPE_CRITICAL);
                            startFail = true;
                            break;
                        }
                        else
                        {
                            if ( currentIPAddr != IPAddr )
                            {
                                log.writeLog(__LINE__, "Module is Running: '" + moduleName + "' / Instance '" + instanceID + "' current IP being reconfigured in Columnstore.xml. old = " + IPAddr + ", new = " + currentIPAddr, LOG_TYPE_DEBUG);

                                // update the Columnstore.xml with the new IP Address
                                string cmd = "sed -i s/" + IPAddr + "/" + currentIPAddr + "/g " + MCSSYSCONFDIR + "/columnstore/Columnstore.xml";
                                system(cmd.c_str());
                            }
                            else
                                log.writeLog(__LINE__, "Module is Running: '" + moduleName + "' / Instance '" + instanceID + "' current IP didn't change.", LOG_TYPE_DEBUG);
                        }
                    }

                    //set Elastic IP Address, if configured
                    if (AmazonElasticIPCount > 0)
                    {
                        bool found = false;
                        int id = 1;

                        for (  ; id < AmazonElasticIPCount + 1 ; id++ )
                        {
                            string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
                            string ELmoduleName;
                            string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
                            string ELIPaddress;

                            try
                            {
                                oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
                                oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
                            }
                            catch (...) {}

                            if ( ELmoduleName == moduleName )
                            {
                                found = true;

                                try
                                {
                                    oam.assignElasticIP(instanceID, ELIPaddress);
                                    log.writeLog(__LINE__, "Assign Elastic IP Address : '" + moduleName + "' / '" + ELIPaddress, LOG_TYPE_DEBUG);
                                }
                                catch (...)
                                {
                                    log.writeLog(__LINE__, "Assign Elastic IP Address failed : '" + moduleName + "' / '" + ELIPaddress, LOG_TYPE_ERROR);
                                    break;
                                }
                                break;
                            }

                            if (found)
                                break;
                        }
                    }
                }
            }
        }

        //continue when no instances are stopped
        if (!startFail)
            break;

        sleep(10);
    }

    //check if an instance is stopped, exit out...
    if (startFail)
    {
        log.writeLog(__LINE__, "A configured Instance isn't running. Check warning log", LOG_TYPE_CRITICAL);
    }

    log.writeLog(__LINE__, "amazonIPCheck function successfully completed", LOG_TYPE_DEBUG);

    return true;

}

/******************************************************************************************
* @brief	amazonVolumeCheck
*
* purpose:	check and setups Amazon EC2 Volume mounts
*
******************************************************************************************/
bool ProcessMonitor::amazonVolumeCheck(int dbrootID)
{
    MonitorLog log;
    Oam oam;

    {
        log.writeLog(__LINE__, "amazonVolumeCheck function called for DBRoot" + oam.itoa(dbrootID), LOG_TYPE_DEBUG);

        string volumeNameID = "PMVolumeName" + oam.itoa(dbrootID);
        string volumeName = oam::UnassignedName;
        string deviceNameID = "PMVolumeDeviceName" + oam.itoa(dbrootID);
        string deviceName = oam::UnassignedName;

        try
        {
            oam.getSystemConfig( volumeNameID, volumeName);
            oam.getSystemConfig( deviceNameID, deviceName);
        }
        catch (...)
        {}

        if ( volumeName.empty() || volumeName == oam::UnassignedName )
        {
            log.writeLog(__LINE__, "amazonVolumeCheck function exiting, no volume assigned to DBRoot " + oam.itoa(dbrootID), LOG_TYPE_WARNING);
            return false;
        }

        string status = oam.getEC2VolumeStatus(volumeName);
        log.writeLog(__LINE__, "amazonVolumeCheck:  getEC2VolumeStatus: " + status, LOG_TYPE_DEBUG);


        if ( status == "attached" )
        {
            log.writeLog(__LINE__, "amazonVolumeCheck function successfully completed, volume attached: " + volumeName, LOG_TYPE_DEBUG);
            return true;
        }

        if ( status != "available" )
        {
            log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume not attached and not available: " + volumeName, LOG_TYPE_WARNING);
            return false;
        }
        else
        {
            //get Module HostName / InstanceName
            string instanceName;

            try
            {
                ModuleConfig moduleconfig;
                oam.getSystemConfig(config.moduleName(), moduleconfig);
                HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
                instanceName = (*pt1).HostName;
            }
            catch (...)
            {}

            if (oam.attachEC2Volume(volumeName, deviceName, instanceName))
            {
				log.writeLog(__LINE__, "amazonVolumeCheck function , volume to attached: " + volumeName, LOG_TYPE_DEBUG);

                string cmd = SUDO + "mount /var/lib/columnstore/data" + oam.itoa(dbrootID) + " > /dev/null";

                system(cmd.c_str());
				log.writeLog(__LINE__, "amazonVolumeCheck function , volume to mounted: " + volumeName, LOG_TYPE_DEBUG);

                cmd = SUDO + "chown -R " + USER + ":" + USER + " /var/lib/columnstore/data" + oam.itoa(dbrootID);
                system(cmd.c_str());

                return true;
            }
            else
            {
                log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume failed to attached: " + volumeName, LOG_TYPE_CRITICAL);
                return false;
            }
        }
    }

    log.writeLog(__LINE__, "amazonVolumeCheck function successfully completed", LOG_TYPE_DEBUG);

    return true;

}

/******************************************************************************************
* @brief	unmountExtraDBroots
*
* purpose:	unmount Extra DBroots which were left mounted during a move
*
*
******************************************************************************************/
void ProcessMonitor::unmountExtraDBroots()
{
    MonitorLog log;
    ModuleConfig moduleconfig;
    Oam oam;

    string DBRootStorageType = "internal";

    try
    {
        oam.getSystemConfig("DBRootStorageType", DBRootStorageType);

        if ( DBRootStorageType == "hdfs" || DBRootStorageType == "storagemanager" ||
                ( DBRootStorageType == "internal" && DataRedundancyConfig == "n") )
            return;
    }
    catch (...) {}

//	if (DataRedundancyConfig == "y")
//		return;

    try
    {
        systemStorageInfo_t t;
        t = oam.getStorageConfig();

        if ( boost::get<1>(t) == 0 )
        {
            return;
        }

        DeviceDBRootList moduledbrootlist = boost::get<2>(t);

        //Flush the cache
        cacheutils::flushPrimProcCache();
        cacheutils::dropPrimProcFdCache();
        flushInodeCache();

        DeviceDBRootList::iterator pt = moduledbrootlist.begin();

        for ( ; pt != moduledbrootlist.end() ; pt++)
        {
            int moduleID = (*pt).DeviceID;

            DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

            for ( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
            {
                int id = *pt1;

                if (config.moduleID() != moduleID)
                {
                    if ( DataRedundancyConfig == "n" )
                    {
                        string cmd = SUDO + "umount /var/lib/columnstore/data" + oam.itoa(id) + " > /dev/null 2>&1";
                        system(cmd.c_str());
                    }
                    else
                    {
                        try
                        {
                            int ret = glusterUnassign(oam.itoa(id));

                            if ( ret != 0 )
                                log.writeLog(__LINE__, "Error unassigning gluster dbroot# " + oam.itoa(id), LOG_TYPE_ERROR);
                            else
                                log.writeLog(__LINE__, "Gluster unassign gluster dbroot# " + oam.itoa(id));
                        }
                        catch (...)
                        {
                            log.writeLog(__LINE__, "Exception unassigning gluster dbroot# " + oam.itoa(id), LOG_TYPE_ERROR);
                        }
                    }
                }
            }
        }
    }
    catch (...)
    {}

    log.writeLog(__LINE__, "unmountExtraDBroots finished ", LOG_TYPE_DEBUG);


    return;
}

/******************************************************************************************
* @brief	checkDataMount
*
* purpose:	Check Data Mounts
*
*
******************************************************************************************/
int ProcessMonitor::checkDataMount()
{
    MonitorLog log;
    ModuleConfig moduleconfig;
    Oam oam;

    //check/update the pmMount files

    string DBRootStorageType = "internal";
    vector <string> dbrootList;

    for ( int retry = 0 ; retry < 10 ; retry++)
    {
        try
        {
            systemStorageInfo_t t;
            t = oam.getStorageConfig();

            if ( boost::get<1>(t) == 0 )
            {
                log.writeLog(__LINE__, "getStorageConfig return: No dbroots are configured in Columnstore.xml file", LOG_TYPE_WARNING);
                return API_INVALID_PARAMETER;
            }

            DeviceDBRootList moduledbrootlist = boost::get<2>(t);

            DeviceDBRootList::iterator pt = moduledbrootlist.begin();

            for ( ; pt != moduledbrootlist.end() ; pt++)
            {
                int moduleID = (*pt).DeviceID;

                DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();

                for ( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
                {
                    if (config.moduleID() == moduleID)
                    {
                        dbrootList.push_back(oam.itoa(*pt1));
                    }
                }
            }

            break;
        }
        catch (exception& ex)
        {
            string error = ex.what();
            log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: " + error, LOG_TYPE_ERROR);
            sleep (1);
        }
        catch (...)
        {
            log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: Caught unknown exception!", LOG_TYPE_ERROR);
            sleep (1);
        }
    }

    try
    {
        oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
    }
    catch (...) {}

    //asign DBRoot is gluster
    if (DataRedundancyConfig == "y")
    {
        vector<string>::iterator p = dbrootList.begin();

        while ( p != dbrootList.end() )
        {
            string dbrootID = *p;
            p++;

            try
            {
                int ret = glusterAssign(dbrootID);

                if ( ret != 0 )
                    log.writeLog(__LINE__, "Error assigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
                else
                    log.writeLog(__LINE__, "Gluster assign gluster dbroot# " + dbrootID);
            }
            catch (...)
            {
                log.writeLog(__LINE__, "Exception assigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
            }
        }
    }

    if ( dbrootList.size() == 0 )
    {
        log.writeLog(__LINE__, "No dbroots are configured in Columnstore.xml file", LOG_TYPE_WARNING);
        return API_INVALID_PARAMETER;
    }

    if ( DBRootStorageType == "hdfs" ||
            (DBRootStorageType == "internal" && DataRedundancyConfig == "n") )
    {
        //create OAM-Test-Flag
        vector<string>::iterator p = dbrootList.begin();

        while ( p != dbrootList.end() )
        {
            string dbroot = "/var/lib/columnstore/data" + *p;
            p++;

            string fileName = dbroot + "/OAMdbrootCheck";
            ofstream fout(fileName.c_str());

            if (!fout)
            {
                log.writeLog(__LINE__, "ERROR: Failed test write to DBRoot: "  + dbroot, LOG_TYPE_ERROR);

                return API_FAILURE;
            }
        }

        return API_SUCCESS;
    }
    else if (DBRootStorageType == "storagemanager")
    {
        /*  StorageManager isn't running yet.  Can't check for writability here. */
        return API_SUCCESS;
    }
    //go unmount disk NOT assigned to this pm
    unmountExtraDBroots();

    //Flush the cache
    cacheutils::flushPrimProcCache();
    cacheutils::dropPrimProcFdCache();
    flushInodeCache();

    //external or gluster
    vector<string>::iterator p = dbrootList.begin();

    while ( p != dbrootList.end() )
    {
        string dbroot = "/var/lib/columnstore/data" + *p;
        string fileName = dbroot + "/OAMdbrootCheck";

        if ( DataRedundancyConfig == "n" )
        {
            //remove any local check flag for starters
            string cmd = SUDO + "umount " + dbroot + " > " + tmpLogDir + "/umount.log 2>&1";
            system(cmd.c_str());

            unlink(fileName.c_str());

            // check if already/still mounted, skip if so
            cmd = "grep " + dbroot + " /proc/mounts > /dev/null 2>&1";

            int status = system(cmd.c_str());

            if (WEXITSTATUS(status) != 0 )
            {
                // not mounted, mount
                string mountLog = tmpLogDir + "/mount.log";
                cmd = "export LC_ALL=C;" + SUDO + "mount " + dbroot + " > " + mountLog + " 2>&1";
                system(cmd.c_str());

                ifstream in(mountLog.c_str());

                in.seekg(0, std::ios::end);
                int size = in.tellg();

                if ( size != 0 )
                {
                    if (!oam.checkLogStatus(mountLog, "already"))
                    {
                        log.writeLog(__LINE__, "checkDataMount: mount failed, DBRoot: " + dbroot, LOG_TYPE_ERROR);

                        try
                        {
                            oam.setDbrootStatus(*p, oam::AUTO_OFFLINE);
                        }
                        catch (exception& ex)
                        {}

                        return API_FAILURE;
                    }
                }
            }

            if ( !rootUser)
            {
                cmd = SUDO + "chown -R " + USER + ":" + USER + " " + dbroot + " > /dev/null 2>&1";
                system(cmd.c_str());
            }

            log.writeLog(__LINE__, "checkDataMount: successfull mount " + dbroot, LOG_TYPE_DEBUG);
        }

        //create OAM-Test-Flag check rw mount
        ofstream fout(fileName.c_str());

        if (!fout)
        {
            log.writeLog(__LINE__, "ERROR: Failed test write to DBRoot: "  + dbroot, LOG_TYPE_ERROR);

            try
            {
                oam.setDbrootStatus(*p, oam::AUTO_OFFLINE);
            }
            catch (exception& ex)
            {}

            return API_FAILURE;
        }

        try
        {
            oam.setDbrootStatus(*p, oam::ACTIVE);
        }
        catch (exception& ex)
        {}

        p++;
    }

    return API_SUCCESS;
}


/******************************************************************************************
* @brief	calTotalUmMemory
*
* purpose:	Calculate TotalUmMemory
*
*
******************************************************************************************/
void ProcessMonitor::calTotalUmMemory()
{
    MonitorLog log;
    Oam oam;

    struct sysinfo myinfo;

    //check/update the pmMount files

    try
    {
        sysinfo(&myinfo);
    }
    catch (...)
    {
        return;
    }

    //get memory stats
    long long total = myinfo.totalram / 1024 / 1000;

    // adjust max memory, 25% of total memory
    string value;

    if ( total <= 2000 )
        value = "256M";
    else if ( total <= 4000 )
        value = "512M";
    else if ( total <= 8000 )
        value = "1G";
    else if ( total <= 16000 )
        value = "2G";
    else if ( total <= 32000 )
        value = "4G";
    else if ( total <= 64000 )
        value = "8G";
    else
        value = "16G";

    try
    {
        Config* sysConfig = Config::makeConfig();
        sysConfig->setConfig("HashJoin", "TotalUmMemory", value);

        //update Calpont Config table
        try
        {
            sysConfig->write();
        }
        catch (...)
        {
            log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
            return;
        }

        log.writeLog(__LINE__, "set TotalUmMemory to " + value, LOG_TYPE_DEBUG);
    }
    catch (...)
    {
        log.writeLog(__LINE__, "Failed to set TotalUmMemory to " + value, LOG_TYPE_ERROR);
    }

    return;

}

/******************************************************************************************
* @brief	flushInodeCache
*
* purpose:	flush cache
*
*
******************************************************************************************/
void ProcessMonitor::flushInodeCache()
{
    int fd;
    ByteStream reply;

#ifdef __linux__
    fd = open("/proc/sys/vm/drop_caches", O_WRONLY);

    if (fd >= 0)
    {
        if (write(fd, "3\n", 2) == 2)
        {
            log.writeLog(__LINE__, "flushInodeCache successful", LOG_TYPE_DEBUG);
        }
        else
        {
            log.writeLog(__LINE__, "flushInodeCache failed", LOG_TYPE_DEBUG);
        }

        close(fd);
    }
    else
    {
        log.writeLog(__LINE__, "flushInodeCache failed to open file", LOG_TYPE_DEBUG);
    }

#endif
}

/******************************************************************************************
* @brief	glusterAssign
*
* purpose:	Gluster Assign DBroot on local module
*
*
******************************************************************************************/
int ProcessMonitor::glusterAssign(std::string dbrootID)
{
    Oam oam;
    Config* sysConfig = Config::makeConfig();
    string command;
    std::string errmsg = "";

    log.writeLog(__LINE__, "glusterAssign called : " + dbrootID, LOG_TYPE_DEBUG);

    string pmid = oam.itoa(config.moduleID());
    string dataDupIPaddr = "ModuleIPAddr" + pmid + "-1-3";
    string moduleIPAddr = sysConfig->getConfig("DataRedundancyConfig", dataDupIPaddr);

    if (moduleIPAddr.empty() || moduleIPAddr == oam::UnassignedIpAddr)
    {
        moduleIPAddr = sysConfig->getConfig("SystemModuleConfig", dataDupIPaddr);
    }

	string tmpLog = tmpLogDir + "/glusterAssign.log";
    command = SUDO + "mount -tglusterfs -odirect-io-mode=enable " + moduleIPAddr + ":/dbroot" + dbrootID + " /var/lib/columnstore/data" + dbrootID + " > " + tmpLog + " 2>&1";

    int ret = system(command.c_str());

    if ( WEXITSTATUS(ret) != 0 )
    {
		//log.writeLog(__LINE__, "glusterAssign mount failure: dbroot: " + dbrootID + " error: " + oam.itoa(WEXITSTATUS(ret)), LOG_TYPE_ERROR);

        ifstream in(tmpLog.c_str());
        in.seekg(0, std::ios::end);
        int size = in.tellg();

        if ( size != 0 )
        {
            if (!oam.checkLogStatus(tmpLog, "already"))
            {
                log.writeLog(__LINE__, "glusterAssign failed.", LOG_TYPE_ERROR);
                string cmd = "mv -f " + tmpLog + " " + tmpLog + "failed";
                system(cmd.c_str());
                return oam::API_FAILURE;
            }
        }
    }

    return oam::API_SUCCESS;

}

/******************************************************************************************
* @brief	glusterAssign
*
* purpose:	Gluster Assign DBroot on local module
*
*
******************************************************************************************/
int ProcessMonitor::glusterUnassign(std::string dbrootID)
{
    Oam oam;
    string command;
    std::string errmsg = "";

    log.writeLog(__LINE__, "glusterUnassign called: " + dbrootID, LOG_TYPE_DEBUG);

	string tmpLog = tmpLogDir + "/glusterUnassign.log";

    command = SUDO + "umount -f /var/lib/columnstore/data" + dbrootID + " > " + tmpLog + " 2>&1";

    int ret = system(command.c_str());

    if ( WEXITSTATUS(ret) != 0 )
    {
		//log.writeLog(__LINE__, "glusterUnassign mount failure: dbroot: " + dbrootID + " error: " + oam.itoa(WEXITSTATUS(ret)), LOG_TYPE_ERROR);

        ifstream in(tmpLog.c_str());
        in.seekg(0, std::ios::end);
        int size = in.tellg();

        if ( size != 0 )
        {
            if (!oam.checkLogStatus(tmpLog, "not mounted"))
            {
                log.writeLog(__LINE__, "glusterUnassign failed.", LOG_TYPE_ERROR);

                string cmd = "mv -f " + tmpLog + " " + tmpLog + "failed";
                system(cmd.c_str());
                return oam::API_FAILURE;
            }
        }
    }

    return oam::API_SUCCESS;
}


int ProcessMonitor::syncFS()
{
    Oam oam;

    string DBRMroot;
    oam.getSystemConfig("DBRMRoot", DBRMroot);

    string currentFileName = DBRMroot + "_current";
    IDBFileSystem &fs = IDBPolicy::getFs(currentFileName.c_str());
    bool success = fs.filesystemSync();
    if (!success)
        return oam::API_FAILURE;
    return oam::API_SUCCESS;
}

} //end of namespace
// vim:ts=4 sw=4:

