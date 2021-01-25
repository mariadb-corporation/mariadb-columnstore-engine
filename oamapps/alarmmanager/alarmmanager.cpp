/* Copyright (C) 2016 MariaDB Corporation

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
* Author: Zhixuan Zhu
******************************************************************************************/

#include <my_global.h>
#define ALARMMANAGER_DLLEXPORT
#include "alarmmanager.h"
#undef ALARMMANAGER_DLLEXPORT

#include <unistd.h>
#include <cstdio>
#include <algorithm>
#include <vector>
#include <iterator>

#include "alarmglobal.h"
#include "liboamcpp.h"
#include "installdir.h"
#include "messagequeue.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace logging;

const unsigned int CTN_INTERVAL = 30 * 60;


namespace alarmmanager
{

#ifdef __linux__
inline pid_t gettid(void)
{
    return syscall(__NR_gettid);
}
#else
inline pid_t gettid(void)
{
    return getpid();
}
#endif

/*****************************************************************************************
* @brief	Constructor
*
* purpose:
*
*****************************************************************************************/

ALARMManager::ALARMManager()
{
    Oam oam;

    // Get Parent OAM Module Name
    try
    {
        oam.getSystemConfig("ParentOAMModuleName", ALARMManager::parentOAMModuleName);
    }
    catch (...)
    {
        //Log event
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("Failed to read Parent OAM Module Name");
        msg.format(args);
        ml.logErrorMessage(msg);
        throw runtime_error ("Failed to read Parent OAM Module Name");
    }

}

/*****************************************************************************************
* @brief	Destructor
*
* purpose:
*
*****************************************************************************************/

ALARMManager::~ALARMManager()
{
}

/*****************************************************************************************
* @brief	rewriteActiveLog
*
* purpose:	Update Active Alarm file, called to remove Cleared alarm
*
*****************************************************************************************/
void rewriteActiveLog (const AlarmList& alarmList)
{
    if (ALARM_DEBUG)
    {
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("rewriteAlarmLog Called");
        msg.format(args);
        ml.logDebugMessage(msg);
    }

    // delete the old file
    unlink (ACTIVE_ALARM_FILE.c_str());

    // create new file
    int fd = open(ACTIVE_ALARM_FILE.c_str(), O_RDWR | O_CREAT, 0664);

    // Aquire an exclusive lock
    if (flock(fd, LOCK_EX) == -1)
    {
        throw runtime_error ("Lock active alarm log file error");
    }

    ofstream activeAlarmFile (ACTIVE_ALARM_FILE.c_str());

    AlarmList::const_iterator i;

    for (i = alarmList.begin(); i != alarmList.end(); ++i)
    {
        activeAlarmFile << i->second;
    }

    activeAlarmFile.close();

    // Release lock
    if (flock(fd, LOCK_UN) == -1)
    {
        throw runtime_error ("Release lock active alarm log file error");
    }

    close(fd);
}

/*****************************************************************************************
* @brief	logAlarm
*
* purpose:	Log Alarm in Active Alarm or Historical Alarm file
*
*****************************************************************************************/
void logAlarm (const Alarm& calAlarm, const string& fileName)
{
    if (ALARM_DEBUG)
    {
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("logAlarm Called");
        msg.format(args);
        ml.logDebugMessage(msg);
    }

    int fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0664);
    ofstream AlarmFile (fileName.c_str(), ios::app);

    // Aquire an exclusive lock
    if (flock(fd, LOCK_EX) == -1)
    {
        throw runtime_error ("Lock file error: " + fileName);
    }

    AlarmFile << calAlarm;
    AlarmFile.close();

    // Release lock
    if (flock(fd, LOCK_UN) == -1)
    {
        throw runtime_error ("Release lock file error: " + fileName);
    }

    close(fd);
}

/*****************************************************************************************
* @brief	processAlarm
*
* purpose:	Process Alarm by updating Active Alarm  and Historical Alarm files
*
*****************************************************************************************/
void processAlarm(const Alarm& calAlarm)
{
    bool logActiveFlag = (calAlarm.getState() == CLEAR ? false : true);
    bool logHistFlag = true;

    if (calAlarm.getState() == CLEAR )
        logHistFlag = false;

    // get active alarms
    AlarmList alarmList;
    ALARMManager sm;
    sm.getActiveAlarm (alarmList);

    if (ALARM_DEBUG)
    {
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("processAlarm Called");
        msg.format(args);
        ml.logDebugMessage(msg);
    }

    AlarmList::iterator i;

    for (i = alarmList.begin(); i != alarmList.end(); ++i)
    {
        // check if matching ID
        if (calAlarm.getAlarmID() != (i->second).getAlarmID() )
        {
            continue;
        }

        // check if the same fault component on same server
        if (calAlarm.getComponentID().compare((i->second).getComponentID()) == 0 &&
                calAlarm.getSname().compare((i->second).getSname()) == 0)
        {
            // for set alarm, don't log
            if (calAlarm.getState() == SET )
            {
                logActiveFlag = false;
                logHistFlag = false;
                break;
            }

            // for clear alarm, remove the set by rewritting the file
            else if (calAlarm.getState() == CLEAR )
            {
                logActiveFlag = false;
                logHistFlag = true;
                //cout << "size before: " << alarmList.size();
                alarmList.erase (i);

                //cout << " size after: " << alarmList.size() << endl;
                try
                {
                    rewriteActiveLog (alarmList);
                }
                catch (runtime_error& e)
                {
                    LoggingID lid(11);
                    MessageLog ml(lid);
                    Message msg;
                    Message::Args args;
                    args.add("rewriteActiveLog error:");
                    args.add(e.what());
                    msg.format(args);
                    ml.logErrorMessage(msg);
                }

                break;
            }
        }
    } // end of for loop

    if (logActiveFlag)
    {
        try
        {
            logAlarm (calAlarm, ACTIVE_ALARM_FILE);
        }
        catch (runtime_error& e)
        {
            LoggingID lid(11);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("logAlarm error:");
            args.add(e.what());
            msg.format(args);
            ml.logErrorMessage(msg);
        }
    }

    if (logHistFlag)
    {
        // log historical alarm
        try
        {
            logAlarm (calAlarm, ALARM_FILE);
        }
        catch (runtime_error& e)
        {
            LoggingID lid(11);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("logAlarm error:");
            args.add(e.what());
            msg.format(args);
            ml.logErrorMessage(msg);
        }
    }
}

/*****************************************************************************************
* @brief	configAlarm
*
* purpose:	Get Config Data for Incoming alarm
*
*****************************************************************************************/
void configAlarm (Alarm& calAlarm)
{
    int alarmID = calAlarm.getAlarmID();
    Oam oam;
    AlarmConfig alarmConfig;

    if (ALARM_DEBUG)
    {
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("configAlarm Called");
        msg.format(args);
        ml.logDebugMessage(msg);
    }

    try
    {
        oam.getAlarmConfig (alarmID, alarmConfig);

        calAlarm.setDesc (alarmConfig.BriefDesc);
        calAlarm.setSeverity (alarmConfig.Severity);
        calAlarm.setCtnThreshold (alarmConfig.Threshold);
        calAlarm.setOccurrence (alarmConfig.Occurrences);
        calAlarm.setLastIssueTime (alarmConfig.LastIssueTime);

        // check lastIssueTime to see if it's time to clear the counter
        time_t now;
        time (&now);

        if ((now - calAlarm.getLastIssueTime()) >= CTN_INTERVAL)
        {
            // reset counter and set lastIssueTime
            oam.setAlarmConfig (alarmID, "LastIssueTime", now);
            oam.setAlarmConfig (alarmID, "Occurrences", 1);
        }

        else
        {
            // increment counter and check the ctnThreshold
            calAlarm.setOccurrence (alarmConfig.Occurrences + 1);
            oam.setAlarmConfig (alarmID, "Occurrences", calAlarm.getOccurrence());

            // if counter over threshold and set alarm, stop processing.
            if (calAlarm.getCtnThreshold() > 0
                    && calAlarm.getOccurrence() >= calAlarm.getCtnThreshold()
                    && calAlarm.getState() == SET)
            {
                if (ALARM_DEBUG)
                {
                    LoggingID lid(11);
                    MessageLog ml(lid);
                    Message msg;
                    Message::Args args;
                    args.add("counter over threshold and set alarm, stop processing.");
                    args.add("threshold:");
                    args.add(calAlarm.getCtnThreshold());
                    args.add("occurances:");
                    args.add(calAlarm.getOccurrence());
                    msg.format(args);
                    ml.logDebugMessage(msg);
                }

                return;
            }
        }
    }
    catch (runtime_error& e)
    {
        if (ALARM_DEBUG)
        {
            LoggingID lid(11);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("runtime error:");
            args.add(e.what());
            msg.format(args);
            ml.logDebugMessage(msg);
        }

        throw;
    }

    // process alarm
    processAlarm (calAlarm);
}

/*****************************************************************************************
* @brief	sendAlarmReport API
*
* purpose:	Send Alarm Report
*
*****************************************************************************************/
void ALARMManager::sendAlarmReport (const char* componentID, int alarmID, int state,
                                    std::string repModuleName, std::string repProcessName)
{

#ifdef SKIP_ALARM
    return;
#else
    LoggingID lid(11);
    MessageLog ml(lid);
    Message msg;
    Message::Args args;

    //Log receiving of Alarm report
    if (ALARM_DEBUG)
    {
        args.add("sendAlarmReport: alarm #");
        args.add(alarmID);
        args.add(", state: ");
        args.add(state);
        args.add(", component: ");
        args.add(componentID);
        msg.format(args);
        ml.logDebugMessage(msg);
    }

    Oam oam;

    // get current Module name
    string ModuleName;

    if ( repModuleName.empty())
    {
        oamModuleInfo_t st;

        try
        {
            st = oam.getModuleInfo();
            ModuleName = boost::get<0>(st);
        }
        catch (...)
        {
            ModuleName = "Unknown Reporting Module";
        }
    }
    else
        ModuleName = repModuleName;

    // get pid, tid info
    int pid = getpid();
    int tid = gettid();

	// get reporting Process Name
    string processName;

    if ( repProcessName.empty())
    {
        // get current process name
        myProcessStatus_t t;

        try
        {
            t = oam.getMyProcessStatus();
            processName = boost::get<1>(t);
        }
        catch (...)
        {
            processName = "Unknown-Reporting-Process";
        }
    }
    else
        processName = repProcessName;

ByteStream msg1;

    // setup message
    msg1 << (ByteStream::byte) alarmID;
    msg1 << (std::string) componentID;
    msg1 << (ByteStream::byte) state;
    msg1 << (std::string) ModuleName;
    msg1 << (std::string) processName;
    msg1 << (ByteStream::byte) pid;
    msg1 << (ByteStream::byte) tid;

    try
    {
        //send the msg to Process Manager
        MessageQueueClient procmgr("ProcMgr_Alarm");
        procmgr.write(msg1);

        // shutdown connection
        procmgr.shutdown();
    }
    catch (...)
    {}

    return;
#endif //SKIP_ALARM
}

/*****************************************************************************************
* @brief	processAlarmReport API
*
* purpose:	Process Alarm Report
*
*****************************************************************************************/
void ALARMManager::processAlarmReport (Alarm& calAlarm)
{
    // Get alarm configuration
    try
    {
        configAlarm (calAlarm);
    }
    catch (runtime_error& e)
    {
        LoggingID lid(11);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("configAlarm error:");
        args.add(e.what());
        msg.format(args);
        ml.logErrorMessage(msg);
    }

    return;

}

/*****************************************************************************************
* @brief	getActiveAlarm API
*
* purpose:	Get List of Active Alarm from activealarm file
*
*****************************************************************************************/
void ALARMManager::getActiveAlarm(AlarmList& alarmList) const
{
    //add-on to fileName with mount name if on non Parent Module
    Oam oam;
    string fileName = ACTIVE_ALARM_FILE;

    int fd = open(fileName.c_str(), O_RDONLY);

    if (fd == -1)
    {
        // file may being deleted temporarily by trapHandler
        sleep (1);
        fd = open(fileName.c_str(), O_RDONLY);

        if (fd == -1)
        {
            // no active alarms, return
            return;
        }
    }

    ifstream activeAlarm (fileName.c_str(), ios::in);

    // acquire read lock
    if (flock(fd, LOCK_SH) == -1)
    {
        throw runtime_error ("Lock active alarm log file error");
    }

    Alarm alarm;

    while (!activeAlarm.eof())
    {
        activeAlarm >> alarm;

        if (alarm.getAlarmID() != INVALID_ALARM_ID)
            //don't sort
//			alarmList.insert (AlarmList::value_type(alarm.getAlarmID(), alarm));
            alarmList.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
    }

    activeAlarm.close();

    // release lock
    if (flock(fd, LOCK_UN) == -1)
    {
        throw runtime_error ("Release lock active alarm log file error");
    }

    close(fd);

    if (ALARM_DEBUG)
    {
        AlarmList :: iterator i;

        for (i = alarmList.begin(); i != alarmList.end(); ++i)
        {
            cout << i->second << endl;
        }
    }

    return;
}

/*****************************************************************************************
* @brief	getAlarm API
*
* purpose:	Get List of Historical Alarms from alarm file
*
*			date = MM/DD/YY format
*
*****************************************************************************************/
void ALARMManager::getAlarm(std::string date, AlarmList& alarmList) const
{

    string alarmFile = startup::StartUp::tmpDir() + "/alarms";

    //make 1 alarm log file made up of archive and current alarm.log
    string cmd = "touch " + alarmFile;
    (void)system(cmd.c_str());

    cmd = "ls " + ALARM_ARCHIVE_FILE + " | grep 'alarm.log' > " + alarmFile;
    (void)system(cmd.c_str());

    string fileName = startup::StartUp::tmpDir() + "/alarmlogfiles";

    ifstream oldFile (fileName.c_str());

    if (oldFile)
    {
        char line[200];
        string buf;

        while (oldFile.getline(line, 200))
        {
            buf = line;
            string cmd = "cat " + ALARM_ARCHIVE_FILE + "/" + buf + " >> " + alarmFile;
            (void)system(cmd.c_str());
        }

        oldFile.close();
        unlink (fileName.c_str());
    }

    cmd = "cat " + ALARM_FILE + " >> " + alarmFile;
    (void)system(cmd.c_str());

    int fd = open(alarmFile.c_str(), O_RDONLY);

    if (fd == -1)
        // doesn't exist yet, return
        return;

    ifstream hisAlarm (alarmFile.c_str(), ios::in);

    // acquire read lock
    if (flock(fd, LOCK_SH) == -1)
    {
        throw runtime_error ("Lock alarm log file error");
    }

    //get mm / dd / yy from incoming date
    string mm = date.substr(0, 2);
    string dd = date.substr(3, 2);
    string yy = date.substr(6, 2);

    Alarm alarm;

    while (!hisAlarm.eof())
    {
        hisAlarm >> alarm;

        if (alarm.getAlarmID() != INVALID_ALARM_ID)
        {
            time_t cal = alarm.getTimestampSeconds();
            struct tm tm;
            localtime_r(&cal, &tm);
            char tmp[3];
            strftime (tmp, 3, "%m", &tm);
            string alarm_mm = tmp;
            alarm_mm = alarm_mm.substr(0, 2);
            strftime (tmp, 3, "%d", &tm);
            string alarm_dd = tmp;
            alarm_dd = alarm_dd.substr(0, 2);
            strftime (tmp, 3, "%y", &tm);
            string alarm_yy = tmp;
            alarm_yy = alarm_yy.substr(0, 2);

            if ( mm == alarm_mm && dd == alarm_dd && yy == alarm_yy )
                //don't sort
                //			alarmList.insert (AlarmList::value_type(alarm.getAlarmID(), alarm));
                alarmList.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
        }
    }

    hisAlarm.close();
    unlink (alarmFile.c_str());

    // release lock
    if (flock(fd, LOCK_UN) == -1)
    {
        throw runtime_error ("Release lock active alarm log file error");
    }

    if (ALARM_DEBUG)
    {
        AlarmList :: iterator i;

        for (i = alarmList.begin(); i != alarmList.end(); ++i)
        {
            cout << i->second << endl;
        }
    }
}

} //namespace alarmmanager
// vim:ts=4 sw=4:

