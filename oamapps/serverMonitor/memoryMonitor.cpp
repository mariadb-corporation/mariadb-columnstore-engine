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
 * $Id: memoryMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "cgroupconfigurator.h"
#include "serverMonitor.h"

using namespace std;
using namespace oam;
using namespace alarmmanager;
using namespace logging;
using namespace servermonitor;
//using namespace procheartbeat;

ProcessMemoryList pml;
int swapFlag = 0;

uint64_t totalMem;

pthread_mutex_t MEMORY_LOCK;

extern string tmpDir;

/*****************************************************************************************
* @brief	memoryMonitor Thread
*
* purpose:	Get current Memory and Swap usage, report alarms
*
*****************************************************************************************/
void memoryMonitor()
{
    ServerMonitor serverMonitor;

    int swapUsagePercent = 0;

    // set defaults
    int memoryCritical = 90,
        memoryMajor = 0,
        memoryMinor = 0,
        swapCritical = 90,
        swapMajor = 80,
        swapMinor = 70;

    int day = 0;

    //set monitoring period to 60 seconds
    int monitorPeriod = MONITOR_PERIOD;
    utils::CGroupConfigurator cg;

    while (true)
    {
        // Get MEMORY usage water mark from server configuration and compare
        ModuleTypeConfig moduleTypeConfig;
        Oam oam;

        try
        {
            oam.getSystemConfig (moduleTypeConfig);
            memoryCritical = moduleTypeConfig.ModuleMemCriticalThreshold;
            memoryMajor = moduleTypeConfig.ModuleMemMajorThreshold;
            memoryMinor = moduleTypeConfig.ModuleMemMinorThreshold;
            swapCritical = moduleTypeConfig.ModuleSwapCriticalThreshold;
            swapMajor = moduleTypeConfig.ModuleSwapMajorThreshold;
            swapMinor = moduleTypeConfig.ModuleSwapMinorThreshold;
        }
        catch (...)
        {
            sleep(5);
            continue;
        }

        //get memory stats
        totalMem = cg.getTotalMemory();
        uint64_t freeMem = cg.getFreeMemory();
        uint64_t usedMem = totalMem - freeMem;

        //get swap stats
        uint64_t totalSwap = cg.getTotalSwapSpace();
        uint64_t usedSwap = cg.getSwapInUse();

        if ( totalSwap == 0 )
        {
            swapUsagePercent = 0;
            swapFlag = 2;

            //get current day, log warning only once a day
            time_t now;
            now = time(NULL);
            struct tm tm;
            localtime_r(&now, &tm);

            if ( day != tm.tm_mday)
            {
                day = tm.tm_mday;

                //Log this event
                LoggingID lid(SERVER_MONITOR_LOG_ID);
                MessageLog ml(lid);
                Message msg;
                Message::Args args;
                args.add("Total Swap space is set to 0");
                msg.format(args);
                ml.logWarningMessage(msg);
            }
        }
        else
            swapUsagePercent =  usedSwap / (totalSwap / 100);

        int memoryUsagePercent;

        if ( totalMem == 0 )
        {
            memoryUsagePercent = 0;

            //Log this event
            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Total Memory space is set to 0");
            msg.format(args);
            ml.logWarningMessage(msg);
        }
        else
            memoryUsagePercent =  (usedMem / (totalMem / 100)) + 1;

        /*LoggingID lid(SERVER_MONITOR_LOG_ID);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("memoryUsagePercent ");
        args.add((uint64_t) memoryUsagePercent);
        args.add("usedMem ");
        args.add((uint64_t) usedMem);
        args.add("totalMem ");
        args.add((uint64_t) totalMem);
        msg.format(args);
        ml.logInfoMessage(msg);
        */
        //first time called, log
        //adjust if over 100%
        if ( swapUsagePercent < 0 )
            swapUsagePercent = 0;

        if ( swapUsagePercent > 100 )
            swapUsagePercent = 100;

        if ( memoryUsagePercent < 0 )
            memoryUsagePercent = 0;

        if ( memoryUsagePercent > 100 )
            memoryUsagePercent = 100;

        // check for Memory alarms
        if (memoryUsagePercent >= memoryCritical && memoryCritical > 0 )
        {
            if ( monitorPeriod == MONITOR_PERIOD )
            {

                LoggingID lid(SERVER_MONITOR_LOG_ID);
                MessageLog ml(lid);
                Message msg;
                Message::Args args;
                args.add("Local Memory above Critical Memory threshold with a percentage of ");
                args.add((int) memoryUsagePercent);
                args.add(" ; Swap ");
                args.add((int) swapUsagePercent);
                msg.format(args);
                ml.logInfoMessage(msg);
                serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_HIGH, SET, memoryUsagePercent);

                pthread_mutex_lock(&MEMORY_LOCK);
                serverMonitor.outputProcMemory(true);
                pthread_mutex_unlock(&MEMORY_LOCK);
            }

            // change to 1 second for quick swap space monitoring
            monitorPeriod = 1;
        }
        else if (memoryUsagePercent >= memoryMajor && memoryMajor > 0 )
        {
            monitorPeriod = MONITOR_PERIOD;
            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Local Memory above Major Memory threshold with a percentage of ");
            args.add((int) memoryUsagePercent);
            msg.format(args);
            ml.logInfoMessage(msg);
            serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_MED, SET, memoryUsagePercent);
        }
        else if (memoryUsagePercent >= memoryMinor && memoryMinor > 0 )
        {
            monitorPeriod = MONITOR_PERIOD;
            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Local Memory above Minor Memory threshold with a percentage of ");
            args.add((int) memoryUsagePercent);
            msg.format(args);
            ml.logInfoMessage(msg);
            serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_LOW, SET, memoryUsagePercent);
        }
        else
        {
            monitorPeriod = MONITOR_PERIOD;
            serverMonitor.checkMemoryAlarm("Local-Memory");
        }

        // check for Swap alarms
        if (swapUsagePercent >= swapCritical && swapCritical > 0 )
        {
            //adjust if over 100%
            if ( swapUsagePercent > 100 )
                swapUsagePercent = 100;

            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Swap above Critical Memory threshold with a percentage of ");
            args.add((int) swapUsagePercent);
            msg.format(args);
            ml.logInfoMessage(msg);
            serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_HIGH, SET, swapUsagePercent);
            serverMonitor.checkSwapAction();
        }
        else if (swapUsagePercent >= swapMajor && swapMajor > 0 )
        {
            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Swap above Major Memory threshold with a percentage of ");
            args.add((int) swapUsagePercent);
            msg.format(args);
            ml.logInfoMessage(msg);
            serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_MED, SET, swapUsagePercent);
            serverMonitor.checkSwapAction();
        }
        else if (swapUsagePercent >= swapMinor && swapMinor > 0 )
        {
            swapFlag = 2;
            LoggingID lid(SERVER_MONITOR_LOG_ID);
            MessageLog ml(lid);
            Message msg;
            Message::Args args;
            args.add("Swap above Minor Memory threshold with a percentage of ");
            args.add((int) swapUsagePercent);
            msg.format(args);
            ml.logInfoMessage(msg);
            serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_LOW, SET, swapUsagePercent);
        }
        else
        {
            swapFlag = 2;
            serverMonitor.checkSwapAlarm("Swap");
        }

        // sleep, 1 minute
        sleep(monitorPeriod);

    } // end of while loop
}

/******************************************************************************************
* @brief	checkMemoryAlarm
*
* purpose:	check to see if an alarm(s) is set on MEMORY and clear if so
*
******************************************************************************************/
void ServerMonitor::checkMemoryAlarm(string alarmItem, ALARMS alarmID)
{
    Oam oam;
    ServerMonitor serverMonitor;

    // get current server name
    string serverName;
    oamModuleInfo_t st;

    try
    {
        st = oam.getModuleInfo();
        serverName = boost::get<0>(st);
    }
    catch (...)
    {
        serverName = "Unknown Server";
    }

    switch (alarmID)
    {
        case ALARM_NONE: 	// clear all alarms set if any found
            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_HIGH);

            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_MED, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_MED);

            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_LOW, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_LOW);

            break;

        case MEMORY_USAGE_LOW: 	// clear high and medium alarms set if any found
            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_HIGH);

            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_MED, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_MED);

            break;

        case MEMORY_USAGE_MED: 	// clear high alarms set if any found
            if ( serverMonitor.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, MEMORY_USAGE_HIGH);

            break;

        default:			// none to clear
            break;
    } // end of switch

    return;
}

/******************************************************************************************
* @brief	checkSwapAlarm
*
* purpose:	check to see if an alarm(s) is set on SWAP and clear if so
*
******************************************************************************************/
void ServerMonitor::checkSwapAlarm(string alarmItem, ALARMS alarmID)
{
    Oam oam;
    ServerMonitor serverMonitor;

    // get current server name
    string serverName;
    oamModuleInfo_t st;

    try
    {
        st = oam.getModuleInfo();
        serverName = boost::get<0>(st);
    }
    catch (...)
    {
        serverName = "Unknown Server";
    }

    switch (alarmID)
    {
        case ALARM_NONE: 	// clear all alarms set if any found
            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_HIGH);

            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_MED, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_MED);

            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_LOW, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_LOW);

            break;

        case SWAP_USAGE_LOW: 	// clear high and medium alarms set if any found
            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_HIGH);

            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_MED, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_MED);

            break;

        case SWAP_USAGE_MED: 	// clear high alarms set if any found
            if ( serverMonitor.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
                //  alarm set, clear it
                clearAlarm(alarmItem, SWAP_USAGE_HIGH);

            break;

        default:			// none to clear
            break;
    } // end of switch

    return;
}

/******************************************************************************************
* @brief	checkSwapAction
*
* purpose:	check if any system action needs tyo be taken
*
******************************************************************************************/
void ServerMonitor::checkSwapAction()
{
    Oam oam;

    if ( swapFlag == 0 )
    {
        LoggingID lid(SERVER_MONITOR_LOG_ID);
        MessageLog ml(lid);
        Message msg;
        Message::Args args;
        args.add("Swap Space usage over Major threashold, startSystem failure");
        msg.format(args);
        ml.logCriticalMessage(msg);

        swapFlag = 1;
        sleep(5);
        return;
    }

    string swapAction = "restartSystem";

    try
    {
        oam.getSystemConfig ("SwapAction", swapAction);
    }
    catch (...)
    {}

    if (swapAction == "none")
        return;

    LoggingID lid(SERVER_MONITOR_LOG_ID);
    MessageLog ml(lid);
    Message msg;
    Message::Args args;
    args.add("Swap Space usage over Major threashold, perform OAM command ");
    args.add( swapAction);
    msg.format(args);
    ml.logCriticalMessage(msg);

    GRACEFUL_FLAG gracefulTemp = GRACEFUL;
    ACK_FLAG ackTemp = ACK_YES;

    if ( swapAction == "stopSystem")
    {
        try
        {
            oam.stopSystem(gracefulTemp, ackTemp);
        }
        catch (exception& e)
        {
        }
    }
    else if ( swapAction == "restartSystem")
    {
        try
        {
            oam.restartSystem(gracefulTemp, ackTemp);
        }
        catch (exception& e)
        {
        }
    }
}

/******************************************************************************************
* @brief	outputProcMemory
*
* purpose:	output Top memory users
*
******************************************************************************************/
void ServerMonitor::outputProcMemory(bool log)
{
    //
    // get top 5 Memory users by process
    //

    string tmpprocessMem = tmpDir + "/processMem";
    
    string cmd = "ps -e -orss=1,args= | sort -b -k1,1n |tail -n 5 | awk '{print $1,$2}' > " + tmpprocessMem;
    system(cmd.c_str());

    ifstream oldFile (tmpprocessMem.c_str());

    string process;
    long long memory;
    int memoryUsage;
    pml.clear();

    char line[400];

    while (oldFile.getline(line, 400))
    {
        string buf = line;
        string::size_type pos = buf.find (' ', 0);

        if (pos != string::npos)
        {
            memory = atol(buf.substr(0, pos - 1).c_str());
            memoryUsage = (memory * 1024 * 1000 / totalMem) + 1 ;
            process = buf.substr(pos + 1, 80);

            //cleanup process name
            pos = process.rfind ('/');

            if (pos != string::npos)
                process = process.substr(pos + 1, 80);
            else
            {
                pos = process.find ('[', 0);

                if (pos != string::npos)
                    process = process.substr(pos + 1, 80);

                pos = process.find (']', 0);

                if (pos != string::npos)
                    process = process.substr(0, pos);
            }

            processMemory pm;
            pm.processName = process;
            pm.usedBlocks = memory;
            pm.usedPercent = memoryUsage;
            pml.push_back(pm);

            if (log)
            {
                LoggingID lid(SERVER_MONITOR_LOG_ID);
                MessageLog ml(lid);
                Message msg;
                Message::Args args;
                args.add("Memory Usage for Process: ");
                args.add(process);
                args.add(" : Memory Used ");
                args.add((int) memory);
                args.add(" : % Used ");
                args.add(memoryUsage);
                msg.format(args);
                ml.logInfoMessage(msg);
            }
        }
    }

    oldFile.close();
}
