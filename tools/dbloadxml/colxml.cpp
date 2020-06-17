/* Copyright (C) 2014 InfiniDB, Inc.

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

/*******************************************************************************
 * $Id: colxml.cpp 2237 2013-05-05 23:42:37Z dcathey $
 *
 ******************************************************************************/


#include <clocale>

#include <libxml/xmlwriter.h>

#include <boost/filesystem.hpp>

#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>
#include "calpontsystemcatalog.h"
#include "inputmgr.h"
#include "we_xmlgenproc.h"
#include "we_config.h"
#include "dbrm.h"

using namespace std;
using namespace bulkloadxml;

int main(int argc, char** argv)
{
    const int DEBUG_LVL_TO_DUMP_SYSCAT_RPT = 4;
    // set effective ID to root
    if( setuid( 0 ) < 0 )
    {
        std::cerr << " colxml: couldn't set uid " << std::endl;
    }
    setlocale(LC_ALL, "");
    WriteEngine::Config::initConfigCache(); // load Columnstore.xml config settings

    //Bug 6137
    std::string aBulkRoot = WriteEngine::Config::getBulkRoot();

    if (!aBulkRoot.empty())
    {
        if (!boost::filesystem::exists(aBulkRoot.c_str()))
        {
            cout << "Creating directory : " << aBulkRoot << endl;
            boost::filesystem::create_directories(aBulkRoot.c_str());
        }

        if (boost::filesystem::exists(aBulkRoot.c_str()))
        {
            std::ostringstream aSS;
            aSS << aBulkRoot;
            aSS << "/job";
            std::string jobDir = aSS.str();

            if (!boost::filesystem::exists(jobDir.c_str()))
            {
                cout << "Creating directory : " << jobDir << endl;
                bool aSuccess = boost::filesystem::create_directories(jobDir.c_str());

                if (!aSuccess)
                {
                    cout << "\nFailed to create job directory, please check permissions\n" << endl;
                    return -1;
                }
            }

            std::ostringstream aSS2;
            aSS2 << aBulkRoot;
            aSS2 << "/log";
            std::string logDir = aSS2.str();

            if (!boost::filesystem::exists(logDir.c_str()))
            {
                cout << "Creating directory : " << logDir << endl;
                bool aSuccess = boost::filesystem::create_directories(logDir.c_str());

                if (!aSuccess)
                {
                    cout << "\nFailed to create directory, please check permissions\n" << endl;
                    return -1;
                }
            }
        }
        else
        {
            cout << "\nFailed to create bulk directory, check for permissions\n" << endl;
            return -1;
        }
    }
    else
    {
        cout << "\nBulkRoot is empty in config file. Failed to create job file.\n\n";
        return -1;
    }

    InputMgr mgr("299"); //@bug 391

    if (! mgr.input(argc, argv))
        return 1;

    int debugLevel = atoi(mgr.getParm(
                              WriteEngine::XMLGenData::RPT_DEBUG).c_str());

    bool bUseLogFile = true;
    bool bSysCatRpt  = false;

    if (debugLevel == DEBUG_LVL_TO_DUMP_SYSCAT_RPT)
    {
        cout << "\nRunning colxml to dump system catalog report:\n\n";
        bUseLogFile = false;
        bSysCatRpt  = true;
    }
    else
    {
        cout << "\nRunning colxml with the following parameters:\n";
    }

    WriteEngine::XMLGenProc curJob(&mgr, bUseLogFile, bSysCatRpt);

    if ( debugLevel > 0 && debugLevel <= 3 )
    {
        curJob.setDebugLevel( debugLevel );
        cout << "\nDebug level is set to " << debugLevel << endl;
    }

    BRM::DBRM dbrm;

    if (dbrm.getSystemReady() < 1)
    {
        std::string errMsg(
            "System is not ready.  Verify that ColumnStore is up and ready "
            "before running colxml.");

        if (bUseLogFile)
            curJob.logErrorMessage(errMsg);
        else
            cout << errMsg << endl;

        return 1;
    }

    bool rc = false;

    const WriteEngine::XMLGenData::TableList& tables = mgr.getTables();

    try
    {
        if (tables.empty())
            mgr.loadCatalogTables();

        if (tables.empty())
        {
            string msg = "Either schema name is invalid or no table "
                         "is in the schema.";
            curJob.logErrorMessage(msg);
        }
        else
        {
            curJob.startXMLFile( );

            for (InputMgr::TableList::const_iterator tbl = tables.begin();
                    tbl != tables.end() ; ++tbl)
            {
                curJob.makeTableData( *tbl );
                rc = curJob.makeColumnData(*tbl);

                if (!rc)
                    cout << "No columns for " << tbl->table << endl;
            }
        }
    }
    catch (runtime_error& ex)
    {
        curJob.logErrorMessage(string( "colxml runtime exception: ") +
                               ex.what() );
        cout << curJob.errorString() << endl;
        return 1;
    }
    catch (exception& ex)
    {
        curJob.logErrorMessage(string( "colxml exception: ") + ex.what() );
        cout << curJob.errorString() << endl;
        return 1;
    }
    catch (...)
    {
        curJob.logErrorMessage(string("colxml unknown exception "));
        cout << curJob.errorString() << endl;
        return 1;
    }

    if (rc)
    {
        if (debugLevel == DEBUG_LVL_TO_DUMP_SYSCAT_RPT)
        {
            std::string xmlFileName("-");
            curJob.writeXMLFile( xmlFileName );
            cout << "\nDump completed for tables:\n\t";
        }
        else
        {
            std::string xmlFileName = curJob.genJobXMLFileName( );
            cout << "Creating job description file: " << xmlFileName << endl;
            curJob.writeXMLFile( xmlFileName );
            cout << "File completed for tables:\n\t";
        }

        copy(tables.begin(), tables.end(),
             ostream_iterator<execplan::CalpontSystemCatalog::TableName>
             (cout, "\n\t"));
        cout << "\nNormal exit.\n";
    }
    else
    {
        cout << "File not made.\n";
        cout << curJob.errorString();
        return 1;
    }

    return 0;
}
