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
 * $Id: dbbuilder.cpp 2101 2013-01-21 14:12:52Z rdempsey $
 *
 *******************************************************************************/

#include <stdio.h>
#include <unistd.h>
#include <cerrno>
#include <stdexcept>
using namespace std;

#include <boost/algorithm/string.hpp>

#include "mcsconfig.h"
#include "dbbuilder.h"
#include "systemcatalog.h"
#include "liboamcpp.h"
#include "configcpp.h"
#include "IDBPolicy.h"

using namespace oam;
using namespace dmlpackageprocessor;
using namespace dmlpackage;
using namespace idbdatafile;

#include "objectidmanager.h"
using namespace execplan;

#include "installdir.h"

string tmpDir;
string logFile;

enum BUILD_OPTION
{
  SYSCATALOG_ONLY = 7,  // Create systables only
};

namespace
{
int setUp()
{
#ifndef _MSC_VER
  string cmd = "/bin/rm -f " + logFile + " >/dev/null 2>&1";
  int rc = system(cmd.c_str());
  cmd = "/bin/touch -f " + logFile + " >/dev/null 2>&1";
  rc = system(cmd.c_str());
#endif
  return rc;
}

int checkNotThere(WriteEngine::FID fid)
{
  WriteEngine::FileOp fileOp;

  return (fileOp.existsOIDDir(fid) ? -1 : 0);
}

void usage()
{
  cerr << "Usage: dbbuilder [-h|f] function" << endl
       << "  -h Display this help info" << endl
       << "  -f Necessary to use any fcn other than 7" << endl
       << " fcn" << endl
       << "  7  Build system tables only" << endl
       << endl
       << "WARNING! Using this tool improperly can render your database unusable!" << endl;
}

const unsigned sysCatalogErr = logging::M0060;

void errorHandler(const unsigned mid, const string& src, const string& msg, bool isCritErr = true)
{
  logging::LoggingID lid(19);
  logging::MessageLog ml(lid);
  logging::Message::Args args;
  logging::Message message(mid);

  if (isCritErr)
  {
    args.add(string("error"));
    args.add(msg);
    message.format(args);
    ml.logCriticalMessage(message);
    cout << src << " was not successful. " << msg << endl;
  }
  else
  {
    args.add(string("status"));
    args.add(msg);
    message.format(args);
    ml.logInfoMessage(message);
    cout << src << " was not completed. " << msg << endl;
  }
}

}  // namespace

int main(int argc, char* argv[])
{
  int buildOption;
  int c;
  std::string schema("tpch");
  Oam oam;
  bool fFlg = false;
  int rc = 0;

  opterr = 0;

  while ((c = getopt(argc, argv, "u:fh")) != EOF)
    switch (c)
    {
      case 'u': schema = optarg; break;

      case 'f': fFlg = true; break;

      case 'h':
      case '?':
      default:
        usage();
        return (c == 'h' ? 0 : 1);
        break;
    }

  if ((argc - optind) < 1)
  {
    usage();
    return 1;
  }

  oamModuleInfo_t t;
  bool parentOAMModuleFlag = false;

  // get local module info; validate running on Active Parent OAM Module
  try
  {
    t = oam.getModuleInfo();
    parentOAMModuleFlag = boost::get<4>(t);
  }
  catch (exception&)
  {
    parentOAMModuleFlag = true;
  }

  if (!parentOAMModuleFlag)
  {
    cerr << "Exiting, dbbuilder can only be run on the Active "
            "Parent OAM Module"
         << endl;
    return 1;
  }

  logFile = string(MCSLOGDIR) + "/install/dbbuilder.status";

  buildOption = atoi(argv[optind++]);

  if (buildOption != 7 && !fFlg)
  {
    usage();
    return 1;
  }

  if (buildOption == SYSCATALOG_ONLY)
  {
    if (setUp())
    {
      cerr << "setUp() call error " << endl;
    }

    bool canWrite = true;

    if (access(logFile.c_str(), W_OK) != 0)
      canWrite = false;

    try
    {
      if (checkNotThere(1001) != 0)
      {
        string cmd = "echo 'FAILED: buildOption=" + oam.itoa(buildOption) + "' > " + logFile;

        if (canWrite)
        {
          rc = system(cmd.c_str());
        }
        else
        {
          cerr << cmd << endl;
        }

        errorHandler(sysCatalogErr, "Build system catalog",
                     "System catalog appears to exist.  It will remain intact "
                     "for reuse.  The database is not recreated.",
                     false);
        return 1;
      }

      //@bug5554, make sure IDBPolicy matches the Columnstore.xml config
      try
      {
        string calpontConfigFile(std::string(MCSSYSCONFDIR) + "/columnstore/Columnstore.xml");
        config::Config* sysConfig = config::Config::makeConfig(calpontConfigFile.c_str());
        string tmp = sysConfig->getConfig("Installation", "DBRootStorageType");

        if (boost::iequals(tmp, "hdfs"))
        {
          // HDFS is configured
          if (!IDBPolicy::useHdfs())  // error install plugin
            throw runtime_error("HDFS is not enabled, installPlugin may have failed.");

          else if (!IDBFileSystem::getFs(IDBDataFile::HDFS).filesystemIsUp())
            throw runtime_error("HDFS FS is NULL, check env variables.");
        }
      }
      catch (const exception& ex)
      {
        string cmd(string("echo 'FAILED: ") + ex.what() + "' > " + logFile);

        if (canWrite)
          rc = system(cmd.c_str());
        else
          cerr << cmd << endl;

        errorHandler(sysCatalogErr, "Build system catalog", ex.what(), false);
        return 1;
      }
      catch (...)
      {
        string cmd = "echo 'FAILED:  HDFS checking.' > " + logFile;

        if (canWrite)
          rc = system(cmd.c_str());
        else
          cerr << cmd << endl;

        errorHandler(sysCatalogErr, "Build system catalog", "HDFS check failed.", false);
        return 1;
      }

      // create an initial oid bitmap file
      {
        ObjectIDManager oidm;
      }

      SystemCatalog sysCatalog;
      sysCatalog.build();

      std::string cmd = "echo 'OK: buildOption=" + oam.itoa(buildOption) + "' > " + logFile;

      if (canWrite)
        rc = system(cmd.c_str());
      else
#ifdef _MSC_VER
        (void)0;

#else
        cerr << cmd << endl;
#endif

      cmd = "save_brm";

      if (canWrite)
      {
        rc = system(cmd.c_str());

        if (rc != 0)
        {
          ostringstream os;
          os << "Warning: running " << cmd << " failed.  This is usually non-fatal.";
          cerr << os.str() << endl;
          errorHandler(sysCatalogErr, "Save BRM", os.str());
        }
      }
      else
        cerr << cmd << endl;

      return 0;
    }
    catch (exception& ex)
    {
      string cmd = "echo 'FAILED: buildOption=" + oam.itoa(buildOption) + "' > " + logFile;

      if (canWrite)
        rc = system(cmd.c_str());
      else
        cerr << cmd << endl;

      errorHandler(sysCatalogErr, "Build system catalog", ex.what());
    }
    catch (...)
    {
      string cmd = "echo 'FAILED: buildOption=" + oam.itoa(buildOption) + "' > " + logFile;

      if (canWrite)
        rc = system(cmd.c_str());
      else
        cerr << cmd << endl;

      string err("Caught unknown exception!");
      errorHandler(sysCatalogErr, "Build system catalog", err);
    }
  }
  else
  {
    usage();
    return 1;
  }

  return 1;
}
// vim:ts=4 sw=4:
