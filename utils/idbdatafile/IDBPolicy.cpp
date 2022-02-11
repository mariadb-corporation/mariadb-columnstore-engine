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

#include <unistd.h>
#include <iostream>
#include <sstream>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/thread/thread.hpp>

#include "configcpp.h"  // for Config
#include "oamcache.h"
#include "liboamcpp.h"
#include "IDBPolicy.h"
#include "PosixFileSystem.h"
//#include "HdfsFileSystem.h"
//#include "HdfsFsCache.h"
#include "IDBLogger.h"
#include "IDBFactory.h"
#ifdef _MSC_VER
#include "utils_utf8.h"  // idb_wcstombs()
#endif

#include "installdir.h"
#include "vlarray.h"

using namespace std;

namespace idbdatafile
{
bool IDBPolicy::s_usehdfs = false;
bool IDBPolicy::s_usecloud = false;
bool IDBPolicy::s_bUseRdwrMemBuffer = false;
int64_t IDBPolicy::s_hdfsRdwrBufferMaxSize = 0;
std::string IDBPolicy::s_hdfsRdwrScratch;
bool IDBPolicy::s_configed = false;
boost::mutex IDBPolicy::s_mutex;
std::vector<uint16_t> IDBPolicy::s_PreallocSpace;

void IDBPolicy::init(bool bEnableLogging, bool bUseRdwrMemBuffer, const string& hdfsRdwrScratch,
                     int64_t hdfsRdwrBufferMaxSize)
{
  IDBFactory::installDefaultPlugins();

  IDBLogger::enable(bEnableLogging);

  s_bUseRdwrMemBuffer = bUseRdwrMemBuffer;
  s_hdfsRdwrBufferMaxSize = hdfsRdwrBufferMaxSize;
  s_hdfsRdwrScratch = hdfsRdwrScratch;

  // Create our scratch directory
  if (hdfsRdwrScratch.length() > 0)
  {
    // TODO-check to make sure this directory has sufficient space, whatever that means.

    boost::filesystem::path tmpfilepath(hdfsRdwrScratch);

    if (boost::filesystem::exists(tmpfilepath))
    {
      if (!boost::filesystem::is_directory(tmpfilepath) && useHdfs())
      {
        // We found a file named what we want our scratch directory to be named.
        // Major issue, since we can't assume anything about where to put our tmp files
        ostringstream oss;
        oss << "IDBPolicy::init: scratch diretory setting " << hdfsRdwrScratch.c_str()
            << " exists as a file. Can't create hdfs buffer files.";
        throw runtime_error(oss.str());
      }
    }
    else
    {
      cout << tmpfilepath << endl;
      bool itWorked = false;

      try
      {
        itWorked = boost::filesystem::create_directories(tmpfilepath);
      }
      catch (...)
      {
      }
      if (!itWorked)
      {
        // We failed to create the scratch directory
        ostringstream oss;
        oss << "IDBPolicy::init: failed to create hdfs scratch directory " << hdfsRdwrScratch.c_str()
            << ". Can't create hdfs buffer files.";
        throw runtime_error(oss.str());
      }
    }
  }
}

bool IDBPolicy::installPlugin(const std::string& plugin)
{
  bool ret = IDBFactory::installPlugin(plugin);

  vector<IDBDataFile::Types> plugins = IDBFactory::listPlugins();
  for (uint i = 0; i < plugins.size(); i++)
    if (plugins[i] == IDBDataFile::HDFS)
      s_usehdfs = true;
    else if (plugins[i] == IDBDataFile::CLOUD)
      s_usecloud = true;

  return ret;
}

bool IDBPolicy::isLocalFile(const std::string& path)
{
  boost::filesystem::path filepath(path);
#ifdef _MSC_VER
  size_t strmblen = utf8::idb_wcstombs(0, filepath.extension().c_str(), 0) + 1;
  char* outbuf = (char*)alloca(strmblen * sizeof(char));
  strmblen = utf8::idb_wcstombs(outbuf, filepath.extension().c_str(), strmblen);
  string fileExt(outbuf, strmblen);
#else
  // string fileExt  = filepath.extension().c_str();
#endif
  bool isXml = filepath.extension() == ".xml";
  // bool isDbrm = path.find("dbrm") != string::npos;   // StorageManager: debatable whether dbrm files should
  // go in the cloud
  bool isVb = filepath.filename() == "versionbuffer.cdf";
  bool isScratch = path.find(s_hdfsRdwrScratch) == 0;

  return isXml || isVb || isScratch;
  // return isXml || isDbrm || isVb || isScratch;
}

IDBDataFile::Types IDBPolicy::getType(const std::string& path, Contexts ctxt)
{
  bool isLocal = isLocalFile(path);

  if (isLocal || (!useHdfs() && !useCloud()))
    if (ctxt == PRIMPROC)
      return IDBDataFile::UNBUFFERED;
    else
      return IDBDataFile::BUFFERED;
  else if (useHdfs())
    return IDBDataFile::HDFS;
  else if (useCloud())
    return IDBDataFile::CLOUD;
  throw runtime_error("IDBPolicy: No appropriate data file type");

#if 0
    if ( ctxt == PRIMPROC )
    {
        if ( isLocal || !useHdfs() )
            return IDBDataFile::UNBUFFERED;
        else
            return IDBDataFile::HDFS;
    }
    else
    {
        if ( isLocal || !useHdfs() )
            return IDBDataFile::BUFFERED;
        else
            return IDBDataFile::HDFS;
    }
#endif
}

IDBFileSystem& IDBPolicy::getFs(const std::string& path)
{
  // for now context doesn't actually matter so just pass PRIMPROC
  // later the whole logic around file -> filesystem type mapping
  // needs to be data driven
  return IDBFactory::getFs(getType(path, PRIMPROC));
}

// ported from we_config.cpp
void IDBPolicy::configIDBPolicy()
{
  // make sure this is done once.
  boost::mutex::scoped_lock lk(s_mutex);

  if (s_configed)
    return;

  config::Config* cf = config::Config::makeConfig();

  //--------------------------------------------------------------------------
  // IDBDataFile logging
  //--------------------------------------------------------------------------
  bool idblog = false;
  string idblogstr = cf->getConfig("SystemConfig", "DataFileLog");

  // Must be faster.
  if (idblogstr.size() == 2 && (idblogstr[0] == 'O' || idblogstr[0] == 'o') &&
      (idblogstr[1] == 'N' || idblogstr[1] == 'n'))
  {
    idblog = true;
  }

  //--------------------------------------------------------------------------
  // Optional File System Plugin - if a HDFS type plugin is loaded
  // then the system will use HDFS for all IDB data files
  //--------------------------------------------------------------------------
  string fsplugin = cf->getConfig("SystemConfig", "DataFilePlugin");

  if (fsplugin.length() != 0)
  {
    IDBPolicy::installPlugin(fsplugin);
  }

  //--------------------------------------------------------------------------
  // HDFS file buffering
  //--------------------------------------------------------------------------
  // Maximum amount of memory to use for hdfs buffering.
  bool bUseRdwrMemBuffer = true;  // If true, use in-memory buffering, else use file buffering
  int64_t hdfsRdwrBufferMaxSize = 0;
  string strBufferMaxSize = cf->getConfig("SystemConfig", "hdfsRdwrBufferMaxSize");

  // Default is use membuf with no maximum size unless changed by config file.
  if (strBufferMaxSize.length() > 0)
  {
    hdfsRdwrBufferMaxSize = static_cast<int64_t>(cf->uFromText(strBufferMaxSize));

    if (hdfsRdwrBufferMaxSize == 0)
    {
      // If we're given a size of 0, turn off membuffering.
      bUseRdwrMemBuffer = false;
    }
  }

  // Directory in which to place file buffer temporary files.
  string tmpDir = startup::StartUp::tmpDir();

  string scratch = cf->getConfig("SystemConfig", "hdfsRdwrScratch");
  string hdfsRdwrScratch = tmpDir + scratch;

  // MCOL-498. Use DBRootX.PreallocSpace to disable
  // dbroots file space preallocation.
  // The feature is used in the FileOp code and enabled by default.
  char configSectionPref[] = "DBRoot";
  int confSectionLen = sizeof(configSectionPref) + oam::MAX_MODULE_ID_SIZE;
  utils::VLArray<char, 1024> configSection(confSectionLen);

  IDBPolicy::init(idblog, bUseRdwrMemBuffer, hdfsRdwrScratch, hdfsRdwrBufferMaxSize);
  s_configed = true;

  oam::OamCache* oamcache = oam::OamCache::makeOamCache();
  int PMId = oamcache->getLocalPMId();

  oam::OamCache::PMDbrootsMap_t pmDbrootsMap;
  pmDbrootsMap.reset(new oam::OamCache::PMDbrootsMap_t::element_type());
  oam::systemStorageInfo_t t;
  oam::Oam oamInst;
  t = oamInst.getStorageConfig();

  oam::DeviceDBRootList moduledbrootlist = boost::get<2>(t);
  oam::DeviceDBRootList::iterator pt = moduledbrootlist.begin();

  while (pt != moduledbrootlist.end() && (*pt).DeviceID != PMId)
  {
    pt++;
    continue;
  }

  // CS could return here b/c it initialised this singleton and
  // there is no DBRootX sections in XML.
  if (pt == moduledbrootlist.end())
  {
    return;
  }

  oam::DBRootConfigList& dbRootVec = (*pt).dbrootConfigList;
  s_PreallocSpace.reserve(dbRootVec.size() >> 1);
  {
    int rc;
    oam::DBRootConfigList::iterator dbRootIter = dbRootVec.begin();
    for (; dbRootIter != dbRootVec.end(); dbRootIter++)
    {
      ::memset(configSection.data() + sizeof(configSectionPref), 0, oam::MAX_MODULE_ID_SIZE);
      rc = snprintf(configSection.data(), confSectionLen, "%s%d", configSectionPref, *dbRootIter);
      // gcc 8.2 warnings
      if (rc < 0 || rc >= confSectionLen)
      {
        ostringstream oss;
        oss << "IDBPolicy::configIDBPolicy: failed to parse DBRootX section.";
        throw runtime_error(oss.str());
      }
      string setting = cf->getConfig(configSection.data(), "PreallocSpace");

      if (setting.length() != 0)
      {
        if (setting.size() == 2 && (setting[0] == 'O' || setting[0] == 'o') &&
            (setting[1] == 'N' || setting[1] == 'n'))
        {
          // int into uint16_t implicit conversion
          s_PreallocSpace.push_back(*dbRootIter);
        }
      }
    }
  }
}

void IDBPolicy::enablePreallocSpace(uint16_t dbRoot)
{
  s_PreallocSpace.push_back(dbRoot);
}

}  // namespace idbdatafile
