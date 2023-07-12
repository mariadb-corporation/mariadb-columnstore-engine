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

#pragma once

#include <unistd.h>
#include <map>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "liboamcpp.h"


namespace oam
{
class OamCache
{
 public:
  typedef boost::shared_ptr<std::map<int, int> > dbRootPMMap_t;
  typedef std::vector<int> dbRoots;
  typedef boost::shared_ptr<std::map<int, dbRoots> > PMDbrootsMap_t;
  virtual ~OamCache() = default;

  void checkReload();
  void forceReload()
  {
    mtime = 0;
  }

  dbRootPMMap_t getDBRootToPMMap();
  dbRootPMMap_t getDBRootToConnectionMap();
  PMDbrootsMap_t getPMToDbrootsMap();
  uint32_t getDBRootCount();
  DBRootConfigList& getDBRootNums();
  std::vector<int>& getModuleIds();
  static OamCache* makeOamCache();
  std::string getOAMParentModuleName();
  int getLocalPMId();  // return the PM id of this machine.
  std::string getSystemName();
  std::string getModuleName();

 private:
  friend struct CacheReloaded;
  OamCache() = default;
  OamCache(const OamCache&) = delete;
  OamCache& operator=(const OamCache&) const = delete;

  dbRootPMMap_t dbRootPMMap;
  dbRootPMMap_t dbRootConnectionMap;
  PMDbrootsMap_t pmDbrootsMap;
  uint32_t numDBRoots = 1;
  time_t mtime = 0;
  DBRootConfigList dbroots;
  std::vector<int> moduleIds;
  std::string OAMParentModuleName;
  int mLocalPMId = 0;  // The PM id running on this machine
  std::string systemName;
  std::string moduleName;
};

}  // namespace oam

