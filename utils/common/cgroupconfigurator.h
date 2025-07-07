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

#pragma once

#include <stdlib.h>
#include <inttypes.h>
#include <string>

#include "configcpp.h"

namespace utils
{
/* This class wraps a few methods for getting configuration variables that potentially
   come from a cgroup.  Might move it to utils/configcpp, and/or change the name. */

class CGroupConfigurator
{
 public:
  enum CGroupVersions
  {
    v1,
    v2,
  };
  CGroupConfigurator();
  virtual ~CGroupConfigurator();

  uint32_t getNumCores();
  uint64_t getTotalMemory();
  uint64_t getFreeMemory();

  bool usingCGroup()
  {
    return cGroupDefined;
  }

 private:
  uint32_t getNumCoresFromProc();
  uint32_t getNumCoresFromCGroup();
  uint64_t getTotalMemoryFromProc();
  uint64_t getTotalMemoryFromCGroup();
  uint64_t getFreeMemoryFromProc();
  uint64_t getMemUsageFromCGroup();

  std::string cGroupName;
  std::string memUsageFilename;
  std::string memStatePrefix;
  bool cGroupDefined;
  config::Config* config;
  uint64_t totalMemory = 0;
  bool printedWarning = false;
  enum CGroupVersions cGroupVersion_;
};

}  // namespace utils
