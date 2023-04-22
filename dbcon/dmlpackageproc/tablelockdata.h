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

// $Id$

/** @file */

#pragma once

#include <stdint.h>

#include <map>
#include <mutex>
#include <condition_variable>

#define EXPORT

namespace dmlpackageprocessor
{
class TablelockData
{
 public:
  typedef std::map<uint32_t, TablelockData*> TablelockDataMap;
  typedef std::map<uint32_t, uint64_t> OIDTablelock;
  EXPORT static TablelockData* makeTablelockData(uint32_t sessionID = 0);
  EXPORT static void removeTablelockData(uint32_t sessionID = 0);
  EXPORT void setTablelock(uint32_t tableOid, uint64_t tablelockId);
  EXPORT uint64_t getTablelockId(uint32_t tableOid);
  OIDTablelock& getOidTablelockMap();

 private:
  /** Constuctors */
  explicit TablelockData();
  explicit TablelockData(const TablelockData& rhs);
  ~TablelockData();

  static std::mutex map_mutex;
  static TablelockDataMap fTablelockDataMap;
  OIDTablelock fOIDTablelockMap;
  std::mutex fOIDTablelock;
};

}  // namespace dmlpackageprocessor

#undef EXPORT

