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

// $Id: systemcatalog.h 2101 2013-01-21 14:12:52Z rdempsey $

/**
 * @file
 */
#pragma once

#include "writeengine.h"
#include <unordered_map>
#include <utility>

using OidTypeT = std::pair<execplan::CalpontSystemCatalog::ColDataType, int>;

class SystemCatalog
{
 public:
  enum class UpgradeState
  {
    Done,          // published and correct
    Needed,        // no extents and no file
    Leftover,      // only unpublished extents (and maybe a file) from a failed attempt
    Inconsistent,  // published extents that don't match the file or the reference column
    Error          // the ExtentMap could not be read, or the reference column has no extents
  };

  void build();
  void remove();
  int upgrade(const std::unordered_map<int, std::pair<int, bool>>&,
              std::unordered_map<int, OidTypeT>,
              std::unordered_map<int, std::string>);

  static UpgradeState checkUpgradeCandidate(int oid, int refOid, std::string& detail);

 private:
  static int removeUpgradeLeftovers(int oid, std::string& detail);
  WriteEngine::WriteEngineWrapper fWriteEngine;
};
