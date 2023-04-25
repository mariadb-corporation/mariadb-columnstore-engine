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

//  $Id: limitedorderby.h 9414 2013-04-22 22:18:30Z xlou $

/** @file */

#pragma once

#include <string>
#include "rowgroup.h"
#include "../../utils/windowfunction/idborderby.h"

namespace joblist
{
// forward reference
struct JobInfo;

// ORDER BY with LIMIT class
// This version is for subqueries, limit the result set to fit in memory,
// use ORDER BY to make the results consistent.
// The actual output are the first or last # of rows, which are NOT ordered.
class LimitedOrderBy : public ordering::IdbOrderBy
{
 public:
  LimitedOrderBy();
  virtual ~LimitedOrderBy();
  using ordering::IdbOrderBy::initialize;
  void initialize(const rowgroup::RowGroup&, const JobInfo&, bool invertRules = false,
                  bool isMultiThreded = false);
  void processRow(const rowgroup::Row&);
  uint64_t getKeyLength() const;
  uint64_t getLimitCount() const
  {
    return fCount;
  }
  const std::string toString() const;

  void finalize();

 protected:
  uint64_t fStart;
  uint64_t fCount;
  uint64_t fUncommitedMemory;
  static const uint64_t fMaxUncommited;
};

}  // namespace joblist
