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

/***********************************************************************
 *   $Id: filtercommand-jl.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file
 * class FilterCommand interface
 */

#pragma once

#include "joblist.h"
#include "command-jl.h"

namespace joblist
{
class FilterCommandJL : public CommandJL
{
 public:
  explicit FilterCommandJL(const FilterStep&);
  ~FilterCommandJL() override;

  void setLBID(uint64_t rid, uint32_t dbroot) override;
  uint8_t getTableColumnType() override;
  CommandType getCommandType() override;
  std::string toString() override;
  void createCommand(messageqcpp::ByteStream& bs) const override;
  void runCommand(messageqcpp::ByteStream& bs) const override;
  uint16_t getWidth() override;
  uint8_t getBOP() const
  {
    return fBOP;
  };

 private:
  FilterCommandJL();
  FilterCommandJL(const FilterCommandJL&);

  uint8_t fBOP;
  execplan::CalpontSystemCatalog::ColType fColType;
};

};  // namespace joblist
