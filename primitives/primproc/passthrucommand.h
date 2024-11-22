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

//
// $Id: passthrucommand.h 2035 2013-01-21 14:12:19Z rdempsey $
// C++ Interface: passthrucommand
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#pragma once

#include "bytestream.h"
#include "command.h"

namespace primitiveprocessor
{
class PassThruCommand : public Command
{
 public:
  PassThruCommand();
  ~PassThruCommand() override;

  void prep(int8_t outputType, bool makeAbsRids) override;
  void execute() override;
  void project(messageqcpp::SBS& bs) override;
  void projectIntoRowGroup(rowgroup::RowGroup& rg, uint32_t col) override;
  uint64_t getLBID() override;
  void nextLBID() override;
  void createCommand(messageqcpp::ByteStream&) override;
  void resetCommand(messageqcpp::ByteStream&) override;
  SCommand duplicate() override;
  bool operator==(const PassThruCommand&) const;
  bool operator!=(const PassThruCommand&) const;

  int getCompType() const override
  {
    return 0;
  }

 private:
  PassThruCommand(const PassThruCommand&);

  uint8_t colWidth;

  /* Minor optimization for projectIntoRowGroup() */
  rowgroup::Row r;
  uint32_t rowSize;
};

}  // namespace primitiveprocessor
