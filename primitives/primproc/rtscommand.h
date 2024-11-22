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
// $Id: rtscommand.h 2035 2013-01-21 14:12:19Z rdempsey $
// C++ Interface: rtscommand
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
#include <boost/scoped_ptr.hpp>
#include <memory>

namespace primitiveprocessor
{
class RTSCommand : public Command
{
 public:
  RTSCommand();
  ~RTSCommand() override;

  void execute() override;
  void project(messageqcpp::SBS& bs) override;
  void projectIntoRowGroup(rowgroup::RowGroup& rg, uint32_t col) override;
  uint64_t getLBID() override;
  void nextLBID() override;
  void createCommand(messageqcpp::ByteStream&) override;
  void resetCommand(messageqcpp::ByteStream&) override;
  SCommand duplicate() override;
  bool operator==(const RTSCommand&) const;
  bool operator!=(const RTSCommand&) const;

  void setBatchPrimitiveProcessor(BatchPrimitiveProcessor*) override;

  /* Put bootstrap code here (ie, build the template primitive msg) */
  void prep(int8_t outputType, bool makeAbsRids) override;
  uint8_t isPassThru()
  {
    return passThru;
  }
  void setAbsNull(bool a = true)
  {
    absNull = a;
  }
  void getLBIDList(uint32_t loopCount, std::vector<int64_t>* lbids) override;

  // TODO: do we need to reference either col or dict?
  int getCompType() const override
  {
    return dict.getCompType();
  }

 private:
  RTSCommand(const RTSCommand&);

  ColumnCommandUniquePtr col;
  DictStep dict;
  uint8_t passThru;
  bool absNull;
};

}  // namespace primitiveprocessor
