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

//  $Id: wf_lead_lag.h 3868 2013-06-06 22:13:05Z xlou $

#pragma once

#include "windowfunctiontype.h"

namespace windowfunction
{
template <typename T>
class WF_lead_lag : public WindowFunctionType
{
 public:
  WF_lead_lag(int id, const std::string& name) : WindowFunctionType(id, name)
  {
    resetData();
  }

  // pure virtual in base
  void operator()(int64_t b, int64_t e, int64_t c) override;
  WindowFunctionType* clone() const override;
  void resetData() override;
  void parseParms(const std::vector<execplan::SRCP>&) override;

  static boost::shared_ptr<WindowFunctionType> makeFunction(int, const string&, int, WindowFunctionColumn*);

 protected:
  T fValue;
  T fDefault;
  int64_t fOffset;
  int64_t fLead;
  bool fOffsetNull;
  bool fDefNull;
  bool fRespectNulls;  // respect null | ignore null
};

}  // namespace windowfunction
