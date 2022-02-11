/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

//  $Id: wf_sum_avg.h 3868 2013-06-06 22:13:05Z xlou $

#ifndef UTILS_WF_SUM_AVG_H
#define UTILS_WF_SUM_AVG_H

#include <set>
#include "windowfunctiontype.h"

namespace windowfunction
{
// T_IN is the data type of the input values.
// T_OUT is the data type we are using for output and internal values
template <typename T_IN, typename T_OUT>
class WF_sum_avg : public WindowFunctionType
{
 public:
  WF_sum_avg(int id, const std::string& name)
   : WindowFunctionType(id, name), fDistinct(id != WF__SUM && id != WF__AVG)
  {
    resetData();
  }

  // pure virtual in base
  void operator()(int64_t b, int64_t e, int64_t c);
  WindowFunctionType* clone() const;
  void resetData();

  static boost::shared_ptr<WindowFunctionType> makeFunction(int, const string&, int, WindowFunctionColumn*);

 protected:
  T_IN fVal;
  T_OUT fAvg;
  T_OUT fSum;
  uint64_t fCount;
  bool fDistinct;
  std::set<T_IN> fSet;

  void checkSumLimit(const T_IN& val, const T_OUT& sum);

  int128_t calculateAvg(const int128_t& sum, const uint64_t count, const int scale);
  long double calculateAvg(const long double& sum, const uint64_t count, const int scale);
};

}  // namespace windowfunction

#endif  // UTILS_WF_SUM_AVG_H

// vim:ts=4 sw=4:
