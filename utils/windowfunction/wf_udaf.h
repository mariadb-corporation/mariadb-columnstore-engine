/************************************************************************************
  Copyright (C) 2017 MariaDB Corporation AB

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/

#ifndef UTILS_WF_UDAF_H
#define UTILS_WF_UDAF_H

#ifndef _MSC_VER
#include <tr1/unordered_map>
#else
#include <unordered_map>
#endif
#include "windowfunctiontype.h"
#include "mcsv1_udaf.h"

namespace windowfunction
{
// Hash classes for the distinct hashmap
class DistinctHasher
{
 public:
  inline size_t operator()(const static_any::any& a) const
  {
    return a.getHash();
  }
};

class DistinctEqual
{
 public:
  inline bool operator()(const static_any::any lhs, static_any::any rhs) const
  {
    return lhs == rhs;
  }
};

// A class to control the execution of User Define Analytic Functions (UDAnF)
// as defined by a specialization of mcsv1sdk::mcsv1_UDAF
class WF_udaf : public WindowFunctionType
{
 public:
  WF_udaf(int id, const std::string& name, mcsv1sdk::mcsv1Context& context)
   : WindowFunctionType(id, name), fUDAFContext(context), fDistinct(false), bHasDropValue(true)
  {
  }
  WF_udaf(WF_udaf& rhs);
  // pure virtual in base
  void operator()(int64_t b, int64_t e, int64_t c);
  WindowFunctionType* clone() const;
  void resetData();
  void parseParms(const std::vector<execplan::SRCP>&);
  virtual bool dropValues(int64_t, int64_t);

  mcsv1sdk::mcsv1Context& getContext()
  {
    return fUDAFContext;
  }

  bool getInterrupted()
  {
    return bInterrupted;
  }

  bool* getInterruptedPtr()
  {
    return &bInterrupted;
  }

  bool getDistinct()
  {
    return fDistinct;
  }

  void setDistinct(bool d = true)
  {
    fDistinct = d;
  }

 protected:
  void SetUDAFValue(static_any::any& valOut, int64_t colOut, int64_t b, int64_t e, int64_t c);

  mcsv1sdk::mcsv1Context fUDAFContext;  // The UDAF context
  bool bInterrupted;                    // Shared by all the threads
  bool fDistinct;
  bool bRespectNulls;  // respect null | ignore null
  bool bHasDropValue;  // Set to false when we discover the UDAnF doesn't implement dropValue.
                       // To hold distinct values and their counts
  typedef std::tr1::unordered_map<static_any::any, uint64_t, DistinctHasher, DistinctEqual> DistinctMap;
  DistinctMap fDistinctMap;

  static_any::any fValOut;  // The return value

 public:
  static boost::shared_ptr<WindowFunctionType> makeFunction(int id, const string& name, int ct,
                                                            mcsv1sdk::mcsv1Context& context,
                                                            WindowFunctionColumn* wc);
};

}  // namespace windowfunction

#endif  // UTILS_WF_UDAF_H

// vim:ts=4 sw=4:
