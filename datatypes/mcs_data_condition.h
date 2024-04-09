/* Copyright (C) 2021 MariaDB Corporation.

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

#include <cstdint>
namespace datatypes
{
/*
  A subset of SQL Conditions related to data processing.
  SQLSTATE terminology is used for categories:
  - S stands for "success"
  - W stands for "warning"
  - X stands for "exceptions"
*/
class DataCondition
{
 public:
  using ErrorType = uint32_t;

  enum Code
  {
    // Code                                Value       SQLSTATE
    S_SUCCESS = 0,                                 // 00000
    W_STRING_DATA_RIGHT_TRUNCATION = 1 << 1,       // 01004
    X_STRING_DATA_RIGHT_TRUNCATION = 1 << 16,      // 22001
    X_NUMERIC_VALUE_OUT_OF_RANGE = 1 << 17,        // 22003
    X_INVALID_CHARACTER_VALUE_FOR_CAST = 1 << 18,  // 22018
  };
  DataCondition() : mError(S_SUCCESS)
  {
  }
  DataCondition(Code code) : mError(code)
  {
  }
  DataCondition& operator|=(Code code)
  {
    mError = (Code)(mError | code);
    return *this;
  }
  DataCondition operator&(Code rhs) const
  {
    return DataCondition((Code)(mError & rhs));
  }
  operator Code() const
  {
    return mError;
  }
  // WIP refactor this casting ErrorType to Code and calling a ctor on it instead.
  static bool isOutOfRange(const ErrorType error)
  {
    return Code::X_NUMERIC_VALUE_OUT_OF_RANGE & error;
  }

  // Adjust a signed integer of any size to the range [-absMaxVal , +absMaxVal]
  template <typename T>
  void adjustSIntXRange(T& val, T absMaxVal)
  {
    if (val > absMaxVal)
    {
      val = absMaxVal;
      *this |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
    }
    else if (val < -absMaxVal)
    {
      val = -absMaxVal;
      *this |= DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE;
    }
  }

 private:
  Code mError;
};

}  // namespace datatypes
