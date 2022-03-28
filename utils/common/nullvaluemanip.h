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

#pragma once

#include "../../dbcon/execplan/calpontsystemcatalog.h"

namespace utils
{
// returns the NULL value for our 'numeric' types including short strings.
// The width is only relevant for long string columns.
uint64_t getNullValue(execplan::CalpontSystemCatalog::ColDataType, uint32_t colWidth = 0);
int64_t getSignedNullValue(execplan::CalpontSystemCatalog::ColDataType, uint32_t colWidth = 0);

// A class for striings that can hold NULL values - a value that is separate from all possible string.
class NullString
{
 protected:
  std::auto_ptr<std::string> mStrPtr;

 public:
  NullString() : mStrPtr(nullptr)
  {
  }
  NullString(const char* str, size_t length) : mStrPtr(new std::string(str, length))
  {
  }
  explicit NullString(const std::string& str) : mStrPtr(new std::string(str)
  {
  }
  const char* str() const
  {
    return mStr;
  }
  const char* end() const
  {
    return mStr + mLength;
  }
  size_t length() const
  {
    return mLength;
  }
  std::string toString() const
  {
    return std::string(mStr, mLength);
  }
  bool eq(char ch) const
  {
    return mLength == 1 && mStr[0] == ch;
  }
  bool eq(const ConstString& rhs) const
  {
    return mLength == rhs.mLength && !memcmp(mStr, rhs.mStr, mLength);
  }
  ConstString& rtrimZero()
  {
    for (; mLength && mStr[mLength - 1] == '\0'; mLength--)
    {
    }
    return *this;
  }
};

}  // namespace utils
