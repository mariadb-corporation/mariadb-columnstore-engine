/* Copyright (C) 2020 MariaDB Corporation.

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

#include <string>
#include <string.h>

namespace utils
{
class ConstString
{
 protected:
  const char* mStr; // it can be NULL now.
  size_t mLength;

 public:
  ConstString(const char* str, size_t length) : mStr(str), mLength(length)
  {
    idbassert(mStr || mLength == 0); // nullptr mStr should have zero length.
  }
  explicit ConstString(const std::string& str) : mStr(str.data()), mLength(str.length())
  {
  }
  const char* str() const
  {
    return mStr;
  }
  const char* end() const
  {
    // end() should be computed for non-nullptr mStrs, otherwise it is undefined behavior.
    idbassert(mStr);
    return mStr + mLength;
  }
  size_t length() const
  {
    return mLength;
  }
  std::string toString() const
  {
    idbassert(mStr);
    return std::string(mStr, mLength);
  }
  bool eq(char ch) const
  {
    return mLength == 1 && mStr[0] == ch;
  }
  bool eq(const ConstString& rhs) const
  {
    if (!mStr)
    {
      return mStr == rhs.mStr;
    }
    return mLength == rhs.mLength && !memcmp(mStr, rhs.mStr, mLength);
  }
  ConstString& rtrimZero()
  {
    for (; mLength && mStr[mLength - 1] == '\0'; mLength--)
    {
    }
    return *this;
  }
  bool isNull() const
  {
    return mStr == nullptr;
  }
};

}  // namespace utils
