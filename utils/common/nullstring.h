/* Copyright (C) 2022, 2023 MariaDB Corporation.

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

// nullstring.h
//
// A class that can reprpesent string-with-NULL (special value not representable by a string value).

#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include "exceptclasses.h"
#include "conststring.h"
#include "mcs_datatype_basic.h"

namespace utils
{
// A class for striings that can hold NULL values - a value that is separate from all possible string.
class NullString
{
 protected:
  std::shared_ptr<std::string> mStrPtr;

 public:
  NullString() : mStrPtr(nullptr)
  {
  }
  NullString(const char* str, size_t length)
  {
    idbassert(str != nullptr || length == 0);

    if (str)
    {
      mStrPtr.reset(new std::string((const char*)str, length));
    }
  }
  // XXX: this constructor is used to construct NullString from char*. Please be
  //      aware of that - std::string(nullptr) throws exception and you should check
  //      for nullptr.
  explicit NullString(const std::string& str) : mStrPtr(new std::string(str))
  {
  }
  explicit NullString(const ConstString& str) : mStrPtr()
  {
    assign((const uint8_t*)str.str(), str.length());
  }
  ConstString toConstString() const
  {
    if (isNull())
    {
      return ConstString(nullptr, 0);
    }
    return ConstString(mStrPtr->c_str(), mStrPtr->length());
  }
  uint64_t toMCSUInt64() const
  {
    if (isNull())
    {
      return 0;
    }

    const uint64_t val = static_cast<uint64_t>(strtoul(str(), 0, 0));
    //@Bug 4632 and 4648: Don't return marker value for NULL, but allow return of marker value for EMPTYROW.
    return val == joblist::UBIGINTNULL ? MAX_UBIGINT : val;
  }
  int64_t toMCSInt64() const
  {
    if (isNull())
    {
      return 0;
    }

    const int64_t val = static_cast<int64_t>(atoll(str()));
    //@Bug 4632 and 4648: Don't return marker value for NULL, but allow return of marker value for EMPTYROW.
    return std::max(val, static_cast<int64_t>(joblist::BIGINTEMPTYROW));
  }
  const char* str() const
  {
    if (!mStrPtr)
    {
      return nullptr;
    }
    return mStrPtr->c_str();
  }
  const char* end() const
  {
    if (!mStrPtr)
    {
      return nullptr;
    }
    return str() + length();
  }
  size_t length() const
  {
    if (!mStrPtr)
    {
      return 0;
    }
    return mStrPtr->length();
  }
  std::string toString() const
  {
    idbassert(mStrPtr);
    return std::string(*mStrPtr);
  }
  // "unsafe" means we do not check for NULL.
  // it should be used after we know there is data in NullString.
  const std::string& unsafeStringRef() const
  {
    idbassert(mStrPtr);
    return (*mStrPtr);
  }
  bool eq(char ch) const
  {
    return length() == 1 && str()[0] == ch;
  }
  // this is SQL-like NULL handling equality. NULL will not be equal to anything, including NULL.
  bool eq(const NullString& rhs) const
  {
    if (!rhs.mStrPtr)
    {
      return false;
    }
    if (!mStrPtr)
    {
      return false;
    }
    return *mStrPtr == *(rhs.mStrPtr);
  }
  NullString& rtrimZero()
  {
    return *this;  // TODO
  }
  // this can be used to safely get a string value, with default value for NULL substitution.
  // it does not raise anything and provides some nonsensical default value for you that will be
  // easy to find.
  std::string safeString(const char* defaultValue = "<<<no default value for null provided>>>") const
  {
    if (!mStrPtr)
    {
      return std::string(defaultValue);
    }
    return std::string(*mStrPtr);
  }
  bool isNull() const
  {
    return !mStrPtr;
  }
  void resize(size_t newSize, char pad)
  {
    if (mStrPtr)
    {
      mStrPtr->resize(newSize, pad);
    }
  }
  NullString& dropString()
  {
    mStrPtr.reset();
    return (*this);
  }
  void assign(const uint8_t* p, size_t len)
  {
    if (!p)
    {
      mStrPtr.reset();
      return;
    }
    mStrPtr.reset(new std::string((const char*)p, len));
  }
  void assign(const std::string& newVal)
  {
    mStrPtr.reset(new std::string(newVal));
  }
  // XXX: here we implement what Row::equals expects.
  //      It is not SQL-NULL-handling compatible, please beware.
  bool operator==(const NullString& a) const
  {
    if (!mStrPtr && !a.mStrPtr)
    {
      return true;
    }
    if (!mStrPtr)
    {
      return false;
    }
    if (!a.mStrPtr)
    {
      return false;
    }
    // fall to std::string equality.
    return (*mStrPtr) == (*a.mStrPtr);
  }
  bool operator==(const std::string& a) const
  {
    if (!mStrPtr)
    {
      return false;
    }
    // fall to std::string equality.
    return (*mStrPtr) == a;
  }
  bool operator<(const NullString& a) const
  {
    // order NULLs first.
    if (isNull() > a.isNull())
    {
      return true;
    }
    if (isNull() < a.isNull())
    {
      return false;
    }
    if (!isNull())
    {
      // fall to std::string equality.
      return (*mStrPtr) < (*a.mStrPtr);
    }
    return false;  // both are NULLs.
  }
  bool operator>(const NullString& a) const
  {
    return a < (*this);
  }
};

}  // namespace utils.
