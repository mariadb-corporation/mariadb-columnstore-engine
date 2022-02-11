/*
   Copyright (C) 2020 MariaDB Corporation

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
   MA 02110-1301, USA.
*/
#ifndef MCS_INT64_H_INCLUDED
#define MCS_INT64_H_INCLUDED

#include "exceptclasses.h"

namespace datatypes
{
class TNullFlag
{
 protected:
  bool mIsNull;

 public:
  explicit TNullFlag(bool val) : mIsNull(val)
  {
  }
  bool isNull() const
  {
    return mIsNull;
  }
};

class TUInt64
{
 protected:
  uint64_t mValue;

 public:
  TUInt64() : mValue(0)
  {
  }

  explicit TUInt64(uint64_t value) : mValue(value)
  {
  }

  explicit operator uint64_t() const
  {
    return mValue;
  }

  void store(uint8_t* dst) const
  {
    *(uint64_t*)dst = mValue;
  }
};

class TSInt64
{
 protected:
  int64_t mValue;

 public:
  TSInt64() : mValue(0)
  {
  }

  explicit TSInt64(int64_t value) : mValue(value)
  {
  }

  explicit operator int64_t() const
  {
    return mValue;
  }
  explicit operator uint64_t() const
  {
    return mValue < 0 ? 0 : static_cast<uint64_t>(mValue);
  }
};

class TUInt64Null : public TUInt64, public TNullFlag
{
 public:
  TUInt64Null() : TNullFlag(true)
  {
  }

  explicit TUInt64Null(uint64_t value, bool isNull = false) : TUInt64(value), TNullFlag(isNull)
  {
  }

  explicit operator uint64_t() const
  {
    idbassert(!mIsNull);
    return mValue;
  }
  uint64_t nullSafeValue(bool& isNullRef) const
  {
    return (isNullRef = isNull()) ? 0 : mValue;
  }

  TUInt64Null operator&(const TUInt64Null& rhs) const
  {
    return TUInt64Null(mValue & rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TUInt64Null operator|(const TUInt64Null& rhs) const
  {
    return TUInt64Null(mValue | rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TUInt64Null operator^(const TUInt64Null& rhs) const
  {
    return TUInt64Null(mValue ^ rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TUInt64Null MariaDBShiftLeft(const TUInt64Null& rhs) const
  {
    return TUInt64Null(rhs.mValue >= 64 ? 0 : mValue << rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TUInt64Null MariaDBShiftRight(const TUInt64Null& rhs) const
  {
    return TUInt64Null(rhs.mValue >= 64 ? 0 : mValue >> rhs.mValue, mIsNull || rhs.mIsNull);
  }
};

class TSInt64Null : public TSInt64, public TNullFlag
{
 public:
  TSInt64Null() : TNullFlag(true)
  {
  }

  explicit TSInt64Null(int64_t value, bool isNull = false) : TSInt64(value), TNullFlag(isNull)
  {
  }

  explicit operator int64_t() const
  {
    idbassert(!mIsNull);
    return mValue;
  }

  int64_t nullSafeValue(bool& isNullRef) const
  {
    return (isNullRef = isNull()) ? 0 : mValue;
  }

  TSInt64Null operator&(const TSInt64Null& rhs) const
  {
    return TSInt64Null(mValue & rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TSInt64Null operator|(const TSInt64Null& rhs) const
  {
    return TSInt64Null(mValue | rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TSInt64Null operator^(const TSInt64Null& rhs) const
  {
    return TSInt64Null(mValue ^ rhs.mValue, mIsNull || rhs.mIsNull);
  }
  TSInt64Null MariaDBShiftLeft(const TUInt64Null& rhs) const
  {
    if (isNull() || rhs.isNull())
      return TSInt64Null();
    return TSInt64Null((uint64_t)rhs >= 64 ? 0 : mValue << (uint64_t)rhs, false);
  }
  TSInt64Null MariaDBShiftRight(const TUInt64Null& rhs) const
  {
    if (isNull() || rhs.isNull())
      return TSInt64Null();
    return TSInt64Null((uint64_t)rhs >= 64 ? 0 : mValue >> (uint64_t)rhs, false);
  }
};

}  // end of namespace datatypes

#endif  // MCS_INT64_H_INCLUDED
// vim:ts=2 sw=2:
