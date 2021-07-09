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


#ifndef MCS_DATATYPES_STRING_H
#define MCS_DATATYPES_STRING_H

#include "mcs_datatype_basic.h"
#include "conststring.h"
#include "collation.h"    // class Charset

namespace datatypes
{

constexpr uint8_t  CHAR1NULL = 0xFE;
constexpr uint16_t CHAR2NULL = 0xFEFF;
constexpr uint32_t CHAR4NULL = 0xFEFFFFFF;
constexpr uint64_t CHAR8NULL = 0xFEFFFFFFFFFFFFFFULL;

constexpr uint8_t  CHAR1EMPTYROW = 0xFF;
constexpr uint16_t CHAR2EMPTYROW = 0xFFFF;
constexpr uint32_t CHAR4EMPTYROW = 0xFFFFFFFF;
constexpr uint64_t CHAR8EMPTYROW = 0xFFFFFFFFFFFFFFFFULL;


class TCharShort
{
    int64_t mValue;
public:
    TCharShort(int64_t value)
        :mValue(value)
    { }
    utils::ConstString toConstString(uint32_t width) const
    {
      utils::ConstString res = utils::ConstString((const char *) &mValue, width);
      return res.rtrimZero();
    }
    static int strnncollsp(const datatypes::Charset &cs, int64_t a, int64_t b, uint32_t width)
    {
      datatypes::TCharShort sa(a);
      datatypes::TCharShort sb(b);
      return cs.strnncollsp(sa.toConstString(width),
                            sb.toConstString(width));
    }

    static int64_t getSIntNullValueByPackedWidth(int32_t width)
    {
        switch (width)
        {
            case 1:
                return (int64_t) ((int8_t) CHAR1NULL);
            case 2:
                return (int64_t) ((int16_t) CHAR2NULL);
            case 3:
            case 4:
                return (int64_t) ((int32_t) CHAR4NULL);
            default:
                break;
        }
        return datatypes::CHAR8NULL;
    }
    static uint64_t getUintNullValueByPackedWidth(int32_t width)
    {
        if (width == 1) return CHAR1NULL;
        if (width == 2) return CHAR2NULL;
        if (width < 5) return CHAR4NULL;
        return datatypes::CHAR8NULL;
    }
    static bool isNullByPackedWidth(int64_t val, int32_t width)
    {
        return (width == 1 && (int8_t) CHAR1NULL == val) ||
               (width == 2 && (int16_t) CHAR2NULL == val) ||
               (width < 5  && (int32_t) CHAR4NULL == val) ||
               ((int64_t) CHAR8NULL == val);
    }
};


class TVarcharShort
{
    int64_t mValue;
public:
    TVarcharShort(int64_t value)
        :mValue(value)
    { }
    const char *str() const
    {
        return 1 + (const char *) &mValue;
    }
    uint8_t length() const
    {
        return ((const unsigned char *) &mValue)[0];
    }
    std::string toString() const
    {
        return std::string(str(), length());
    }

    static int64_t getSIntNullValueByPackedWidth(int32_t width)
    {
        switch (width)
        {
            case 1:
            case 2:
                return (int64_t) ((int16_t) CHAR2NULL);
            case 3:
            case 4:
                return (int64_t) ((int32_t) CHAR4NULL);
            default:
                break;
        }
        return datatypes::CHAR8NULL;
    }
    static uint64_t getUintNullValueByPackedWidth(int32_t width)
    {
        if (width < 3) return datatypes::CHAR2NULL;
        if (width < 5) return datatypes::CHAR4NULL;
        return datatypes::CHAR8NULL;
    }
    static bool isNullByPackedWidth(int64_t val, int32_t width)
    {
        return (width < 3 && (int16_t) CHAR2NULL == val) ||
               (width < 5 && (int32_t) CHAR4NULL == val) ||
               ((int64_t) CHAR8NULL == val);
    }
};


/*
  A common parent class for CHAR(N) and VARCHAR(N) data types.
  Currently they reuse the same NULL and EMPTY magic values.
  This may change in the future.
*/
class TXCharCommon
{
protected:
  char *mPtr;
  uint32_t mWidth;
public:
  TXCharCommon(char *ptr, uint32_t width)
   :mPtr(ptr), mWidth(width)
  { }
  TXCharCommon(uint8_t *ptr, uint32_t width)
   :mPtr((char *) ptr), mWidth(width)
  { }
  static bool isNullByPackedWidth(const char *ptr, uint8_t width)
  {
    switch (width)
    {
      case 1:
        return *((uint8_t*) ptr) == CHAR1NULL;
      case 2:
        return *((uint16_t *) ptr) == CHAR2NULL;
      case 3:
      case 4:
            return *((uint32_t *) ptr) == CHAR4NULL;
      case 5:
      case 6:
      case 8:
      default:
        break;
    }
    return *((uint64_t *) ptr) == CHAR8NULL;
  }
  static void setNullByPackedWidth(const char *ptr, uint32_t width)
  {
    switch (width)
    {
      case 1:
        *((uint8_t*) ptr) = datatypes::CHAR1NULL;
        break;

      case 2:
        *((uint16_t*) ptr) = datatypes::CHAR2NULL;
        break;

      case 3:
      case 4:
        *((uint32_t*) ptr) = datatypes::CHAR4NULL;
        break;

      case 5:
      case 6:
      case 7:
      case 8:
        break;
    }
    *((uint64_t*) ptr) = datatypes::CHAR8NULL;
  }
};


class TChar: public TXCharCommon
{
public:
  using TXCharCommon::TXCharCommon;
  // This method is OK only when we know "str" fits into mWidth.
  void storeAndExtend(const utils::ConstString &str)
  {
      idbassert(str.length() <= mWidth);
      memcpy(mPtr, str.str(), str.length());
      memset(mPtr + str.length(), 0,
             mWidth - str.length() - unusedGapSizeByPackedWidth(mWidth));
  }
  void storeWithTruncation(utils::ConstString str)
  {
      storeAndExtend(str.truncate(mWidth));
  }
  // Some tricks to avoid slow strnlen() as much as possible
  static size_t zeroTrimmedLength1(const char *str)
  {
      return str[0] ? 1 : 0;
  }
  static size_t zeroTrimmedLength2(const char *str)
  {
      return str[1] ? 2 : str[0] ? 1 : 0;
  }
  static size_t zeroTrimmedLength4(const char *str)
  {
      return *((const uint16_t*) (str + 2)) ?
             2 + zeroTrimmedLength2(str + 2) :
             zeroTrimmedLength2(str);
  }
  static size_t zeroTrimmedLength8(const char *str)
  {
      return *((const uint32_t*) (str + 4)) ?
             4 + zeroTrimmedLength4(str + 4) :
             zeroTrimmedLength4(str);
  }
  static size_t zeroTrimmedLength(const char *str, uint32_t width)
  {
      switch (width)
      {
          case 1:
              return zeroTrimmedLength1(str);
          case 2:
              return zeroTrimmedLength2(str);
          case 3:
          case 4:
              return zeroTrimmedLength4(str);
          case 5:
          case 6:
          case 7:
          case 8:
              return zeroTrimmedLength8(str);
      }
      return str[8] ? 8 + strnlen(str + 8, width - 8) :
                      zeroTrimmedLength8(str);
  }
};


class TVarchar: public TXCharCommon
{
public:
  using TXCharCommon::TXCharCommon;

  // This method is OK only when we know "str" fits into mWidth.
  void storeAndExtend(const utils::ConstString &str)
  {
    size_t packedLength = str.length() + 1;
    idbassert(packedLength <= mWidth);
    *((uint8_t*) mPtr)= str.length();
    memcpy(mPtr + 1, str.str(), str.length());
    size_t roundedWidth = mWidth + unusedGapSizeByPackedWidth(mWidth);
    if (roundedWidth > packedLength) // Safety
      memset(mPtr + packedLength, 0, roundedWidth - packedLength);
  }

  void storeAndExtendNotNullCharByPackedWidth(const char *src, uint32_t width)
  {
    storeAndExtend(utils::ConstString(src, TChar::zeroTrimmedLength(src, width - 1)));
  }

  void storeAndExtendCharByPackedWidth(const char *src, uint32_t width)
  {
    // QQ: do we need to handle EMPTY values here?
    if (datatypes::TXCharCommon::isNullByPackedWidth(src, width))
    {
       // VARCHAR and CHAR reuse the same NULL magic values
       memcpy(mPtr, src, width);
    }
    else
    {
      storeAndExtendNotNullCharByPackedWidth(src, width);
    }
  }

  void storeWithTruncation(utils::ConstString str)
  {
      storeAndExtend(str.truncate(mWidth - 1));
  }

};


} // namespace datatypes


#endif // MCS_DATATYPES_STRING_H
