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
   MA 02110-1301, USA. */
#ifndef MCS_DATATYPE_H_INCLUDED
#define MCS_DATATYPE_H_INCLUDED

#include <limits>
#include <sstream>
#include <boost/any.hpp>
#include "exceptclasses.h"
#include "mcs_decimal.h"


#ifdef _MSC_VER
typedef int     mcs_sint32_t;
#else
typedef int32_t mcs_sint32_t;
#endif


// Because including my_sys.h in a Columnstore header causes too many conflicts
struct charset_info_st;
typedef const struct charset_info_st CHARSET_INFO;


#ifdef _MSC_VER
#define __attribute__(x)
#endif

namespace
{
const int64_t MIN_TINYINT         __attribute__ ((unused)) = std::numeric_limits<int8_t>::min() + 2;  // -126;
const int64_t MAX_TINYINT         __attribute__ ((unused)) = std::numeric_limits<int8_t>::max();      //  127;
const int64_t MIN_SMALLINT        __attribute__ ((unused)) = std::numeric_limits<int16_t>::min() + 2; // -32766;
const int64_t MAX_SMALLINT        __attribute__ ((unused)) = std::numeric_limits<int16_t>::max();     //  32767;
const int64_t MIN_MEDINT          __attribute__ ((unused)) = -(1ULL << 23);                           // -8388608;
const int64_t MAX_MEDINT          __attribute__ ((unused)) = (1ULL << 23) - 1;                        //  8388607;
const int64_t MIN_INT             __attribute__ ((unused)) = std::numeric_limits<int32_t>::min() + 2; // -2147483646;
const int64_t MAX_INT             __attribute__ ((unused)) = std::numeric_limits<int32_t>::max();     //  2147483647;
const int64_t MIN_BIGINT          __attribute__ ((unused)) = std::numeric_limits<int64_t>::min() + 2; // -9223372036854775806LL;
const int64_t MAX_BIGINT          __attribute__ ((unused)) = std::numeric_limits<int64_t>::max();     //  9223372036854775807

const uint64_t MIN_UINT           __attribute__ ((unused)) = 0;
const uint64_t MIN_UTINYINT       __attribute__ ((unused)) = 0;
const uint64_t MIN_USMALLINT      __attribute__ ((unused)) = 0;
const uint64_t MIN_UMEDINT        __attribute__ ((unused)) = 0;
const uint64_t MIN_UBIGINT        __attribute__ ((unused)) = 0;
const uint64_t MAX_UINT           __attribute__ ((unused)) = std::numeric_limits<uint32_t>::max() - 2; // 4294967293
const uint64_t MAX_UTINYINT       __attribute__ ((unused)) = std::numeric_limits<uint8_t>::max() - 2;  // 253;
const uint64_t MAX_USMALLINT      __attribute__ ((unused)) = std::numeric_limits<uint16_t>::max() - 2; // 65533;
const uint64_t MAX_UMEDINT        __attribute__ ((unused)) = (1ULL << 24) - 1;                         // 16777215
const uint64_t MAX_UBIGINT        __attribute__ ((unused)) = std::numeric_limits<uint64_t>::max() - 2; // 18446744073709551613

const float MAX_FLOAT             __attribute__ ((unused)) = std::numeric_limits<float>::max();        // 3.402823466385289e+38
const float MIN_FLOAT             __attribute__ ((unused)) = -std::numeric_limits<float>::max();
const double MAX_DOUBLE           __attribute__ ((unused)) = std::numeric_limits<double>::max();       // 1.7976931348623157e+308
const double MIN_DOUBLE           __attribute__ ((unused)) = -std::numeric_limits<double>::max();
const long double MAX_LONGDOUBLE  __attribute__ ((unused)) = std::numeric_limits<long double>::max();  // 1.7976931348623157e+308
const long double MIN_LONGDOUBLE  __attribute__ ((unused)) = -std::numeric_limits<long double>::max();

const uint64_t AUTOINCR_SATURATED __attribute__ ((unused)) = std::numeric_limits<uint64_t>::max();
}


using namespace std; // e.g. string


namespace ddlpackage
{
  struct ColumnType;
};


namespace BRM
{
  struct EMEntry;
  class DBRM;
};


namespace rowgroup
{
  struct Row;
};


namespace execplan
{
  struct SimpleColumn;
};



namespace datatypes
{

class SystemCatalog
{
public:

  /** the set of Calpont column widths
   *
   */
  enum ColWidth { ONE_BIT, ONE_BYTE, TWO_BYTE, THREE_BYTE, FOUR_BYTE, FIVE_BYTE, SIX_BYTE, SEVEN_BYTE, EIGHT_BYTE };


  /** the set of Calpont column data types
   *
   */
  enum ColDataType
  {
      BIT,                  /*!< BIT type */
      TINYINT,              /*!< TINYINT type */
      CHAR,                 /*!< CHAR type */
      SMALLINT,             /*!< SMALLINT type */
      DECIMAL,              /*!< DECIMAL type */
      MEDINT,               /*!< MEDINT type */
      INT,                  /*!< INT type */
      FLOAT,                /*!< FLOAT type */
      DATE,                 /*!< DATE type */
      BIGINT,               /*!< BIGINT type */
      DOUBLE,               /*!< DOUBLE type */
      DATETIME,             /*!< DATETIME type */
      VARCHAR,              /*!< VARCHAR type */
      VARBINARY,            /*!< VARBINARY type */
      CLOB,                 /*!< CLOB type */
      BLOB,                 /*!< BLOB type */
      UTINYINT,             /*!< Unsigned TINYINT type */
      USMALLINT,            /*!< Unsigned SMALLINT type */
      UDECIMAL,             /*!< Unsigned DECIMAL type */
      UMEDINT,              /*!< Unsigned MEDINT type */
      UINT,                 /*!< Unsigned INT type */
      UFLOAT,               /*!< Unsigned FLOAT type */
      UBIGINT,              /*!< Unsigned BIGINT type */
      UDOUBLE,              /*!< Unsigned DOUBLE type */
      TEXT,                 /*!< TEXT type */
      TIME,                 /*!< TIME type */
      TIMESTAMP,            /*!< TIMESTAMP type */
      NUM_OF_COL_DATA_TYPE, /* NEW TYPES ABOVE HERE */
      LONGDOUBLE,           /* @bug3241, dev and variance calculation only */
      STRINT,               /* @bug3532, string as int for fast comparison */
      UNDEFINED,            /*!< Undefined - used in UDAF API */
      BINARY,               /*!< BINARY type */
  };

  class TypeAttributesStd
  {
  public:
      int32_t colWidth;
      int32_t scale;  //number after decimal points
      int32_t precision;
      TypeAttributesStd()
         :colWidth(0),
          scale(0),
          precision(-1)
      {}
      /**
          @brief Convenience method to get int128 from a std::string.
      */
      int128_t decimal128FromString(const std::string& value) const;

      /**
          @brief The method sets the legacy scale and precision of a wide decimal
          column which is the result of an arithmetic operation.
      */
      inline void setDecimalScalePrecisionLegacy(unsigned int p, unsigned int s)
      {
          scale = s;

          if (s == 0)
              precision = p - 1;
          else
              precision = p - s;
      }

      /**
          @brief The method sets the scale and precision of a wide decimal
          column which is the result of an arithmetic operation.
      */
      inline void setDecimalScalePrecision(unsigned int p, unsigned int s)
      {
          colWidth = (p > INT64MAXPRECISION) ?
                     MAXDECIMALWIDTH : MAXLEGACYWIDTH;

          precision = (p > INT128MAXPRECISION) ?
                      INT128MAXPRECISION : p;

          scale = s;
      }

      /**
          @brief The method sets the scale and precision of a wide decimal
          column which is the result of an arithmetic operation, based on a heuristic.
      */
      inline void setDecimalScalePrecisionHeuristic(unsigned int p, unsigned int s)
      {
          unsigned int diff = 0;

          if (p > INT128MAXPRECISION)
          {
              precision = INT128MAXPRECISION;
              diff = p - INT128MAXPRECISION;
          }
          else
          {
              precision = p;
          }

          scale = s;

          if (diff != 0)
          {
              scale = s - (int)(diff * (38.0/65.0));

              if (scale < 0)
                  scale = 0;
          }
      }

      /**
          @brief The method returns true if the column precision
          belongs to a wide decimal range.
      */
      inline bool isWideDecimalPrecision() const
      {
          return ((precision > INT64MAXPRECISION) &&
              (precision <= INT128MAXPRECISION));
      }

  };

  class TypeHolderStd: public TypeAttributesStd
  {
  public:
    ColDataType colDataType;
    TypeHolderStd()
     :colDataType(MEDINT)
    { }
    const class TypeHandler *typeHandler() const;
    boost::any getNullValueForType() const;

    /**
        @brief The method detects whether decimal type is wide
        using csc colType.
    */
    inline bool isWideDecimalType() const
    {
        return (colDataType == DECIMAL ||
                colDataType == UDECIMAL) &&
               colWidth == MAXDECIMALWIDTH;
    }
  };

};


/**
    @brief The method detects whether decimal type is wide
    using datatype and width.
*/
static inline bool isWideDecimalType(const datatypes::SystemCatalog::ColDataType &dt,
                                               const int32_t width)
{
  return width == MAXDECIMALWIDTH &&
         (dt == SystemCatalog::DECIMAL ||
          dt == SystemCatalog::UDECIMAL);
}


/** convenience function to determine if column type is a char
 *  type
 */
inline bool isCharType(const datatypes::SystemCatalog::ColDataType type)
{
  return (datatypes::SystemCatalog::VARCHAR == type ||
          datatypes::SystemCatalog::CHAR == type ||
          datatypes::SystemCatalog::BLOB == type ||
          datatypes::SystemCatalog::TEXT == type);
}

/** convenience function to determine if column type is a
 *  numeric type
 */
inline bool isNumeric(const datatypes::SystemCatalog::ColDataType type)
{
  switch (type)
  {
    case datatypes::SystemCatalog::TINYINT:
    case datatypes::SystemCatalog::SMALLINT:
    case datatypes::SystemCatalog::MEDINT:
    case datatypes::SystemCatalog::INT:
    case datatypes::SystemCatalog::BIGINT:
    case datatypes::SystemCatalog::FLOAT:
    case datatypes::SystemCatalog::DOUBLE:
    case datatypes::SystemCatalog::DECIMAL:
    case datatypes::SystemCatalog::UTINYINT:
    case datatypes::SystemCatalog::USMALLINT:
    case datatypes::SystemCatalog::UMEDINT:
    case datatypes::SystemCatalog::UINT:
    case datatypes::SystemCatalog::UBIGINT:
    case datatypes::SystemCatalog::UFLOAT:
    case datatypes::SystemCatalog::UDOUBLE:
    case datatypes::SystemCatalog::UDECIMAL:
        return true;

    default:
        return false;
  }
}

inline bool isDecimal(const datatypes::SystemCatalog::ColDataType type)
{
  return (type == datatypes::SystemCatalog::DECIMAL ||
          type == datatypes::SystemCatalog::UDECIMAL);
}

/** convenience function to determine if column type is an
 *  unsigned type
 */
inline bool isUnsigned(const datatypes::SystemCatalog::ColDataType type)
{
  switch (type)
  {
    case datatypes::SystemCatalog::UTINYINT:
    case datatypes::SystemCatalog::USMALLINT:
    case datatypes::SystemCatalog::UMEDINT:
    case datatypes::SystemCatalog::UINT:
    case datatypes::SystemCatalog::UBIGINT:
        return true;

    default:
        return false;
  }
}

inline bool isSignedInteger(const datatypes::SystemCatalog::ColDataType type)
{
  switch (type)
  {
    case datatypes::SystemCatalog::TINYINT:
    case datatypes::SystemCatalog::SMALLINT:
    case datatypes::SystemCatalog::MEDINT:
    case datatypes::SystemCatalog::INT:
    case datatypes::SystemCatalog::BIGINT:
        return true;

    default:
        return false;
  }
}


/**
    @brief Returns true if all arguments have a DECIMAL/UDECIMAL type
*/
static inline bool isDecimalOperands(const SystemCatalog::ColDataType resultDataType,
    const SystemCatalog::ColDataType leftColDataType,
    const SystemCatalog::ColDataType rightColDataType)
{
  return ((resultDataType == SystemCatalog::DECIMAL ||
           resultDataType == SystemCatalog::UDECIMAL) &&
          (leftColDataType == SystemCatalog::DECIMAL ||
           leftColDataType == SystemCatalog::UDECIMAL) &&
          (rightColDataType == SystemCatalog::DECIMAL ||
           rightColDataType == SystemCatalog::UDECIMAL));
}

} // end of namespace datatypes




namespace datatypes
{

static constexpr int128_t minInt128 = int128_t(0x8000000000000000LL) << 64;
static constexpr int128_t maxInt128 = (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;

class ConstString
{
  const char *m_str;
  size_t m_length;
public:
  ConstString(const char *str, size_t length)
   :m_str(str), m_length(length)
  { }
  const char *str() const { return m_str; }
  const char *end() const { return m_str + m_length; }
  size_t length() const { return m_length; }
  void bin2hex(char *o)
  {
    static const char hexdig[] = { '0', '1', '2', '3', '4', '5', '6', '7',
                                   '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    const char *e= end();
    for (const char *s= m_str; s < e; s++)
    {
      *o++ = hexdig[*s >> 4];
      *o++ = hexdig[*s & 0xf];
    }
  }
};


enum class round_style_t: uint8_t
{
  NONE= 0,
  POS= 0x01,
  NEG= 0x80
};


class SessionParam
{
  const char *m_tzname;
public:
  SessionParam(const char *tzname)
   :m_tzname(tzname)
  { }
  const char *tzname() const
  {
    return m_tzname;
  }
};


class ConvertFromStringParam
{
  const std::string& m_timeZone;
  const bool m_noRoundup;
  const bool m_isUpdate;
public:
  ConvertFromStringParam(const std::string& timeZone,
                         bool noRoundup, bool isUpdate)
   :m_timeZone(timeZone),
    m_noRoundup(noRoundup),
    m_isUpdate(isUpdate)
  { }
  const std::string& timeZone() const { return m_timeZone; }
  bool noRoundup() const { return m_noRoundup; }
  bool isUpdate() const { return m_isUpdate; }
};


class SimpleValue
{
  int64_t  m_sint64;
  int128_t m_sint128;
  const char *m_tzname;
public:
  SimpleValue(const int64_t sint64,
              const int128_t &sint128,
              const char *tzname)
   :m_sint64(sint64), m_sint128(sint128), m_tzname(tzname)
  { }
  SimpleValue()
   :m_sint64(0), m_sint128(0), m_tzname(0)
  { }
  int64_t toSInt64() const { return m_sint64; }
  uint64_t toUInt64() const { return static_cast<uint64_t>(m_sint64); }
  int128_t toSInt128() const { return m_sint128; }
  const char *tzname() const { return m_tzname; }
};


class SimpleValueSInt64: public SimpleValue
{
public:
  SimpleValueSInt64(int64_t value)
   :SimpleValue(value, 0, NULL)
  { }
};


class SimpleValueUInt64: public SimpleValue
{
public:
  SimpleValueUInt64(uint64_t value)
   :SimpleValue(static_cast<int64_t>(value), 0, NULL)
  { }
};


class SimpleValueSInt128: public SimpleValue
{
public:
  SimpleValueSInt128(int128_t value)
   :SimpleValue(0, value, NULL)
  { }
};


class SimpleValueTimestamp: public SimpleValue
{
public:
  SimpleValueTimestamp(uint64_t value, const char *tzname)
   :SimpleValue(static_cast<int64_t>(value), 0, tzname)
  { }
};


class MinMaxInfo
{
public:
  int64_t min;
  int64_t max;
  union
  {
    int128_t int128Min;
    int64_t min_;
  };
  union
  {
    int128_t int128Max;
    int64_t max_;
  };
  MinMaxInfo()
   :min((uint64_t)0x8000000000000001ULL),
    max((uint64_t) - 0x8000000000000001LL)
  {
    int128Min = datatypes::minInt128;
    int128Max = datatypes::maxInt128;
  };
  bool isEmptyOrNullSInt64() const
  {
    return min == std::numeric_limits<int64_t>::max() &&
           max == std::numeric_limits<int64_t>::min();
  }
  bool isEmptyOrNullUInt64() const
  {
    return static_cast<uint64_t>(min) == std::numeric_limits<uint64_t>::max() &&
           static_cast<uint64_t>(max) == std::numeric_limits<uint64_t>::min();
  }
  bool isEmptyOrNullSInt128() const
  {
    return int128Min == datatypes::maxInt128 &&
           int128Max == datatypes::minInt128;
  }
  void widenSInt64(const MinMaxInfo &partInfo)
  {
    min = partInfo.min < min ? partInfo.min : min;
    max = partInfo.max > max ? partInfo.max : max;
  }
  void widenUInt64(const MinMaxInfo &partInfo)
  {
    min = static_cast<uint64_t>(static_cast<uint64_t>(partInfo.min) < static_cast<uint64_t>(min) ? partInfo.min : min);
    max = static_cast<uint64_t>(static_cast<uint64_t>(partInfo.max) > static_cast<uint64_t>(max) ? partInfo.max : max);
  }
  void widenSInt128(const MinMaxInfo &partInfo)
  {
    int128Min = partInfo.int128Min < int128Min ? partInfo.int128Min : int128Min;
    int128Max = partInfo.int128Max > int128Max ? partInfo.int128Max : int128Max;
  }
  static MinMaxInfo widenSInt64(const MinMaxInfo &a,
                                const MinMaxInfo &b)
  {
    MinMaxInfo tmp= a;
    tmp.widenSInt64(b);
    return tmp;
  }
  static MinMaxInfo widenUInt64(const MinMaxInfo &a,
                                const MinMaxInfo &b)
  {
    MinMaxInfo tmp(a);
    tmp.widenUInt64(b);
    return tmp;
  }
  static MinMaxInfo widenSInt128(const MinMaxInfo &a,
                                 const MinMaxInfo &b)
  {
    MinMaxInfo tmp= a;
    tmp.widenSInt128(b);
    return tmp;
  }
};


class MinMaxPartitionInfo: public MinMaxInfo
{
  enum status_flag_t : uint64_t
  {
    ET_DISABLED = 0x0002,
    CPINVALID = 0x0004
  };
  uint64_t m_status;
public:
  MinMaxPartitionInfo()
   :m_status(0)
  { };
  MinMaxPartitionInfo(const BRM::EMEntry &entry);
  void set_invalid() { m_status|= CPINVALID; }
  bool is_invalid() const { return m_status & CPINVALID; }
  bool is_disabled() const { return m_status & ET_DISABLED; }

  bool isSuitableSInt64(const SimpleValue &startVal,
                        round_style_t rfMin,
                        const SimpleValue &endVal,
                        round_style_t rfMax) const
  {
    if (min >= startVal.toSInt64() &&
        max <= endVal.toSInt64() &&
        !(min == std::numeric_limits<int64_t>::max() &&
          max == std::numeric_limits<int64_t>::min()))
    {
      if (rfMin == round_style_t::POS && min == startVal.toSInt64())
        return false;

      if (rfMax == round_style_t::NEG && max == endVal.toSInt64())
        return false;

      return true;
    }
    return false;
  }
  bool isSuitableUInt64(const SimpleValue &startVal,
                        round_style_t rfMin,
                        const SimpleValue &endVal,
                        round_style_t rfMax) const
  {
    if (static_cast<uint64_t>(min) >= startVal.toUInt64() &&
        static_cast<uint64_t>(max) <= endVal.toUInt64() &&
        !(static_cast<uint64_t>(min) == std::numeric_limits<uint64_t>::max() &&
          max == 0))
    {
      if (rfMin == round_style_t::POS && min == startVal.toSInt64())
        return false;

      if (rfMax == round_style_t::NEG && max == endVal.toSInt64())
        return false;
      return true;
    }
    return false;
  }
  bool isSuitableSInt128(const SimpleValue &startVal,
                         round_style_t rfMin,
                         const SimpleValue &endVal,
                         round_style_t rfMax) const
  {
    if (int128Min >= startVal.toSInt128() &&
        int128Max <= endVal.toSInt128() &&
        !(int128Min == datatypes::maxInt128 &&
          int128Max == datatypes::minInt128))
    {
      if (rfMin == round_style_t::POS && int128Min == startVal.toSInt128())
        return false;

      if (rfMax == round_style_t::NEG && int128Max == endVal.toSInt128())
        return false;

      return true;
    }
    return false;
  }
};


class ColBatchWriter
{
  FILE *m_filePtr;
  char m_delimiter;
  char m_enclosed_by;
  bool m_utf8;
public:
  ColBatchWriter(FILE *f,
                 char delimiter,
                 char enclosed_by,
                 bool utf8)
   :m_filePtr(f),
    m_delimiter(delimiter),
    m_enclosed_by(enclosed_by),
    m_utf8(utf8)
  { }
  FILE *filePtr() const { return m_filePtr; }
  char delimiter() const { return m_delimiter; }
  char enclosed_by() const { return m_enclosed_by; }
  bool utf8() const { return m_utf8; }
};


class SimpleColumnParam
{
  uint32_t m_sessionid;
  bool m_columnStore;
public:
  SimpleColumnParam(uint32_t sessionid, bool columnStore)
   :m_sessionid(sessionid),
    m_columnStore(columnStore)
  { }
  uint32_t sessionid() const { return m_sessionid; }
  bool columnStore() const { return m_columnStore; }
  void columnStore(bool value) { m_columnStore= value; }
};


class DatabaseQualifiedColumnName
{
  std::string m_db;
  std::string m_table;
  std::string m_column;
public:
  DatabaseQualifiedColumnName(const std::string &db,
                              const std::string &table,
                              const std::string &column)
   :m_db(db),
    m_table(table),
    m_column(column)
  { }
  const std::string &db() const { return m_db; }
  const std::string &table() const { return m_table; }
  const std::string &column() const { return m_column; }
};


class StoreField
{
public:
  virtual ~StoreField() {}
  virtual int32_t colWidth() const = 0;
  virtual int32_t precision() const = 0;
  virtual int32_t scale() const = 0;
  virtual int store_date(int64_t val) = 0;
  virtual int store_datetime(int64_t val) = 0;
  virtual int store_time(int64_t val) = 0;
  virtual int store_timestamp(int64_t val) = 0;
  virtual int store_string(const char *str, size_t length) = 0;
  virtual int store_varbinary(const char *str, size_t length) = 0;
  virtual int store_xlonglong(int64_t val) = 0;
  virtual int store_float(float val) = 0;
  virtual int store_double(double val) = 0;
  virtual int store_long_double(long double val) = 0;
  virtual int store_decimal64(const datatypes::VDecimal& dec) = 0;
  virtual int store_decimal128(const datatypes::VDecimal& dec) = 0;
  virtual int store_lob(const char *str, size_t length) = 0;
};


class WriteBatchField
{
public:
  virtual ~WriteBatchField() { }
  virtual size_t ColWriteBatchDate(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchDatetime(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchTime(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchTimestamp(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchChar(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchVarchar(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchSInt64(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchUInt64(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchSInt32(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchUInt32(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchSInt16(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchUInt16(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchSInt8(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchUInt8(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchXFloat(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchXDouble(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchSLongDouble(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchXDecimal(const unsigned char *buf, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchVarbinary(const unsigned char *buf0, bool nullVal, ColBatchWriter &ci) = 0;
  virtual size_t ColWriteBatchBlob(const unsigned char *buf0, bool nullVal, ColBatchWriter &ci) = 0;
};


class TypeHandler
{
public:
  using code_t = datatypes::SystemCatalog::ColDataType;
protected:
  std::string formatPartitionInfoSInt64(const SystemCatalog::TypeAttributesStd &attr,
                                        const MinMaxInfo &i)
                                        const;
  std::string formatPartitionInfoUInt64(const SystemCatalog::TypeAttributesStd &attr,
                                        const MinMaxInfo &i)
                                        const;

  std::string PrintPartitionValueSInt64(const SystemCatalog::TypeAttributesStd &attr,
                                        const MinMaxPartitionInfo &partInfo,
                                        const SimpleValue &startVal,
                                        round_style_t rfMin,
                                        const SimpleValue &endVal,
                                        round_style_t rfMax) const;

  std::string PrintPartitionValueUInt64(const SystemCatalog::TypeAttributesStd &attr,
                                        const MinMaxPartitionInfo &partInfo,
                                        const SimpleValue &startVal,
                                        round_style_t rfMin,
                                        const SimpleValue &endVal,
                                        round_style_t rfMax) const;

public:
  static const TypeHandler *find(SystemCatalog::ColDataType typeCode,
                                 const SystemCatalog::TypeAttributesStd &attr);
  static const TypeHandler *find_by_ddltype(const ddlpackage::ColumnType &ct);
  virtual ~TypeHandler() { }
  virtual const string & name() const= 0;
  virtual const string print(const SystemCatalog::TypeAttributesStd &attr) const
  {
    return name();
  }
  virtual code_t code() const= 0;
  virtual bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const
  {
    return false;
  }
  virtual uint8_t PartitionValueCharLength(const SystemCatalog::TypeAttributesStd &attr)
                                           const
  {
    return 30;
  }
  virtual size_t ColWriteBatch(WriteBatchField *field,
                               const unsigned char *buf,
                               bool nullVal,
                               ColBatchWriter & writer) const= 0;
  virtual int storeValueToField(rowgroup::Row &row, int pos,
                                StoreField *f) const= 0;

  virtual std::string format(const SimpleValue &v,
                             const SystemCatalog::TypeAttributesStd &attr) const = 0;

  virtual std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const= 0;
  virtual execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                                  SystemCatalog::TypeHolderStd &ct,
                                                  const SimpleColumnParam &prm)
                                                  const= 0;
  virtual SimpleValue getMinValueSimple() const
  {
    return SimpleValue(std::numeric_limits<int64_t>::min(),
                       std::numeric_limits<int64_t>::min(),
                       0);
  }
  virtual SimpleValue getMaxValueSimple() const
  {
    return SimpleValue(std::numeric_limits<int64_t>::max(),
                       std::numeric_limits<int64_t>::max(),
                       0);
  }
  virtual SimpleValue toSimpleValue(const SessionParam &sp,
                                    const SystemCatalog::TypeAttributesStd &attr,
                                    const char *str, round_style_t & rf) const= 0;
  virtual MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                                     const MinMaxInfo &a,
                                     const MinMaxInfo &b) const
  {
    return MinMaxInfo::widenSInt64(a, b);
  }
  virtual MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                                     BRM::DBRM &em,
                                                     const BRM::EMEntry &entry,
                                                     int *state) const;
  virtual string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                                     const MinMaxPartitionInfo &partInfo,
                                     const SimpleValue &startVal,
                                     round_style_t rfMin,
                                     const SimpleValue &endVal,
                                     round_style_t rfMax) const
  {
    return PrintPartitionValueSInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  virtual bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                                   const MinMaxPartitionInfo &part,
                                   const SimpleValue &startVal,
                                   round_style_t rfMin,
                                   const SimpleValue &endVal,
                                   round_style_t rfMax) const
  {
    return part.isSuitableSInt64(startVal, rfMin, endVal, rfMax);
  }
  virtual boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                   const = 0;

  virtual boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                       const ConvertFromStringParam& prm,
                                       const std::string& str,
                                       bool& pushWarning) const = 0;
};


// QQ: perhaps not needed yet
class TypeHandlerBit: public TypeHandler
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::BIT;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    idbassert(0); // QQ
    return 0;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    idbassert(0); // QQ
    return 1;
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return "0"; // QQ
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i) const override
  {
    idbassert(0);
    return "Error";
  }

  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override
  {
    idbassert(0);
    return NULL;
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override
  {
    idbassert(0);
    return SimpleValue();
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override
  {
    //TODO: How to communicate with write engine?
    return boost::any();
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerInt: public TypeHandler
{
protected:
  int storeValueToFieldSInt32(rowgroup::Row &row, int pos,
                              StoreField *f) const;
  int storeValueToFieldUInt32(rowgroup::Row &row, int pos,
                              StoreField *f) const;
  std::string formatSInt64(const SimpleValue &v,
                           const SystemCatalog::TypeAttributesStd &attr) const;
  std::string formatUInt64(const SimpleValue &v,
                           const SystemCatalog::TypeAttributesStd &attr) const;

};


class TypeHandlerSInt8: public TypeHandlerInt
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::TINYINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSInt8(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatSInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int8_t>::min());
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int8_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSInt16: public TypeHandlerInt
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::SMALLINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSInt16(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatSInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int16_t>::min());
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int16_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSInt24: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::MEDINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSInt32(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldSInt32(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatSInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(MIN_MEDINT);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(MAX_MEDINT);
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSInt32: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::INT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSInt32(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldSInt32(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatSInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int32_t>::min());
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int32_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSInt64: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::BIGINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSInt64(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatSInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int64_t>::min());
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int64_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUInt8: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UTINYINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchUInt8(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatUInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoUInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;

  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueUInt64(std::numeric_limits<uint8_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  {
    return MinMaxInfo::widenUInt64(a, b);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueUInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableUInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUInt16: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::USMALLINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchUInt16(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatUInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoUInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
      return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueUInt64(std::numeric_limits<uint16_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  {
    return MinMaxInfo::widenUInt64(a, b);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueUInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableUInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUInt24: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UMEDINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchUInt32(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldUInt32(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatUInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoUInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
      return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
      return SimpleValueUInt64(MAX_UMEDINT);
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  {
    return MinMaxInfo::widenUInt64(a, b);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueUInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableUInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUInt32: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchUInt32(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldUInt32(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatUInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoUInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
      return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
      return SimpleValueUInt64(std::numeric_limits<uint32_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  {
    return MinMaxInfo::widenUInt64(a, b);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueUInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableUInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUInt64: public TypeHandlerInt
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::BIGINT;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchUInt64(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return formatUInt64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoUInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueUInt64(std::numeric_limits<uint64_t>::max());
  }
  SimpleValue toSimpleValue(const SessionParam &sp,
                               const SystemCatalog::TypeAttributesStd &attr,
                               const char *str, round_style_t & rf) const override;
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  {
    return MinMaxInfo::widenUInt64(a, b);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueUInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableUInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerXDecimal: public TypeHandler
{
protected:

  static bool isValidXDecimal64(const SystemCatalog::TypeAttributesStd &attr)
  {
    return attr.colWidth <= 8;
  }
  static bool isValidXDecimal128(const SystemCatalog::TypeAttributesStd &attr)
  {
    return attr.colWidth == 16;
  }
  int storeValueToField64(rowgroup::Row &row, int pos,
                          StoreField *f) const;
  int storeValueToField128(rowgroup::Row &row, int pos,
                           StoreField *f) const;

  std::string format64(const SimpleValue &v,
                       const SystemCatalog::TypeAttributesStd &attr) const;
  std::string format128(const SimpleValue &v,
                        const SystemCatalog::TypeAttributesStd &attr) const;
  std::string formatPartitionInfo128(const SystemCatalog::TypeAttributesStd &attr,
                                     const MinMaxInfo &i)
                                     const;

  MinMaxPartitionInfo getExtentPartitionInfo64(const SystemCatalog::TypeAttributesStd &attr,
                                               BRM::DBRM &em,
                                               const BRM::EMEntry &entry,
                                               int *state) const;
  MinMaxPartitionInfo getExtentPartitionInfo128(const SystemCatalog::TypeAttributesStd &attr,
                                                BRM::DBRM &em,
                                                const BRM::EMEntry &entry,
                                                int *state) const;
  string PrintPartitionValue128(const SystemCatalog::TypeAttributesStd &attr,
                                const MinMaxPartitionInfo &partInfo,
                                const SimpleValue &startVal,
                                round_style_t rfMin,
                                const SimpleValue &endVal,
                                round_style_t rfMax) const;

public:
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchXDecimal(buf, nullVal, writer);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
};


class TypeHandlerSDecimal64: public TypeHandlerXDecimal
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::DECIMAL;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  uint8_t PartitionValueCharLength(const SystemCatalog::TypeAttributesStd &attr)
                                   const override
  {
    return 30;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToField64(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return format64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i) const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int64_t>::min());
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt64(std::numeric_limits<int64_t>::max());
  }
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  { 
    return MinMaxInfo::widenSInt64(a, b);
  }
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override
  {
    return getExtentPartitionInfo64(attr, em, entry, state);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueSInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableSInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUDecimal64: public TypeHandlerXDecimal
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UDECIMAL;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  uint8_t PartitionValueCharLength(const SystemCatalog::TypeAttributesStd &attr)
                                   const override
  {
    return 30;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToField64(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return format64(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i) const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueUInt64(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueUInt64(std::numeric_limits<uint64_t>::max());
  }
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  { 
    return MinMaxInfo::widenSInt64(a, b);
  }
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override
  {
    return getExtentPartitionInfo64(attr, em, entry, state);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValueSInt64(attr, partInfo,
                                     startVal, rfMin,
                                     endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableSInt64(startVal, rfMin, endVal, rfMax);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSDecimal128: public TypeHandlerXDecimal
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::DECIMAL;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  uint8_t PartitionValueCharLength(const SystemCatalog::TypeAttributesStd &attr)
                                   const override
  {
    return Decimal::MAXLENGTH16BYTES;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToField128(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return format128(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i) const override
  {
    return formatPartitionInfo128(attr, i);
  }
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValue(std::numeric_limits<int64_t>::min(),
                       datatypes::minInt128,
                       0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValue(std::numeric_limits<int64_t>::max(),
                       datatypes::maxInt128,
                       0);
  }
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  { 
    return MinMaxInfo::widenSInt128(a, b);
  }
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override
  {
    return getExtentPartitionInfo128(attr, em, entry, state);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValue128(attr, partInfo, startVal, rfMin, endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableSInt128(startVal, rfMin, endVal, rfMax);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUDecimal128: public TypeHandlerXDecimal
{
public:
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UDECIMAL;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  uint8_t PartitionValueCharLength(const SystemCatalog::TypeAttributesStd &attr)
                                   const override
  {
    return Decimal::MAXLENGTH16BYTES;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToField128(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return format128(v, attr);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i) const override
  {
    return formatPartitionInfo128(attr, i);
  }
  SimpleValue getMinValueSimple() const override
  {
    return SimpleValueSInt128(0);
  }
  SimpleValue getMaxValueSimple() const override
  {
    return SimpleValueSInt128(-1);
  }
  MinMaxInfo widenMinMaxInfo(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxInfo &a,
                             const MinMaxInfo &b) const override
  { 
    return MinMaxInfo::widenSInt128(a, b);
  }
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override
  {
    return getExtentPartitionInfo128(attr, em, entry, state);
  }
  string PrintPartitionValue(const SystemCatalog::TypeAttributesStd &attr,
                             const MinMaxPartitionInfo &partInfo,
                             const SimpleValue &startVal,
                             round_style_t rfMin,
                             const SimpleValue &endVal,
                             round_style_t rfMax) const override
  {
    return PrintPartitionValue128(attr, partInfo, startVal, rfMin, endVal, rfMax);
  }
  bool isSuitablePartition(const SystemCatalog::TypeAttributesStd &attr,
                           const MinMaxPartitionInfo &part,
                           const SimpleValue &startVal,
                           round_style_t rfMin,
                           const SimpleValue &endVal,
                           round_style_t rfMax) const override
  {
    return part.isSuitableSInt128(startVal, rfMin, endVal, rfMax);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerReal: public TypeHandler
{
public:
  int storeValueToFieldXFloat(rowgroup::Row &row, int pos,
                              StoreField *f) const;
  int storeValueToFieldXDouble(rowgroup::Row &row, int pos,
                               StoreField *f) const;
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override
  {
    return SimpleValue(); // QQ: real types were not handled in IDB_format()
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return "0"; // QQ
  }
};


class TypeHandlerSFloat: public TypeHandlerReal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::FLOAT;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchXFloat(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldXFloat(row, pos, f);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i)
                                  const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSDouble: public TypeHandlerReal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::DOUBLE;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchXDouble(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldXDouble(row, pos, f);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUFloat: public TypeHandlerReal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UFLOAT;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchXFloat(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldXFloat(row, pos, f);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerUDouble: public TypeHandlerReal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::UDOUBLE;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchXDouble(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldXDouble(row, pos, f);
  }
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerSLongDouble: public TypeHandlerReal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::LONGDOUBLE;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchSLongDouble(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    idbassert(0);
    return "Error";
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override
  {
    // QQ: DDLPackageProcessor::getNullValueForType() did not handle LONGDOUBLE
    return boost::any();
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override
  {
    throw logging::QueryDataExcept("convertColumnData: unknown column data type.", logging::dataTypeErr);
    return boost::any();
  }
};


class TypeHandlerTemporal: public TypeHandler
{
public:
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
};


class TypeHandlerDate: public TypeHandlerTemporal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::DATE;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchDate(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerDatetime: public TypeHandlerTemporal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::DATETIME;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchDatetime(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerTime: public TypeHandlerTemporal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::TIME;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchTime(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerTimestamp: public TypeHandlerTemporal
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::TIMESTAMP;
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return true;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchTimestamp(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerStr: public TypeHandler
{
protected:
  std::string formatPartitionInfoSmallCharVarchar(
                                  const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i)
                                  const;
  boost::any getNullValueForTypeVarcharText(const SystemCatalog::TypeAttributesStd &attr)
                                                                  const;
public:
  int storeValueToFieldCharVarchar(rowgroup::Row &row, int pos,
                                   StoreField *f) const;
  int storeValueToFieldBlobText(rowgroup::Row &row, int pos,
                                StoreField *f) const;
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                          const MinMaxInfo &i)
                                          const override
  {
    // QQ: Check with Roman if this correct:
    return formatPartitionInfoSInt64(attr, i);
  }
  execplan::SimpleColumn *newSimpleColumn(const DatabaseQualifiedColumnName &name,
                                          SystemCatalog::TypeHolderStd &ct,
                                          const SimpleColumnParam &prm)
                                          const override;
  SimpleValue toSimpleValue(const SessionParam &sp,
                            const SystemCatalog::TypeAttributesStd &attr,
                            const char *str, round_style_t & rf) const override;
};


class TypeHandlerChar: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::CHAR;
  }
  const string print(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    ostringstream oss;
    oss << name () << "(" << attr.colWidth << ")";
    return oss.str();
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return attr.colWidth <= 8;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchChar(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldCharVarchar(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i)
                                  const override;
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerVarchar: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::VARCHAR;
  }
  const string print(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    ostringstream oss;
    oss << name () << "(" << attr.colWidth << ")";
    return oss.str();
  }
  bool CP_type(const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return attr.colWidth <= 7;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchVarchar(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldCharVarchar(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  std::string formatPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                  const MinMaxInfo &i)
                                  const override;
  MinMaxPartitionInfo getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd &attr,
                                             BRM::DBRM &em,
                                             const BRM::EMEntry &entry,
                                             int *state) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override
  {
    return getNullValueForTypeVarcharText(attr);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerVarbinary: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::VARBINARY;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchVarbinary(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override;
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override;
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerBlob: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::BLOB;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchBlob(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldBlobText(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return "0"; // QQ
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override;
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerText: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::TEXT;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    return field->ColWriteBatchBlob(buf, nullVal, writer);
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    return storeValueToFieldBlobText(row, pos, f);
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return "0"; // QQ
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override
  {
    return getNullValueForTypeVarcharText(attr);
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


class TypeHandlerClob: public TypeHandlerStr
{
  const string &name() const override;
  code_t code() const override
  {
    return SystemCatalog::CLOB;
  }
  size_t ColWriteBatch(WriteBatchField *field,
                       const unsigned char *buf,
                       bool nullVal,
                       ColBatchWriter & writer) const override
  {
    idbassert(0); // QQ
    return 0;
  }
  int storeValueToField(rowgroup::Row &row, int pos,
                        StoreField *f) const override
  {
    idbassert(0); // QQ
    return 1;
  }
  std::string format(const SimpleValue &v,
                     const SystemCatalog::TypeAttributesStd &attr) const override
  {
    return "0"; // QQ
  }
  boost::any getNullValueForType(const SystemCatalog::TypeAttributesStd &attr)
                                                                const override
  {
    return boost::any(); // QQ
  }
  boost::any convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                               const ConvertFromStringParam& prm,
                               const std::string& str,
                               bool& pushWarning) const override;
};


}// end of namespace datatypes

#endif //MCS_DATATYPE_H_INCLUDED

// vim:ts=2 sw=2:
