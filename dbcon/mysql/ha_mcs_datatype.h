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
#ifndef HA_MCS_DATATYPE_H_INCLUDED
#define HA_MCS_DATATYPE_H_INCLUDED

/*
  Interface classes for MariaDB data types (e.g. Field) for TypeHandler.
  These classes are needed to avoid TypeHandler dependency on
  MariaDB header files.
*/

namespace datatypes
{
class StoreFieldMariaDB : public StoreField
{
  Field* m_field;
  const CalpontSystemCatalog::ColType& m_type;
  long m_timeZone;

 public:
  StoreFieldMariaDB(Field* f, const CalpontSystemCatalog::ColType& type, const long timeZone)
   : m_field(f), m_type(type), m_timeZone(timeZone)
  {
  }

  const CalpontSystemCatalog::ColType& type() const
  {
    return m_type;
  }
  int32_t colWidth() const override
  {
    return m_type.colWidth;
  }
  int32_t precision() const override
  {
    return m_type.precision;
  }
  int32_t scale() const override
  {
    return m_type.scale;
  }

  int store_date(int64_t val) override
  {
    char tmp[256];
    DataConvert::dateToString(val, tmp, sizeof(tmp) - 1);
    return store_string(tmp, strlen(tmp));
  }

  int store_datetime(int64_t val) override
  {
    char tmp[256];
    DataConvert::datetimeToString(val, tmp, sizeof(tmp) - 1, m_type.precision);
    return store_string(tmp, strlen(tmp));
  }

  int store_time(int64_t val) override
  {
    char tmp[256];
    DataConvert::timeToString(val, tmp, sizeof(tmp) - 1, m_type.precision);
    return store_string(tmp, strlen(tmp));
  }

  int store_timestamp(int64_t val) override
  {
    char tmp[256];
    DataConvert::timestampToString(val, tmp, sizeof(tmp), m_timeZone, m_type.precision);
    return store_string(tmp, strlen(tmp));
  }

  int store_string(const char* str, size_t length) override
  {
    return m_field->store(str, length, m_field->charset());
  }
  int store_varbinary(const char* str, size_t length) override
  {
    if (get_varbin_always_hex(current_thd))
    {
      size_t ll = length * 2;
      boost::scoped_array<char> sca(new char[ll]);
      ConstString(str, length).bin2hex(sca.get());
      return m_field->store_binary(sca.get(), ll);
    }
    return m_field->store_binary(str, length);
  }

  int store_xlonglong(int64_t val) override
  {
    idbassert(dynamic_cast<Field_num*>(m_field));
    return m_field->store(val, static_cast<Field_num*>(m_field)->unsigned_flag);
  }

  int store_float(float dl) override
  {
    if (dl == std::numeric_limits<float>::infinity())
    {
      m_field->set_null();
      return 1;
    }

    // bug 3485, reserve enough space for the longest float value
    // -3.402823466E+38 to -1.175494351E-38, 0, and
    // 1.175494351E-38 to 3.402823466E+38.
    m_field->field_length = 40;
    return m_field->store(dl);
  }

  int store_double(double dl) override
  {
    if (dl == std::numeric_limits<double>::infinity())
    {
      m_field->set_null();
      return 1;
    }

    if (m_field->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      char buf[310];
      // reserve enough space for the longest double value
      // -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and
      // 2.2250738585072014E-308 to 1.7976931348623157E+308.
      snprintf(buf, 310, "%.18g", dl);
      return m_field->store(buf, strlen(buf), m_field->charset());
    }

    // The server converts dl=-0 to dl=0 in (*f)->store().
    // This happens in the call to truncate_double().
    // This is an unexpected behaviour, so we directly store the
    // double value using the lower level float8store() function.
    // TODO Remove this when (*f)->store() handles this properly.
    m_field->field_length = 310;
    if (dl == 0)
    {
      float8store(m_field->ptr, dl);
      return 0;
    }
    return m_field->store(dl);
  }

  int store_long_double(long double dl) override
  {
    if (dl == std::numeric_limits<long double>::infinity())
    {
      m_field->set_null();
      return 1;
    }

    if (m_field->type() == MYSQL_TYPE_NEWDECIMAL)
    {
      char buf[310];
      snprintf(buf, 310, "%.20Lg", dl);
      return m_field->store(buf, strlen(buf), m_field->charset());
    }

    // reserve enough space for the longest double value
    // -1.7976931348623157E+308 to -2.2250738585072014E-308, 0, and
    // 2.2250738585072014E-308 to 1.7976931348623157E+308.
    m_field->field_length = 310;
    return m_field->store(static_cast<double>(dl));
  }

  int store_decimal64(const datatypes::Decimal& dec) override
  {
    std::string decAsAStr = dec.toString();
    return m_field->store(decAsAStr.c_str(), decAsAStr.length(), m_field->charset());
  }

  int store_decimal128(const datatypes::Decimal& dec) override
  {
    std::string decAsAStr = dec.toString(true);
    return m_field->store(decAsAStr.c_str(), decAsAStr.length(), m_field->charset());
  }

  int store_lob(const char* str, size_t length) override
  {
    idbassert(dynamic_cast<Field_blob*>(m_field));
    Field_blob* f2 = static_cast<Field_blob*>(m_field);
    f2->set_ptr(length, (uchar*)str);
    return 0;
  }
};

/*******************************************************************************/
class WriteBatchFieldMariaDB : public WriteBatchField
{
  // Maximum number of decimal digits that can be represented in 4 bytes
  static const int DIG_PER_DEC = 9;
  // See strings/decimal.c
  const int dig2bytes[DIG_PER_DEC + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  // Returns the number of bytes required to store a given number
  // of decimal digits
  int numDecimalBytes(int digits)
  {
    return (((digits / DIG_PER_DEC) * 4) + dig2bytes[digits % DIG_PER_DEC]);
  }

 public:
  Field* m_field;
  const CalpontSystemCatalog::ColType& m_type;
  uint32_t m_mbmaxlen;
  long m_timeZone;
  WriteBatchFieldMariaDB(Field* field, const CalpontSystemCatalog::ColType& type, uint32_t mbmaxlen,
                         const long timeZone)
   : m_field(field), m_type(type), m_mbmaxlen(mbmaxlen), m_timeZone(timeZone)
  {
  }
  size_t ColWriteBatchDate(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    // QQ: OLD DATE is not handled
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    }
    else
    {
      const uchar* tmp1 = buf;
      uint32_t tmp = (tmp1[2] << 16) + (tmp1[1] << 8) + tmp1[0];
      int day = tmp & 0x0000001fl;
      int month = (tmp >> 5) & 0x0000000fl;
      int year = tmp >> 9;
      fprintf(ci.filePtr(), "%04d-%02d-%02d%c", year, month, day, ci.delimiter());
    }
    return 3;
  }
  size_t ColWriteBatchDatetime(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());

      if (m_field->real_type() == MYSQL_TYPE_DATETIME2)
        return m_field->pack_length();
      else
        return 8;
    }

    if (m_field->real_type() == MYSQL_TYPE_DATETIME2)
    {
      // mariadb 10.1 compatibility -- MYSQL_TYPE_DATETIME2 introduced in mysql 5.6
      MYSQL_TIME ltime;
      longlong tmp = my_datetime_packed_from_binary(buf, m_field->decimals());
      TIME_from_longlong_datetime_packed(&ltime, tmp);

      if (!ltime.second_part)
      {
        fprintf(ci.filePtr(), "%04d-%02d-%02d %02d:%02d:%02d%c", ltime.year, ltime.month, ltime.day,
                ltime.hour, ltime.minute, ltime.second, ci.delimiter());
      }
      else
      {
        fprintf(ci.filePtr(), "%04d-%02d-%02d %02d:%02d:%02d.%ld%c", ltime.year, ltime.month, ltime.day,
                ltime.hour, ltime.minute, ltime.second, ltime.second_part, ci.delimiter());
      }

      return m_field->pack_length();
    }

    // Old DATETIME
    long long value = *((long long*)buf);
    long datePart = (long)(value / 1000000ll);
    int day = datePart % 100;
    int month = (datePart / 100) % 100;
    int year = datePart / 10000;
    fprintf(ci.filePtr(), "%04d-%02d-%02d ", year, month, day);

    long timePart = (long)(value - (long long)datePart * 1000000ll);
    int second = timePart % 100;
    int min = (timePart / 100) % 100;
    int hour = timePart / 10000;
    fprintf(ci.filePtr(), "%02d:%02d:%02d%c", hour, min, second, ci.delimiter());
    return 8;
  }
  size_t ColWriteBatchTime(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    // QQ: why old TIME is not handled?
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());
      return m_field->pack_length();
    }

    MYSQL_TIME ltime;
    longlong tmp = my_time_packed_from_binary(buf, m_field->decimals());
    TIME_from_longlong_time_packed(&ltime, tmp);

    if (ltime.neg)
      fprintf(ci.filePtr(), "-");

    if (!ltime.second_part)
    {
      fprintf(ci.filePtr(), "%02d:%02d:%02d%c", ltime.hour, ltime.minute, ltime.second, ci.delimiter());
    }
    else
    {
      fprintf(ci.filePtr(), "%02d:%02d:%02d.%ld%c", ltime.hour, ltime.minute, ltime.second, ltime.second_part,
              ci.delimiter());
    }

    return m_field->pack_length();
  }

  size_t ColWriteBatchTimestamp(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    // QQ: old TIMESTAMP is not handled

    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());
      return m_field->pack_length();
    }

    struct timeval tm;
    my_timestamp_from_binary(&tm, buf, m_field->decimals());

    MySQLTime time;
    gmtSecToMySQLTime(tm.tv_sec, time, m_timeZone);

    if (!tm.tv_usec)
    {
      fprintf(ci.filePtr(), "%04d-%02d-%02d %02d:%02d:%02d%c", time.year, time.month, time.day, time.hour,
              time.minute, time.second, ci.delimiter());
    }
    else
    {
      fprintf(ci.filePtr(), "%04d-%02d-%02d %02d:%02d:%02d.%ld%c", time.year, time.month, time.day, time.hour,
              time.minute, time.second, tm.tv_usec, ci.delimiter());
    }
    return m_field->pack_length();
  }

  static inline void ColWriteBatchTextStringPrintout(std::string& escape, const ColBatchWriter& ci)
  {
    boost::replace_all(escape, "\\", "\\\\");
    fprintf(ci.filePtr(), "%c%.*s%c%c", ci.enclosed_by(), (int)escape.length(), escape.c_str(),
            ci.enclosed_by(), ci.delimiter());
  }

  static void ColWriteBatchTextString(const String& value, const ColBatchWriter& ci,
                                      const size_t colWidthInBytes)
  {
    std::string escape;
    escape.assign(value.ptr(), value.length());
    ColWriteBatchTextStringPrintout(escape, ci);
  }

  static void ColWriteBatchPaddedTextString(const String& value, const ColBatchWriter& ci,
                                            const size_t colWidthInBytes)
  {
    std::string escape;
    escape.assign(value.ptr(), colWidthInBytes);
    ColWriteBatchTextStringPrintout(escape, ci);
  }

  static void ColWriteBatchBlobString(const String& value, const ColBatchWriter& ci,
                                      const size_t colWidthInBytes)
  {
    const char* ptr = value.ptr();
    for (uint32_t i = 0; i < value.length(); i++)
    {
      fprintf(ci.filePtr(), "%02x", *(uint8_t*)(ptr + i));
    }
    fprintf(ci.filePtr(), "%c", ci.delimiter());
  }

  size_t ColWriteBatchString(const uchar* buf, bool nullVal, ColBatchWriter& ci,
                             void (*printFuncPtr)(const String&, const ColBatchWriter&,
                                                  const size_t colWidthInBytes)) const
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    }
    else
    {
      String value;
      // We need to set table->read_set for a field first.
      // This happens in ha_mcs_impl_start_bulk_insert().
      m_field->val_str(&value);
      (*printFuncPtr)(value, ci, m_field->pack_length());
    }
    return m_field->pack_length();
  }

  size_t ColWriteBatchChar(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    return (current_thd->variables.sql_mode & MODE_PAD_CHAR_TO_FULL_LENGTH)
               ? ColWriteBatchString(buf, nullVal, ci, &ColWriteBatchPaddedTextString)
               : ColWriteBatchString(buf, nullVal, ci, &ColWriteBatchTextString);
  }

  size_t ColWriteBatchVarchar(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    return ColWriteBatchString(buf, nullVal, ci, &ColWriteBatchTextString);
  }

  size_t ColWriteBatchSInt64(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%lld%c", *((long long*)buf), ci.delimiter());
    return 8;
  }

  size_t ColWriteBatchUInt64(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%llu%c", *((long long unsigned*)buf), ci.delimiter());
    return 8;
  }

  size_t ColWriteBatchSInt32(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%d%c", *((int32_t*)buf), ci.delimiter());
    return 4;
  }

  size_t ColWriteBatchUInt32(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%u%c", *((uint32_t*)buf), ci.delimiter());
    return 4;
  }

  size_t ColWriteBatchSInt16(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%d%c", *((int16_t*)buf), ci.delimiter());
    return 2;
  }

  size_t ColWriteBatchUInt16(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%u%c", *((uint16_t*)buf), ci.delimiter());
    return 2;
  }

  size_t ColWriteBatchSInt8(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%d%c", *((int8_t*)buf), ci.delimiter());
    return 1;
  }

  size_t ColWriteBatchUInt8(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%u%c", *((uint8_t*)buf), ci.delimiter());
    return 1;
  }

  size_t ColWriteBatchXFloat(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
    {
      float val = *((float*)buf);

      if ((fabs(val) > (1.0 / IDB_pow[4])) && (fabs(val) < (float)IDB_pow[6]))
      {
        fprintf(ci.filePtr(), "%.7f%c", val, ci.delimiter());
      }
      else
      {
        fprintf(ci.filePtr(), "%e%c", val, ci.delimiter());
      }

      // fprintf(ci.filePtr(), "%.7g|", *((float*)buf2));
      // printf("%.7f|", *((float*)buf2));
    }

    return 4;
  }

  size_t ColWriteBatchXDouble(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
    {
      fprintf(ci.filePtr(), "%.15g%c", *((double*)buf), ci.delimiter());
      // printf("%.15g|", *((double*)buf));
    }

    return 8;
  }

  size_t ColWriteBatchSLongDouble(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
      fprintf(ci.filePtr(), "%c", ci.delimiter());
    else
      fprintf(ci.filePtr(), "%.15Lg%c", *((long double*)buf), ci.delimiter());
    return sizeof(long double);
  }

  size_t ColWriteBatchXDecimal(const uchar* buf, bool nullVal, ColBatchWriter& ci) override
  {
    uint bytesBefore = numDecimalBytes(m_type.precision - m_type.scale);
    uint totalBytes = bytesBefore + numDecimalBytes(m_type.scale);

    if (nullVal && (m_type.constraintType != CalpontSystemCatalog::NOTNULL_CONSTRAINT))
    {
      fprintf(ci.filePtr(), "%c", ci.delimiter());
      // printf("|");
    }
    else if (m_type.precision > datatypes::INT64MAXPRECISION)
    {
      // TODO MCOL-641 The below else block for narrow decimal
      // i.e. (m_type.precision <= datatypes::INT64MAXPRECISION)
      // converts the decimal binary representation in buf directly
      // to a string, while here, the my_decimal ctor first calls
      // bin2decimal() on buf, and then we construct the string from
      // the my_decimal. This approach might be a bit slower than the
      // narrow decimal approach.
      my_decimal dec(buf, m_type.precision, m_type.scale);
      String str;
      dec.to_string(&str);
      fprintf(ci.filePtr(), "%s%c", str.c_ptr(), ci.delimiter());
    }
    else
    {
      uint32_t mask[5] = {0, 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF};
      char neg = '-';

      if (m_type.scale == 0)
      {
        const uchar* tmpBuf = buf;
        // test flag bit for sign
        bool posNum = tmpBuf[0] & (0x80);
        uchar tmpChr = tmpBuf[0];
        tmpChr ^= 0x80;  // flip the bit
        int32_t tmp1 = tmpChr;

        if (totalBytes > 4)
        {
          for (uint i = 1; i < (totalBytes - 4); i++)
          {
            tmp1 = (tmp1 << 8) + tmpBuf[i];
          }

          if ((tmp1 != 0) && (tmp1 != -1))
          {
            if (!posNum)
            {
              tmp1 = mask[totalBytes - 4] - tmp1;

              if (tmp1 != 0)
              {
                fprintf(ci.filePtr(), "%c", neg);
                // printf("%c", neg);
              }
            }

            if (tmp1 != 0)
            {
              fprintf(ci.filePtr(), "%d", tmp1);
              ////printf("%d", tmp1);
            }
          }

          int32_t tmp2 = tmpBuf[totalBytes - 4];

          for (uint i = (totalBytes - 3); i < totalBytes; i++)
          {
            tmp2 = (tmp2 << 8) + tmpBuf[i];
          }

          if (tmp1 != 0)
          {
            if (!posNum)
            {
              tmp2 = mask[4] - tmp2;

              if (tmp1 == -1)
              {
                fprintf(ci.filePtr(), "%c", neg);
                fprintf(ci.filePtr(), "%d%c", tmp2, ci.delimiter());
                ////printf("%c", neg);
                //////printf( "%d|", tmp2);
              }
              else
              {
                fprintf(ci.filePtr(), "%09u%c", tmp2, ci.delimiter());
                ////printf("%09u|", tmp2);
              }
            }
            else
            {
              fprintf(ci.filePtr(), "%09u%c", tmp2, ci.delimiter());
              // printf("%09u|", tmp2);
            }
          }
          else
          {
            if (!posNum)
            {
              tmp2 = mask[4] - tmp2;
              fprintf(ci.filePtr(), "%c", neg);
              // printf("%c", neg);
            }

            fprintf(ci.filePtr(), "%d%c", tmp2, ci.delimiter());
            // printf("%d|", tmp2);
          }
        }
        else
        {
          for (uint i = 1; i < totalBytes; i++)
          {
            tmp1 = (tmp1 << 8) + tmpBuf[i];
          }

          if (!posNum)
          {
            tmp1 = mask[totalBytes] - tmp1;
            fprintf(ci.filePtr(), "%c", neg);
            // printf("%c", neg);
          }

          fprintf(ci.filePtr(), "%d%c", tmp1, ci.delimiter());
          // printf("%d|", tmp1);
        }
      }
      else
      {
        const uchar* tmpBuf = buf;
        // test flag bit for sign
        bool posNum = tmpBuf[0] & (0x80);
        uchar tmpChr = tmpBuf[0];
        tmpChr ^= 0x80;  // flip the bit
        int32_t tmp1 = tmpChr;

        // fetch the digits before decimal point
        if (bytesBefore == 0)
        {
          if (!posNum)
          {
            fprintf(ci.filePtr(), "%c", neg);
            // printf("%c", neg);
          }

          fprintf(ci.filePtr(), "0.");
          // printf("0.");
        }
        else if (bytesBefore > 4)
        {
          for (uint i = 1; i < (bytesBefore - 4); i++)
          {
            tmp1 = (tmp1 << 8) + tmpBuf[i];
          }

          if (!posNum)
          {
            tmp1 = mask[bytesBefore - 4] - tmp1;
          }

          if ((tmp1 != 0) && (tmp1 != -1))
          {
            if (!posNum)
            {
              fprintf(ci.filePtr(), "%c", neg);
              // printf("%c", neg);
            }

            fprintf(ci.filePtr(), "%d", tmp1);
            // printf("%d", tmp1);
          }

          tmpBuf += (bytesBefore - 4);
          int32_t tmp2 = *((int32_t*)tmpBuf);
          tmp2 = ntohl(tmp2);

          if (tmp1 != 0)
          {
            if (!posNum)
            {
              tmp2 = mask[4] - tmp2;
            }

            if (tmp1 == -1)
            {
              fprintf(ci.filePtr(), "%c", neg);
              fprintf(ci.filePtr(), "%d.", tmp2);
              // printf("%c", neg);
              // printf("%d.", tmp2);
            }
            else
            {
              fprintf(ci.filePtr(), "%09u.", tmp2);
              // printf("%09u.", tmp2);
            }
          }
          else
          {
            if (!posNum)
            {
              tmp2 = mask[4] - tmp2;
              fprintf(ci.filePtr(), "%c", neg);
              // printf("%c", neg);
            }

            fprintf(ci.filePtr(), "%d.", tmp2);
            // printf("%d.", tmp2);
          }
        }
        else
        {
          for (uint i = 1; i < bytesBefore; i++)
          {
            tmp1 = (tmp1 << 8) + tmpBuf[i];
          }

          if (!posNum)
          {
            tmp1 = mask[bytesBefore] - tmp1;
            fprintf(ci.filePtr(), "%c", neg);
            // printf("%c", neg);
          }

          fprintf(ci.filePtr(), "%d.", tmp1);
          // printf("%d.", tmp1);
        }

        // fetch the digits after decimal point
        int32_t tmp2 = 0;

        if (bytesBefore > 4)
          tmpBuf += 4;
        else
          tmpBuf += bytesBefore;

        tmp2 = tmpBuf[0];

        if ((totalBytes - bytesBefore) < 5)
        {
          for (uint j = 1; j < (totalBytes - bytesBefore); j++)
          {
            tmp2 = (tmp2 << 8) + tmpBuf[j];
          }

          int8_t digits = m_type.scale - 9;  // 9 digits is a 4 bytes chunk

          if (digits <= 0)
            digits = m_type.scale;

          if (!posNum)
          {
            tmp2 = mask[totalBytes - bytesBefore] - tmp2;
          }

          fprintf(ci.filePtr(), "%0*u%c", digits, tmp2, ci.delimiter());
          // printf("%0*u|", digits, tmp2);
        }
        else
        {
          for (uint j = 1; j < 4; j++)
          {
            tmp2 = (tmp2 << 8) + tmpBuf[j];
          }

          if (!posNum)
          {
            tmp2 = mask[4] - tmp2;
          }

          fprintf(ci.filePtr(), "%09u", tmp2);
          // printf("%09u", tmp2);

          tmpBuf += 4;
          int32_t tmp3 = tmpBuf[0];

          for (uint j = 1; j < (totalBytes - bytesBefore - 4); j++)
          {
            tmp3 = (tmp3 << 8) + tmpBuf[j];
          }

          int8_t digits = m_type.scale - 9;  // 9 digits is a 4 bytes chunk

          if (digits < 0)
            digits = m_type.scale;

          if (!posNum)
          {
            tmp3 = mask[totalBytes - bytesBefore - 4] - tmp3;
          }

          fprintf(ci.filePtr(), "%0*u%c", digits, tmp3, ci.delimiter());
          // printf("%0*u|", digits, tmp3);
        }
      }
    }

    return totalBytes;
  }

  size_t ColWriteBatchVarbinary(const uchar* buf0, bool nullVal, ColBatchWriter& ci) override
  {
    return ColWriteBatchString(buf0, nullVal, ci, &ColWriteBatchBlobString);
  }

  size_t ColWriteBatchBlob(const uchar* buf0, bool nullVal, ColBatchWriter& ci) override
  {
    return (UNLIKELY(m_type.colDataType == CalpontSystemCatalog::BLOB))
               ? ColWriteBatchString(buf0, nullVal, ci, &ColWriteBatchBlobString)
               : ColWriteBatchString(buf0, nullVal, ci, &ColWriteBatchTextString);
  }
};

}  // end of namespace datatypes

#endif

// vim:ts=2 sw=2:
