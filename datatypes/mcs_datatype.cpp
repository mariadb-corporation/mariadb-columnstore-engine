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

#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
#include <utility>
#include <cassert>
using namespace std;

#include <boost/algorithm/string/replace.hpp>

#include "dataconvert.h"
using namespace dataconvert;

#include "simplecolumn.h"
#include "simplecolumn_int.h"
#include "simplecolumn_uint.h"
#include "simplecolumn_decimal.h"

#include "rowgroup.h"
#include "ddlpkg.h"
#include "dbrm.h"
#include "we_typeext.h"
#include "joblisttypes.h"

namespace datatypes
{
int128_t SystemCatalog::TypeAttributesStd::decimal128FromString(const std::string& value,
                                                                bool* saturate) const
{
  int128_t result = 0;
  bool pushWarning = false;
  bool noRoundup = false;
  dataconvert::number_int_value<int128_t>(value, SystemCatalog::DECIMAL, *this, pushWarning, noRoundup,
                                          result, saturate);
  return result;
}

const string& TypeHandlerSInt8::name() const
{
  static const string xname = "TINYINT";
  return xname;
}

const string& TypeHandlerUInt8::name() const
{
  static const string xname = "UTINYINT";
  return xname;
}

const string& TypeHandlerSInt16::name() const
{
  static const string xname = "SMALLINT";
  return xname;
}

const string& TypeHandlerUInt16::name() const
{
  static const string xname = "USMALLINT";
  return xname;
}

const string& TypeHandlerSInt24::name() const
{
  static const string xname = "MEDINT";
  return xname;
}

const string& TypeHandlerUInt24::name() const
{
  static const string xname = "UMEDINT";
  return xname;
}

const string& TypeHandlerSInt32::name() const
{
  static const string xname = "INT";
  return xname;
}

const string& TypeHandlerUInt32::name() const
{
  static const string xname = "UINT";
  return xname;
}

const string& TypeHandlerSInt64::name() const
{
  static const string xname = "BIGINT";
  return xname;
}

const string& TypeHandlerUInt64::name() const
{
  static const string xname = "UBIGINT";
  return xname;
}

const string& TypeHandlerSFloat::name() const
{
  static const string xname = "FLOAT";
  return xname;
}

const string& TypeHandlerUFloat::name() const
{
  static const string xname = "UFLOAT";
  return xname;
}

const string& TypeHandlerSDouble::name() const
{
  static const string xname = "DOUBLE";
  return xname;
}

const string& TypeHandlerUDouble::name() const
{
  static const string xname = "UDOUBLE";
  return xname;
}

const string& TypeHandlerSLongDouble::name() const
{
  static const string xname = "LONGDOUBLE";
  return xname;
}

const string& TypeHandlerSDecimal64::name() const
{
  static const string xname = "DECIMAL";
  return xname;
}

const string& TypeHandlerUDecimal64::name() const
{
  static const string xname = "UDECIMAL";
  return xname;
}

const string& TypeHandlerSDecimal128::name() const
{
  static const string xname = "DECIMAL";
  return xname;
}

const string& TypeHandlerUDecimal128::name() const
{
  static const string xname = "UDECIMAL";
  return xname;
}

const string& TypeHandlerDate::name() const
{
  static const string xname = "DATE";
  return xname;
}

const string& TypeHandlerDatetime::name() const
{
  static const string xname = "DATETIME";
  return xname;
}

const string& TypeHandlerTime::name() const
{
  static const string xname = "TIME";
  return xname;
}

const string& TypeHandlerTimestamp::name() const
{
  static const string xname = "TIMESTAMP";
  return xname;
}

const string& TypeHandlerChar::name() const
{
  static const string xname = "CHAR";
  return xname;
}

const string& TypeHandlerVarchar::name() const
{
  static const string xname = "VARCHAR";
  return xname;
}

const string& TypeHandlerVarbinary::name() const
{
  static const string xname = "VARBINARY";
  return xname;
}

const string& TypeHandlerBlob::name() const
{
  static const string xname = "BLOB";
  return xname;
}

const string& TypeHandlerClob::name() const
{
  static const string xname = "CLOB";
  return xname;
}

const string& TypeHandlerText::name() const
{
  static const string xname = "TEXT";
  return xname;
}

const string& TypeHandlerBit::name() const
{
  static const string xname = "BIT";
  return xname;
}

TypeHandlerBit mcs_type_handler_bit;

TypeHandlerSInt8 mcs_type_handler_sint8;
TypeHandlerSInt16 mcs_type_handler_sint16;
TypeHandlerSInt24 mcs_type_handler_sint24;
TypeHandlerSInt32 mcs_type_handler_sint32;
TypeHandlerSInt64 mcs_type_handler_sint64;

TypeHandlerUInt8 mcs_type_handler_uint8;
TypeHandlerUInt16 mcs_type_handler_uint16;
TypeHandlerUInt24 mcs_type_handler_uint24;
TypeHandlerUInt32 mcs_type_handler_uint32;
TypeHandlerUInt64 mcs_type_handler_uint64;

TypeHandlerSFloat mcs_type_handler_sfloat;
TypeHandlerSDouble mcs_type_handler_sdouble;
TypeHandlerSLongDouble mcs_type_handler_slongdouble;

TypeHandlerUFloat mcs_type_handler_ufloat;
TypeHandlerUDouble mcs_type_handler_udouble;

TypeHandlerSDecimal64 mcs_type_handler_sdecimal64;
TypeHandlerUDecimal64 mcs_type_handler_udecimal64;

TypeHandlerSDecimal128 mcs_type_handler_sdecimal128;
TypeHandlerUDecimal128 mcs_type_handler_udecimal128;

TypeHandlerDate mcs_type_handler_date;
TypeHandlerTime mcs_type_handler_time;
TypeHandlerDatetime mcs_type_handler_datetime;
TypeHandlerTimestamp mcs_type_handler_timestamp;

TypeHandlerChar mcs_type_handler_char;
TypeHandlerVarchar mcs_type_handler_varchar;
TypeHandlerText mcs_type_handler_text;
TypeHandlerClob mcs_type_handler_clob;
TypeHandlerVarbinary mcs_type_handler_varbinary;
TypeHandlerBlob mcs_type_handler_blob;

const TypeHandler* TypeHandler::find(SystemCatalog::ColDataType typeCode,
                                     const SystemCatalog::TypeAttributesStd& ct)
{
  switch (typeCode)
  {
    case SystemCatalog::BIT: return &mcs_type_handler_bit;
    case SystemCatalog::TINYINT: return &mcs_type_handler_sint8;
    case SystemCatalog::SMALLINT: return &mcs_type_handler_sint16;
    case SystemCatalog::MEDINT: return &mcs_type_handler_sint24;
    case SystemCatalog::INT: return &mcs_type_handler_sint32;
    case SystemCatalog::BIGINT: return &mcs_type_handler_sint64;
    case SystemCatalog::UTINYINT: return &mcs_type_handler_uint8;
    case SystemCatalog::USMALLINT: return &mcs_type_handler_uint16;
    case SystemCatalog::UMEDINT: return &mcs_type_handler_uint24;
    case SystemCatalog::UINT: return &mcs_type_handler_uint32;
    case SystemCatalog::UBIGINT: return &mcs_type_handler_uint64;
    case SystemCatalog::FLOAT: return &mcs_type_handler_sfloat;
    case SystemCatalog::DOUBLE: return &mcs_type_handler_sdouble;
    case SystemCatalog::LONGDOUBLE: return &mcs_type_handler_slongdouble;
    case SystemCatalog::UFLOAT: return &mcs_type_handler_ufloat;
    case SystemCatalog::UDOUBLE: return &mcs_type_handler_udouble;

    case SystemCatalog::DECIMAL:
      if (static_cast<uint32_t>(ct.colWidth) < datatypes::MAXDECIMALWIDTH)
        return &mcs_type_handler_sdecimal64;
      else
        return &mcs_type_handler_sdecimal128;

    case SystemCatalog::UDECIMAL:
      if (static_cast<uint32_t>(ct.colWidth) < datatypes::MAXDECIMALWIDTH)
        return &mcs_type_handler_udecimal64;
      else
        return &mcs_type_handler_udecimal128;

    case SystemCatalog::TIME: return &mcs_type_handler_time;
    case SystemCatalog::DATE: return &mcs_type_handler_date;
    case SystemCatalog::DATETIME: return &mcs_type_handler_datetime;
    case SystemCatalog::TIMESTAMP: return &mcs_type_handler_timestamp;
    case SystemCatalog::CHAR: return &mcs_type_handler_char;
    case SystemCatalog::VARCHAR: return &mcs_type_handler_varchar;
    case SystemCatalog::TEXT: return &mcs_type_handler_text;
    case SystemCatalog::CLOB: return &mcs_type_handler_clob;
    case SystemCatalog::VARBINARY: return &mcs_type_handler_varbinary;
    case SystemCatalog::BLOB: return &mcs_type_handler_blob;

    case SystemCatalog::NUM_OF_COL_DATA_TYPE:
    case SystemCatalog::STRINT:
    case SystemCatalog::UNDEFINED: break;
  }
  return NULL;
}

const TypeHandler* SystemCatalog::TypeHolderStd::typeHandler() const
{
  return TypeHandler::find(colDataType, *this);
}

boost::any SystemCatalog::TypeHolderStd::getNullValueForType() const
{
  const TypeHandler* h = typeHandler();
  if (!h)
  {
    throw std::runtime_error("getNullValueForType: unkown column data type");
    return boost::any();
  }
  return h->getNullValueForType(*this);
}

const TypeHandler* TypeHandler::find_by_ddltype(const ddlpackage::ColumnType& ct)
{
  switch (ct.fType)
  {
    case ddlpackage::DDL_CHAR: return &mcs_type_handler_char;
    case ddlpackage::DDL_VARCHAR: return &mcs_type_handler_varchar;
    case ddlpackage::DDL_VARBINARY: return &mcs_type_handler_varbinary;
    case ddlpackage::DDL_BIT: return &mcs_type_handler_bit;

    case ddlpackage::DDL_REAL:
    case ddlpackage::DDL_DECIMAL:
    case ddlpackage::DDL_NUMERIC:
    case ddlpackage::DDL_NUMBER:

      if (ct.fLength < datatypes::MAXDECIMALWIDTH)
        return &mcs_type_handler_sdecimal64;
      return &mcs_type_handler_sdecimal128;

    case ddlpackage::DDL_FLOAT: return &mcs_type_handler_sfloat;
    case ddlpackage::DDL_DOUBLE: return &mcs_type_handler_sdouble;

    case ddlpackage::DDL_INT:
    case ddlpackage::DDL_INTEGER: return &mcs_type_handler_sint32;

    case ddlpackage::DDL_BIGINT: return &mcs_type_handler_sint64;
    case ddlpackage::DDL_MEDINT: return &mcs_type_handler_sint24;
    case ddlpackage::DDL_SMALLINT: return &mcs_type_handler_sint16;
    case ddlpackage::DDL_TINYINT: return &mcs_type_handler_sint8;

    case ddlpackage::DDL_DATE: return &mcs_type_handler_date;
    case ddlpackage::DDL_DATETIME: return &mcs_type_handler_datetime;
    case ddlpackage::DDL_TIME: return &mcs_type_handler_time;
    case ddlpackage::DDL_TIMESTAMP: return &mcs_type_handler_timestamp;

    case ddlpackage::DDL_CLOB: return &mcs_type_handler_clob;
    case ddlpackage::DDL_BLOB: return &mcs_type_handler_blob;
    case ddlpackage::DDL_TEXT: return &mcs_type_handler_text;

    case ddlpackage::DDL_UNSIGNED_TINYINT: return &mcs_type_handler_uint8;
    case ddlpackage::DDL_UNSIGNED_SMALLINT: return &mcs_type_handler_uint16;
    case ddlpackage::DDL_UNSIGNED_MEDINT: return &mcs_type_handler_uint24;
    case ddlpackage::DDL_UNSIGNED_INT: return &mcs_type_handler_uint32;
    case ddlpackage::DDL_UNSIGNED_BIGINT: return &mcs_type_handler_uint64;

    case ddlpackage::DDL_UNSIGNED_DECIMAL:
    case ddlpackage::DDL_UNSIGNED_NUMERIC:

      if (ct.fLength < datatypes::MAXDECIMALWIDTH)
        return &mcs_type_handler_udecimal64;
      return &mcs_type_handler_udecimal128;

    case ddlpackage::DDL_UNSIGNED_FLOAT: return &mcs_type_handler_ufloat;
    case ddlpackage::DDL_UNSIGNED_DOUBLE: return &mcs_type_handler_udouble;

    case ddlpackage::DDL_INVALID_DATATYPE: break;
  }
  return NULL;
}

/****************************************************************************/

int TypeHandlerDate::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t intColVal = row.getUintField<4>(pos);
  return f->store_date(intColVal);
}

int TypeHandlerDatetime::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t intColVal = row.getUintField<8>(pos);
  return f->store_datetime(intColVal);
}

int TypeHandlerTime::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t intColVal = row.getUintField<8>(pos);
  return f->store_time(intColVal);
}

int TypeHandlerTimestamp::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t intColVal = row.getUintField<8>(pos);
  return f->store_timestamp(intColVal);
}

int TypeHandlerStr::storeValueToFieldCharVarchar(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t intColVal;
  switch (f->colWidth())
  {
    case 1:
      intColVal = row.getUintField<1>(pos);
      return f->store_string((const char*)(&intColVal), strlen((char*)(&intColVal)));

    case 2:
      intColVal = row.getUintField<2>(pos);
      return f->store_string((char*)(&intColVal), strlen((char*)(&intColVal)));

    case 4:
      intColVal = row.getUintField<4>(pos);
      return f->store_string((char*)(&intColVal), strlen((char*)(&intColVal)));

    case 8:
    {
      // make sure we don't send strlen off into the weeds...
      intColVal = row.getUintField<8>(pos);
      char tmp[256];
      memcpy(tmp, &intColVal, 8);
      tmp[8] = 0;
      return f->store_string(tmp, strlen(tmp));
    }
    default: return f->storeConstString(row.getConstString(pos));
  }
}

int TypeHandlerVarbinary::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  uint32_t l;
  const uint8_t* p = row.getVarBinaryField(l, pos);
  return f->store_varbinary((const char*)p, l);
}

int TypeHandlerSInt64::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t val = row.getIntField<8>(pos);
  return f->store_xlonglong(val);
}

int TypeHandlerUInt64::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  uint64_t val = row.getUintField<8>(pos);
  return f->store_xlonglong(static_cast<int64_t>(val));
}

int TypeHandlerInt::storeValueToFieldSInt32(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t val = row.getIntField<4>(pos);
  return f->store_xlonglong(val);
}

int TypeHandlerInt::storeValueToFieldUInt32(rowgroup::Row& row, int pos, StoreField* f) const
{
  uint64_t val = row.getUintField<4>(pos);
  return f->store_xlonglong(static_cast<int64_t>(val));
}

int TypeHandlerSInt16::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t val = row.getIntField<2>(pos);
  return f->store_xlonglong(val);
}

int TypeHandlerUInt16::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  uint64_t val = row.getUintField<2>(pos);
  return f->store_xlonglong(static_cast<int64_t>(val));
}

int TypeHandlerSInt8::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  int64_t val = row.getIntField<1>(pos);
  return f->store_xlonglong(val);
}

int TypeHandlerUInt8::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  uint64_t val = row.getUintField<1>(pos);
  return f->store_xlonglong(static_cast<int64_t>(val));
}

/*
  In this case, we're trying to load a double output column with float data. This is the
  case when you do sum(floatcol), e.g.
*/
int TypeHandlerReal::storeValueToFieldXFloat(rowgroup::Row& row, int pos, StoreField* f) const
{
  float dl = row.getFloatField(pos);
  return f->store_float(dl);
}

int TypeHandlerReal::storeValueToFieldXDouble(rowgroup::Row& row, int pos, StoreField* f) const
{
  double dl = row.getDoubleField(pos);
  return f->store_double(dl);
}

int TypeHandlerSLongDouble::storeValueToField(rowgroup::Row& row, int pos, StoreField* f) const
{
  long double dl = row.getLongDoubleField(pos);
  return f->store_long_double(dl);
}

int TypeHandlerXDecimal::storeValueToField64(rowgroup::Row& row, int pos, StoreField* f) const
{
  return f->store_decimal64(datatypes::Decimal(row.getIntField(pos), f->scale(), f->precision()));
}

int TypeHandlerXDecimal::storeValueToField128(rowgroup::Row& row, int pos, StoreField* f) const
{
  int128_t* decPtr = row.getBinaryField<int128_t>(pos);
  return f->store_decimal128(datatypes::Decimal(0, f->scale(), f->precision(), decPtr));
}

int TypeHandlerStr::storeValueToFieldBlobText(rowgroup::Row& row, int pos, StoreField* f) const
{
  return f->store_lob((const char*)row.getVarBinaryField(pos), row.getVarBinaryLength(pos));
}

/*
int TypeHandlerBinary::storeValueToField(rowgroup::Row &row, int pos,
                                         StoreField *f) const
{
  Field_varstring* f2 = static_cast<Field_varstring*>(f);
  // TODO MCOL-641 Binary representation could contain \0.
  char* binaryString = row.getBinaryField<char>(pos);
  return f2->store(binaryString, colType.colWidth, f2->charset());
}
*/

/*
  Default behaviour: treat as int64
          int64_t intColVal = row.getUintField<8>(pos);
          storeNumericField(f, intColVal, colType);
*/

/****************************************************************************/

string TypeHandlerDate::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  return DataConvert::dateToString(v.toSInt64());
}

string TypeHandlerDatetime::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  return DataConvert::datetimeToString(v.toSInt64());
}

string TypeHandlerTimestamp::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  return DataConvert::timestampToString(v.toSInt64(), v.timeZone());
}

string TypeHandlerTime::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  return DataConvert::timeToString(v.toSInt64());
}

string TypeHandlerChar::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  // swap again to retain the string byte order
  ostringstream oss;
  uint64_t tmp = uint64ToStr(v.toSInt64());
  oss << (char*)(&tmp);
  return oss.str();
}

string TypeHandlerVarchar::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  // swap again to retain the string byte order
  ostringstream oss;
  uint64_t tmp = uint64ToStr(v.toSInt64());
  oss << (char*)(&tmp);
  return oss.str();
}

string TypeHandlerInt::formatSInt64(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  ostringstream oss;
  oss << v.toSInt64();
  return oss.str();
}

string TypeHandlerInt::formatUInt64(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  ostringstream oss;
  oss << static_cast<uint64_t>(v.toSInt64());
  return oss.str();
}

string TypeHandlerVarbinary::format(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  return "N/A";
}

string TypeHandlerXDecimal::format64(const SimpleValue& v, const SystemCatalog::TypeAttributesStd& attr) const
{
  idbassert(isValidXDecimal64(attr));
  if (attr.scale > 0)
  {
    datatypes::Decimal dec(TSInt64(v.toSInt64()), attr.scale, attr.precision);
    return dec.toString();
  }
  ostringstream oss;
  oss << v.toSInt64();
  return oss.str();
}

string TypeHandlerXDecimal::format128(const SimpleValue& v,
                                      const SystemCatalog::TypeAttributesStd& attr) const
{
  idbassert(isValidXDecimal128(attr));
  datatypes::Decimal dec(0, attr.scale, attr.precision, v.toSInt128());
  return dec.toString(true);
}

/****************************************************************************/

class ostringstreamL : public ostringstream
{
 public:
  ostringstreamL()
  {
    setf(ios::left, ios::adjustfield);
  }
};

string TypeHandler::formatPartitionInfoSInt64(const SystemCatalog::TypeAttributesStd& attr,
                                              const MinMaxInfo& pi) const
{
  ostringstreamL output;
  if (pi.isEmptyOrNullSInt64())
    output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
  else
    output << setw(30) << format(SimpleValueSInt64(pi.min), attr) << setw(30)
           << format(SimpleValueSInt64(pi.max), attr);
  return output.str();
}

string TypeHandler::formatPartitionInfoUInt64(const SystemCatalog::TypeAttributesStd& attr,
                                              const MinMaxInfo& pi) const
{
  ostringstreamL output;
  if (pi.isEmptyOrNullUInt64())
    output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
  else
    output << setw(30) << format(SimpleValueSInt64(pi.min), attr) << setw(30)
           << format(SimpleValueSInt64(pi.max), attr);
  return output.str();
}

string TypeHandlerXDecimal::formatPartitionInfo128(const SystemCatalog::TypeAttributesStd& attr,
                                                   const MinMaxInfo& pi) const
{
  ostringstreamL output;
  if (pi.isEmptyOrNullSInt128())
    output << setw(datatypes::Decimal::MAXLENGTH16BYTES) << "Empty/Null"
           << setw(datatypes::Decimal::MAXLENGTH16BYTES) << "Empty/Null";
  else
    output << setw(datatypes::Decimal::MAXLENGTH16BYTES) << format(SimpleValueSInt128(pi.int128Min), attr)
           << setw(datatypes::Decimal::MAXLENGTH16BYTES) << format(SimpleValueSInt128(pi.int128Max), attr);
  return output.str();
}

string TypeHandlerStr::formatPartitionInfoSmallCharVarchar(const SystemCatalog::TypeAttributesStd& attr,
                                                           const MinMaxInfo& pi) const
{
  ostringstreamL output;
  int64_t maxLimit = std::numeric_limits<int64_t>::max();
  int64_t minLimit = std::numeric_limits<int64_t>::min();
  maxLimit = uint64ToStr(maxLimit);
  minLimit = uint64ToStr(minLimit);
  if (pi.min == maxLimit && pi.max == minLimit)
    output << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
  else
    output << setw(30) << format(SimpleValueSInt64(pi.min), attr) << setw(30)
           << format(SimpleValueSInt64(pi.max), attr);
  return output.str();
}

string TypeHandlerChar::formatPartitionInfo(const SystemCatalog::TypeAttributesStd& attr,
                                            const MinMaxInfo& pi) const
{
  // char column order swap for compare in subsequent loop
  if (attr.colWidth <= 8)
    return formatPartitionInfoSmallCharVarchar(attr, pi);
  return formatPartitionInfoSInt64(attr, pi);
}

string TypeHandlerVarchar::formatPartitionInfo(const SystemCatalog::TypeAttributesStd& attr,
                                               const MinMaxInfo& pi) const
{
  // varchar column order swap for compare in subsequent loop
  if (attr.colWidth <= 7)
    return formatPartitionInfoSmallCharVarchar(attr, pi);
  return formatPartitionInfoSInt64(attr, pi);
}

/****************************************************************************/

execplan::SimpleColumn* TypeHandlerSInt8::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                          SystemCatalog::TypeHolderStd& ct,
                                                          const SimpleColumnParam& prm) const
{
  if (ct.scale == 0)
    return new execplan::SimpleColumn_INT<1>(name.db(), name.table(), name.column(), prm.columnStore(),
                                             prm.sessionid());
  ct.colDataType = SystemCatalog::DECIMAL;
  return new execplan::SimpleColumn_Decimal<1>(name.db(), name.table(), name.column(), prm.columnStore(),
                                               prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerSInt16::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  if (ct.scale == 0)
    return new execplan::SimpleColumn_INT<2>(name.db(), name.table(), name.column(), prm.columnStore(),
                                             prm.sessionid());
  ct.colDataType = SystemCatalog::DECIMAL;
  return new execplan::SimpleColumn_Decimal<2>(name.db(), name.table(), name.column(), prm.columnStore(),
                                               prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerSInt24::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  if (ct.scale == 0)
    return new execplan::SimpleColumn_INT<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                             prm.sessionid());
  ct.colDataType = SystemCatalog::DECIMAL;
  return new execplan::SimpleColumn_Decimal<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                               prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerSInt32::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  if (ct.scale == 0)
    return new execplan::SimpleColumn_INT<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                             prm.sessionid());
  ct.colDataType = SystemCatalog::DECIMAL;
  return new execplan::SimpleColumn_Decimal<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                               prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerSInt64::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  if (ct.scale == 0)
    return new execplan::SimpleColumn_INT<8>(name.db(), name.table(), name.column(), prm.columnStore(),
                                             prm.sessionid());
  ct.colDataType = SystemCatalog::DECIMAL;
  return new execplan::SimpleColumn_Decimal<8>(name.db(), name.table(), name.column(), prm.columnStore(),
                                               prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerUInt8::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                          SystemCatalog::TypeHolderStd& ct,
                                                          const SimpleColumnParam& prm) const
{
  // QQ: why scale is not checked (unlike SInt1)?
  return new execplan::SimpleColumn_UINT<1>(name.db(), name.table(), name.column(), prm.columnStore(),
                                            prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerUInt16::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  return new execplan::SimpleColumn_UINT<2>(name.db(), name.table(), name.column(), prm.columnStore(),
                                            prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerUInt24::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  return new execplan::SimpleColumn_UINT<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                            prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerUInt32::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  return new execplan::SimpleColumn_UINT<4>(name.db(), name.table(), name.column(), prm.columnStore(),
                                            prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerUInt64::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                           SystemCatalog::TypeHolderStd& ct,
                                                           const SimpleColumnParam& prm) const
{
  return new execplan::SimpleColumn_UINT<8>(name.db(), name.table(), name.column(), prm.columnStore(),
                                            prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerReal::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                         SystemCatalog::TypeHolderStd& ct,
                                                         const SimpleColumnParam& prm) const
{
  // QQ
  return new execplan::SimpleColumn(name.db(), name.table(), name.column(), prm.columnStore(),
                                    prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerXDecimal::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                             SystemCatalog::TypeHolderStd& ct,
                                                             const SimpleColumnParam& prm) const
{
  // QQ
  return new execplan::SimpleColumn(name.db(), name.table(), name.column(), prm.columnStore(),
                                    prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerStr::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                        SystemCatalog::TypeHolderStd& ct,
                                                        const SimpleColumnParam& prm) const
{
  // QQ
  return new execplan::SimpleColumn(name.db(), name.table(), name.column(), prm.columnStore(),
                                    prm.sessionid());
}

execplan::SimpleColumn* TypeHandlerTemporal::newSimpleColumn(const DatabaseQualifiedColumnName& name,
                                                             SystemCatalog::TypeHolderStd& ct,
                                                             const SimpleColumnParam& prm) const
{
  // QQ
  return new execplan::SimpleColumn(name.db(), name.table(), name.column(), prm.columnStore(),
                                    prm.sessionid());
}

/****************************************************************************/

class SimpleConverter : public boost::any
{
  bool& initPushWarning()
  {
    m_pushWarning = false;
    return m_pushWarning;
  }
  bool m_pushWarning;

 public:
  SimpleConverter(const SessionParam& sp, const TypeHandler* h, const SystemCatalog::TypeAttributesStd& attr,
                  const char* str)
   : boost::any(h->convertFromString(attr, ConvertFromStringParam(sp.timeZone(), true, false), str,
                                     initPushWarning()))
  {
  }
  round_style_t roundStyle() const
  {
    return m_pushWarning ? round_style_t::POS : round_style_t::NONE;
  }
  round_style_t roundStyle(const char* str) const
  {
    return m_pushWarning ? roundStyleDetect(str) : round_style_t::NONE;
  }
  static round_style_t roundStyleDetect(const char* str)
  {
    // get rid of leading white spaces and parentheses
    string data(str);
    size_t fpos = data.find_first_of(" \t()");
    while (string::npos != fpos)
    {
      data.erase(fpos, 1);
      fpos = data.find_first_of(" \t()");
    }
    return (data[0] == '-') ? round_style_t::NEG : round_style_t::POS;
  }
  int64_t to_sint64() const
  {
    return boost::any_cast<long long>(*this);
  }
  uint64_t to_uint64() const
  {
    return boost::any_cast<uint64_t>(*this);
  }
  uint32_t to_uint32() const
  {
    return boost::any_cast<uint32_t>(*this);
  }
  int128_t to_sint128() const
  {
    return boost::any_cast<int128_t>(*this);
  }
};

class SimpleConverterSNumeric : public SimpleConverter
{
 public:
  SimpleConverterSNumeric(const SessionParam& sp, const TypeHandler* h,
                          const SystemCatalog::TypeAttributesStd& attr, const char* str, round_style_t& rf)
   : SimpleConverter(sp, h, attr, str)
  {
    rf = roundStyle(str);
  }
};

template <typename T>
SimpleValue toSimpleValueSInt(const SessionParam& sp, const TypeHandler* h,
                              const SystemCatalog::TypeAttributesStd& attr, const char* str,
                              round_style_t& rf)
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverterSNumeric anyVal(sp, h, attr, str, rf);
  return SimpleValueSInt64(static_cast<int64_t>(boost::any_cast<T>(anyVal)));
}

SimpleValue TypeHandlerSInt8::toSimpleValue(const SessionParam& sp,
                                            const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                            round_style_t& rf) const
{
  return toSimpleValueSInt<char>(sp, this, attr, str, rf);
}

SimpleValue TypeHandlerSInt16::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueSInt<int16_t>(sp, this, attr, str, rf);
}

SimpleValue TypeHandlerSInt24::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueSInt<mcs_sint32_t>(sp, this, attr, str, rf);
}

SimpleValue TypeHandlerSInt32::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueSInt<mcs_sint32_t>(sp, this, attr, str, rf);
}

SimpleValue TypeHandlerSInt64::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueSInt<long long>(sp, this, attr, str, rf);
}

template <typename T>
SimpleValue toSimpleValueUInt(const SessionParam& sp, const TypeHandler* h,
                              const SystemCatalog::TypeAttributesStd& attr, const char* str)
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverter anyVal(sp, h, attr, str);
  return SimpleValueSInt64(static_cast<int64_t>(boost::any_cast<T>(anyVal)));
}

SimpleValue TypeHandlerUInt8::toSimpleValue(const SessionParam& sp,
                                            const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                            round_style_t& rf) const
{
  return toSimpleValueUInt<uint8_t>(sp, this, attr, str);
}

SimpleValue TypeHandlerUInt16::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueUInt<uint16_t>(sp, this, attr, str);
}

SimpleValue TypeHandlerUInt24::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueUInt<uint32_t>(sp, this, attr, str);
}

SimpleValue TypeHandlerUInt32::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueUInt<uint32_t>(sp, this, attr, str);
}

SimpleValue TypeHandlerUInt64::toSimpleValue(const SessionParam& sp,
                                             const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                             round_style_t& rf) const
{
  return toSimpleValueUInt<uint64_t>(sp, this, attr, str);
}

SimpleValue TypeHandlerDate::toSimpleValue(const SessionParam& sp,
                                           const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                           round_style_t& rf) const
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverter anyVal(sp, this, attr, str);
  return SimpleValueSInt64(static_cast<int64_t>(anyVal.to_uint32()));
}

SimpleValue TypeHandlerDatetime::toSimpleValue(const SessionParam& sp,
                                               const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                               round_style_t& rf) const
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverter anyVal(sp, this, attr, str);
  return SimpleValueSInt64(static_cast<int64_t>(anyVal.to_uint64()));
}

SimpleValue TypeHandlerTimestamp::toSimpleValue(const SessionParam& sp,
                                                const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                                round_style_t& rf) const
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverter anyVal(sp, this, attr, str);
  return SimpleValueTimestamp(anyVal.to_uint64(), sp.timeZone());
}

SimpleValue TypeHandlerTime::toSimpleValue(const SessionParam& sp,
                                           const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                           round_style_t& rf) const
{
  idbassert(attr.colWidth <= SystemCatalog::EIGHT_BYTE);
  SimpleConverter anyVal(sp, this, attr, str);
  return SimpleValueSInt64(anyVal.to_sint64());
}

SimpleValue TypeHandlerXDecimal::toSimpleValue(const SessionParam& sp,
                                               const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                               round_style_t& rf) const
{
  if (attr.colWidth <= SystemCatalog::EIGHT_BYTE)
  {
    SimpleConverterSNumeric anyVal(sp, this, attr, str, rf);
    int64_t v;
    if (attr.colWidth == SystemCatalog::ONE_BYTE)
      v = boost::any_cast<char>(anyVal);
    else if (attr.colWidth == SystemCatalog::TWO_BYTE)
      v = boost::any_cast<int16_t>(anyVal);
    else if (attr.colWidth == SystemCatalog::FOUR_BYTE)
      v = boost::any_cast<mcs_sint32_t>(anyVal);
    else if (attr.colWidth == SystemCatalog::EIGHT_BYTE)
      v = anyVal.to_sint64();
    else
    {
      idbassert(0);
      v = 0;
    }
    return SimpleValueSInt64(v);
  }
  else
  {
    idbassert(attr.colWidth == datatypes::MAXDECIMALWIDTH);
    SimpleConverterSNumeric anyVal(sp, this, attr, str, rf);
    return SimpleValueSInt128(anyVal.to_sint128());
  }
}

SimpleValue TypeHandlerStr::toSimpleValue(const SessionParam& sp,
                                          const SystemCatalog::TypeAttributesStd& attr, const char* str,
                                          round_style_t& rf) const
{
  SimpleConverter anyVal(sp, this, attr, str);
  rf = anyVal.roundStyle();
  string i = boost::any_cast<string>(anyVal);
  // bug 1932, pad nulls up to the size of v
  i.resize(sizeof(int64_t), 0);
  return SimpleValueSInt64(static_cast<int64_t>(uint64ToStr(*((uint64_t*)i.data()))));
}

/****************************************************************************/

MinMaxPartitionInfo::MinMaxPartitionInfo(const BRM::EMEntry& entry)
 : m_status(entry.status == BRM::EXTENTOUTOFSERVICE ? ET_DISABLED : EXPL_NULL)
{
}

MinMaxPartitionInfo TypeHandler::getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd& attr,
                                                        BRM::DBRM& em, const BRM::EMEntry& entry,
                                                        int* state) const
{
  int32_t seqNum;
  MinMaxPartitionInfo partInfo(entry);
  *state = em.getExtentMaxMin(entry.range.start, partInfo.max, partInfo.min, seqNum);
  return partInfo;
}

MinMaxPartitionInfo TypeHandlerXDecimal::getExtentPartitionInfo64(
    const SystemCatalog::TypeAttributesStd& attr, BRM::DBRM& em, const BRM::EMEntry& entry, int* state) const
{
  int32_t seqNum;
  MinMaxPartitionInfo partInfo(entry);
  *state = em.getExtentMaxMin(entry.range.start, partInfo.max, partInfo.min, seqNum);
  return partInfo;
}

MinMaxPartitionInfo TypeHandlerXDecimal::getExtentPartitionInfo128(
    const SystemCatalog::TypeAttributesStd& attr, BRM::DBRM& em, const BRM::EMEntry& entry, int* state) const
{
  int32_t seqNum;
  MinMaxPartitionInfo partInfo(entry);
  *state = em.getExtentMaxMin(entry.range.start, partInfo.int128Max, partInfo.int128Min, seqNum);
  return partInfo;
}

MinMaxPartitionInfo TypeHandlerChar::getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd& attr,
                                                            BRM::DBRM& em, const BRM::EMEntry& entry,
                                                            int* state) const
{
  int32_t seqNum;
  MinMaxPartitionInfo partInfo(entry);
  *state = em.getExtentMaxMin(entry.range.start, partInfo.max, partInfo.min, seqNum);
  // char column order swap
  if (attr.colWidth <= 8)
  {
    partInfo.max = uint64ToStr(partInfo.max);
    partInfo.min = uint64ToStr(partInfo.min);
  }
  return partInfo;
}

MinMaxPartitionInfo TypeHandlerVarchar::getExtentPartitionInfo(const SystemCatalog::TypeAttributesStd& attr,
                                                               BRM::DBRM& em, const BRM::EMEntry& entry,
                                                               int* state) const
{
  int32_t seqNum;
  MinMaxPartitionInfo partInfo(entry);
  *state = em.getExtentMaxMin(entry.range.start, partInfo.max, partInfo.min, seqNum);
  // char column order swap
  if (attr.colWidth <= 7)
  {
    partInfo.max = uint64ToStr(partInfo.max);
    partInfo.min = uint64ToStr(partInfo.min);
  }
  return partInfo;
}

/****************************************************************************/

string TypeHandler::PrintPartitionValueSInt64(const SystemCatalog::TypeAttributesStd& attr,
                                              const MinMaxPartitionInfo& partInfo,
                                              const SimpleValue& startVal, round_style_t rfMin,
                                              const SimpleValue& endVal, round_style_t rfMax) const
{
  if (!partInfo.isSuitableSInt64(startVal, rfMin, endVal, rfMax))
    return "";

  ostringstreamL oss;
  if (partInfo.min > partInfo.max)
    oss << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
  else
    oss << setw(30) << format(SimpleValueSInt64(partInfo.min), attr) << setw(30)
        << format(SimpleValueSInt64(partInfo.max), attr);
  return oss.str();
}

string TypeHandler::PrintPartitionValueUInt64(const SystemCatalog::TypeAttributesStd& attr,
                                              const MinMaxPartitionInfo& partInfo,
                                              const SimpleValue& startVal, round_style_t rfMin,
                                              const SimpleValue& endVal, round_style_t rfMax) const
{
  if (!partInfo.isSuitableUInt64(startVal, rfMin, endVal, rfMax))
    return "";

  ostringstreamL oss;
  if (static_cast<uint64_t>(partInfo.min) > static_cast<uint64_t>(partInfo.max))
    oss << setw(30) << "Empty/Null" << setw(30) << "Empty/Null";
  else
    oss << setw(30) << format(SimpleValueSInt64(partInfo.min), attr) << setw(30)
        << format(SimpleValueSInt64(partInfo.max), attr);
  return oss.str();
}

string TypeHandlerXDecimal::PrintPartitionValue128(const SystemCatalog::TypeAttributesStd& attr,
                                                   const MinMaxPartitionInfo& partInfo,
                                                   const SimpleValue& startVal, round_style_t rfMin,
                                                   const SimpleValue& endVal, round_style_t rfMax) const
{
  if (!partInfo.isSuitableSInt128(startVal, rfMin, endVal, rfMax))
    return "";

  ostringstreamL oss;
  if (partInfo.int128Min > partInfo.int128Max)
    oss << setw(datatypes::Decimal::MAXLENGTH16BYTES) << "Empty/Null"
        << setw(datatypes::Decimal::MAXLENGTH16BYTES) << "Empty/Null";
  else
    oss << setw(datatypes::Decimal::MAXLENGTH16BYTES) << format(SimpleValueSInt128(partInfo.int128Min), attr)
        << setw(datatypes::Decimal::MAXLENGTH16BYTES) << format(SimpleValueSInt128(partInfo.int128Max), attr);
  return oss.str();
}

/****************************************************************************/

boost::any TypeHandlerSInt8::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  char tinyintvalue = joblist::TINYINTNULL;
  boost::any value = tinyintvalue;
  return value;
}

boost::any TypeHandlerUInt8::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint8_t utinyintvalue = joblist::UTINYINTNULL;
  boost::any value = utinyintvalue;
  return value;
}

boost::any TypeHandlerSInt16::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  short smallintvalue = joblist::SMALLINTNULL;
  boost::any value = smallintvalue;
  return value;
}

boost::any TypeHandlerUInt16::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint16_t usmallintvalue = joblist::USMALLINTNULL;
  boost::any value = usmallintvalue;
  return value;
}

boost::any TypeHandlerSInt24::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  int intvalue = joblist::INTNULL;
  boost::any value = intvalue;
  return value;
}

boost::any TypeHandlerSInt32::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  int intvalue = joblist::INTNULL;
  boost::any value = intvalue;
  return value;
}

boost::any TypeHandlerUInt24::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint32_t uintvalue = joblist::UINTNULL;
  boost::any value = uintvalue;
  return value;
}

boost::any TypeHandlerUInt32::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint32_t uintvalue = joblist::UINTNULL;
  boost::any value = uintvalue;
  return value;
}

boost::any TypeHandlerSInt64::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  long long bigint = joblist::BIGINTNULL;
  boost::any value = bigint;
  return value;
}

boost::any TypeHandlerUInt64::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint64_t ubigint = joblist::UBIGINTNULL;
  boost::any value = ubigint;
  return value;
}

boost::any TypeHandlerXDecimal::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  if (LIKELY(attr.colWidth == 16))
  {
    int128_t val;
    datatypes::Decimal::setWideDecimalNullValue(val);
    boost::any value = val;
    return value;
  }
  if (attr.colWidth == SystemCatalog::EIGHT_BYTE)
  {
    long long eightbyte = joblist::BIGINTNULL;
    boost::any value = eightbyte;
    return value;
  }
  if (attr.colWidth == SystemCatalog::FOUR_BYTE)
  {
    int intvalue = joblist::INTNULL;
    boost::any value = intvalue;
    return value;
  }
  if (attr.colWidth == SystemCatalog::TWO_BYTE)
  {
    short smallintvalue = joblist::SMALLINTNULL;
    boost::any value = smallintvalue;
    return value;
  }
  if (attr.colWidth == SystemCatalog::ONE_BYTE)
  {
    char tinyintvalue = joblist::TINYINTNULL;
    boost::any value = tinyintvalue;
    return value;
  }
  WriteEngine::Token nullToken;
  boost::any value = nullToken;
  return value;
}

boost::any TypeHandlerSFloat::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint32_t jlfloatnull = joblist::FLOATNULL;
  float* fp = reinterpret_cast<float*>(&jlfloatnull);
  boost::any value = *fp;
  return value;
}

boost::any TypeHandlerUFloat::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint32_t jlfloatnull = joblist::FLOATNULL;
  float* fp = reinterpret_cast<float*>(&jlfloatnull);
  boost::any value = *fp;
  return value;
}

boost::any TypeHandlerSDouble::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint64_t jldoublenull = joblist::DOUBLENULL;
  double* dp = reinterpret_cast<double*>(&jldoublenull);
  boost::any value = *dp;
  return value;
}

boost::any TypeHandlerUDouble::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint64_t jldoublenull = joblist::DOUBLENULL;
  double* dp = reinterpret_cast<double*>(&jldoublenull);
  boost::any value = *dp;
  return value;
}

boost::any TypeHandlerDate::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint32_t d = joblist::DATENULL;
  boost::any value = d;
  return value;
}

boost::any TypeHandlerDatetime::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint64_t d = joblist::DATETIMENULL;
  boost::any value = d;
  return value;
}

boost::any TypeHandlerTime::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  int64_t d = joblist::TIMENULL;
  boost::any value = d;
  return value;
}

boost::any TypeHandlerTimestamp::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  uint64_t d = joblist::TIMESTAMPNULL;
  boost::any value = d;
  return value;
}

boost::any TypeHandlerChar::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  switch (attr.colWidth)
  {
    case 1:
    {
      // charnull = joblist::CHAR1NULL;
      std::string charnull = "\376";
      boost::any value = charnull;
      return value;
    }
    case 2:
    {
      // charnull = joblist::CHAR2NULL;
      std::string charnull = "\377\376";
      boost::any value = charnull;
      return value;
    }
    case 3:
    case 4:
    {
      // charnull = joblist::CHAR4NULL;
      std::string charnull = "\377\377\377\376";
      boost::any value = charnull;
      return value;
    }
    case 5:
    case 6:
    case 7:
    case 8:
    {
      // charnull = joblist::CHAR8NULL;
      std::string charnull = "\377\377\377\377\377\377\377\376";
      boost::any value = charnull;
      return value;
    }
  }
  WriteEngine::Token nullToken;
  boost::any value = nullToken;
  return value;
}

boost::any TypeHandlerStr::getNullValueForTypeVarcharText(const SystemCatalog::TypeAttributesStd& attr) const
{
  switch (attr.colWidth)
  {
    case 1:
    {
      // charnull = joblist::CHAR2NULL;
      std::string charnull = "\377\376";
      boost::any value = charnull;
      return value;
    }
    case 2:
    case 3:
    {
      // charnull = joblist::CHAR4NULL;
      std::string charnull = "\377\377\377\376";
      boost::any value = charnull;
      return value;
    }
    case 4:
    case 5:
    case 6:
    case 7:
    {
      // charnull = joblist::CHAR8NULL;
      std::string charnull = "\377\377\377\377\377\377\377\376";
      boost::any value = charnull;
      return value;
    }
  }
  WriteEngine::Token nullToken;
  boost::any value = nullToken;
  return value;
}

boost::any TypeHandlerBlob::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  WriteEngine::Token nullToken;
  boost::any value = nullToken;
  return value;
}

boost::any TypeHandlerVarbinary::getNullValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  WriteEngine::Token nullToken;
  boost::any value = nullToken;
  return value;
}

/****************************************************************************/

boost::any TypeHandlerBit::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                             const ConvertFromStringParam& prm, const std::string& data,
                                             bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToBit(colType, prm, data, pushWarning);
}

boost::any TypeHandlerSInt8::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                               const ConvertFromStringParam& prm, const std::string& data,
                                               bool& pushWarning) const
{
  int64_t val64;
  dataconvert::number_int_value(data, SystemCatalog::TINYINT, colType, pushWarning, prm.noRoundup(), val64);
  boost::any value = (char)val64;
  return value;
}

boost::any TypeHandlerSInt16::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  int64_t val64;
  dataconvert::number_int_value(data, SystemCatalog::SMALLINT, colType, pushWarning, prm.noRoundup(), val64);
  boost::any value = (short)val64;
  return value;
}

boost::any TypeHandlerSInt24::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  int64_t val64;
  dataconvert::number_int_value(data, SystemCatalog::MEDINT, colType, pushWarning, prm.noRoundup(), val64);
  boost::any value = (int)val64;
  return value;
}

boost::any TypeHandlerSInt32::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  int64_t val64;
  dataconvert::number_int_value(data, SystemCatalog::INT, colType, pushWarning, prm.noRoundup(), val64);
  boost::any value = (int)val64;
  return value;
}

boost::any TypeHandlerSInt64::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  int64_t val64;
  dataconvert::number_int_value(data, SystemCatalog::BIGINT, colType, pushWarning, prm.noRoundup(), val64);
  boost::any value = (long long)val64;
  return value;
}

boost::any TypeHandlerUInt8::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                               const ConvertFromStringParam& prm, const std::string& data,
                                               bool& pushWarning) const

{
  boost::any value = (uint8_t)dataconvert::number_uint_value(data, SystemCatalog::UTINYINT, colType,
                                                             pushWarning, prm.noRoundup());
  return value;
}

boost::any TypeHandlerUInt16::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  boost::any value = (uint16_t)dataconvert::number_uint_value(data, SystemCatalog::USMALLINT, colType,
                                                              pushWarning, prm.noRoundup());
  return value;
}

boost::any TypeHandlerUInt24::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  boost::any value = (uint32_t)dataconvert::number_uint_value(data, SystemCatalog::UMEDINT, colType,
                                                              pushWarning, prm.noRoundup());
  return value;
}

boost::any TypeHandlerUInt32::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  boost::any value = (uint32_t)dataconvert::number_uint_value(data, SystemCatalog::UINT, colType, pushWarning,
                                                              prm.noRoundup());
  return value;
}

boost::any TypeHandlerUInt64::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  boost::any value = (uint64_t)dataconvert::number_uint_value(data, SystemCatalog::UBIGINT, colType,
                                                              pushWarning, prm.noRoundup());
  return value;
}

boost::any TypeHandlerSFloat::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToFloat(SystemCatalog::FLOAT, data, pushWarning);
}

boost::any TypeHandlerUFloat::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                const ConvertFromStringParam& prm, const std::string& data,
                                                bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToFloat(SystemCatalog::UFLOAT, data, pushWarning);
}

boost::any TypeHandlerSDouble::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                 const ConvertFromStringParam& prm, const std::string& data,
                                                 bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToDouble(SystemCatalog::DOUBLE, data, pushWarning);
}

boost::any TypeHandlerUDouble::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                 const ConvertFromStringParam& prm, const std::string& data,
                                                 bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToDouble(SystemCatalog::UDOUBLE, data, pushWarning);
}

boost::any TypeHandlerDate::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToDate(data, pushWarning);
}

boost::any TypeHandlerDatetime::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                  const ConvertFromStringParam& prm, const std::string& data,
                                                  bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToDatetime(data, pushWarning);
}

boost::any TypeHandlerTime::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToTime(colType, data, pushWarning);
}

boost::any TypeHandlerTimestamp::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                   const ConvertFromStringParam& prm, const std::string& data,
                                                   bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToTimestamp(prm, data, pushWarning);
}

boost::any TypeHandlerChar::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToString(colType, data, pushWarning);
}

boost::any TypeHandlerVarchar::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                 const ConvertFromStringParam& prm, const std::string& data,
                                                 bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToString(colType, data, pushWarning);
}

boost::any TypeHandlerText::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToString(colType, data, pushWarning);
}

boost::any TypeHandlerClob::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  boost::any value = data;
  return value;
}

boost::any TypeHandlerBlob::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                              const ConvertFromStringParam& prm, const std::string& data,
                                              bool& pushWarning) const
{
  boost::any value = data;
  return value;
}

boost::any TypeHandlerVarbinary::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                   const ConvertFromStringParam& prm, const std::string& data,
                                                   bool& pushWarning) const
{
  boost::any value = data;
  return value;
}

boost::any TypeHandlerSDecimal64::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                    const ConvertFromStringParam& prm,
                                                    const std::string& data, bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToSDecimal(colType, prm, data, pushWarning);
}

boost::any TypeHandlerUDecimal64::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                    const ConvertFromStringParam& prm,
                                                    const std::string& data, bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToUDecimal(colType, prm, data, pushWarning);
}

boost::any TypeHandlerSDecimal128::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                     const ConvertFromStringParam& prm,
                                                     const std::string& data, bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToSDecimal(colType, prm, data, pushWarning);
}

boost::any TypeHandlerUDecimal128::convertFromString(const SystemCatalog::TypeAttributesStd& colType,
                                                     const ConvertFromStringParam& prm,
                                                     const std::string& data, bool& pushWarning) const
{
  return dataconvert::DataConvert::StringToUDecimal(colType, prm, data, pushWarning);
}

/****************************************************************************/
const uint8_t* getEmptyTypeHandlerSInt8()
{
  const static uint8_t TINYINTEMPTYROW = joblist::TINYINTEMPTYROW;
  return &TINYINTEMPTYROW;
}

const uint8_t* TypeHandlerSInt8::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerSInt8();
}

const uint8_t* getEmptyTypeHandlerSInt16()
{
  const static uint16_t SMALLINTEMPTYROW = joblist::SMALLINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&SMALLINTEMPTYROW);
}

const uint8_t* TypeHandlerSInt16::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerSInt16();
}

const uint8_t* getEmptyTypeHandlerSInt32()
{
  const static uint32_t INTEMPTYROW = joblist::INTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&INTEMPTYROW);
}

const uint8_t* TypeHandlerSInt24::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerSInt32();
}

const uint8_t* TypeHandlerSInt32::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerSInt32();
}

const uint8_t* getEmptyTypeHandlerSInt64()
{
  const static uint64_t BIGINTEMPTYROW = joblist::BIGINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&BIGINTEMPTYROW);
}

const uint8_t* TypeHandlerSInt64::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerSInt64();
}

const uint8_t* TypeHandlerUInt8::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  const static uint8_t UTINYINTEMPTYROW = joblist::UTINYINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&UTINYINTEMPTYROW);
}

const uint8_t* TypeHandlerUInt16::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  const static uint16_t USMALLINTEMPTYROW = joblist::USMALLINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&USMALLINTEMPTYROW);
}

const uint8_t* getEmptyTypeHandlerUInt32()
{
  const static uint32_t UINTEMPTYROW = joblist::UINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&UINTEMPTYROW);
}

const uint8_t* TypeHandlerUInt24::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerUInt32();
}

const uint8_t* TypeHandlerUInt32::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerUInt32();
}

const uint8_t* TypeHandlerUInt64::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  const static uint64_t UBIGINTEMPTYROW = joblist::UBIGINTEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&UBIGINTEMPTYROW);
}

const uint8_t* getEmptyTypeHandlerFloat(const SystemCatalog::TypeAttributesStd& attr)
{
  const static uint32_t FLOATEMPTYROW = joblist::FLOATEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&FLOATEMPTYROW);
}

const uint8_t* TypeHandlerUFloat::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerFloat(attr);
}

const uint8_t* TypeHandlerSFloat::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerFloat(attr);
}

const uint8_t* getEmptyTypeHandlerDouble(const SystemCatalog::TypeAttributesStd& attr)
{
  const static uint64_t DOUBLEEMPTYROW = joblist::DOUBLEEMPTYROW;
  return reinterpret_cast<const uint8_t*>(&DOUBLEEMPTYROW);
}

const uint8_t* TypeHandlerUDouble::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerDouble(attr);
}

const uint8_t* TypeHandlerSDouble::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerDouble(attr);
}

// returns a ptr to an empty magic value for TypeHandlerStr types
// args
//  attr - width, precision and scale
//  offset - offset value to reduce width for VARCHAR by 1
const uint8_t* getEmptyTypeHandlerStr(const SystemCatalog::TypeAttributesStd& attr, int8_t offset)
{
  const static uint8_t CHAR1EMPTYROW = joblist::CHAR1EMPTYROW;
  const static uint16_t CHAR2EMPTYROW = joblist::CHAR2EMPTYROW;
  const static uint32_t CHAR4EMPTYROW = joblist::CHAR4EMPTYROW;
  const static uint64_t CHAR8EMPTYROW = joblist::CHAR8EMPTYROW;

  if (attr.colWidth == (2 + offset))
    return reinterpret_cast<const uint8_t*>(&CHAR2EMPTYROW);
  else if (attr.colWidth >= (3 + offset) && attr.colWidth <= (4 + offset))
    return reinterpret_cast<const uint8_t*>(&CHAR4EMPTYROW);
  else if (attr.colWidth >= (5 + offset))
    return reinterpret_cast<const uint8_t*>(&CHAR8EMPTYROW);

  return reinterpret_cast<const uint8_t*>(&CHAR1EMPTYROW);
}

const uint8_t* TypeHandlerStr::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerStr(attr, 0);
}

const uint8_t* TypeHandlerVarchar::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyTypeHandlerStr(attr, -1);
}

inline const uint8_t* getEmptyValueForDecimal64(const SystemCatalog::TypeAttributesStd& attr)
{
  if (attr.colWidth <= 1)
    return getEmptyTypeHandlerSInt8();
  else if (attr.colWidth <= 2)
    return getEmptyTypeHandlerSInt16();
  else if (attr.colWidth <= 4)
    return getEmptyTypeHandlerSInt32();
  else
    return getEmptyTypeHandlerSInt64();
}

const uint8_t* TypeHandlerSDecimal64::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyValueForDecimal64(attr);
}

const uint8_t* TypeHandlerUDecimal64::getEmptyValueForType(const SystemCatalog::TypeAttributesStd& attr) const
{
  return getEmptyValueForDecimal64(attr);
}

const uint8_t* TypeHandlerSDecimal128::getEmptyValueForType(
    const SystemCatalog::TypeAttributesStd& attr) const
{
  return reinterpret_cast<const uint8_t*>(&datatypes::Decimal128Empty);
}

const uint8_t* TypeHandlerUDecimal128::getEmptyValueForType(
    const SystemCatalog::TypeAttributesStd& attr) const
{
  return reinterpret_cast<const uint8_t*>(&datatypes::Decimal128Empty);
}

/****************************************************************************/

}  // end of namespace datatypes

// vim:ts=2 sw=2:
