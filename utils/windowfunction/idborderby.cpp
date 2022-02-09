/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

//  $Id: idborderby.cpp 3932 2013-06-25 16:08:10Z xlou $

#include <iostream>
#include <cassert>
#include <string>
#include <stack>
using namespace std;

#include "objectreader.h"
#include "calpontselectexecutionplan.h"
#include "rowgroup.h"

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "resourcemanager.h"
using namespace joblist;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"

#include "joblisttypes.h"
#include "mcs_decimal.h"

// See agg_arg_charsets in sql_type.h to see conversion rules for
// items that have different char sets
namespace ordering
{
int TinyIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  int8_t v1 = l->row1().getIntField(fSpec.fIndex);
  int8_t v2 = l->row2().getIntField(fSpec.fIndex);
  int8_t nullValue = static_cast<int8_t>(joblist::TINYINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int SmallIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  int16_t v1 = l->row1().getIntField(fSpec.fIndex);
  int16_t v2 = l->row2().getIntField(fSpec.fIndex);
  int16_t nullValue = static_cast<int16_t>(joblist::SMALLINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int IntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  int32_t v1 = l->row1().getIntField(fSpec.fIndex);
  int32_t v2 = l->row2().getIntField(fSpec.fIndex);
  int32_t nullValue = static_cast<int32_t>(joblist::INTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int BigIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  int64_t v1 = l->row1().getIntField(fSpec.fIndex);
  int64_t v2 = l->row2().getIntField(fSpec.fIndex);
  int64_t nullValue = static_cast<int64_t>(joblist::BIGINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int WideDecimalCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;

  int128_t v1, v2;
  l->row1().getInt128Field(fSpec.fIndex, v1);
  l->row2().getInt128Field(fSpec.fIndex, v2);

  bool v1IsNull = v1 == datatypes::Decimal128Null;
  bool v2IsNull = v2 == datatypes::Decimal128Null;

  if (v1IsNull || v2IsNull)
  {
    if (!v1IsNull && v2IsNull)
      ret = fSpec.fNf;
    else if (v1IsNull && !v2IsNull)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int UTinyIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint8_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint8_t v2 = l->row2().getUintField(fSpec.fIndex);
  uint8_t nullValue = static_cast<uint8_t>(joblist::UTINYINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int USmallIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint16_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint16_t v2 = l->row2().getUintField(fSpec.fIndex);
  uint16_t nullValue = static_cast<uint16_t>(joblist::USMALLINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int UIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint32_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint32_t v2 = l->row2().getUintField(fSpec.fIndex);
  uint32_t nullValue = static_cast<uint32_t>(joblist::UINTNULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int UBigIntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint64_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint64_t v2 = l->row2().getUintField(fSpec.fIndex);

  if (v1 == joblist::UBIGINTNULL || v2 == joblist::UBIGINTNULL)
  {
    if (v1 != joblist::UBIGINTNULL && v2 == joblist::UBIGINTNULL)
      ret = fSpec.fNf;
    else if (v1 == joblist::UBIGINTNULL && v2 != joblist::UBIGINTNULL)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int StringCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  bool b1 = l->row1().isNullValue(fSpec.fIndex);
  bool b2 = l->row2().isNullValue(fSpec.fIndex);

  int ret = 0;

  if (b1 == true || b2 == true)
  {
    if (b1 == false && b2 == true)
      ret = fSpec.fNf;
    else if (b1 == true && b2 == false)
      ret = -fSpec.fNf;
  }
  else
  {
    auto const str1 = l->row1().getConstString(fSpec.fIndex);
    auto const str2 = l->row2().getConstString(fSpec.fIndex);

    if (!cs)
      cs = l->rowGroup()->getCharset(fSpec.fIndex);

    ret = fSpec.fAsc * cs->strnncollsp(str1.str(), str1.length(), str2.str(), str2.length());
  }

  return ret;
}

int DoubleCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  uint64_t uiv1 = l->row1().getUintField(fSpec.fIndex);
  uint64_t uiv2 = l->row2().getUintField(fSpec.fIndex);
  int ret = 0;

  if (uiv1 == joblist::DOUBLENULL || uiv2 == joblist::DOUBLENULL)
  {
    if (uiv1 != joblist::DOUBLENULL && uiv2 == joblist::DOUBLENULL)
      ret = fSpec.fNf;
    else if (uiv1 == joblist::DOUBLENULL && uiv2 != joblist::DOUBLENULL)
      ret = -fSpec.fNf;
  }
  else
  {
    double v1 = l->row1().getDoubleField(fSpec.fIndex);
    double v2 = l->row2().getDoubleField(fSpec.fIndex);
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int FloatCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int32_t iv1 = l->row1().getIntField(fSpec.fIndex);
  int32_t iv2 = l->row2().getIntField(fSpec.fIndex);
  int32_t nullValue = static_cast<int32_t>(joblist::FLOATNULL);
  int ret = 0;

  if (iv1 == nullValue || iv2 == nullValue)
  {
    if (iv1 != nullValue && iv2 == nullValue)
      ret = fSpec.fNf;
    else if (iv1 == nullValue && iv2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    float v1 = l->row1().getFloatField(fSpec.fIndex);
    float v2 = l->row2().getFloatField(fSpec.fIndex);
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int LongDoubleCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  long double v1 = l->row1().getLongDoubleField(fSpec.fIndex);
  long double v2 = l->row2().getLongDoubleField(fSpec.fIndex);

  int ret = 0;

  if (v1 == joblist::LONGDOUBLENULL || v2 == joblist::LONGDOUBLENULL)
  {
    if (v1 != joblist::LONGDOUBLENULL && v2 == joblist::LONGDOUBLENULL)
      ret = fSpec.fNf;
    else if (v1 == joblist::LONGDOUBLENULL && v2 != joblist::LONGDOUBLENULL)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int DateCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint32_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint32_t v2 = l->row2().getUintField(fSpec.fIndex);
  uint32_t nullValue = static_cast<uint32_t>(joblist::DATENULL);

  if (v1 == nullValue || v2 == nullValue)
  {
    if (v1 != nullValue && v2 == nullValue)
      ret = fSpec.fNf;
    else if (v1 == nullValue && v2 != nullValue)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int DatetimeCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int ret = 0;
  uint64_t v1 = l->row1().getUintField(fSpec.fIndex);
  uint64_t v2 = l->row2().getUintField(fSpec.fIndex);

  if (v1 == joblist::DATETIMENULL || v2 == joblist::DATETIMENULL)
  {
    if (v1 != joblist::DATETIMENULL && v2 == joblist::DATETIMENULL)
      ret = fSpec.fNf;
    else if (v1 == joblist::DATETIMENULL && v2 != joblist::DATETIMENULL)
      ret = -fSpec.fNf;
  }
  else
  {
    if (v1 > v2)
      ret = fSpec.fAsc;
    else if (v1 < v2)
      ret = -fSpec.fAsc;
  }

  return ret;
}

int TimeCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
  l->row1().setData(r1);
  l->row2().setData(r2);

  int64_t v1 = l->row1().getIntField(fSpec.fIndex);
  int64_t v2 = l->row2().getIntField(fSpec.fIndex);

  bool b1 = (joblist::TIMENULL == (uint64_t)v1);
  bool b2 = (joblist::TIMENULL == (uint64_t)v2);

  int ret = 0;

  if (b1 == true || b2 == true)
  {
    if (b1 == false && b2 == true)
      ret = fSpec.fNf;
    else if (b1 == true && b2 == false)
      ret = -fSpec.fNf;
  }
  else
  {
    // ((int64_t) -00:00:26) > ((int64_t) -00:00:25)
    // i.e. For 2 negative TIME values, we invert the order of
    // comparison operations to force "-00:00:26" to appear before
    // "-00:00:25" in ascending order.
    if (v1 < 0 && v2 < 0)
    {
      // Unset the MSB.
      v1 &= ~(1ULL << 63);
      v2 &= ~(1ULL << 63);
      if (v1 < v2)
        ret = fSpec.fAsc;
      else if (v1 > v2)
        ret = -fSpec.fAsc;
    }
    else
    {
      if (v1 > v2)
        ret = fSpec.fAsc;
      else if (v1 < v2)
        ret = -fSpec.fAsc;
    }
  }

  return ret;
}

bool CompareRule::less(Row::Pointer r1, Row::Pointer r2)
{
  for (vector<Compare*>::iterator i = fCompares.begin(); i != fCompares.end(); i++)
  {
    int c = ((*(*i))(fIdbCompare, r1, r2));

    if (c < 0)
      return true;
    else if (c > 0)
      return false;
  }

  return false;
}

void CompareRule::revertRules()
{
  std::vector<Compare*>::iterator fCompareIter = fCompares.begin();
  for (; fCompareIter != fCompares.end(); fCompareIter++)
  {
    (*fCompareIter)->revertSortSpec();
  }
}

void CompareRule::compileRules(const std::vector<IdbSortSpec>& spec, const rowgroup::RowGroup& rg)
{
  const vector<CalpontSystemCatalog::ColDataType>& types = rg.getColTypes();
  const auto& offsets = rg.getOffsets();

  for (vector<IdbSortSpec>::const_iterator i = spec.begin(); i != spec.end(); i++)
  {
    switch (types[i->fIndex])
    {
      case CalpontSystemCatalog::TINYINT:
      {
        Compare* c = new TinyIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::SMALLINT:
      {
        Compare* c = new SmallIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      {
        Compare* c = new IntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::BIGINT:
      {
        Compare* c = new BigIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        Compare* c;
        uint32_t len = rg.getColumnWidth(i->fIndex);
        switch (len)
        {
          case datatypes::MAXDECIMALWIDTH: c = new WideDecimalCompare(*i, offsets[i->fIndex]); break;
          case datatypes::MAXLEGACYWIDTH: c = new BigIntCompare(*i); break;
          case 1: c = new TinyIntCompare(*i); break;
          case 2: c = new SmallIntCompare(*i); break;
          case 4: c = new IntCompare(*i); break;
        }

        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::UTINYINT:
      {
        Compare* c = new UTinyIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::USMALLINT:
      {
        Compare* c = new USmallIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      {
        Compare* c = new UIntCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::UBIGINT:
      {
        Compare* c = new UBigIntCompare(*i);
        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::TEXT:
      {
        Compare* c = new StringCompare(*i);
        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      {
        Compare* c = new DoubleCompare(*i);
        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      {
        Compare* c = new FloatCompare(*i);
        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::LONGDOUBLE:
      {
        Compare* c = new LongDoubleCompare(*i);
        fCompares.push_back(c);
        break;
      }

      case CalpontSystemCatalog::DATE:
      {
        Compare* c = new DateCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIMESTAMP:
      {
        Compare* c = new DatetimeCompare(*i);
        fCompares.push_back(c);
        break;
      }
      case CalpontSystemCatalog::TIME:
      {
        Compare* c = new TimeCompare(*i);
        fCompares.push_back(c);
        break;
      }

      default:
      {
        break;
      }
    }
  }
}

void IdbCompare::initialize(const RowGroup& rg)
{
  fRowGroup = rg;
  fRowGroup.initRow(&fRow1);
  fRowGroup.initRow(&fRow2);
}

void IdbCompare::setStringTable(bool b)
{
  fRowGroup.setUseStringTable(b);
  fRowGroup.initRow(&fRow1);
  fRowGroup.initRow(&fRow2);
}

// OrderByData class implementation
OrderByData::OrderByData(const std::vector<IdbSortSpec>& spec, const rowgroup::RowGroup& rg)
{
  IdbCompare::initialize(rg);
  fRule.compileRules(spec, rg);
  fRule.fIdbCompare = this;
}

// OrderByData class dtor
OrderByData::~OrderByData()
{
  // delete compare objects
  vector<Compare*>::iterator i = fRule.fCompares.begin();

  while (i != fRule.fCompares.end())
  {
    if (*i)
    {
      delete *i;
      *i = NULL;
    }
    i++;
  }
}

// IdbOrderBy class implementation
IdbOrderBy::IdbOrderBy()
 : fDistinct(false), fMemSize(0), fRowsPerRG(rowgroup::rgCommonSize), fErrorCode(0), fRm(NULL)
{
}

IdbOrderBy::~IdbOrderBy()
{
  if (fRm)
    fRm->returnMemory(fMemSize, fSessionMemLimit);

  // delete compare objects
  vector<Compare*>::iterator i = fRule.fCompares.begin();

  while (i != fRule.fCompares.end())
    delete *i++;
}

void IdbOrderBy::initialize(const RowGroup& rg)
{
  // initialize rows
  IdbCompare::initialize(rg);

  uint64_t newSize = rg.getSizeWithStrings(fRowsPerRG);
  if (fRm && !fRm->getMemory(newSize, fSessionMemLimit))
  {
    cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
    throw IDBExcept(fErrorCode);
  }
  fMemSize += newSize;
  fData.reinit(fRowGroup, fRowsPerRG);
  fRowGroup.setData(&fData);
  fRowGroup.resetRowGroup(0);
  fRowGroup.initRow(&fRow0);
  fRowGroup.getRow(0, &fRow0);

  // set compare functors
  fRule.compileRules(fOrderByCond, fRowGroup);

  fRowGroup.initRow(&row1);
  fRowGroup.initRow(&row2);

  if (fDistinct)
  {
    fDistinctMap.reset(new DistinctMap_t(10, Hasher(this, getKeyLength()), Eq(this, getKeyLength())));
  }
}

bool IdbOrderBy::getData(RGData& data)
{
  if (fDataQueue.empty())
    return false;

  data = fDataQueue.front();
  fDataQueue.pop();

  return true;
}

bool EqualCompData::operator()(Row::Pointer a, Row::Pointer b)
{
  bool eq = true;
  fRow1.setData(a);
  fRow2.setData(b);

  for (vector<uint64_t>::const_iterator i = fIndex.begin(); i != fIndex.end() && eq; i++)
  {
    CalpontSystemCatalog::ColDataType type = fRow1.getColType(*i);

    switch (type)
    {
      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::BIGINT:
      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT:
      case CalpontSystemCatalog::DATE:
      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIMESTAMP:
      case CalpontSystemCatalog::TIME:
      {
        // equal compare. ignore sign and null
        eq = (fRow1.getUintField(*i) == fRow2.getUintField(*i));
        break;
      }

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        // equal compare. ignore sign and null
        if (UNLIKELY(fRow1.getColumnWidth(*i) < datatypes::MAXDECIMALWIDTH))
        {
          eq = (fRow1.getUintField(*i) == fRow2.getUintField(*i));
        }
        else if (fRow1.getColumnWidth(*i) == datatypes::MAXDECIMALWIDTH)
        {
          eq = (*fRow1.getBinaryField<int128_t>(*i) == *fRow2.getBinaryField<int128_t>(*i));
        }
        break;
      }

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      {
        eq = (fRow1.getStringField(*i) == fRow2.getStringField(*i));
        break;
      }

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      {
        eq = (fRow1.getDoubleField(*i) == fRow2.getDoubleField(*i));
        break;
      }

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      {
        eq = (fRow1.getFloatField(*i) == fRow2.getFloatField(*i));
        break;
      }

      case CalpontSystemCatalog::LONGDOUBLE:
      {
        eq = (fRow1.getLongDoubleField(*i) == fRow2.getLongDoubleField(*i));
        break;
      }

      default:
      {
        eq = false;
        uint64_t ec = ERR_WF_UNKNOWN_COL_TYPE;
        cerr << IDBErrorInfo::instance()->errorMsg(ec, type) << " @" << __FILE__ << ":" << __LINE__;
        throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ec, type), ec);
        break;
      }
    }
  }

  return eq;
}

uint64_t IdbOrderBy::Hasher::operator()(const Row::Pointer& p) const
{
  Row& row = ts->row1;
  row.setPointer(p);
  // MCOL-1829 Row::hash uses colcount - 1 as an array idx down a callstack.
  uint64_t ret = row.hash(colCount - 1);
  return ret;
}

bool IdbOrderBy::Eq::operator()(const Row::Pointer& d1, const Row::Pointer& d2) const
{
  Row &r1 = ts->row1, &r2 = ts->row2;
  r1.setPointer(d1);
  r2.setPointer(d2);
  // MCOL-1829 Row::equals uses 2nd argument as key columns container size boundary
  bool ret = r1.equals(r2, colCount - 1);

  return ret;
}

}  // namespace ordering
// vim:ts=4 sw=4:
