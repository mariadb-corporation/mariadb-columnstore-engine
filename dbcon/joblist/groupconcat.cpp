/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019-2023 MariaDB Corporation

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

//  $Id: groupconcat.cpp 9705 2013-07-17 20:06:07Z pleblanc $

#include <algorithm>
#include <iostream>
// #define NDEBUG
#include <cassert>
#include <ranges>
#include <string>
#include "windowfunction/idborderby.h"
using namespace std;

#include "errorids.h"
using namespace logging;

#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "rowcolumn.h"
#include "groupconcatcolumn.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "rowgroup.h"
#include "rowaggregation.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#include "groupconcat.h"

using namespace ordering;

#include "jobstep.h"
#include "jlf_common.h"
#include "mcs_decimal.h"

#include "utils/json/json.hpp"
using namespace nlohmann;

namespace joblist
{
// GroupConcatInfo class implementation
GroupConcatInfo::GroupConcatInfo() = default;
GroupConcatInfo::~GroupConcatInfo() = default;

void GroupConcatInfo::prepGroupConcat(JobInfo& jobInfo)
{
  for (const auto& gccol : jobInfo.groupConcatCols)
  {
    auto* gcc = dynamic_cast<GroupConcatColumn*>(gccol.get());
    const auto* rcp = dynamic_cast<const RowColumn*>(gcc->aggParms()[0].get());

    SP_GroupConcat groupConcat(new GroupConcat(jobInfo.rm, jobInfo.umMemLimit));
    groupConcat->fSeparator = gcc->separator();
    groupConcat->fDistinct = gcc->distinct();
    groupConcat->fSize = gcc->resultType().colWidth;
    groupConcat->fTimeZone = jobInfo.timeZone;

    int key = -1;
    const vector<SRCP>& cols = rcp->columnVec();

    for (uint64_t j = 0, k = 0; j < cols.size(); j++)
    {
      const auto* cc = dynamic_cast<const ConstantColumn*>(cols[j].get());

      if (cc == nullptr)
      {
        key = getColumnKey(cols[j], jobInfo);
        fColumns.insert(key);
        groupConcat->fGroupCols.emplace_back(key, k++);
      }
      else
      {
        groupConcat->fConstCols.emplace_back(cc->constval(), j);
      }
    }

    vector<SRCP>& orderCols = gcc->orderCols();

    for (const auto& orderCol : orderCols)
    {
      if (dynamic_cast<const ConstantColumn*>(orderCol.get()) != nullptr)
        continue;

      key = getColumnKey(orderCol, jobInfo);
      fColumns.insert(key);
      groupConcat->fOrderCols.emplace_back(key, orderCol->asc());
    }

    groupConcat->id = fGroupConcat.size();
    fGroupConcat.emplace_back(std::move(groupConcat));
  }

  // Rare case: all columns in group_concat are constant columns, use a column in column map.
  if (!jobInfo.groupConcatCols.empty() && fColumns.empty())
  {
    int key = -1;

    for (auto i = jobInfo.tableList.begin(); i != jobInfo.tableList.end() && key == -1; ++i)
    {
      if (!jobInfo.columnMap[*i].empty())
      {
        key = *(jobInfo.columnMap[*i].begin());
      }
    }

    if (key != -1)
    {
      fColumns.insert(key);
    }
    else
    {
      throw runtime_error("Empty column map.");
    }
  }
}

uint32_t GroupConcatInfo::getColumnKey(const SRCP& srcp, JobInfo& jobInfo) const
{
  int colKey = -1;
  const auto* sc = dynamic_cast<const SimpleColumn*>(srcp.get());

  if (sc != nullptr)
  {
    if (sc->schemaName().empty())
    {
      // bug3839, handle columns from subquery.
      SimpleColumn tmp(*sc, jobInfo.sessionId);
      tmp.oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
      colKey = getTupleKey(jobInfo, &tmp);
    }
    else
    {
      colKey = getTupleKey(jobInfo, sc);
    }

    // check if this is a dictionary column
    if (jobInfo.keyInfo->dictKeyMap.find(colKey) != jobInfo.keyInfo->dictKeyMap.end())
      colKey = jobInfo.keyInfo->dictKeyMap[colKey];
  }
  else
  {
    const auto* ac = dynamic_cast<const ArithmeticColumn*>(srcp.get());
    const auto* fc = dynamic_cast<const FunctionColumn*>(srcp.get());

    if (ac != nullptr || fc != nullptr)
    {
      colKey = getExpTupleKey(jobInfo, srcp->expressionId());
    }
    else
    {
      cerr << "Unsupported GROUP_CONCAT/JSON_ARRAYAGG column. " << srcp->toString() << endl;
      throw runtime_error("Unsupported GROUP_CONCAT/JSON_ARRAYAGG column.");
    }
  }

  return colKey;
}

void GroupConcatInfo::mapColumns(const RowGroup& projRG)
{
  map<uint32_t, uint32_t> projColumnMap;
  const vector<uint32_t>& keysProj = projRG.getKeys();

  for (uint64_t i = 0; i < projRG.getColumnCount(); i++)
    projColumnMap[keysProj[i]] = i;

  for (auto k = fGroupConcat.begin(); k != fGroupConcat.end(); k++)
  {
    vector<uint32_t> pos;
    vector<uint32_t> oids;
    vector<uint32_t> keys;
    vector<uint32_t> scale;
    vector<uint32_t> precision;
    vector<CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> csNums;
    pos.push_back(2);

    auto i1 = (*k)->fGroupCols.begin();

    while (i1 != (*k)->fGroupCols.end())
    {
      map<uint32_t, uint32_t>::iterator j = projColumnMap.find(i1->first);

      if (j == projColumnMap.end())
      {
        cerr << "Concat/ArrayAgg Key:" << i1->first << " is not projected." << endl;
        throw runtime_error("Project error.");
      }

      pos.push_back(pos.back() + projRG.getColumnWidth(j->second));
      oids.push_back(projRG.getOIDs()[j->second]);
      keys.push_back(projRG.getKeys()[j->second]);
      types.push_back(projRG.getColTypes()[j->second]);
      csNums.push_back(projRG.getCharsetNumber(j->second));
      scale.push_back(projRG.getScale()[j->second]);
      precision.push_back(projRG.getPrecision()[j->second]);

      ++i1;
    }

    auto i2 = (*k)->fOrderCols.begin();

    while (i2 != (*k)->fOrderCols.end())
    {
      auto j = projColumnMap.find(i2->first);

      if (j == projColumnMap.end())
      {
        cerr << "Order Key:" << i2->first << " is not projected." << endl;
        throw runtime_error("Project error.");
      }

      vector<uint32_t>::iterator i3 = find(keys.begin(), keys.end(), j->first);
      int idx = 0;

      if (i3 == keys.end())
      {
        idx = keys.size();

        pos.push_back(pos.back() + projRG.getColumnWidth(j->second));
        oids.push_back(projRG.getOIDs()[j->second]);
        keys.push_back(projRG.getKeys()[j->second]);
        types.push_back(projRG.getColTypes()[j->second]);
        csNums.push_back(projRG.getCharsetNumber(j->second));
        scale.push_back(projRG.getScale()[j->second]);
        precision.push_back(projRG.getPrecision()[j->second]);
      }
      else
      {
        idx = std::distance(keys.begin(), i3);
      }

      (*k)->fOrderCond.push_back(make_pair(idx, i2->second));

      ++i2;
    }

    (*k)->fRowGroup = RowGroup(oids.size(), pos, oids, keys, types, csNums, scale, precision,
                               projRG.getStringTableThreshold(), false);

    // MCOL-5429/MCOL-5491 Use stringstore if the datatype of the groupconcat/json_arrayagg
    // field is a long string.
    if ((*k)->fRowGroup.hasLongString())
    {
      (*k)->fRowGroup.setUseStringTable(true);
    }

    (*k)->fMapping = makeMapping(projRG, (*k)->fRowGroup);
  }
}

std::shared_ptr<int[]> GroupConcatInfo::makeMapping(const RowGroup& in, const RowGroup& out) const
{
  // For some reason using the rowgroup mapping fcns don't work completely right in this class
  std::shared_ptr<int[]> mapping(new int[out.getColumnCount()]);

  for (uint64_t i = 0; i < out.getColumnCount(); i++)
  {
    for (uint64_t j = 0; j < in.getColumnCount(); j++)
    {
      if ((out.getKeys()[i] == in.getKeys()[j]))
      {
        mapping[i] = j;
        break;
      }
    }
  }

  return mapping;
}

const string GroupConcatInfo::toString() const
{
  ostringstream oss;
  oss << "GroupConcatInfo: toString() to be implemented.";
  oss << endl;

  return oss.str();
}

GroupConcatAg::GroupConcatAg(rowgroup::SP_GroupConcat& gcc, bool isJsonArrayAgg)
 : fGroupConcat(gcc), fIsJsonArrayAgg(isJsonArrayAgg)
{
  initialize();
}

GroupConcatAg::~GroupConcatAg() = default;

void GroupConcatAg::initialize()
{
  if (fGroupConcat->fDistinct || fGroupConcat->fOrderCols.size() > 0)
    fConcator.reset(new GroupConcatOrderBy(fIsJsonArrayAgg));
  else
    fConcator.reset(new GroupConcatNoOrder(fIsJsonArrayAgg));

  fConcator->initialize(fGroupConcat);

  // MCOL-5429/MCOL-5491 Use stringstore if the datatype of the group_concat/json_arrayagg
  // field is a long string.
  if (fGroupConcat->fRowGroup.hasLongString())
  {
    fRowGroup = fGroupConcat->fRowGroup;
    fRowGroup.setUseStringTable(true);
    fRowGroup.setUseOnlyLongString(true);
    fRowRGData.reinit(fRowGroup, 1);
    fRowGroup.setData(&fRowRGData);
    fRowGroup.resetRowGroup(0);
    fRowGroup.initRow(&fRow);
    fRowGroup.getRow(0, &fRow);
    fMemSize = fRowGroup.getSizeWithStrings(1);
  }
  else
  {
    fGroupConcat->fRowGroup.initRow(&fRow, true);
    fData.reset(new uint8_t[fRow.getSize()]);
    fRow.setData(rowgroup::Row::Pointer(fData.get()));
    fMemSize = fRow.getSize();
  }
}

void GroupConcatAg::processRow(const rowgroup::Row& inRow)
{
  applyMapping(fGroupConcat->fMapping, inRow);
  fConcator->processRow(fRow);
}

void GroupConcatAg::merge(const rowgroup::Row& inRow, uint64_t i)
{
  auto* gccAg = dynamic_cast<joblist::GroupConcatAg*>(inRow.getAggregateData(i));
  fConcator->merge(gccAg->concator().get());
}

uint8_t* GroupConcatAg::getResult()
{
  return fConcator->getResult(fGroupConcat->fSeparator);
}

void GroupConcatAg::serialize(messageqcpp::ByteStream& bs) const
{
  bs << (uint8_t)fIsJsonArrayAgg;
  fGroupConcat->serialize(bs);
  fConcator->serialize(bs);
  if (fRowGroup.hasLongString())
  {
    bs << uint8_t(1);
    fRowRGData.serialize(bs, fRowGroup.getDataSize(1));
  }
  else
  {
    bs << uint8_t(0);
    bs.append(fData.get(), fRow.getSize());
  }
}

void GroupConcatAg::deserialize(messageqcpp::ByteStream& bs)
{
  uint8_t tmp8;
  bs >> tmp8;
  fIsJsonArrayAgg = tmp8;
  fGroupConcat->deserialize(bs);
  if (fGroupConcat->fDistinct || !fGroupConcat->fOrderCols.empty())
  {
    fConcator.reset(new GroupConcatOrderBy(fIsJsonArrayAgg));
  }
  else
  {
    fConcator.reset(new GroupConcatNoOrder(fIsJsonArrayAgg));
  }
  fConcator->initialize(fGroupConcat);
  fConcator->deserialize(bs);
  bs >> tmp8;
  if (tmp8)
  {
    fRowRGData.deserialize(bs, fRow.getSize());
    fRowGroup.setData(&fRowRGData);
    fRowGroup.initRow(&fRow);
  }
  else
  {
    RGDataSizeType size;
    bs >> size;
    fData.reset(new uint8_t[size]);
    memcpy(fData.get(), bs.buf(), size);
    bs.advance(size);
    fRow.setData(rowgroup::Row::Pointer(fData.get()));
  }
}

rowgroup::RGDataSizeType GroupConcatAg::getDataSize() const
{
  return fMemSize + fConcator->getDataSize();
}

void GroupConcatAg::applyMapping(const std::shared_ptr<int[]>& mapping, const Row& row)
{
  // For some reason the rowgroup mapping fcns don't work right in this class.
  for (uint64_t i = 0; i < fRow.getColumnCount(); i++)
  {
    if (fRow.getColumnWidth(i) > datatypes::MAXLEGACYWIDTH)
    {
      if (fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::CHAR ||
          fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::VARCHAR ||
          fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::TEXT)
      {
        // TODO: free previous string if it is in the StringStorage
        fRow.setStringField(row.getConstString(mapping[i]), i);
      }
      else if (fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::LONGDOUBLE)
      {
        fRow.setLongDoubleField(row.getLongDoubleField(mapping[i]), i);
      }
      else if (datatypes::isWideDecimalType(fRow.getColType(i), fRow.getColumnWidth(i)))
      {
        row.copyBinaryField(fRow, i, mapping[i]);
      }
    }
    else
    {
      if (fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::CHAR ||
          fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::VARCHAR)
      {
        fRow.setIntField(row.getUintField(mapping[i]), i);
      }
      else
      {
        fRow.setIntField(row.getIntField(mapping[i]), i);
      }
    }
  }
}

// GroupConcator class implementation
void GroupConcator::initialize(const rowgroup::SP_GroupConcat& gcc)
{
  // MCOL-901 This value comes from the Server and it is
  // too high(1MB or 3MB by default) to allocate it for every instance.
  fGroupConcatLen = gcc->fSize;
  size_t sepSize = gcc->fSeparator.size();
  fCurrentLength -= sepSize;  // XXX Yet I have to find out why spearator has c_str() as nullptr here.
  fTimeZone = gcc->fTimeZone;

  fConstCols = gcc->fConstCols;
  fConstantLen = sepSize;

  fRm = gcc->fRm;
  fSessionMemLimit = gcc->fSessionMemLimit;

  for (const auto& str : views::keys(gcc->fConstCols))
  {
    fConstantLen += str.length();
  }
}

void GroupConcator::outputRow(std::ostringstream& oss, const rowgroup::Row& row)
{
  const CalpontSystemCatalog::ColDataType* types = row.getColTypes();
  vector<uint32_t>::iterator i = fConcatColumns.begin();
  auto j = fConstCols.begin();

  uint64_t groupColCount = fConcatColumns.size() + fConstCols.size();

  for (uint64_t k = 0; k < groupColCount; k++)
  {
    if (j != fConstCols.end() && k == j->second)
    {
      oss << j->first.safeString();
      j++;
      continue;
    }

    switch (types[*i])
    {
      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::BIGINT:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          int64_t intVal = row.getIntField(*i);

          oss << intVal;
        }

        break;
      }

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          oss << fixed << row.getDecimalField(*i);
        }
        break;
      }

      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          uint64_t uintVal = row.getUintField(*i);
          int scale = (int)row.getScale(*i);

          if (scale == 0)
          {
            oss << uintVal;
          }
          else
          {
            oss << fixed
                << datatypes::Decimal(datatypes::TSInt128((int128_t)uintVal), scale,
                                      datatypes::INT128MAXPRECISION);
          }
        }

        break;
      }

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::TEXT:
      {
        if (fIsJsonArrayAgg)
        {
          auto maybeJson =
              row.getStringField(*i).safeString("null");  // XXX: MULL??? it is not checked anywhere.
          const auto j = json::parse(maybeJson, nullptr, false);
          if (j.is_discarded())
          {
            oss << std::quoted(maybeJson.c_str());
          }
          else
          {
            oss << maybeJson.c_str();
          }
        }
        else
        {
          oss << row.getStringField(*i).str();
        }
        break;
      }

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          oss << setprecision(15) << row.getDoubleField(*i);
        }
        break;
      }

      case CalpontSystemCatalog::LONGDOUBLE:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          oss << setprecision(15) << row.getLongDoubleField(*i);
        }
        break;
      }

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      {
        if (fIsJsonArrayAgg && row.isNullValue(*i))
        {
          oss << "null";
        }
        else
        {
          oss << row.getFloatField(*i);
        }
        break;
      }

      case CalpontSystemCatalog::DATE:
      {
        if (fIsJsonArrayAgg)
        {
          if (row.isNullValue(*i))
          {
            oss << "null";
          }
          else
          {
            oss << std::quoted(DataConvert::dateToString(row.getUintField(*i)));
          }
        }
        else
        {
          oss << DataConvert::dateToString(row.getUintField(*i));
        }
        break;
      }

      case CalpontSystemCatalog::DATETIME:
      {
        if (fIsJsonArrayAgg)
        {
          if (row.isNullValue(*i))
          {
            oss << "null";
          }
          else
          {
            oss << std::quoted(DataConvert::datetimeToString(row.getUintField(*i)));
          }
        }
        else
        {
          oss << DataConvert::datetimeToString(row.getUintField(*i));
        }
        break;
      }

      case CalpontSystemCatalog::TIMESTAMP:
      {
        if (fIsJsonArrayAgg)
        {
          if (row.isNullValue(*i))
          {
            oss << "null";
          }
          else
          {
            oss << std::quoted(DataConvert::timestampToString(row.getUintField(*i), fTimeZone));
          }
        }
        else
        {
          oss << DataConvert::timestampToString(row.getUintField(*i), fTimeZone);
        }
        break;
      }

      case CalpontSystemCatalog::TIME:
      {
        if (fIsJsonArrayAgg)
        {
          if (row.isNullValue(*i))
          {
            oss << "null";
          }
          else
          {
            oss << std::quoted(DataConvert::timeToString(row.getUintField(*i)));
          }
        }
        else
        {
          oss << DataConvert::timeToString(row.getUintField(*i));
        }
        break;
      }

      default:
      {
        break;
      }
    }

    ++i;
  }
}

bool GroupConcator::concatColIsNull(const rowgroup::Row& row)
{
  return ranges::any_of(fConcatColumns, [&](uint32_t idx) { return row.isNullValue(idx); });
}

int64_t GroupConcator::lengthEstimate(const rowgroup::Row& row)
{
  int64_t rowLen = fConstantLen;  // fixed constant and separator length
  const CalpontSystemCatalog::ColDataType* types = row.getColTypes();

  // null values are already skipped.
  for (vector<uint32_t>::iterator i = fConcatColumns.begin(); i != fConcatColumns.end(); i++)
  {
    if (row.isNullValue(*i))
      continue;

    int64_t fieldLen = 0;

    switch (types[*i])
    {
      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::BIGINT:
      {
        int64_t v = row.getIntField(*i);

        if (v < 0)
          fieldLen++;

        while ((v /= 10) != 0)
          fieldLen++;

        fieldLen += 1;
        break;
      }

      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT:
      {
        uint64_t v = row.getUintField(*i);

        while ((v /= 10) != 0)
          fieldLen++;

        fieldLen += 1;
        break;
      }

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        fieldLen += 1;

        break;
      }

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::TEXT:
      {
        fieldLen += row.getConstString(*i).length();
        break;
      }

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      case CalpontSystemCatalog::LONGDOUBLE:
      {
        fieldLen = 1;  // minimum length
        break;
      }

      case CalpontSystemCatalog::DATE:
      {
        fieldLen = 10;  // YYYY-MM-DD
        break;
      }

      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIMESTAMP:
      {
        fieldLen = 19;  // YYYY-MM-DD HH24:MI:SS
        // Decimal point and milliseconds
        uint64_t colPrecision = row.getPrecision(*i);

        if (colPrecision > 0 && colPrecision < 7)
        {
          fieldLen += colPrecision + 1;
        }

        break;
      }

      case CalpontSystemCatalog::TIME:
      {
        fieldLen = 10;  //  -HHH:MI:SS
        // Decimal point and milliseconds
        uint64_t colPrecision = row.getPrecision(*i);

        if (colPrecision > 0 && colPrecision < 7)
        {
          fieldLen += colPrecision + 1;
        }

        break;
      }

      default:
      {
        break;
      }
    }

    rowLen += fieldLen;
  }

  return rowLen;
}

const string GroupConcator::toString() const
{
  ostringstream oss;
  oss << "GroupConcat size-" << fGroupConcatLen;
  oss << "Concat   cols: ";
  vector<uint32_t>::const_iterator i = fConcatColumns.begin();
  auto j = fConstCols.begin();
  uint64_t groupColCount = fConcatColumns.size() + fConstCols.size();

  for (uint64_t k = 0; k < groupColCount; k++)
  {
    if (j != fConstCols.end() && k == j->second)
    {
      oss << 'c' << " ";
      j++;
    }
    else
    {
      oss << (*i) << " ";
      i++;
    }
  }

  oss << endl;

  return oss.str();
}

void GroupConcator::serialize(messageqcpp::ByteStream& bs) const
{
  messageqcpp::serializeInlineVector(bs, fConcatColumns);
  RGDataSizeType size = fConstCols.size();
  bs << size;
  for (const auto& [k, v] : fConstCols)
  {
    bs << k;
    bs << v;
  }
  bs << fCurrentLength;
  bs << fGroupConcatLen;
  bs << fConstantLen;
  bs << fTimeZone;
}

void GroupConcator::deserialize(messageqcpp::ByteStream& bs)
{
  fConstCols.clear();
  messageqcpp::deserializeInlineVector(bs, fConcatColumns);
  RGDataSizeType size;
  bs >> size;
  fConstCols.reserve(size);
  for (RGDataSizeType i = 0; i < size; i++)
  {
    NullString f;
    bs >> f;
    uint32_t s;
    bs >> s;
    fConstCols.emplace_back(f, s);
  }
  bs >> fCurrentLength;
  bs >> fGroupConcatLen;
  bs >> fConstantLen;
  bs >> fTimeZone;
}

class GroupConcatOrderByRow
{
 public:
  GroupConcatOrderByRow(const rowgroup::Row& r, uint64_t rowIdx, ordering::CompareRule& c)
   : fData(r.getPointer()), fIdx(rowIdx), fRule(&c)
  {
  }
  bool operator<(const GroupConcatOrderByRow& rhs) const
  {
    return fRule->less(fData, rhs.fData);
  }
  rowgroup::Row::Pointer fData;
  uint64_t fIdx;
  ordering::CompareRule* fRule;
};

class GroupConcatOrderBy::SortingPQ
 : public priority_queue<GroupConcatOrderByRow, vector<GroupConcatOrderByRow>, less<GroupConcatOrderByRow>>
{
 public:
  using BaseType =
      std::priority_queue<GroupConcatOrderByRow, vector<GroupConcatOrderByRow>, less<GroupConcatOrderByRow>>;
  using size_type = BaseType::size_type;

  SortingPQ(size_type capacity) : BaseType()
  {
    reserve(capacity);
  }

  SortingPQ(const container_type& v) : BaseType(less<GroupConcatOrderByRow>(), v)
  {
  }

  void reserve(size_type capacity)
  {
    this->c.reserve(capacity);
  }

  size_type capacity() const
  {
    return this->c.capacity();
  }

  container_type::const_iterator begin() const
  {
    return this->c.begin();
  }
  container_type::const_iterator end() const
  {
    return this->c.end();
  }

  using BaseType::empty;
  using BaseType::pop;
  using BaseType::push;
  using BaseType::size;
  using BaseType::top;
};

// GroupConcatOrderBy class implementation
GroupConcatOrderBy::GroupConcatOrderBy(bool isJsonArrayAgg) : GroupConcator(isJsonArrayAgg)
{
  fRule.fIdbCompare = this;
}

GroupConcatOrderBy::~GroupConcatOrderBy()
{
  // delete compare objects
  for (auto& compare : fRule.fCompares)
  {
    delete compare;
    compare = nullptr;
  }
  fRule.fCompares.clear();
}

void GroupConcatOrderBy::initialize(const rowgroup::SP_GroupConcat& gcc)
{
  gcc->fRowGroup.setUseOnlyLongString(true);
  ordering::IdbCompare::initialize(gcc->fRowGroup);
  GroupConcator::initialize(gcc);

  fOrderByCond.resize(0);

  for (const auto& [idx, asc] : gcc->fOrderCond)
  {
    fOrderByCond.emplace_back(idx, asc);
  }

  fDistinct = gcc->fDistinct;

  for (uint32_t x : views::values(gcc->fGroupCols))
  {
    fConcatColumns.emplace_back(x);
  }

  auto size = fRowGroup.getSizeWithStrings(fRowsPerRG);
  fMemSize += size;
  RGDataUnPtr rgdata(new RGData(fRowGroup, fRowsPerRG));
  fRowGroup.setData(rgdata.get());
  fRowGroup.resetRowGroup((0));
  fRowGroup.initRow(&fRow0);
  fRowGroup.getRow(0, &fRow0);
  fDataVec.emplace_back(std::move(rgdata));

  fRule.compileRules(fOrderByCond, fRowGroup);

  fRowGroup.initRow(&row1);
  fRowGroup.initRow(&row2);

  if (fDistinct)
  {
    fDistinctMap.reset(new DistinctMap(10, Hasher(this, getKeyLength()), Eq(this, getKeyLength())));
  }
  fOrderByQueue.reset(new SortingPQ(10));
}

uint64_t GroupConcatOrderBy::getKeyLength() const
{
  // only distinct the concatenated columns
  return fConcatColumns.size();  // cols 0 to fConcatColumns.size() - 1 will be compared
}

void GroupConcatOrderBy::serialize(messageqcpp::ByteStream& bs) const
{
  GroupConcator::serialize(bs);
  uint64_t sz = fOrderByCond.size();
  bs << sz;
  for (const auto& obcond : fOrderByCond)
  {
    bs << obcond.fIndex;
    bs << obcond.fAsc;
    bs << obcond.fNf;
  }
  sz = fDataVec.size();
  bs << sz;
  for (const auto& rgdata : fDataVec)
  {
    rgdata->serialize(bs, fRowGroup.getDataSize(fRowsPerRG));
  }
  bs << uint8_t(fDistinct);
  if (fDistinct)
  {
    sz = fDistinctMap->size();
    bs << sz;
    for (const auto& idx : views::values(*fDistinctMap))
    {
      bs << idx;
    }
  }
  sz = fOrderByQueue->size();
  bs << sz;
  for (const auto& obq : *fOrderByQueue)
  {
    bs << obq.fIdx;
  }
}

void GroupConcatOrderBy::deserialize(messageqcpp::ByteStream& bs)
{
  GroupConcator::deserialize(bs);
  fMemSize = 0;
  uint64_t sz;
  bs >> sz;
  fOrderByCond.resize(sz);
  uint8_t tmp8;
  for (uint8_t i = 0; i < sz; ++i)
  {
    bs >> fOrderByCond[i].fIndex;
    bs >> fOrderByCond[i].fAsc;
    bs >> fOrderByCond[i].fNf;
  }

  bs >> sz;
  fDataVec.resize(sz);
  if (sz > 0)
  {
    for (uint64_t i = 0; i < sz; ++i)
    {
      fDataVec[i].reset(new rowgroup::RGData(fRowGroup, fRowsPerRG));
      fDataVec[i]->deserialize(bs, fRowGroup.getDataSize(fRowsPerRG));
      fRowGroup.setData(fDataVec[i].get());
      auto rgsize = fRowGroup.getSizeWithStrings(fRowsPerRG);
      fMemSize += rgsize;
    }

    fRowGroup.initRow(&fRow0);
    fRowGroup.getRow(fRowGroup.getRowCount() - 1, &fRow0);
  }
  else
  {
    createNewRGData();
  }

  fRule.fIdbCompare = this;
  for (auto& compare : fRule.fCompares)
  {
    delete compare;
    compare = nullptr;
  }
  fRule.fCompares.clear();
  fRule.compileRules(fOrderByCond, fRowGroup);
  fRowGroup.initRow(&row1);
  fRowGroup.initRow(&row2);

  bs >> tmp8;
  fDistinct = tmp8;
  if (fDistinct)
  {
    bs >> sz;
    fDistinctMap.reset(new DistinctMap(sz, Hasher(this, getKeyLength()), Eq(this, getKeyLength())));
    for (uint64_t i = 0; i < sz; ++i)
    {
      uint64_t idx;
      bs >> idx;
      auto [gid, rid] = rowIdxToGidRid(idx, fRowsPerRG);
      rowgroup::Row row;
      fRowGroup.setData(fDataVec[gid].get());
      fRowGroup.initRow(&row);
      fRowGroup.getRow(rid, &row);
      fDistinctMap->emplace(row.getPointer(), idx);
    }
  }

  bs >> sz;
  fOrderByQueue.reset(new SortingPQ(sz));
  for (uint64_t i = 0; i < sz; ++i)
  {
    uint64_t idx;
    bs >> idx;
    auto [gid, rid] = rowIdxToGidRid(idx, fRowsPerRG);
    rowgroup::Row row;
    fRowGroup.setData(fDataVec[gid].get());
    fRowGroup.initRow(&row);
    fRowGroup.getRow(rid, &row);
    fOrderByQueue->push(GroupConcatOrderByRow(row, idx, fRule));
  }
  fRowGroup.setData(fDataVec.back().get());
  fRowGroup.getRow(fRowGroup.getRowCount() - 1, &fRow0);
}

void GroupConcatOrderBy::createNewRGData()
{
  auto newSize = fRowGroup.getSizeWithStrings(fRowsPerRG);

  fMemSize += newSize;

  fDataVec.emplace_back(make_unique<rowgroup::RGData>(fRowGroup, fRowsPerRG));
  fRowGroup.setData(fDataVec.back().get());
  fRowGroup.setUseOnlyLongString(true);
  fRowGroup.resetRowGroup(0);
  fRowGroup.initRow(&fRow0);
  fRowGroup.getRow(0, &fRow0);
}

rowgroup::RGDataSizeType GroupConcatOrderBy::getDataSize() const
{
  return fMemSize + fOrderByQueue->capacity() * sizeof(GroupConcatOrderByRow) +
         (fDistinct ? fDistinctMap->size() : 0) * 32 /* TODO: speculative unordered_map memory consumption per
                                                        item, replace it with counting allocator */
      ;
}

void GroupConcatOrderBy::processRow(const rowgroup::Row& row)
{
  // check if this is a distinct row
  if (fDistinct && fDistinctMap->find(row.getPointer()) != fDistinctMap->end())
    return;

  // this row is skipped if any concatenated column is null.
  if (!fIsJsonArrayAgg && concatColIsNull(row))
    return;

  // if the row count is less than the limit
  if (fCurrentLength < fGroupConcatLen)
  {
    copyRow(row, &fRow0);
    // the RID is no meaning here, use it to store the estimated length.
    int16_t estLen = lengthEstimate(fRow0);
    fRow0.setRid(estLen);
    fRowGroup.incRowCount();

    GroupConcatOrderByRow newRow(fRow0, getCurrentRowIdx(), fRule);
    fOrderByQueue->push(newRow);
    fCurrentLength += estLen;

    // add to the distinct map
    if (fDistinct)
      fDistinctMap->emplace(fRow0.getPointer(), getCurrentRowIdx());

    fRow0.nextRow();

    if (fRowGroup.getRowCount() >= fRowsPerRG)
    {
      // A "postfix" but accurate RAM accounting that sums up sizes of RGDatas.
      uint64_t newSize = fRowGroup.getSizeWithStrings();

      fMemSize += newSize;

      rowgroup::RGDataUnPtr rgdata(new rowgroup::RGData(fRowGroup, fRowsPerRG));
      fRowGroup.setData(rgdata.get());
      fRowGroup.resetRowGroup(0);
      fRowGroup.getRow(0, &fRow0);
      fDataVec.emplace_back(std::move(rgdata));
    }
  }
  else if (fOrderByCond.size() > 0 && fRule.less(row.getPointer(), fOrderByQueue->top().fData))
  {
    GroupConcatOrderByRow swapRow = fOrderByQueue->top();
    fRow1.setData(swapRow.fData);
    fOrderByQueue->pop();
    fCurrentLength -= fRow1.getRelRid();
    fRow2.setData(swapRow.fData);

    if (!fDistinct)
    {
      copyRow(row, &fRow1);
    }
    else
    {
      // only the copyRow does useful work here
      fDistinctMap->erase(swapRow.fData);
      copyRow(row, &fRow2);
      fDistinctMap->emplace(swapRow.fData, swapRow.fIdx);
    }

    int16_t estLen = lengthEstimate(fRow2);
    fRow2.setRid(estLen);
    fCurrentLength += estLen;

    fOrderByQueue->push(swapRow);
  }
}

void GroupConcatOrderBy::merge(GroupConcator* gc)
{
  GroupConcatOrderBy* go = dynamic_cast<GroupConcatOrderBy*>(gc);
  fMemSize += go->fMemSize;
  go->fMemSize = 0;
  uint32_t shift = fDataVec.size();
  auto& rgdatas = go->getRGDatas();
  for (auto& rgdata : rgdatas)
  {
    fDataVec.emplace_back(std::move(rgdata));
  }
  rgdatas.clear();

  auto* orderByQueue = go->getQueue();
  while (!orderByQueue->empty())
  {
    GroupConcatOrderByRow row = orderByQueue->top();
    row.fIdx = shiftGroupIdxBy(row.fIdx, shift);
    row.fRule = &fRule;

    // check if the distinct row already exists
    if (fDistinct && fDistinctMap->find(row.fData) != fDistinctMap->end())
    {
      ;  // no op;
    }
    // if the row count is less than the limit
    else if (fCurrentLength < fGroupConcatLen)
    {
      fOrderByQueue->push(row);
      row1.setData(row.fData);
      fCurrentLength += row1.getRelRid();

      // add to the distinct map
      if (fDistinct)
        fDistinctMap->emplace(row.fData, row.fIdx);
    }
    else if (fOrderByCond.size() > 0 && fRule.less(row.fData, fOrderByQueue->top().fData))
    {
      GroupConcatOrderByRow swapRow = fOrderByQueue->top();
      row1.setData(swapRow.fData);
      fOrderByQueue->pop();
      fCurrentLength -= row1.getRelRid();

      if (fDistinct)
      {
        fDistinctMap->erase(swapRow.fData);
        fDistinctMap->emplace(row.fData, row.fIdx);
      }

      row1.setData(row.fData);
      fCurrentLength += row1.getRelRid();

      fOrderByQueue->push(row);
    }

    orderByQueue->pop();
  }
}

uint8_t* GroupConcatOrderBy::getResultImpl(const string& sep)
{
  ostringstream oss;
  bool addSep = false;

  // need to reverse the order
  stack<GroupConcatOrderByRow> rowStack;
  while (fOrderByQueue->size() > 0)
  {
    rowStack.push(fOrderByQueue->top());
    fOrderByQueue->pop();
  }

  size_t prevResultSize = 0;
  size_t rowsProcessed = 0;
  bool isNull = true;
  if (rowStack.size() > 0)
  {
    if (fIsJsonArrayAgg)
      oss << "[";
    while (rowStack.size() > 0)
    {
      if (addSep)
        oss << sep;
      else
        addSep = true;

      const GroupConcatOrderByRow& topRow = rowStack.top();
      fRow0.setData(topRow.fData);
      outputRow(oss, fRow0);
      isNull = false;
      rowStack.pop();
      ++rowsProcessed;
      if (rowsProcessed >= fRowsPerRG)
      {
        size_t sizeDiff = oss.str().size() - prevResultSize;
        prevResultSize = oss.str().size();
        fMemSize += sizeDiff;
        rowsProcessed = 0;
      }
    }
    if (fIsJsonArrayAgg)
      oss << "]";
  }

  return swapStreamWithStringAndReturnBuf(oss, isNull);
}

uint8_t* GroupConcator::swapStreamWithStringAndReturnBuf(ostringstream& oss, bool isNull)
{
  if (isNull)
  {
    outputBuf_.reset();
    return nullptr;
  }
  // XXX: what is all this black magic for?
  int64_t resultSize = oss.str().size();
  oss << '\0' << '\0';
  outputBuf_.reset(new std::string(std::move(*oss.rdbuf()).str()));

  if (resultSize >= fGroupConcatLen + 1)
  {
    (*outputBuf_)[fGroupConcatLen] = '\0';
  }
  if (resultSize >= fGroupConcatLen + 2)
  {
    (*outputBuf_)[fGroupConcatLen + 1] = '\0';
  }

  // FIXME: a string_view can be returned here to get rid of strlen() later
  return reinterpret_cast<uint8_t*>(outputBuf_->data());
}

uint8_t* GroupConcator::getResult(const string& sep)
{
  return getResultImpl(sep);
}

const string GroupConcatOrderBy::toString() const
{
  string baseStr = GroupConcator::toString();

  ostringstream oss;
  oss << "OrderBy   cols: ";
  vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();

  for (; i != fOrderByCond.end(); i++)
    oss << "(" << i->fIndex << "," << ((i->fAsc) ? "Asc" : "Desc") << ","
        << ((i->fNf) ? "null first" : "null last") << ") ";

  if (fDistinct)
    oss << endl << " distinct";

  oss << endl;

  return (baseStr + oss.str());
}

uint64_t GroupConcatOrderBy::Hasher::operator()(const rowgroup::Row::Pointer& p) const
{
  Row& row = ts->row1;
  row.setPointer(p);
  return row.hash(colCount - 1);
}

bool GroupConcatOrderBy::Eq::operator()(const rowgroup::Row::Pointer& p1,
                                        const rowgroup::Row::Pointer& p2) const
{
  Row& r1 = ts->row1;
  Row& r2 = ts->row2;
  r1.setPointer(p1);
  r2.setPointer(p2);
  return r1.equals(r2, colCount - 1);
}

uint64_t GroupConcatOrderBy::getCurrentRowIdx() const
{
  return rowGidRidToIdx(fDataVec.size() - 1, fRowGroup.getRowCount() - 1, fRowsPerRG);
}

uint64_t GroupConcatOrderBy::shiftGroupIdxBy(uint64_t idx, uint32_t shift)
{
  auto [gid, rid] = rowIdxToGidRid(idx, fRowsPerRG);
  return rowGidRidToIdx(gid + shift, rid, fRowsPerRG);
}

// GroupConcatNoOrder class implementation
GroupConcatNoOrder::~GroupConcatNoOrder()
{
}

void GroupConcatNoOrder::initialize(const rowgroup::SP_GroupConcat& gcc)
{
  GroupConcator::initialize(gcc);

  fRowGroup = gcc->fRowGroup;
  fRowGroup.setUseOnlyLongString(true);
  fRowsPerRG = 128;

  for (uint32_t colIdx : views::values(gcc->fGroupCols))
  {
    fConcatColumns.push_back(colIdx);
  }

  createNewRGData();
}

void GroupConcatNoOrder::processRow(const rowgroup::Row& row)
{
  // if the row count is less than the limit
  if (fCurrentLength < fGroupConcatLen && (fIsJsonArrayAgg || concatColIsNull(row) == false))
  {
    copyRow(row, &fRow);

    // the RID is no meaning here, use it to store the estimated length.
    int16_t estLen = lengthEstimate(fRow);
    fRow.setRid(estLen);
    fCurrentLength += estLen;
    fRowGroup.incRowCount();
    fRow.nextRow();
    auto newSize = fRowGroup.getSizeWithStrings(fRowsPerRG);
    if (newSize > fCurMemSize)
    {
      auto diff = newSize - fCurMemSize;
      fCurMemSize = newSize;
      fMemSize += diff;
    }

    if (fRowGroup.getRowCount() >= fRowsPerRG)
    {
      createNewRGData();
    }
  }
}

void GroupConcatNoOrder::merge(GroupConcator* gc)
{
  auto* in = dynamic_cast<GroupConcatNoOrder*>(gc);
  assert(in != nullptr);

  for (auto& i : in->getRGDatas())
  {
    fDataVec.emplace_back(std::move(i));
  }
  fRowGroup.setData(fDataVec.back().get());
  fRowGroup.setUseOnlyLongString(true);
  fRowGroup.initRow(&fRow);
  fRowGroup.getRow(fRowGroup.getRowCount(), &fRow);
  fMemSize += in->fMemSize;
  fCurMemSize = in->fCurMemSize;
  in->fMemSize = in->fCurMemSize = 0;
}

uint8_t* GroupConcatNoOrder::getResultImpl(const string& sep)
{
  ostringstream oss;
  bool addSep = false;

  size_t prevResultSize = 0;

  bool isNull = true;
  bool addBrackets = true;
  for (auto& rgdata : fDataVec)
  {
    fRowGroup.setData(rgdata.get());
    fRowGroup.initRow(&fRow);
    fRowGroup.getRow(0, &fRow);

    for (uint64_t i = 0; i < fRowGroup.getRowCount(); i++)
    {
      if (addBrackets && fIsJsonArrayAgg)
      {
        oss << "[";
        addBrackets = false;
      }
      if (addSep)
        oss << sep;
      else
        addSep = true;

      outputRow(oss, fRow);
      isNull = false;
      fRow.nextRow();
    }
    size_t sizeDiff = oss.str().size() - prevResultSize;
    prevResultSize = oss.str().size();
    fMemSize += sizeDiff;
    rgdata.reset();
  }
  if (fIsJsonArrayAgg && !addBrackets)
    oss << "]";

  return swapStreamWithStringAndReturnBuf(oss, isNull);
}

void GroupConcatNoOrder::serialize(messageqcpp::ByteStream& bs) const
{
  GroupConcator::serialize(bs);
  RGDataSizeType sz = fDataVec.size();
  bs << sz;
  for (auto& rgdata : fDataVec)
  {
    if (rgdata)
    {
      rgdata->serialize(bs, fRowGroup.getDataSize());
    }
  }
}

void GroupConcatNoOrder::deserialize(messageqcpp::ByteStream& bs)
{
  GroupConcator::deserialize(bs);
  RGDataSizeType sz;
  bs >> sz;
  fMemSize = fCurMemSize = 0;
  fDataVec.resize(sz);
  if (sz == 0)
  {
    createNewRGData();
  }
  else
  {
    for (RGDataSizeType i = 0; i < sz; i++)
    {
      fDataVec[i].reset(new RGData(fRowGroup, fRowsPerRG));
      fDataVec[i]->deserialize(bs, fRowGroup.getDataSize(fRowsPerRG));
      fRowGroup.setData(fDataVec[i].get());
      fCurMemSize = fRowGroup.getSizeWithStrings(fRowsPerRG);
      fMemSize += fCurMemSize;
    }
  }
}

const string GroupConcatNoOrder::toString() const
{
  return GroupConcator::toString();
}

void GroupConcatNoOrder::createNewRGData()
{
  auto newSize = fRowGroup.getDataSize(fRowsPerRG);

  fMemSize += newSize;
  fCurMemSize = newSize;

  fDataVec.emplace_back(make_unique<rowgroup::RGData>(fRowGroup, fRowsPerRG));
  fRowGroup.setData(fDataVec.back().get());
  fRowGroup.setUseOnlyLongString(true);
  fRowGroup.resetRowGroup(0);
  fRowGroup.initRow(&fRow);
  fRowGroup.getRow(0, &fRow);
}
}  // namespace joblist
