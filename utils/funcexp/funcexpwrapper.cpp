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

//
// $Id: funcexpwrapper.cpp 3495 2013-01-21 14:09:51Z rdempsey $
//
// C++ Implementation: funcexpwrapper
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "funcexpwrapper.h"
#include "objectreader.h"
#include "rowgroup.h"

using namespace messageqcpp;
using namespace rowgroup;
using namespace execplan;

namespace funcexp
{
FuncExpWrapper::FuncExpWrapper()
{
  fe = FuncExp::instance();
}

FuncExpWrapper::FuncExpWrapper(const FuncExpWrapper& f)
{
  uint32_t i;

  fe = FuncExp::instance();

  filters.resize(f.filters.size());

  for (i = 0; i < f.filters.size(); i++)
    filters[i].reset(new ParseTree(*(f.filters[i])));

  rcs.resize(f.rcs.size());

  for (i = 0; i < f.rcs.size(); i++)
    rcs[i].reset(f.rcs[i]->clone());
}

FuncExpWrapper::~FuncExpWrapper()
{
}

void FuncExpWrapper::operator=(const FuncExpWrapper& f)
{
  uint32_t i;

  filters.resize(f.filters.size());

  for (i = 0; i < f.filters.size(); i++)
    filters[i].reset(new ParseTree(*(f.filters[i])));

  rcs.resize(f.rcs.size());

  for (i = 0; i < f.rcs.size(); i++)
    rcs[i].reset(f.rcs[i]->clone());
}

void FuncExpWrapper::serialize(ByteStream& bs) const
{
  uint32_t i;

  bs << (uint32_t)filters.size();
  bs << (uint32_t)rcs.size();

  for (i = 0; i < filters.size(); i++)
    ObjectReader::writeParseTree(filters[i].get(), bs);

  for (i = 0; i < rcs.size(); i++)
    rcs[i]->serialize(bs);
}

void FuncExpWrapper::deserialize(ByteStream& bs)
{
  uint32_t fCount, rcsCount, i;

  bs >> fCount;
  bs >> rcsCount;

  for (i = 0; i < fCount; i++)
    filters.push_back(boost::shared_ptr<ParseTree>(ObjectReader::createParseTree(bs)));

  for (i = 0; i < rcsCount; i++)
  {
    ReturnedColumn* rc = (ReturnedColumn*)ObjectReader::createTreeNode(bs);
    rcs.push_back(boost::shared_ptr<ReturnedColumn>(rc));
  }
}

bool FuncExpWrapper::evaluate(Row* r)
{
  fe->evaluate(*r, rcs);
  return true;
}

void FuncExpWrapper::evaluate(RowGroup &input, RowGroup &output, uint32_t rowCount, boost::shared_array<int> &mapping) {

  uint32_t i, j = 0;

  Row in, out;
  input.initRow(&in);
  output.initRow(&out);

  set<uint32_t> colSet;

  for (execplan::SRCP expr : rcs) 
  {
    expr->setSimpleColumnList();
    for (execplan::SimpleColumn *col : expr->simpleColumnList())
    {
      colSet.insert(col->execplan::ReturnedColumn::inputIndex());
    }
  }

  vector<uint32_t> colList(colSet.begin(), colSet.end()), colWidth(colList.size()); // vector with column indexes
  vector<vector<uint8_t>> colData = vector<vector<uint8_t>> (colList.size()); // according values for each column


  input.getRow(0, &in);
  for (i = 0; i < colList.size(); ++i)
  {
    colWidth[i] = in.getColumnWidth(colList[i]);
  }

  for (j = 0; j < rowCount; ++j, in.nextRow())
  {
    for (i = 0; i < colList.size(); ++i)
    {
      colData[i].insert(colData[i].end(), in.getData() + in.getOffset(colList[i]), in.getData() + in.getOffset(colList[i] + 1));
    }
  }

  for (i = 0; i < rcs.size(); ++i)
  {
    bool allowSimd = false;    
    switch (rcs[i]->resultType().colDataType)
    {
      case execplan::CalpontSystemCatalog::TINYINT:
      case execplan::CalpontSystemCatalog::SMALLINT:
      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::BIGINT:
      case execplan::CalpontSystemCatalog::UTINYINT:
      case execplan::CalpontSystemCatalog::USMALLINT:
      case execplan::CalpontSystemCatalog::UINT:
      case execplan::CalpontSystemCatalog::UBIGINT:
        allowSimd = true;
        break;

      case execplan::CalpontSystemCatalog::FLOAT:
      case execplan::CalpontSystemCatalog::DOUBLE:
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::DATE:
      case execplan::CalpontSystemCatalog::TIME:
      case execplan::CalpontSystemCatalog::DATETIME:
      case execplan::CalpontSystemCatalog::TIMESTAMP:
      default:
        break;
    }
    if (allowSimd)
    {
      uint32_t width = rcs[i]->resultType().colWidth, batchCount = 16 / width;
      vector<uint8_t> retCol = vector<uint8_t>((rowCount / batchCount) * batchCount * width);

      for (j = 0; j + batchCount <= rowCount; j += batchCount)
      {
        fe->evaluateSimd(rcs[i], colList, colWidth, colData, retCol, j, batchCount);
      }
      if (j < rowCount)
      {
        input.getRow(j, &in);
        for (; j < rowCount; ++j, in.nextRow())
        {
          fe->evaluate(in, rcs[i]);
          retCol.insert(retCol.end(), in.getData() + in.getOffset(rcs[i]->outputIndex()), in.getData() + in.getOffset(rcs[i]->outputIndex()) + width);
        }
      }

      input.getRow(0, &in);
      output.getRow(0, &out);
      for (j = 0; j < rowCount; ++j, in.nextRow()) 
      {
        in.setBinaryField_offset(retCol.data() + j * width, width, in.getOffset(rcs[i]->outputIndex()));
        applyMapping(mapping, in, &out);
        out.setRid(in.getRelRid());
        output.incRowCount();
        out.nextRow();
      }
    }
    else 
    {
      input.getRow(0, &in);
      for (j = 0; j < rowCount; ++j, in.nextRow())
      {
        fe->evaluate(in, rcs[i]);
      }    
      input.getRow(0, &in);
      output.getRow(0, &out);
      for (j = 0; j < rowCount; ++j, in.nextRow()) 
      {
        applyMapping(mapping, in, &out);
        out.setRid(in.getRelRid());
        output.incRowCount();
        out.nextRow();
      }
    }
  }
}

void FuncExpWrapper::addFilter(const boost::shared_ptr<ParseTree>& f)
{
  filters.push_back(f);
}

void FuncExpWrapper::addReturnedColumn(const boost::shared_ptr<ReturnedColumn>& rc)
{
  rcs.push_back(rc);
}

void FuncExpWrapper::resetReturnedColumns()
{
  rcs.clear();
}

FuncExp* FuncExpWrapper::getFuncExp() const
{
  return fe;
}

};  // namespace funcexp
