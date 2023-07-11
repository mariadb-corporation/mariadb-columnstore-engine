/* Copyright (C) 2021 MariaDB Corporation

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

// includes
#include <iostream>
#include <gtest/gtest.h>
#include <benchmark/benchmark.h>


#include "funcexp.h"
#include "funcexpwrapper.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "rowgroup.h"
#include "row.h"
#include "simplecolumn.h"
#include "simplecolumn_int.h"
#include "constantcolumn.h"
#include "values.h"

using namespace std;
using namespace rowgroup;
using namespace funcexp;
using namespace joblist;
using namespace execplan;


class EvaluateBenchFixture : public benchmark::Fixture
{

public:
  FuncExpWrapper fe;
  RowGroup rg;
  vector<uint32_t> offsets, roids, tkeys, charSetNumbers, scale, precision;
  vector<CalpontSystemCatalog::ColDataType> types;
  boost::shared_ptr<ReturnedColumn> expr;
  rowgroup::RGData rgD;
  TreeNode *tn1, *tn2, *tn3, *tn4, *tn5, *tn6, *tn7;
  ReturnedColumn *rc1, *rc2, *rc3, *rc4, *rc5, *rc6, *rc7;
  ParseTree *pt1, *pt2, *pt3, *pt4, *pt5;
  const uint32_t baseOffset = 2;
  const uint32_t oid = 3000;
  uint32_t rowCount = 2048, testWidth = 0;
  Row row;
  vector<uint32_t> colList = {0, 1};
  vector<uint32_t> colWidth = {0, 0};
  vector<vector<uint8_t>> colData; // according values for each column

  enum TestType
  {
    TwoColumnAdd
  };

  template <int len>
  void initRG(uint32_t colCount, vector<CalpontSystemCatalog::ColDataType> dataTypes, vector<uint32_t> widths, bool includeResult = true) 
  {
    if (len == testWidth)
      return;
    // cout << "initializing row group" << endl;
    offsets.clear(), roids.clear(), tkeys.clear(), charSetNumbers.clear(), scale.clear(), precision.clear();
    types = dataTypes;
    uint32_t offset = baseOffset;
    for (uint32_t i = 0; i <= colCount; ++i)
    {
      offsets.push_back(offset);
      if (i < widths.size())
        offset += widths[i];
      if (i) 
      {
        roids.push_back(oid + i);
        tkeys.push_back(i);
        scale.push_back(0);
        precision.push_back(0);
        charSetNumbers.push_back(8);
      }
    }
    // cout << "offsets: ";
    // for (uint32_t i = 0; i < offsets.size(); ++i)
    //   cout << offsets[i] << " ";
    // cout << endl;

    rg = RowGroup(colCount,              // column count
                  offsets,        // oldOffset
                  roids,          // column oids
                  tkeys,          // keys
                  types,          // types
                  charSetNumbers,  // charset numbers
                  scale,         // scale
                  precision,     // precision
                  20,             // sTableThreshold
                  false           // useStringTable
    );

    rgD = rowgroup::RGData(rg);
    rg.setData(&rgD);
    rg.initRow(&row); 
    uint32_t rowSize = row.getSize();

    // cout << "row size: " << rowSize << endl;
    // cout << "now initialize row group with data" << endl;

    for (uint32_t i = 0; i < colCount; ++i)
    {
      rg.getRow(0, &row);
      switch (dataTypes[i])
      {
        case execplan::CalpontSystemCatalog::UTINYINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setUintField<1>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<1>(joblist::UTINYINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setUintField<2>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<2>(joblist::USMALLINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setUintField<4>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<4>(joblist::UINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::DATETIME:
        case execplan::CalpontSystemCatalog::UBIGINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setUintField<8>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<8>(joblist::UBIGINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::TINYINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setIntField<1>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setIntField<1>(joblist::TINYINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setIntField<2>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setIntField<2>(joblist::SMALLINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::DATE:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setIntField<4>(values[i][j], i);
            row.nextRow(rowSize);
          }
          if (dataTypes[i] == execplan::CalpontSystemCatalog::DATE)
            row.setIntField<4>(joblist::DATENULL, i);
          else
            row.setIntField<4>(joblist::INTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::BIGINT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setIntField<8>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setIntField<8>(joblist::BIGINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
          switch (widths[i])
          {
            case 1:
            {
              for (uint32_t j = 0; j < rowCount; ++j)
              {
                row.setUintField<1>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<1>(joblist::TINYINTNULL, i);
              break;
            }
            case 2:
            {
              for (uint32_t j = 0; j < rowCount; ++j)
              {
                row.setUintField<2>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<2>(joblist::SMALLINTNULL, i);
              break;
            }
            case 4:
            {
              for (uint32_t j = 0; j < rowCount; ++j)
              {
                row.setUintField<4>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<4>(joblist::INTNULL, i);
              break;
            }
            case 8:
            {
              for (uint32_t j = 0; j < rowCount; ++j)
              {
                row.setUintField<8>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<8>(joblist::BIGINTNULL, i);
              break;
            }
            case 16:
            {
              for (uint32_t j = 0; j < rowCount; ++j)
              {
                uint128_t v = values[i][j];
                row.setBinaryField(&v, i);
                row.nextRow(rowSize);
              }
              row.setBinaryField(const_cast<int128_t*>(&datatypes::Decimal128Null), i);
              break;
            }
            default:
            {
              std::cout << "ERROR: invalid decimal width " << widths[i] << std::endl;
              exit(1);
            }
          }
          break;
        }
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setFloatField(values[i][j] + 0.1, i);
            row.nextRow(rowSize);
          }
          row.setUintField(joblist::FLOATNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setDoubleField(values[i][j] + 0.1, i);
            row.nextRow(rowSize);
          }
          row.setUintField(joblist::DOUBLENULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
          for (uint32_t j = 0; j < rowCount; ++j)
          {
            row.setLongDoubleField(values[i][j] + 0.1, i);
            row.nextRow(rowSize);
          }
          row.setLongDoubleField(joblist::LONGDOUBLENULL, i);
          break;
        }
        default:
          break;
      }
    }

    rg.setRowCount(rowCount);
    rg.getRow(0, &row);

    // cout << "done setting values" << endl;
  }

  template <int len>
  void initExp(TestType testType)
  {
    switch (testType)
    {
      case TestType::TwoColumnAdd:
      {
        if (len == testWidth)
          return;
        fe.resetReturnedColumns();
        string exp = "col1 + 2 + col2";
        tn1 = new SimpleColumn_INT<len>();
        (reinterpret_cast<ReturnedColumn*>(tn1))->inputIndex(0);
        pt1 = new ParseTree(tn1);

        tn2 = new SimpleColumn_INT<len>();
        (reinterpret_cast<ReturnedColumn*>(tn2))->inputIndex(1);
        pt2 = new ParseTree(tn2);

        tn3 = new ConstantColumn("2");
        pt3 = new ParseTree(tn3);

        tn4 = new ArithmeticOperator("*");
        pt4 = new ParseTree(tn4);
        pt4->left(pt1);
        pt4->right(pt3);

        tn5 = new ArithmeticOperator("+");
        pt5 = new ParseTree(tn5);
        pt5->left(pt4);
        pt5->right(pt2);
        rc1 = new ArithmeticColumn();
        auto cp_pt5 = pt5;
        (reinterpret_cast<ArithmeticColumn*> (rc1))->expression(cp_pt5);
        rc1->outputIndex(2);

        CalpontSystemCatalog::ColType *colType = new CalpontSystemCatalog::ColType();

        switch (len)
        {
          case 1: 
            colType->colDataType = execplan::CalpontSystemCatalog::TINYINT;
            break;
          case 2:
            colType->colDataType = execplan::CalpontSystemCatalog::SMALLINT;
            break;
          case 4:
            colType->colDataType = execplan::CalpontSystemCatalog::INT;
            break;
          case 8:
            colType->colDataType = execplan::CalpontSystemCatalog::BIGINT;
            break;
          default:
            break;
        }
        colType->colWidth = len;

        rc1->resultType(*colType);

        delete colType;

        expr = boost::shared_ptr<ReturnedColumn>(rc1);
        fe.addReturnedColumn(expr);
        break;
      }
    }
  }

  template <int len>
  void initData(TestType testType) 
  {
    switch (testType)
    {
      case TestType::TwoColumnAdd:
      {
        if (testWidth == len)
          return;
        colWidth[0] = colWidth[1] = len;
        uint32_t i, j;
        colData = vector<vector<uint8_t>> (colList.size());
        rg.getRow(0, &row);
        for (i = 0; i < colList.size(); ++i)
        {
          colWidth[i] = row.getColumnWidth(colList[i]);
        }

        for (j = 0; j < rowCount; ++j, row.nextRow())
        {
          for (i = 0; i < colList.size(); ++i)
          {
            colData[i].insert(colData[i].end(), row.getData() + row.getOffset(colList[i]), row.getData() + row.getOffset(colList[i] + 1));
          }
        }
        break;
      }
    }
  }


  EvaluateBenchFixture()
  {
    fe = FuncExpWrapper();
  }

  void SetUp(benchmark::State& state)
  {

  }

  void SetUp(const benchmark::State& state)
  {
    SetUp(const_cast<benchmark::State&>(state));
  }
  


};

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd1ByteEvaluate)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::TINYINT);
      widths.push_back(1);
    }
    types.push_back(execplan::CalpontSystemCatalog::TINYINT);
    widths.push_back(1);
    initRG<1>(3, types, widths);
    initExp<1>(TestType::TwoColumnAdd);
    testWidth = 1;
    // cout << row.toString() << endl;
    // for (int i = 0; i < 3; ++i)
    // {
    //   cout << "col " << i << " type " << rg.getColType(i) << endl;
    // }
    state.ResumeTiming();
    rg.getRow(0, &row);
    for (i = 0; i < rowCount; ++i, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd1ByteEvaluate);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd1ByteEvaluateVectorized)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i, j;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::TINYINT);
      widths.push_back(1);
    }
    types.push_back(execplan::CalpontSystemCatalog::TINYINT);
    widths.push_back(1);
    initRG<1>(3, types, widths);
    initExp<1>(TestType::TwoColumnAdd);
    initData<1>(TestType::TwoColumnAdd);
    testWidth = 1;

    uint32_t width = expr->resultType().colWidth, batchCount = 16 / width;
    vector<uint8_t> retCol = vector<uint8_t>((rowCount / batchCount) * batchCount * width);
    state.ResumeTiming();
    for (j = 0; j + batchCount < rowCount; j += batchCount)
    {
      (fe.getFuncExp())->evaluateSimd(expr, colList, colWidth, colData, retCol, j, batchCount);
    }
    rg.getRow(j, &row);
    for (; j < rowCount; ++j, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
      retCol.insert(retCol.end(), row.getData() + row.getOffset(expr->outputIndex()), row.getData() + row.getOffset(expr->outputIndex()) + width);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd1ByteEvaluateVectorized);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd2ByteEvaluate)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::SMALLINT);
      widths.push_back(2);
    }
    types.push_back(execplan::CalpontSystemCatalog::SMALLINT);
    widths.push_back(2);
    initRG<2>(3, types, widths);
    initExp<2>(TestType::TwoColumnAdd);
    testWidth = 2;
    // cout << row.toString() << endl;
    // for (int i = 0; i < 3; ++i)
    // {
    //   cout << "col " << i << " type " << rg.getColType(i) << endl;
    // }
    state.ResumeTiming();
    rg.getRow(0, &row);
    for (i = 0; i < rowCount; ++i, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd2ByteEvaluate);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd2ByteEvaluateVectorized)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i, j;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::SMALLINT);
      widths.push_back(2);
    }
    types.push_back(execplan::CalpontSystemCatalog::SMALLINT);
    widths.push_back(2);
    initRG<2>(3, types, widths);
    initExp<2>(TestType::TwoColumnAdd);
    initData<2>(TestType::TwoColumnAdd);
    testWidth = 2;

    uint32_t width = expr->resultType().colWidth, batchCount = 16 / width;
    vector<uint8_t> retCol = vector<uint8_t>((rowCount / batchCount) * batchCount * width);
    state.ResumeTiming();
    for (j = 0; j + batchCount < rowCount; j += batchCount)
    {
      (fe.getFuncExp())->evaluateSimd(expr, colList, colWidth, colData, retCol, j, batchCount);
    }
    rg.getRow(j, &row);
    for (; j < rowCount; ++j, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
      retCol.insert(retCol.end(), row.getData() + row.getOffset(expr->outputIndex()), row.getData() + row.getOffset(expr->outputIndex()) + width);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd2ByteEvaluateVectorized);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluate)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::INT);
      widths.push_back(4);
    }
    types.push_back(execplan::CalpontSystemCatalog::INT);
    widths.push_back(4);
    initRG<4>(3, types, widths);
    initExp<4>(TestType::TwoColumnAdd);
    testWidth = 4;
    // cout << row.toString() << endl;
    // cout << rg.getRowCount() << endl;
    // for (int i = 0; i < 3; ++i)
    // {
      // cout << "col " << i << " type " << rg.getColType(i) << endl;
    // }
    state.ResumeTiming();
    rg.getRow(0, &row);
    for (i = 0; i < rowCount; ++i, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluate);


BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluateVectorized)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i, j;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::INT);
      widths.push_back(4);
    }
    types.push_back(execplan::CalpontSystemCatalog::INT);
    widths.push_back(4);
    initRG<4>(3, types, widths);
    initExp<4>(TestType::TwoColumnAdd);
    initData<4>(TestType::TwoColumnAdd);
    testWidth = 4;
    
    uint32_t width = expr->resultType().colWidth, batchCount = 16 / width;
    vector<uint8_t> retCol = vector<uint8_t>((rowCount / batchCount) * batchCount * width);
    state.ResumeTiming();
    for (j = 0; j + batchCount < rowCount; j += batchCount)
    {
      (fe.getFuncExp())->evaluateSimd(expr, colList, colWidth, colData, retCol, j, batchCount);
    }
    rg.getRow(j, &row);
    for (; j < rowCount; ++j, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
      retCol.insert(retCol.end(), row.getData() + row.getOffset(expr->outputIndex()), row.getData() + row.getOffset(expr->outputIndex()) + width);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluateVectorized);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd8ByteEvaluate)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::BIGINT);
      widths.push_back(8);
    }
    types.push_back(execplan::CalpontSystemCatalog::BIGINT);
    widths.push_back(8);
    initRG<8>(3, types, widths);
    initExp<8>(TestType::TwoColumnAdd);
    testWidth = 8;
    // cout << row.toString() << endl;
    // for (int i = 0; i < 3; ++i)
    // {
    //   cout << "col " << i << " type " << rg.getColType(i) << endl;
    // }
    state.ResumeTiming();
    rg.getRow(0, &row);
    for (i = 0; i < rowCount; ++i, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd8ByteEvaluate);

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd8ByteEvaluateVectorized)(benchmark::State& state)
{
  testWidth = 0;
  for (auto _ : state)
  {  
    state.PauseTiming();
    uint32_t i, j;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::BIGINT);
      widths.push_back(8);
    }
    types.push_back(execplan::CalpontSystemCatalog::BIGINT);
    widths.push_back(8);
    initRG<8>(3, types, widths);
    initExp<8>(TestType::TwoColumnAdd);
    initData<8>(TestType::TwoColumnAdd);
    testWidth = 8;
    
    uint32_t width = expr->resultType().colWidth, batchCount = 16 / width;
    vector<uint8_t> retCol = vector<uint8_t>((rowCount / batchCount) * batchCount * width);
    state.ResumeTiming();
    for (j = 0; j + batchCount < rowCount; j += batchCount)
    {
      (fe.getFuncExp())->evaluateSimd(expr, colList, colWidth, colData, retCol, j, batchCount);
    }
    rg.getRow(j, &row);
    for (; j < rowCount; ++j, row.nextRow())
    {
      (fe.getFuncExp())->evaluate(row, expr);
      retCol.insert(retCol.end(), row.getData() + row.getOffset(expr->outputIndex()), row.getData() + row.getOffset(expr->outputIndex()) + width);
    }
    // rg.getRow(0, &row);
    // for (i = 0; i < rowCount; ++i, row.nextRow())
    // {
    //   // fe.evaluate(&row);
    //   cout << "row after evaluation " << i << " " << row.toString() << endl;
    // }

  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd8ByteEvaluateVectorized);

BENCHMARK_MAIN();
