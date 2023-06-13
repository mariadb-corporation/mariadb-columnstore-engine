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
  ArithmeticColumn* col;
  const uint32_t baseOffset = 2;
  const uint32_t oid = 3000;
  Row row;


  enum TestType
  {
    TwoColumnAdd
  };

  const uint64_t values[2][16] = {
    {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8},
    {51, 52, 53, 54, 55, 56, 57, 58, 51, 52, 53, 54, 55, 56, 57, 58}
  };

  void initRG(uint32_t colCount, vector<CalpontSystemCatalog::ColDataType> &dataTypes, vector<uint32_t> &widths, bool includeResult = true) 
  {
    const uint32_t rowCount = 16;

    cout << "initializing row group" << endl;
    offsets.clear(), roids.clear(), tkeys.clear(), charSetNumbers.clear(), scale.clear(), precision.clear();
    types.clear();
    for (uint32_t i = 0; i < colCount; ++i)
      types.push_back(dataTypes[i]);
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

    rowgroup::RGData rgD = rowgroup::RGData(rg);
    rg.setData(&rgD);
    rg.initRow(&row); 
    uint32_t rowSize = row.getSize();

    cout << "row size: " << rowSize << endl;
    cout << "now initialize row group with data" << endl;

    for (uint32_t i = 0; i < colCount - includeResult; ++i)
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

    cout << "done setting values" << endl;
  }

  void initExp(TestType testType)
  {
    switch (testType)
    {
      case TestType::TwoColumnAdd:
      {
        string exp = "a * 2 + b";
        fe.resetReturnedColumns();
        col = new ArithmeticColumn(exp);
        col->outputIndex(2);
        fe.addReturnedColumn(boost::shared_ptr<ArithmeticColumn>(col));
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

BENCHMARK_DEFINE_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluate)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    uint32_t num_rows = 16, i;
    vector<execplan::CalpontSystemCatalog::ColDataType> types;
    vector<uint32_t> widths;
    for (int i = 0; i < 2; ++i)
    {
      types.push_back(execplan::CalpontSystemCatalog::INT);
      widths.push_back(4);
    }
    types.push_back(execplan::CalpontSystemCatalog::INT);
    widths.push_back(4);
    initRG(3, types, widths);
    initExp(TestType::TwoColumnAdd);
    cout << row.toString() << endl;
    for (int i = 0; i < 3; ++i)
    {
      cout << "col " << i << " type " << rg.getColType(i) << endl;
    }
    state.ResumeTiming();
    for (i = 0; i < num_rows; ++i, row.nextRow())
    {
      cout << "row " << i << " " << row.toString() << endl;
      fe.evaluate(&row);
    }
  }
}

BENCHMARK_REGISTER_F(EvaluateBenchFixture, BM_TowColumnAdd4ByteEvaluate)->Unit(benchmark::kMillisecond);


BENCHMARK_MAIN();
