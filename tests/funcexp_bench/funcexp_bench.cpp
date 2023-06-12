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


#include "primitives/linux-port/primitiveprocessor.h"
#include "utils/funcexp/funcexp.h"
#include "utils/funcexp/rowgroup.h"
#include "dbcon/execplan/returnedcolumn.h"
#include "jobstep.h"


using namespace std;
using namespace rowgroup;
using namespace funcexp;
using namespace joblist;


class EvaluateBenchFixture : public ::benchmark::Fixture
{
 

private: 
  FuncExpWrapper fe;
  RowGroup rg;
  vector<uint32_t> offsets, roids, tkeys, types, charSetNumbers, scale, precision;
  const uint32_t baseOffset = 2;
  const uint32_t oid = 3000;
  Row row;

  const uint64_t values[2][16] = {
    {1, 2},
    {3, 4}, 
    {5, 6}, 
    {7, 8},
    {1, 2},
    {3, 4}, 
    {5, 6}, 
    {7, 8},
    {51, 52},
    {53, 54}, 
    {55, 56}, 
    {57, 58},     
    {51, 52},
    {53, 54}, 
    {55, 56}, 
    {57, 58}
  };

  void init(uint32_t colCount, vector<execplan::CalpontSystemCatalog::ColDataType> dataTypes, uint32_t width) 
  {
    offsets.clear(), roids.clear(), tkeys.clear(), types.clear(), charSetNumbers.clear(), scale.clear(), precision.clear();
    for (uint32_t i = 0; i <= colCount; ++i)
    {
      offsets.push_back(baseOffset + i * width);
      if (i) 
      {
        roids.push_back(oid + i);
        tkeys.push_back(i);
        types.push_back(dataTypes[i - 1]);
        scale.push_back(0);
        precision.push_back(0);
        charSetNumbers.push_back(8);
      }
    }

    rg = RowGroup(colCount,              // column count
                            offsets,        // oldOffset
                            roids,          // column oids
                            tkeys,          // keys
                            types,          // types
                            charSetNumbers,  // charset numbers
                            scale,         // scale
                            precision,     // precision
    );

    rowgroup::RGData rgD = rowgroup::RGData(inRG);
    inRG.setData(&rgD);
    inRG.initRow(&row);
    uint32_t rowSize = rg.getSize();
    inRG.getRow(0, &row);

    for (uint32_t i = 0; i < colCount; ++i)
    {
      switch (type)
      {
        case execplan::CalpontSystemCatalog::UTINYINT:
        {
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setUintField<1>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<1>(joblist::UTINYINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
          for (uint32_t j = 0; j < 16; ++j)
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
          for (uint32_t j = 0; j < 16; ++j)
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
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setUintField<8>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setUintField<8>(joblist::UBIGINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::TINYINT:
        {
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setIntField<1>(values[i][j], i);
            row.nextRow(rowSize);
          }
          row.setIntField<1>(joblist::TINYINTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
          for (uint32_t j = 0; j < 16; ++j)
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
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setIntField<4>(values[i][j], i);
            row.nextRow(rowSize);
          }
          if (type == execplan::CalpontSystemCatalog::DATE)
            row.setIntField<4>(joblist::DATENULL, i);
          else
            row.setIntField<4>(joblist::INTNULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::BIGINT:
        {
          for (uint32_t j = 0; j < 16; ++j)
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
          switch (width)
          {
            case 1:
            {
              for (uint32_t j = 0; j < 16; ++j)
              {
                row.setUintField<1>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<1>(joblist::TINYINTNULL, i);
              break;
            }
            case 2:
            {
              for (uint32_t j = 0; j < 16; ++j)
              {
                row.setUintField<2>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<2>(joblist::SMALLINTNULL, i);
              break;
            }
            case 4:
            {
              for (uint32_t j = 0; j < 16; ++j)
              {
                row.setUintField<4>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<4>(joblist::INTNULL, i);
              break;
            }
            case 8:
            {
              for (uint32_t j = 0; j < 16; ++j)
              {
                row.setUintField<8>(values[i][j], i);
                row.nextRow(rowSize);
              }
              row.setUintField<8>(joblist::BIGINTNULL, i);
              break;
            }
            case 16:
            {
              for (uint32_t j = 0; j < 16; ++j)
              {
                uint128_t v = values[i][j];
                row.setBinaryField<16>(&v, i);
                row.nextRow(rowSize);
              }
              row.setBinaryField(const_cast<int128_t*>(&datatypes::Decimal128Null), i);
              break;
            }
          }
          break;
        }
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
          for (uint32_t j = 0; j < 16; ++j)
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
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setDoubleField(values[i][j] + 0.1, i);
            row.nextRow(rowSize);
          }
          row.setUintField(joblist::DOUBLENULL, i);
          break;
        }
        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
          for (uint32_t j = 0; j < 16; ++j)
          {
            row.setLongDoubleField(values[i][j] + 0.1, i);
            row.nextRow(rowSize);
          }
          row.setLongDoubleField(joblist::LONGDOUBLENULL, i);
          break;
        }
      }
      
    }
  }
  
public:

  EvaluateBenchFixture()
  {

  }

  void SetUp(const ::benchmark::State& state)
  {

  }

  void SetUp(const benchmark::State& state)
  {
    SetUp(const_cast<benchmark::State&>(state));
  }


};