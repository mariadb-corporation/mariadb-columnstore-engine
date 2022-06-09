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
// $Id: dictstep-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
// C++ Implementation: dictstep-js
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "bpp-jl.h"
#include "string_prefixes.h"

using namespace std;
using namespace messageqcpp;

namespace joblist
{
DictStepJL::DictStepJL()
{
}

DictStepJL::DictStepJL(const pDictionaryStep& dict)
{
  BOP = dict.fBOP;
  OID = dict.oid();
  colName = dict.name();
  compressionType = dict.colType().compressionType;

  hasEqFilter = dict.hasEqualityFilter;

  if (hasEqFilter)
  {
    eqOp = dict.tmpCOP;
    eqFilter = dict.eqFilter;
  }
  else
    filterString = dict.fFilterString;

  filterCount = dict.fFilterCount;
  charsetNumber = dict.fColType.charsetNumber;
}

DictStepJL::~DictStepJL()
{
}

void DictStepJL::setLBID(uint64_t token, uint32_t dbroot)
{
  // 	lbid = token >> 10;  // the lbid is calculated on the PM
}

void DictStepJL::createCommand(ByteStream& bs) const
{
  bs << (uint8_t)DICT_STEP;
  bs << BOP;
  bs << (uint8_t)compressionType;
  bs << charsetNumber;
  bs << filterCount;
  bs << (uint8_t)hasEqFilter;

  if (hasEqFilter)
  {
    idbassert(filterCount == eqFilter.size());
    bs << eqOp;

    for (uint32_t i = 0; i < filterCount; i++)
      bs << eqFilter[i];
  }
  else
    bs << filterString;
  CommandJL::createCommand(bs);
}

void DictStepJL::runCommand(ByteStream& bs) const
{
}

uint8_t DictStepJL::getTableColumnType()
{
  throw logic_error("Don't call DictStepJL::getTableColumn(); it's not a projection step");
}

string DictStepJL::toString()
{
  ostringstream os;

  os << "DictStepJL: " << filterCount << " filters, BOP=" << (int)BOP << ", oid=" << OID
     << " name=" << colName << endl;
  return os.str();
}

uint16_t DictStepJL::getWidth()
{
  return colWidth;
}

void DictStepJL::setWidth(uint16_t w)
{
  colWidth = w;
}

messageqcpp::ByteStream DictStepJL::reencodedFilterString() const
{
  messageqcpp::ByteStream bs;

  if (hasEqFilter)
  {
    idbassert(filterCount == eqFilter.size());

    for (uint32_t i = 0; i < filterCount; i++)
    {
      uint8_t roundFlag = 0;
      int64_t encodedPrefix = encodeStringPrefix((unsigned char*)eqFilter[i].c_str(), eqFilter[i].size(), charsetNumber);
      bs << eqOp;
      bs << roundFlag;
      bs << encodedPrefix;
    }
  }
  else
  {
    messageqcpp::ByteStream filterStringCopy(
        filterString);  // XXX I am not sure about real semantics of messagecpp::ByteStream. So - copy.
    // please erfer to pdictionary.cpp in this dicrectory, addFilter function for a proper encoding of string
    // filters.
    for (uint32_t i = 0; i < filterCount; i++)
    {
      uint8_t cop, roundFlag = 0;
      uint16_t size;
      const uint8_t* ptr;
      int64_t encodedPrefix;
      filterStringCopy >> cop;
      // as we are dealing with prefixes, we have to use "... or equal" conditions instead of
      // strict ones.
      // Consider this: ... WHERE col > 'customer#001' AND col < 'customer#100'.
      // "Working with prefixes of 8 bytes" means these conditions reduce to ... WHERE col > 'customer' AND
      // col < 'customer' and their AND relation is impossible to satisfy. We do not pass this string to
      // primproc and that means we can reencode operation codes here.
      switch (cop)
      {
        case COMPARE_LT:
        case COMPARE_NGE: cop = COMPARE_LE; break;

        case COMPARE_GT:
        case COMPARE_NLE: cop = COMPARE_GE; break;

        default: break;
      }

      bs << cop;
      bs << roundFlag;
      filterStringCopy >> size;
      ptr = filterStringCopy.buf();
      encodedPrefix = encodeStringPrefix(ptr, size, charsetNumber);
      bs << encodedPrefix;
      filterStringCopy.advance(size);
    }
  }
  return bs;
}

};  // namespace joblist
