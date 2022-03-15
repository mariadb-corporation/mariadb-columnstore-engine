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
// $Id: dictstep.cpp 2110 2013-06-19 15:51:38Z bwilkinson $
// C++ Implementation: dictstep
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <algorithm>

#include "bpp.h"
#include "primitiveserver.h"
#include "pp_logger.h"
#include "../linux-port/primitiveprocessor.h"

using namespace std;
using namespace messageqcpp;
using namespace rowgroup;

namespace primitiveprocessor
{
extern uint32_t dictBufferSize;

DictStep::DictStep() : Command(DICT_STEP), strValues(NULL), filterCount(0), bufferSize(0)
{
}

DictStep::~DictStep()
{
}

DictStep& DictStep::operator=(const DictStep& d)
{
  // Not all fields are copied for the sake of efficiency.
  // Right now, these only need to copy fields deserialized from the create msg.
  BOP = d.BOP;
  fFilterFeeder = d.fFilterFeeder;
  compressionType = d.compressionType;
  filterString = d.filterString;
  hasEqFilter = d.hasEqFilter;
  eqFilter = d.eqFilter;
  eqOp = d.eqOp;
  filterCount = d.filterCount;
  charsetNumber = d.charsetNumber;
  return *this;
}

void DictStep::createCommand(ByteStream& bs)
{
  uint8_t tmp8;

  bs.advance(1);
  bs >> BOP;
  bs >> tmp8;
  compressionType = tmp8;
  bs >> charsetNumber;
  bs >> filterCount;
  bs >> tmp8;
  hasEqFilter = tmp8;

  if (hasEqFilter)
  {
    string strTmp;
    datatypes::Charset cs(charsetNumber);
    eqFilter.reset(new primitives::DictEqualityFilter(cs));
    bs >> eqOp;

    for (uint32_t i = 0; i < filterCount; i++)
    {
      bs >> strTmp;
      eqFilter->insert(strTmp);
    }
  }
  else
    bs >> filterString;

  Command::createCommand(bs);
}

void DictStep::resetCommand(ByteStream& bs)
{
}

void DictStep::prep(int8_t outputType, bool makeAbsRids)
{
  // (at most there are 8192 tokens to fetch)
  bufferSize = sizeof(DictInput) + filterString.length() + (8192 * sizeof(OldGetSigParams));
  inputMsg.reset(new uint8_t[bufferSize]);
  primMsg = (DictInput*)inputMsg.get();

  primMsg->ism.Interleave = 0;
  primMsg->ism.Flags = 0;
  primMsg->ism.Command = DICT_SIGNATURE;
  primMsg->ism.Size = bufferSize;
  primMsg->ism.Type = 2;
  primMsg->hdr.SessionID = bpp->sessionID;
  // primMsg->hdr.StatementID = 0;
  primMsg->hdr.TransactionID = bpp->txnID;
  primMsg->hdr.VerID = bpp->versionInfo.currentScn;
  primMsg->hdr.StepID = bpp->stepID;
  primMsg->BOP = BOP;
  primMsg->InputFlags = 1;  // TODO: Use the new p_Dict functionality
  primMsg->OutputType =
      (eqFilter || filterCount || fFilterFeeder != NOT_FEEDER ? OT_RID : OT_RID | OT_DATAVALUE);
  primMsg->NOPS = (eqFilter ? 0 : filterCount);
  primMsg->NVALS = 0;
}

void DictStep::issuePrimitive(bool isFilter)
{
  bool wasCached;
  uint32_t blocksRead;

  if (!(primMsg->LBID & 0x8000000000000000LL))
  {
    // std::cerr << "DS issuePrimitive lbid: " << (uint64_t)primMsg->LBID << endl;
    primitiveprocessor::loadBlock(primMsg->LBID, bpp->versionInfo, bpp->txnID, compressionType,
                                  bpp->blockData, &wasCached, &blocksRead, bpp->LBIDTrace, bpp->sessionID);

    if (wasCached)
      bpp->cachedIO++;

    bpp->physIO += blocksRead;
    bpp->touchedBlocks++;
  }

  bpp->pp.p_Dictionary(primMsg, &result, isFilter, charsetNumber, eqFilter, eqOp);
}

void DictStep::copyResultToTmpSpace(OrderedToken* ot)
{
  uint32_t i;
  uint8_t* pos;
  uint16_t len;
  uint64_t rid64;
  uint16_t rid16;

  idbassert(primMsg->OutputType & OT_RID);
  DictOutput* header = (DictOutput*)&result[0];

  if (header->NVALS == 0)
    return;

  pos = &result[sizeof(DictOutput)];

  for (i = 0; i < header->NVALS; i++)
  {
    rid64 = *((uint64_t*)pos);
    pos += 8;
    rid16 = rid64 & 0x1fff;
    ot[rid16].inResult = true;
    tmpResultCounter++;

    if (primMsg->OutputType & OT_DATAVALUE)
    {
      len = *((uint16_t*)pos);
      pos += 2;
      ot[rid16].str = string((char*)pos, len);
      pos += len;

      if (rid64 & 0x8000000000000000LL)
        ot[rid16].str = joblist::CPNULLSTRMARK;
    }
  }
}

void DictStep::copyResultToFinalPosition(OrderedToken* ot)
{
  uint32_t i, resultPos = 0;

  for (i = 0; i < inputRidCount; i++)
  {
    if (ot[i].inResult)
    {
      bpp->absRids[resultPos] = ot[i].rid;
      bpp->relRids[resultPos] = ot[i].rid - bpp->baseRid;

      if (primMsg->OutputType & OT_DATAVALUE)
        (*strValues)[resultPos] = ot[i].str;

      resultPos++;
    }
  }
}

void DictStep::processResult()
{
  uint32_t i;
  uint8_t* pos;
  uint16_t len;
  DictOutput* header = (DictOutput*)&result[0];

  if (header->NVALS == 0)
    return;

  pos = &result[sizeof(DictOutput)];

  for (i = 0; i < header->NVALS; i++, tmpResultCounter++)
  {
    if (primMsg->OutputType & OT_RID)
    {
      bpp->absRids[tmpResultCounter] = *((uint64_t*)pos);
      pos += 8;
      // bpp->relRids[tmpResultCounter] = bpp->absRids[tmpResultCounter] & 0x1fff;
      bpp->relRids[tmpResultCounter] = bpp->absRids[tmpResultCounter] - bpp->baseRid;
    }

    if (primMsg->OutputType & OT_DATAVALUE)
    {
      len = *((uint16_t*)pos);
      pos += 2;
      (*strValues)[tmpResultCounter] = string((char*)pos, len);
      pos += len;
    }

    // cout << "  stored " << (*strValues)[tmpResultCounter] << endl;
    /* XXXPAT: disclaimer: this is how we do it in DictionaryStep; don't know
            if it's necessary or not yet */
    if ((bpp->absRids[tmpResultCounter] & 0x8000000000000000LL) != 0)
    {
      if (primMsg->OutputType & OT_DATAVALUE)
        (*strValues)[tmpResultCounter] = joblist::CPNULLSTRMARK.c_str();

      bpp->absRids[tmpResultCounter] &= 0x7FFFFFFFFFFFFFFFLL;
    }
  }
}

void DictStep::projectResult(string* strings)
{
  uint32_t i;
  uint8_t* pos;
  uint16_t len;
  DictOutput* header = (DictOutput*)&result[0];

  if (header->NVALS == 0)
    return;

  pos = &result[sizeof(DictOutput)];

  // cout << "projectResult() l: " << primMsg->LBID << " NVALS: " << header->NVALS << endl;
  for (i = 0; i < header->NVALS; i++)
  {
    len = *((uint16_t*)pos);
    pos += 2;
    strings[tmpResultCounter++] = string((char*)pos, len);
    // cout << "serialized length is " << len << " string is " << strings[tmpResultCounter-1] << " string
    // length = " <<
    //  strings[tmpResultCounter-1].length() << endl;
    pos += len;
    totalResultLength += len + 4;
  }
}

// bug4901 -
// This version of projectResult needs to stay in sync with
// the above.  They are separate methods because the
// _projectToRG() method can deal with this optimized version
// where we only need to return the pointer and length.  This
// is desirable because it avoids an unnecessary temporary
// string copy.  The above version is still needed for the
// _project() method where it has to serialize the totalResultLength
// before starting to serialize strings.
void DictStep::projectResult(StringPtr* strings)
{
  uint32_t i;
  uint8_t* pos;
  uint16_t len;
  DictOutput* header = (DictOutput*)&result[0];

  if (header->NVALS == 0)
    return;

  pos = &result[sizeof(DictOutput)];

  // cout << "projectResult() l: " << primMsg->LBID << " NVALS: " << header->NVALS << endl;
  for (i = 0; i < header->NVALS; i++)
  {
    len = *((uint16_t*)pos);
    pos += 2;
    strings[tmpResultCounter++] = StringPtr(pos, len);
    // cout << "serialized length is " << len << " string is " << strings[tmpResultCounter-1] << " string
    // length = " << 	strings[tmpResultCounter-1].length() << endl;
    pos += len;
    totalResultLength += len + 4;
  }
}

void DictStep::execute()
{
  if (fFilterFeeder == LEFT_FEEDER)
    strValues = &(bpp->fFiltStrValues[0]);
  else if (fFilterFeeder == RIGHT_FEEDER)
    strValues = &(bpp->fFiltStrValues[1]);
  else
    strValues = &(bpp->strValues);

  _execute();
}

void DictStep::_execute()
{
  /* Need to loop over bpp->values, issuing a primitive for each LBID */
  uint64_t i;
  int64_t l_lbid;
  OldGetSigParams* pt;
  boost::scoped_array<OrderedToken> newRidList;

  // make the OrderedToken list
  newRidList.reset(new OrderedToken[bpp->ridCount]);

  for (i = 0; i < bpp->ridCount; i++)
  {
    newRidList[i].rid = bpp->absRids[i];
    newRidList[i].token = bpp->values[i];
    newRidList[i].pos = i;
  }

  sort(&newRidList[0], &newRidList[bpp->ridCount], TokenSorter());

  tmpResultCounter = 0;
  i = 0;

  while (i < bpp->ridCount)
  {
    l_lbid = ((int64_t)newRidList[i].token) >> 10;
    primMsg->LBID = (l_lbid == -1) ? l_lbid : l_lbid & 0xFFFFFFFFFL;
    primMsg->NVALS = 0;

    /* When this is used as a filter, the strings can be thrown out.  JLF currently
     * constructs joblists s.t. only a FilterCommand will use the strings.
     */
    primMsg->OutputType = (fFilterFeeder == NOT_FEEDER ? OT_RID : OT_RID | OT_DATAVALUE);

    pt = (OldGetSigParams*)(primMsg->tokens);

    while (i < bpp->ridCount && ((((int64_t)newRidList[i].token) >> 10) == l_lbid))
    {
      if (UNLIKELY(l_lbid < 0))
        pt[primMsg->NVALS].rid = (fFilterFeeder == NOT_FEEDER ? newRidList[i].rid : i) | 0x8000000000000000LL;
      else
        pt[primMsg->NVALS].rid = (fFilterFeeder == NOT_FEEDER ? newRidList[i].rid : i);

      pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
      idbassert(pt[primMsg->NVALS].offsetIndex != 0);
      primMsg->NVALS++;
      i++;
    }

    memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
    issuePrimitive(true);

    if (fFilterFeeder == NOT_FEEDER)
      processResult();
    else
      copyResultToTmpSpace(newRidList.get());
  }

  inputRidCount = bpp->ridCount;
  bpp->ridCount = tmpResultCounter;

  // check if feeding a filtercommand
  if (fFilterFeeder != NOT_FEEDER)
  {
    sort(&newRidList[0], &newRidList[inputRidCount], PosSorter());
    copyResultToFinalPosition(newRidList.get());
    copyRidsForFilterCmd();
  }

  // cout << "DS: /_execute()\n";
}

/* This will do the same thing as execute() but put the result in bpp->serialized */
void DictStep::_project()
{
  /* Need to loop over bpp->values, issuing a primitive for each LBID */
  uint32_t i;
  int64_t l_lbid = 0;
  OldGetSigParams* pt;
  string tmpStrings[LOGICAL_BLOCK_RIDS];
  boost::scoped_array<OrderedToken> newRidList;

  // make the OrderedToken list
  newRidList.reset(new OrderedToken[bpp->ridCount]);

  for (i = 0; i < bpp->ridCount; i++)
  {
    newRidList[i].rid = bpp->absRids[i];
    newRidList[i].token = values[i];
    newRidList[i].pos = i;
  }

  // cout << "DS: _project()\n";
  tmpResultCounter = 0;
  totalResultLength = 0;
  i = 0;

  while (i < bpp->ridCount)
  {
    l_lbid = ((int64_t)newRidList[i].token) >> 10;
    primMsg->LBID = (l_lbid == -1) ? l_lbid : l_lbid & 0xFFFFFFFFFL;
    primMsg->NVALS = 0;
    primMsg->OutputType = OT_DATAVALUE;
    pt = (OldGetSigParams*)(primMsg->tokens);

    //@bug 972
    while (i < bpp->ridCount && ((((int64_t)newRidList[i].token) >> 10) == l_lbid))
    {
      if (l_lbid < 0)
        pt[primMsg->NVALS].rid = newRidList[i].rid | 0x8000000000000000LL;
      else
        pt[primMsg->NVALS].rid = newRidList[i].rid;

      pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
      idbassert(pt[primMsg->NVALS].offsetIndex > 0);
      primMsg->NVALS++;
      i++;
    }

    memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
    issuePrimitive(false);
    projectResult(tmpStrings);
  }

  idbassert(tmpResultCounter == bpp->ridCount);
  *bpp->serialized << totalResultLength;

  // cout << "_project() total length = " << totalResultLength << endl;
  for (i = 0; i < tmpResultCounter; i++)
  {
    // cout << "serializing " << tmpStrings[i] << endl;
    *bpp->serialized << tmpStrings[i];
  }

  // cout << "DS: /_project() l: " << l_lbid << endl;
}

void DictStep::_projectToRG(RowGroup& rg, uint32_t col)
{
  /* Need to loop over bpp->values, issuing a primitive for each LBID */
  uint32_t i;
  int64_t l_lbid = 0;
  int64_t o_lbid = 0;
  OldGetSigParams* pt;
  StringPtr* tmpStrings = new StringPtr[LOGICAL_BLOCK_RIDS];
  rowgroup::Row r;
  boost::scoped_array<OrderedToken> newRidList;

  // make the OrderedToken list
  newRidList.reset(new OrderedToken[bpp->ridCount]);

  for (i = 0; i < bpp->ridCount; i++)
  {
    newRidList[i].rid = bpp->absRids[i];
    newRidList[i].token = values[i];
    newRidList[i].pos = i;
  }

  sort(&newRidList[0], &newRidList[bpp->ridCount], TokenSorter());

  rg.initRow(&r);
  uint32_t curResultCounter = 0;
  tmpResultCounter = 0;
  totalResultLength = 0;
  i = 0;

  // cout << "DS: projectingToRG rids: " << bpp->ridCount << endl;
  while (i < bpp->ridCount)
  {
    l_lbid = ((int64_t)newRidList[i].token) >> 10;
    primMsg->LBID = (l_lbid == -1) ? l_lbid : l_lbid & 0xFFFFFFFFFL;
    primMsg->NVALS = 0;
    primMsg->OutputType = OT_DATAVALUE;
    pt = (OldGetSigParams*)(primMsg->tokens);

    //@bug 972
    //@bug 1821
    while (i < bpp->ridCount && ((((int64_t)newRidList[i].token) >> 10) == l_lbid || l_lbid == -1 ||
                                 ((((int64_t)newRidList[i].token) >> 10) & 0x8000000000000000LL)))
    {
      //@bug 1821
      if (newRidList[i].token == 0)
      {
        ostringstream oss;
        ostringstream oss2;
        oss << l_lbid;
        logging::Message::Args args;
        args.add("0");
        args.add(oss.str());
        oss2 << newRidList[i].rid;
        args.add(oss2.str());
        primitiveprocessor::mlp->logMessage(logging::M0068, args, true);
        newRidList[i].token = 0xfffffffffffffffeLL;
        values[newRidList[i].pos] = 0xfffffffffffffffeLL;
      }

      if ((((int64_t)newRidList[i].token) >> 10) < 0)
      {
        pt[primMsg->NVALS].rid = newRidList[i].rid | 0x8000000000000000LL;
      }
      else
      {
        if ((((int64_t)newRidList[i].token) >> 10) > 0 && o_lbid == 0)
          l_lbid = o_lbid = (((int64_t)newRidList[i].token) >> 10);

        pt[primMsg->NVALS].rid = newRidList[i].rid;
      }

      pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
      idbassert(pt[primMsg->NVALS].offsetIndex > 0);
      primMsg->NVALS++;
      // 			pt++;
      i++;
    }

    if (((int64_t)primMsg->LBID) < 0 && o_lbid > 0)
      primMsg->LBID = o_lbid & 0xFFFFFFFFFL;

    memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
    issuePrimitive(false);
    projectResult(tmpStrings);
    o_lbid = 0;
    // cout << "DS: project & issue l: " << (int64_t)primMsg->LBID << " NVALS: " << primMsg->NVALS << endl;

    // bug 4901 - move this inside the loop and call incrementally
    // to save the unnecessary string copy
    if ((rg.getColTypes()[col] != execplan::CalpontSystemCatalog::VARBINARY) &&
        (rg.getColTypes()[col] != execplan::CalpontSystemCatalog::BLOB) &&
        (rg.getColTypes()[col] != execplan::CalpontSystemCatalog::TEXT))
    {
      for (i = curResultCounter; i < tmpResultCounter; i++)
      {
        rg.getRow(newRidList[i].pos, &r);
        // std::cerr << "serializing " << tmpStrings[i] << endl;
        r.setStringField(tmpStrings[i].getConstString(), col);
      }
    }
    else
    {
      uint32_t firstTmpResultCounter = tmpResultCounter;
      std::map<uint32_t, string*> result;

      for (i = curResultCounter; i < firstTmpResultCounter; i++)
      {
        rg.getRow(newRidList[i].pos, &r);

        // If this is a multi-block blob, get all the blocks
        // We do string copy here, should maybe have a RowGroup
        // function to append strings or something?
        if (((newRidList[i].token >> 46) < 0x3FFFF) && ((newRidList[i].token >> 46) > 0))
        {
          StringPtr multi_part[1];
          uint16_t old_offset = primMsg->tokens[0].offset;

          if (result.empty())
          {
            // String copy here because tmpStrings pointers will be blown away below
            for (uint32_t x = curResultCounter; x < firstTmpResultCounter; x++)
            {
              result[x] = new string((char*)tmpStrings[x].ptr, tmpStrings[x].len);
            }
          }

          uint64_t origin_lbid = primMsg->LBID;
          uint32_t lbid_count = newRidList[i].token >> 46;
          primMsg->tokens[0].offset = 1;  // first offset of a sig

          for (uint32_t j = 1; j <= lbid_count; j++)
          {
            tmpResultCounter = 0;
            primMsg->LBID = origin_lbid + j;
            primMsg->NVALS = 1;
            primMsg->tokens[0].LBID = origin_lbid + j;
            issuePrimitive(false);
            projectResult(multi_part);
            result[i]->append((char*)multi_part[0].ptr, multi_part[0].len);
          }

          primMsg->tokens[0].offset = old_offset;
          primMsg->LBID = origin_lbid;
          tmpResultCounter = firstTmpResultCounter;
          r.setVarBinaryField((unsigned char*)result[i]->c_str(), result[i]->length(), col);
          delete result[i];
        }
        else
        {
          r.setVarBinaryField(tmpStrings[i].ptr, tmpStrings[i].len, col);
        }
      }
    }

    curResultCounter = tmpResultCounter;
  }

  // cout << "_projectToRG() total length = " << totalResultLength << endl;
  idbassert(tmpResultCounter == bpp->ridCount);

  delete[] tmpStrings;
  // cout << "DS: /projectingToRG l: " << (int64_t)primMsg->LBID
  //	<< " len: " << tmpResultCounter
  //	<<  endl;
}

void DictStep::project()
{
  values = bpp->values;
  _project();
}

void DictStep::project(int64_t* vals)
{
  values = vals;
  _project();
}

void DictStep::projectIntoRowGroup(RowGroup& rg, uint32_t col)
{
  values = bpp->values;
  _projectToRG(rg, col);
}

void DictStep::projectIntoRowGroup(RowGroup& rg, int64_t* vals, uint32_t col)
{
  values = vals;
  _projectToRG(rg, col);
}

uint64_t DictStep::getLBID()
{
  return 0;
}

void DictStep::nextLBID()
{
}

SCommand DictStep::duplicate()
{
  SCommand ret;
  DictStep* ds;

  ret.reset(new DictStep());
  ds = (DictStep*)ret.get();
  ds->BOP = BOP;
  ds->fFilterFeeder = fFilterFeeder;
  ds->compressionType = compressionType;
  ds->hasEqFilter = hasEqFilter;
  ds->eqFilter = eqFilter;
  ds->eqOp = eqOp;
  ds->filterString = filterString;
  ds->filterCount = filterCount;
  ds->charsetNumber = charsetNumber;
  ds->Command::operator=(*this);
  return ret;
}

bool DictStep::operator==(const DictStep& ds) const
{
  return ((BOP == ds.BOP) && (fFilterFeeder == ds.fFilterFeeder) && (compressionType == ds.compressionType) &&
          (filterString == ds.filterString) && (filterCount == ds.filterCount));
}

bool DictStep::operator!=(const DictStep& ds) const
{
  return !(*this == ds);
}

};  // namespace primitiveprocessor
