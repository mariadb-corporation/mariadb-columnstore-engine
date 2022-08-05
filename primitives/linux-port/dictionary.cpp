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

/*
 * $Id: dictionary.cpp 2122 2013-07-08 16:33:50Z bpaul $
 */

#include <iostream>
#include <string>
#include <boost/scoped_array.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <sys/types.h>
using namespace std;

#include "primitiveprocessor.h"
#include "we_type.h"
#include "messagelog.h"
#include "messageobj.h"
#include "exceptclasses.h"
#include "dataconvert.h"
#include "string_prefixes.h"
#include <sstream>
#include "mcs_datatype.h"
#include "simd_sse.h"
#include "simd_arm.h"
#include "hasher.h"
#include <unordered_set>

using namespace logging;

const char* nullString = " ";  // this is not NULL to preempt segfaults.
const int nullStringLen = 0;

namespace
{
const char* signatureNotFound = joblist::CPSTRNOTFOUND.c_str();
}

namespace primitives
{
#if defined(__x86_64__) || defined(__aarch64__)
//only length greater than 32 the weightSub is faster than 
inline int8_t weightSub(const char* str1, size_t length1, const char* str2, size_t length2)
{
  using SimdTypeWrapper = typename simd::IntegralToSIMD<uint8_t, KIND_DEFAULT>::type;
  using VT=simd::SimdFilterProcessor<SimdTypeWrapper, uint8_t>;
  using SimdType =VT::SimdType;
  VT simdProcessor;
  const uint16_t mlength = min(length1, length2);
  uint16_t i;
//the sse SimdFilterProcessor doesn't have the instruction like vmaxvq_u8
#if defined(__aarch64__)
  SimdType res;
  for (i = 0; i <= mlength - 16; i += 16)
  {
    SimdType op1 = simdProcessor.loadFrom(str1 + i);
    SimdType op2 = simdProcessor.loadFrom(str2 + i);
    res = simdProcessor.sub(op1, op2);
    if (simdProcessor.maxScalar(res) != 0)
    {
      int8_t* resPtr = reinterpret_cast<int8_t*>(&res);
      for (int j = 0; j < 16; ++j)
      {
        if (resPtr[j] != 0)
          return resPtr[j];
      }
    }
  }
#else
  uint16_t res;
  for (i = 0; i <= mlength - 16; i += 16)
  {
    SimdType op1 = simdProcessor.loadFrom(str1 + i);
    SimdType op2 = simdProcessor.loadFrom(str2 + i);
    res = simdProcessor.cmpNe(op1, op2);
    if (res!=0)
    {
      int8_t* op1Ptr = reinterpret_cast<int8_t*>(&op1);
      int8_t* op2Ptr = reinterpret_cast<int8_t*>(&op2);
      for (int j = 0; j < 16; ++j)
      {
        if (op1Ptr[j]-op2Ptr[j] != 0)
          return op1Ptr[j]-op2Ptr[j];
      }
    }
  }
#endif
  for (; i < mlength; ++i)
  {
    if ((str1[i] - str2[i]) != 0)
    {
      return str1[i] - str2[i];
    }
  }
  return length1 - length2;
}
//When the COP is COMPARE_LIKE,the str1 and str2... are used 
inline bool compare(uint8_t COP, const char* weightArray1, size_t length1, const char* weightArray2,size_t length2, 
                    const char* str1 = nullptr, size_t lengthStr1 = 0,const char* str2 = nullptr, size_t lengthStr2=0, 
                    const datatypes::Charset* cs=nullptr)

{
  if (COP & COMPARE_LIKE)
    return cs->like(COP & COMPARE_NOT, ConstString(str1, lengthStr1), ConstString(str2, lengthStr2));
  int8_t cmp = weightSub(weightArray1, length1, weightArray2.length2);
  switch (COP)
  {
    case COMPARE_NIL: return false;

    case COMPARE_LT: return cmp < 0;

    case COMPARE_EQ: return cmp == 0;

    case COMPARE_LE: return cmp <= 0;

    case COMPARE_GT: return cmp > 0;

    case COMPARE_NE: return cmp != 0;

    case COMPARE_GE: return cmp >= 0;
	default:
	return false;
  }
}
class WeightArrayHasherComparator
{
public:
  bool operator()(const std::string& str1, const std::string& str2) const
  {
    return compare(COMPARE_EQ, str1.c_str(), str1.length(), str2.c_str(), str2.length());
  }
};
class WeightHasher:utils::Hasher_r
{
private:
  constexpr static int seed=0x315f;
public:
  inline uint32_t operator()(const std::string& str) const
  {
	return Hasher_r::operator()(str.c_str(),str.length(),seed);
  }
};
inline size_t WeightArrayFromStr(const datatypes::Charset& cs, char* dst, const char* src, size_t length)
{
  utils::ConstString s(src, length);
  length = s.rtrimZero().length();
  cs.strnxfrm(dst, src, length);
  return length;
}
using DictWeightEqualityFilter=std::unordered_set<string, WeightHasher, WeightArrayHasherComparator>;
void filter2WeightFilter(const datatypes::Charset& cs,boost::shared_ptr<DictEqualityFilter> eqFilter,DictWeightEqualityFilter* eqWeightFilter)
{
  if (eqFilter)
  {
    for (auto &x : *eqFilter)
    {
	  //make sure memory be allocateed in heap
      string eqWeightArray(x.length()<16?16:x.length(),'\0');
      size_t weightArrayLength = WeightArrayFromStr(cs,const_cast<char*>(eqWeightArray.c_str()), x.c_str(), x.length());
      eqWeightArray.resize(weightArrayLength);
      eqWeightFilter->insert(move(eqWeightArray));
    }
  }
}
#endif
inline bool PrimitiveProcessor::compare(const datatypes::Charset& cs, uint8_t COP, const char* str1,
                                        size_t length1, const char* str2, size_t length2) throw()
{
  int error = 0;
  bool rc = primitives::StringComparator(cs).op(&error, COP, ConstString(str1, length1),
                                                ConstString(str2, length2));
  if (error)
  {
    MessageLog logger(LoggingID(28));
    logging::Message::Args colWidth;
    Message msg(34);

    colWidth.add(COP);
    colWidth.add("compare");
    msg.format(colWidth);
    logger.logErrorMessage(msg);
    return false;  // throw an exception here?
  }
  return rc;
}

/*
Notes:
        - assumes no continuation pointer
*/

void PrimitiveProcessor::p_TokenByScan(const TokenByScanRequestHeader* h, TokenByScanResultHeader* ret,
                                       unsigned outSize, boost::shared_ptr<DictEqualityFilter> eqFilter)
{
  const DataValue* args;
  const uint8_t* niceBlock;  // block cast to a byte-indexed type
  const uint8_t* niceInput;  // h cast to a byte-indexed type
  const uint16_t* offsets;
  int offsetIndex, argIndex, argsOffset;
  bool cmpResult = false;
  int i;
  const char* sig;
  uint16_t siglen;

  PrimToken* retTokens;
  DataValue* retDataValues;
  int rdvOffset;
  uint8_t* niceRet;  // ret cast to a byte-indexed type

  // set up pointers to fields within each structure

  // either retTokens or retDataValues will be used but not both.
  niceRet = reinterpret_cast<uint8_t*>(ret);
  rdvOffset = sizeof(TokenByScanResultHeader);

  retTokens = reinterpret_cast<PrimToken*>(&niceRet[rdvOffset]);
  retDataValues = reinterpret_cast<DataValue*>(&niceRet[rdvOffset]);

  {
    void* retp = static_cast<void*>(ret);
    memcpy(retp, h, sizeof(PrimitiveHeader) + sizeof(ISMPacketHeader));
  }
  ret->NVALS = 0;
  ret->NBYTES = sizeof(TokenByScanResultHeader);
  ret->ism.Command = DICT_SCAN_COMPARE_RESULTS;

  //...Initialize I/O counts
  ret->CacheIO = 0;
  ret->PhysicalIO = 0;

  niceBlock = reinterpret_cast<const uint8_t*>(block);
  offsets = reinterpret_cast<const uint16_t*>(&niceBlock[10]);
  niceInput = reinterpret_cast<const uint8_t*>(h);

  const datatypes::Charset cs(h->charsetNumber);

  for (offsetIndex = 1; offsets[offsetIndex] != 0xffff; offsetIndex++)
  {
    siglen = offsets[offsetIndex - 1] - offsets[offsetIndex];
    sig = reinterpret_cast<const char*>(&niceBlock[offsets[offsetIndex]]);
    argsOffset = sizeof(TokenByScanRequestHeader);
    argIndex = 0;
    args = reinterpret_cast<const DataValue*>(&niceInput[argsOffset]);

    if (eqFilter)
    {
      if (&cs.getCharset() != &eqFilter->getCharset())
      {
        // throw runtime_error("Collations mismatch: TokenByScanRequestHeader and DicEqualityFilter");
      }
      string strData(sig, siglen);
      bool gotIt = eqFilter->find(strData) != eqFilter->end();

      if ((h->COP1 == COMPARE_EQ && gotIt) || (h->COP1 == COMPARE_NE && !gotIt))
        goto store;

      goto no_store;
    }

    cmpResult = compare(cs, h->COP1, sig, siglen, args->data, args->len);

    switch (h->NVALS)
    {
      case 1:
      {
        if (cmpResult)
          goto store;

        goto no_store;
      }

      case 2:
      {
        if (!cmpResult && h->BOP == BOP_AND)
          goto no_store;

        if (cmpResult && h->BOP == BOP_OR)
          goto store;

        argsOffset += sizeof(uint16_t) + args->len;
        argIndex++;
        args = (DataValue*)&niceInput[argsOffset];

        cmpResult = compare(cs, h->COP2, sig, siglen, args->data, args->len);

        if (cmpResult)
          goto store;

        goto no_store;
      }

      default:
      {
        idbassert(0);
        for (i = 0, cmpResult = true; i < h->NVALS; i++)
        {
          cmpResult = compare(cs, h->COP1, sig, siglen, args->data, args->len);

          if (!cmpResult && h->BOP == BOP_AND)
            goto no_store;

          if (cmpResult && h->BOP == BOP_OR)
            goto store;

          argsOffset += sizeof(uint16_t) + args->len;
          argIndex++;
          args = (DataValue*)&niceInput[argsOffset];
        }

        if (i == h->NVALS && cmpResult)
          goto store;
        else
          goto no_store;
      }
    }

  store:

    if (h->OutputType == OT_DATAVALUE)
    {
      if ((ret->NBYTES + sizeof(DataValue) + siglen) > outSize)
      {
        MessageLog logger(LoggingID(28));
        logging::Message::Args marker;
        Message msg(35);

        marker.add(8);
        msg.format(marker);
        logger.logErrorMessage(msg);

        throw logging::DictionaryBufferOverflow();
      }

      retDataValues->len = siglen;
      memcpy(retDataValues->data, sig, siglen);
      rdvOffset += sizeof(DataValue) + siglen;
      retDataValues = (DataValue*)&niceRet[rdvOffset];
      ret->NVALS++;
      ret->NBYTES += sizeof(DataValue) + siglen;
    }
    else if (h->OutputType == OT_TOKEN)
    {
      if ((ret->NBYTES + sizeof(PrimToken)) > outSize)
      {
        MessageLog logger(LoggingID(28));
        logging::Message::Args marker;
        Message msg(35);

        marker.add(9);
        msg.format(marker);
        logger.logErrorMessage(msg);

        throw logging::DictionaryBufferOverflow();
      }

      retTokens[ret->NVALS].LBID = h->LBID;
      retTokens[ret->NVALS].offset = offsetIndex;  // need index rather than the block offset... rp 12/19/06
      retTokens[ret->NVALS].len = args->len;
      ret->NVALS++;
      ret->NBYTES += sizeof(PrimToken);
    }
    /*
     * XXXPAT: HACK!  Ron requested a special case where the input string
     * that matched and the token of the matched string were returned.
     * It will not be used in cases where there are multiple input strings.
     * We need to rethink the requirements for this primitive after Dec 15.
     */
    else if (h->OutputType == OT_BOTH)
    {
      if (ret->NBYTES + sizeof(PrimToken) + sizeof(DataValue) + args->len > outSize)
      {
        MessageLog logger(LoggingID(28));
        logging::Message::Args marker;
        Message msg(35);

        marker.add(10);
        msg.format(marker);
        logger.logErrorMessage(msg);

        throw logging::DictionaryBufferOverflow();
      }

      retDataValues->len = args->len;
      memcpy(retDataValues->data, args->data, args->len);
      rdvOffset += sizeof(DataValue) + args->len;
      retTokens = reinterpret_cast<PrimToken*>(&niceRet[rdvOffset]);
      retTokens->LBID = h->LBID;
      retTokens->offset = offsetIndex;  // need index rather than the block offset... rp 12/19/06
      retTokens->len = args->len;
      rdvOffset += sizeof(PrimToken);
      retDataValues = reinterpret_cast<DataValue*>(&niceRet[rdvOffset]);
      ret->NBYTES += sizeof(PrimToken) + sizeof(DataValue) + args->len;
      ret->NVALS++;
    }

  no_store:;  // this is intentional
  }

  return;
}

void PrimitiveProcessor::nextSig(int NVALS, const PrimToken* tokens, p_DataValue* ret, uint8_t outputFlags,
                                 bool oldGetSigBehavior, bool skipNulls) throw()
{
  const uint8_t* niceBlock = reinterpret_cast<const uint8_t*>(block);
  const uint16_t* offsets = reinterpret_cast<const uint16_t*>(&niceBlock[10]);

  if (NVALS == 0)
  {
    if (offsets[dict_OffsetIndex + 1] == 0xffff)
    {
      ret->len = -1;
      return;
    }

    ret->len = offsets[dict_OffsetIndex] - offsets[dict_OffsetIndex + 1];
    ret->data = &niceBlock[offsets[dict_OffsetIndex + 1]];

    if (outputFlags & OT_TOKEN)
      currentOffsetIndex = dict_OffsetIndex + 1;
  }
  else
  {
  again:

    if (dict_OffsetIndex >= NVALS)
    {
      ret->len = -1;
      return;
    }

    if (oldGetSigBehavior)
    {
      const OldGetSigParams* oldParams = reinterpret_cast<const OldGetSigParams*>(tokens);

      if (oldParams[dict_OffsetIndex].rid & 0x8000000000000000LL)
      {
        if (skipNulls)
        {
          /* Bug 3321.  For some cases the NULL token should be skipped.  The
           * isnull filter is handled by token columncommand or by the F & E
           * framework.  This primitive should only process nulls
           * when it's for projection.
           */
          dict_OffsetIndex++;
          goto again;
        }

        ret->len = nullStringLen;
        ret->data = (const uint8_t*)nullString;
      }
      else
      {
        ret->len = offsets[oldParams[dict_OffsetIndex].offsetIndex - 1] -
                   offsets[oldParams[dict_OffsetIndex].offsetIndex];
        // Whoa! apparently we have come across a missing signature! That is, the requested ordinal
        //  is larger than the number of signatures in this block. Return a "special" string so that
        //  the query keeps going, but that can be recognized as an internal error upon inspection.
        //@Bug 2534. Change the length check to 8000

        // MCOL-267:
        // With BLOB support we have had to increase this to 8176
        // because a BLOB can take 8176 bytes of a dictionary block
        // instead of the fixed 8000 with CHAR/VARCHAR
        if (ret->len < 0 || ret->len > 8176)
        {
          ret->data = reinterpret_cast<const uint8_t*>(signatureNotFound);
          ret->len = strlen(reinterpret_cast<const char*>(ret->data));
        }
        else
          ret->data = &niceBlock[offsets[oldParams[dict_OffsetIndex].offsetIndex]];
      }

      // 			idbassert(ret->len >= 0);
      currentOffsetIndex = oldParams[dict_OffsetIndex].offsetIndex;
      dict_OffsetIndex++;
      return;
    }

    /* XXXPAT: Need to check for the NULL token here */
    ret->len = tokens[dict_OffsetIndex].len;
    ret->data = &niceBlock[tokens[dict_OffsetIndex].offset];

    if (outputFlags & OT_TOKEN)
    {
      // offsets = reinterpret_cast<const uint16_t *>(&niceBlock[10]);
      for (currentOffsetIndex = 1; offsets[currentOffsetIndex] != 0xffff; currentOffsetIndex++)
        if (tokens[dict_OffsetIndex].offset == offsets[currentOffsetIndex])
          break;

      if (offsets[currentOffsetIndex] == 0xffff)
      {
        MessageLog logger(LoggingID(28));
        logging::Message::Args offset;
        Message msg(38);

        offset.add(tokens[dict_OffsetIndex].offset);
        msg.format(offset);
        logger.logErrorMessage(msg);

        currentOffsetIndex = -1;
        dict_OffsetIndex++;
        return;
      }
    }
  }

  dict_OffsetIndex++;
}

#if defined(__x86_64__) || defined(__aarch64__)
//vectorizedP_Dictionary use the vectorized compare.the store and no store just borrow the code in p_Dictionary
void PrimitiveProcessor::vectorizedP_Dictionary(const DictInput* in, vector<uint8_t>* out, bool skipNulls,
#if defined(XXX_PRIMITIVES_TOKEN_RANGES_XXX)
                                      uint32_t charsetNumber, boost::shared_ptr<DictEqualityFilter> eqFilter,
                                      uint8_t eqOp, uint64_t minMax[2])
#else
                                      uint32_t charsetNumber, boost::shared_ptr<DictEqualityFilter> eqFilter,
                                      uint8_t eqOp)
#endif
{
  PrimToken* outToken;
  const DictFilterElement* filter = 0;
  const uint8_t* in8;
  DataValue* outValue;
  p_DataValue min = {0, NULL}, max = {0, NULL}, sigptr = {0, NULL};
  int tmp, filterIndex, filterOffset;
  uint16_t aggCount;
  bool cmpResult;
  DictOutput header;
  const datatypes::Charset& cs(charsetNumber);

  // default size of the ouput to something sufficiently large to prevent
  // excessive reallocation and copy when resizing
  const unsigned DEF_OUTSIZE = 16 * 1024;
  // use this factor to scale out size of future resize calls
  const int SCALE_FACTOR = 2;
  out->resize(DEF_OUTSIZE);

  in8 = reinterpret_cast<const uint8_t*>(in);

  {
    void* hp = static_cast<void*>(&header);
    memcpy(hp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
  }
  header.ism.Command = DICT_RESULTS;
  header.NVALS = 0;
  header.LBID = in->LBID;
  dict_OffsetIndex = 0;
  filterIndex = 0;
  aggCount = 0;
  min.len = 0;
  max.len = 0;

  //...Initialize I/O counts
  header.CacheIO = 0;
  header.PhysicalIO = 0;

  header.NBYTES = sizeof(DictOutput);

  char filterWeightArray[4096];
  char weightArray[1024];
  vector<utils::ConstString> filWeightArrayPtr;
  if (in->InputFlags == 1)
    filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(OldGetSigParams));
  else
    filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(PrimToken));
  int weightArrayOffset = 0;
  for (filterIndex = 0; filterIndex < in->NOPS; filterIndex++)
  {
    filter = reinterpret_cast<const DictFilterElement*>(&in8[filterOffset]);
    size_t weightArraylength =
        WeightArrayFromStr(cs, filterWeightArray + weightArrayOffset, (const char*)filter->data, filter->len);
    filWeightArrayPtr.emplace_back(filterWeightArray + weightArrayOffset, weightArraylength);
    weightArrayOffset += weightArraylength;
    filterOffset += sizeof(DictFilterElement) + filter->len;
  }
  DictWeightEqualityFilter eqWeightFilter;
  filter2WeightFilter(cs,eqFilter,&eqWeightFilter);
  string maxWeightArray, minWeightArray;


  for (nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType, (in->InputFlags ? true : false), skipNulls);
       sigptr.len != -1;
       nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType, (in->InputFlags ? true : false), skipNulls))
  {
#if defined(XXX_PRIMITIVES_TOKEN_RANGES_XXX)
    if (minMax)
    {
      uint64_t v = encodeStringPrefix_check_null(sigptr.data, sigptr.len, charsetNumber);
      minMax[1] = minMax[1] < v ? v : minMax[1];
      minMax[0] = minMax[0] > v ? v : minMax[0];
    }
#endif
    size_t valueLength = WeightArrayFromStr(cs, weightArray,reinterpret_cast<const char*>(sigptr.data), sigptr.len);
    if (in->OutputType & OT_AGGREGATE)
    {
      // len == 0 indicates this is the first pass
      if (max.len != 0)
      {
        tmp = weightSub(weightArray, valueLength, maxWeightArray.c_str(), maxWeightArray.length());
        if (tmp > 0)
        {
          max = sigptr;
          maxWeightArray = string(weightArray, valueLength);
        }
      }
      else
      {
        max = sigptr;
        maxWeightArray = string(weightArray, valueLength);
      }

      if (min.len != 0)
      {
        tmp = weightSub(weightArray, valueLength, minWeightArray.c_str(), minWeightArray.length());
        if (tmp < 0)
        {
          min = sigptr;
          minWeightArray = string(weightArray, valueLength);
        }
      }
      else
      {
        min = sigptr;
        minWeightArray = string(weightArray, valueLength);
      }

      aggCount++;
    }
    // filter processing
    if (in->InputFlags == 1)
      filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(OldGetSigParams));
    else
      filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(PrimToken));

    if (eqFilter)
    {
      if (eqFilter->size() > 1 && in->BOP == BOP_AND && eqOp == COMPARE_EQ)
        goto no_store;

      if (eqFilter->size() > 1 && in->BOP == BOP_OR && eqOp == COMPARE_NE)
        goto store;
      string strData(weightArray, valueLength);
      bool gotIt = eqWeightFilter.find(strData)!=eqWeightFilter.end();
      if ((gotIt && eqOp == COMPARE_EQ) || (!gotIt && eqOp == COMPARE_NE))
        goto store;

      goto no_store;
    }

    for (filterIndex = 0; filterIndex < in->NOPS; filterIndex++)
    {
      filter = reinterpret_cast<const DictFilterElement*>(&in8[filterOffset]);
      cmpResult = primitives::compare(filter->COP, weightArray, valueLength, filWeightArrayPtr[filterIndex].str(),
                          filWeightArrayPtr[filterIndex].length(),(const char*)sigptr.data, sigptr.len, (const char*)filter->data,
                          filter->len,&cs);
      if (!cmpResult && in->BOP != BOP_OR)
        goto no_store;

      if (cmpResult && in->BOP != BOP_AND)
        goto store;

      filterOffset += sizeof(DictFilterElement) + filter->len;
    }

    if (filterIndex == in->NOPS && in->BOP != BOP_OR)
    {
    store:
      header.NVALS++;

      if (in->OutputType & OT_RID && in->InputFlags == 1)  // hack that indicates old GetSignature behavior
      {
        const OldGetSigParams* oldParams;
        uint64_t* outRid;
        oldParams = reinterpret_cast<const OldGetSigParams*>(in->tokens);
        uint32_t newlen = header.NBYTES + 8;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outRid = (uint64_t*)&(*out)[header.NBYTES];
        // mask off the upper bit of the rid; signifies the NULL token was passed in
        *outRid = (oldParams[dict_OffsetIndex - 1].rid & 0x7fffffffffffffffLL);
        header.NBYTES += 8;
      }

      if (in->OutputType & OT_INPUTARG && in->InputFlags == 0)
      {
        uint32_t newlen = header.NBYTES + sizeof(DataValue) + filter->len;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outValue = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
        outValue->len = filter->len;
        memcpy(outValue->data, filter->data, filter->len);
        header.NBYTES += sizeof(DataValue) + filter->len;
      }

      if (in->OutputType & OT_TOKEN)
      {
        uint32_t newlen = header.NBYTES + sizeof(PrimToken);

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outToken = reinterpret_cast<PrimToken*>(&(*out)[header.NBYTES]);
        outToken->LBID = in->LBID;
        outToken->offset = currentOffsetIndex;
        outToken->len = filter->len;
        header.NBYTES += sizeof(PrimToken);
      }

      if (in->OutputType & OT_DATAVALUE)
      {
        uint32_t newlen = header.NBYTES + sizeof(DataValue) + sigptr.len;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outValue = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
        outValue->len = sigptr.len;
        memcpy(outValue->data, sigptr.data, sigptr.len);
        header.NBYTES += sizeof(DataValue) + sigptr.len;
      }
    }

  no_store:;  // intentional
  }

  if (in->OutputType & OT_AGGREGATE)
  {
    uint32_t newlen = header.NBYTES + 3 * sizeof(uint16_t) + min.len + max.len;

    if (newlen > out->size())
    {
      out->resize(out->size() * SCALE_FACTOR);
    }

    uint16_t* tmp16 = reinterpret_cast<uint16_t*>(&(*out)[header.NBYTES]);
    DataValue* tmpDV = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES + sizeof(uint16_t)]);

    *tmp16 = aggCount;
    tmpDV->len = min.len;
    memcpy(tmpDV->data, min.data, min.len);
    header.NBYTES += 2 * sizeof(uint16_t) + min.len;

    tmpDV = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
    tmpDV->len = max.len;
    memcpy(tmpDV->data, max.data, max.len);
    header.NBYTES += sizeof(uint16_t) + max.len;
  }

  out->resize(header.NBYTES);

  memcpy(&(*out)[0], &header, sizeof(DictOutput));
}
#endif
void PrimitiveProcessor::p_Dictionary(const DictInput* in, vector<uint8_t>* out, bool skipNulls,
#if defined(XXX_PRIMITIVES_TOKEN_RANGES_XXX)
                                      uint32_t charsetNumber, boost::shared_ptr<DictEqualityFilter> eqFilter,
                                      uint8_t eqOp, uint64_t minMax[2])
#else
                                      uint32_t charsetNumber, boost::shared_ptr<DictEqualityFilter> eqFilter,
                                      uint8_t eqOp)
#endif
{
#if defined(__x86_64__) || defined(__aarch64__)
#if defined(XXX_PRIMITIVES_TOKEN_RANGES_XXX)
  vectorizedP_Dictionary(in,out,skipNulls,charsetNumber,eqFilter,eqOp,minMax);
#else
  vectorizedP_Dictionary(in, out, skipNulls, charsetNumber, eqFilter, eqOp);
#endif
  return;
#endif 
  PrimToken* outToken;
  const DictFilterElement* filter = 0;
  const uint8_t* in8;
  DataValue* outValue;
  p_DataValue min = {0, NULL}, max = {0, NULL}, sigptr = {0, NULL};
  int tmp, filterIndex, filterOffset;
  uint16_t aggCount;
  bool cmpResult;
  DictOutput header;
  const datatypes::Charset& cs(charsetNumber);

  // default size of the ouput to something sufficiently large to prevent
  // excessive reallocation and copy when resizing
  const unsigned DEF_OUTSIZE = 16 * 1024;
  // use this factor to scale out size of future resize calls
  const int SCALE_FACTOR = 2;
  out->resize(DEF_OUTSIZE);

  in8 = reinterpret_cast<const uint8_t*>(in);

  {
    void* hp = static_cast<void*>(&header);
    memcpy(hp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
  }
  header.ism.Command = DICT_RESULTS;
  header.NVALS = 0;
  header.LBID = in->LBID;
  dict_OffsetIndex = 0;
  filterIndex = 0;
  aggCount = 0;
  min.len = 0;
  max.len = 0;

  //...Initialize I/O counts
  header.CacheIO = 0;
  header.PhysicalIO = 0;

  header.NBYTES = sizeof(DictOutput);

  for (nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType, (in->InputFlags ? true : false), skipNulls);
       sigptr.len != -1;
       nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType, (in->InputFlags ? true : false), skipNulls))
  {
#if defined(XXX_PRIMITIVES_TOKEN_RANGES_XXX)
    if (minMax)
    {
      uint64_t v = encodeStringPrefix_check_null(sigptr.data, sigptr.len, charsetNumber);
      minMax[1] = minMax[1] < v ? v : minMax[1];
      minMax[0] = minMax[0] > v ? v : minMax[0];
    }
#endif
    // do aggregate processing
    if (in->OutputType & OT_AGGREGATE)
    {
      // len == 0 indicates this is the first pass
      if (max.len != 0)
      {
        tmp = cs.strnncollsp(sigptr.data, sigptr.len, max.data, max.len);

        if (tmp > 0)
          max = sigptr;
      }
      else
        max = sigptr;

      if (min.len != 0)
      {
        tmp = cs.strnncollsp(sigptr.data, sigptr.len, min.data, min.len);

        if (tmp < 0)
          min = sigptr;
      }
      else
        min = sigptr;

      aggCount++;
    }
    // filter processing
    if (in->InputFlags == 1)
      filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(OldGetSigParams));
    else
      filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(PrimToken));

    if (eqFilter)
    {
	  // MCOL-4407.
      // Support filters:
      // `where key = value0 and key = value1 {and key = value2}`
      //
      // The problem occurs only when HWM > columnstore_string_scan_threshold - 1
      // because in this case:
      // CS uses `tryCombineDictionary` which combines `DictStep`s into one as result
      // `eqFilter` has more than 1 filter and applies the logic below which is `or` by
      // default.
      // Note: The case HWM <= columnstore_string_scan_threshold - 1 has the same problem,
      // function `p_TokenByScan`, but it does not occur because `tryCombineDictionaryScan`
      // was turned off.
      if (eqFilter->size() > 1 && in->BOP == BOP_AND && eqOp == COMPARE_EQ)
        goto no_store;

      if (eqFilter->size() > 1 && in->BOP == BOP_OR && eqOp == COMPARE_NE)
        goto store;
      // MCOL-1246 Trim whitespace before match
      string strData((char*)sigptr.data, sigptr.len);
      boost::trim_right_if(strData, boost::is_any_of(" "));
      bool gotIt = eqFilter->find(strData) != eqFilter->end();

      if ((gotIt && eqOp == COMPARE_EQ) || (!gotIt && eqOp == COMPARE_NE))
        goto store;

      goto no_store;
    }

    for (filterIndex = 0; filterIndex < in->NOPS; filterIndex++)
    {
      filter = reinterpret_cast<const DictFilterElement*>(&in8[filterOffset]);
      cmpResult = compare(cs, filter->COP, (const char*)sigptr.data, sigptr.len, (const char*)filter->data,
                          filter->len);
      if (!cmpResult && in->BOP != BOP_OR)
        goto no_store;

      if (cmpResult && in->BOP != BOP_AND)
        goto store;

      filterOffset += sizeof(DictFilterElement) + filter->len;
    }

    if (filterIndex == in->NOPS && in->BOP != BOP_OR)
    {
    store:
      // cout << "storing it, str = " << string((char *)sigptr.data, sigptr.len) << endl;
      header.NVALS++;

      if (in->OutputType & OT_RID && in->InputFlags == 1)  // hack that indicates old GetSignature behavior
      {
        const OldGetSigParams* oldParams;
        uint64_t* outRid;
        oldParams = reinterpret_cast<const OldGetSigParams*>(in->tokens);
        uint32_t newlen = header.NBYTES + 8;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outRid = (uint64_t*)&(*out)[header.NBYTES];
        // mask off the upper bit of the rid; signifies the NULL token was passed in
        *outRid = (oldParams[dict_OffsetIndex - 1].rid & 0x7fffffffffffffffLL);
        header.NBYTES += 8;
      }

      if (in->OutputType & OT_INPUTARG && in->InputFlags == 0)
      {
        uint32_t newlen = header.NBYTES + sizeof(DataValue) + filter->len;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outValue = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
        outValue->len = filter->len;
        memcpy(outValue->data, filter->data, filter->len);
        header.NBYTES += sizeof(DataValue) + filter->len;
      }

      if (in->OutputType & OT_TOKEN)
      {
        uint32_t newlen = header.NBYTES + sizeof(PrimToken);

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outToken = reinterpret_cast<PrimToken*>(&(*out)[header.NBYTES]);
        outToken->LBID = in->LBID;
        outToken->offset = currentOffsetIndex;
        outToken->len = filter->len;
        header.NBYTES += sizeof(PrimToken);
      }

      if (in->OutputType & OT_DATAVALUE)
      {
        uint32_t newlen = header.NBYTES + sizeof(DataValue) + sigptr.len;

        if (newlen > out->size())
        {
          out->resize(out->size() * SCALE_FACTOR);
        }

        outValue = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
        outValue->len = sigptr.len;
        memcpy(outValue->data, sigptr.data, sigptr.len);
        header.NBYTES += sizeof(DataValue) + sigptr.len;
      }
    }

  no_store:;  // intentional
  }

  if (in->OutputType & OT_AGGREGATE)
  {
    uint32_t newlen = header.NBYTES + 3 * sizeof(uint16_t) + min.len + max.len;

    if (newlen > out->size())
    {
      out->resize(out->size() * SCALE_FACTOR);
    }

    uint16_t* tmp16 = reinterpret_cast<uint16_t*>(&(*out)[header.NBYTES]);
    DataValue* tmpDV = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES + sizeof(uint16_t)]);

    *tmp16 = aggCount;
    tmpDV->len = min.len;
    memcpy(tmpDV->data, min.data, min.len);
    header.NBYTES += 2 * sizeof(uint16_t) + min.len;

    tmpDV = reinterpret_cast<DataValue*>(&(*out)[header.NBYTES]);
    tmpDV->len = max.len;
    memcpy(tmpDV->data, max.data, max.len);
    header.NBYTES += sizeof(uint16_t) + max.len;
  }

  out->resize(header.NBYTES);

  memcpy(&(*out)[0], &header, sizeof(DictOutput));
}

}  // namespace primitives
