/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2016-2020 MariaDB

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

/***********************************************************************
 *   $Id: pdictionary.cpp 9655 2013-06-25 23:08:13Z xlou $
 *
 *
 ***********************************************************************/

#include <iostream>
#include <stdexcept>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"
#include "jlf_common.h"
#include "primitivestep.h"

namespace joblist
{
// struct pDictionaryStepPrimitive
//{
//    pDictionaryStepPrimitive(pDictionaryStep* pDictStep) : fPDictionaryStep(pDictStep)
//    {}
//
//	pDictionaryStep *fPDictionaryStep;
//
//    void operator()()
//    {
//        try
//        {
//            fPDictionaryStep->sendPrimitiveMessages();
//        } catch(runtime_error&)
//        {
//		}
//    }
//
//};
//
// struct pDictStepAggregator
//{
//    pDictStepAggregator(pDictionaryStep* pDictStep) : fPDictStep(pDictStep)
//    {}
//    pDictionaryStep *fPDictStep;
//    void operator()()
//    {
//        try
//        {
//            fPDictStep->receivePrimitiveMessages();
//        }
//        catch(runtime_error&)
//        {
//	}
//    }
//};

pDictionaryStep::pDictionaryStep(CalpontSystemCatalog::OID o, CalpontSystemCatalog::OID t,
                                 const CalpontSystemCatalog::ColType& ct, const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fOid(o)
 , fTableOid(t)
 , fBOP(BOP_NONE)
 , msgsSent(0)
 , msgsRecvd(0)
 , finishedSending(false)
 , recvWaiting(false)
 , ridCount(0)
 , fColType(ct)
 , pThread(0)
 , cThread(0)
 , fFilterCount(0)
 , requestList(0)
 , fInterval(jobInfo.flushInterval)
 , fMsgBytesIn(0)
 , fMsgBytesOut(0)
 , fRm(jobInfo.rm)
 , hasEqualityFilter(false)
{
}

void pDictionaryStep::addFilter(int8_t COP, const string& value)
{
  fFilterString << (uint8_t)COP;
  fFilterString << (uint16_t)value.size();
  fFilterString.append((const uint8_t*)value.c_str(), value.size());
  fFilterCount++;

  if (fFilterCount == 1 && (COP == COMPARE_EQ || COP == COMPARE_NE))
  {
    hasEqualityFilter = true;
    tmpCOP = COP;
  }

  if (hasEqualityFilter)
  {
    if (COP != tmpCOP)
    {
      hasEqualityFilter = false;
      eqFilter.clear();
    }
    else
      eqFilter.push_back(value);
  }
}

const string pDictionaryStep::toString() const
{
  ostringstream oss;

  oss << "pDictionaryStep ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId
      << " tb/col:" << fTableOid << "/" << fOid;
  oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
  {
    oss << fInputJobStepAssociation.outAt(i) << ", ";
  }

#ifdef FIFO_SINK

  if (fOid < 3000)
    oss << " (sink)";

#endif
  return oss.str();
}

void pDictionaryStep::appendFilter(const messageqcpp::ByteStream& filter, unsigned count)
{
  ByteStream bs(filter);  // need to preserve the input BS
  uint8_t* buf;
  uint8_t COP;
  uint16_t size;
  string value;

  while (bs.length() > 0)
  {
    bs >> COP;
    bs >> size;
    buf = bs.buf();
    value = string((char*)buf, size);
    addFilter(COP, value);
    bs.advance(size);
  }
}

void pDictionaryStep::addFilter(const Filter* f)
{
  if (NULL != f)
    fFilters.push_back(f);
}

void pDictionaryStep::appendFilter(const std::vector<const execplan::Filter*>& fs)
{
  fFilters.insert(fFilters.end(), fs.begin(), fs.end());
}

}  // namespace joblist
