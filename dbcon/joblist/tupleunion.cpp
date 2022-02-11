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

/*****************************************************************************
 * $Id: tupleunion.cpp 9665 2013-07-02 21:47:39Z pleblanc $
 *
 ****************************************************************************/

#include <string>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "querytele.h"
using namespace querytele;

#include "dataconvert.h"
#include "hasher.h"
#include "jlf_common.h"
#include "resourcemanager.h"
#include "tupleunion.h"

using namespace std;
using namespace std::tr1;
using namespace boost;
using namespace execplan;
using namespace rowgroup;
using namespace dataconvert;

#ifndef __linux__
#ifndef M_LN10
#define M_LN10 2.30258509299404568402 /* log_e 10 */
#endif
namespace
{
// returns the value of 10 raised to the power x.
inline double exp10(double x)
{
  return exp(x * M_LN10);
}
}  // namespace
#endif

namespace joblist
{
inline uint64_t TupleUnion::Hasher::operator()(const RowPosition& p) const
{
  Row& row = ts->row;

  if (p.group & RowPosition::normalizedFlag)
    ts->normalizedData[p.group & ~RowPosition::normalizedFlag].getRow(p.row, &row);
  else
    ts->rowMemory[p.group].getRow(p.row, &row);

  return row.hash();
}

inline bool TupleUnion::Eq::operator()(const RowPosition& d1, const RowPosition& d2) const
{
  Row &r1 = ts->row, &r2 = ts->row2;

  if (d1.group & RowPosition::normalizedFlag)
    ts->normalizedData[d1.group & ~RowPosition::normalizedFlag].getRow(d1.row, &r1);
  else
    ts->rowMemory[d1.group].getRow(d1.row, &r1);

  if (d2.group & RowPosition::normalizedFlag)
    ts->normalizedData[d2.group & ~RowPosition::normalizedFlag].getRow(d2.row, &r2);
  else
    ts->rowMemory[d2.group].getRow(d2.row, &r2);

  return r1.equals(r2);
}

TupleUnion::TupleUnion(CalpontSystemCatalog::OID tableOID, const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , fTableOID(tableOID)
 , output(NULL)
 , outputIt(-1)
 , memUsage(0)
 , rm(jobInfo.rm)
 , runnersDone(0)
 , distinctCount(0)
 , distinctDone(0)
 , fRowsReturned(0)
 , runRan(false)
 , joinRan(false)
 , sessionMemLimit(jobInfo.umMemLimit)
 , fTimeZone(jobInfo.timeZone)
{
  uniquer.reset(new Uniquer_t(10, Hasher(this), Eq(this), allocator));
  fExtendedInfo = "TUN: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TUN;
}

TupleUnion::~TupleUnion()
{
  rm->returnMemory(memUsage, sessionMemLimit);

  if (!runRan && output)
    output->endOfInput();
}

CalpontSystemCatalog::OID TupleUnion::tableOid() const
{
  return fTableOID;
}

void TupleUnion::setInputRowGroups(const vector<rowgroup::RowGroup>& in)
{
  inputRGs = in;
}

void TupleUnion::setOutputRowGroup(const rowgroup::RowGroup& out)
{
  outputRG = out;
  rowLength = outputRG.getRowSizeWithStrings();
}

void TupleUnion::setDistinctFlags(const vector<bool>& v)
{
  distinctFlags = v;
}

void TupleUnion::readInput(uint32_t which)
{
  /* The handling of the output got a little kludgey with the string table enhancement.
   * When there is no distinct check, the outputs are all generated independently of
   * each other locally in this fcn.  When there is a distinct check, threads
   * share the output, which is built in the 'rowMemory' vector rather than in
   * thread-local memory.  Building the result in a common space allows us to
   * store 8-byte offsets in rowMemory rather than 16-bytes for absolute pointers.
   */

  RowGroupDL* dl = NULL;
  bool more = true;
  RGData inRGData, outRGData, *tmpRGData;
  uint32_t it = numeric_limits<uint32_t>::max();
  RowGroup l_inputRG, l_outputRG, l_tmpRG;
  Row inRow, outRow, tmpRow;
  bool distinct;
  uint64_t memUsageBefore, memUsageAfter, memDiff;
  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;

  l_outputRG = outputRG;
  dl = inputs[which];
  l_inputRG = inputRGs[which];
  l_inputRG.initRow(&inRow);
  l_outputRG.initRow(&outRow);
  distinct = distinctFlags[which];

  if (distinct)
  {
    l_tmpRG = outputRG;
    tmpRGData = &normalizedData[which];
    l_tmpRG.initRow(&tmpRow);
    l_tmpRG.setData(tmpRGData);
    l_tmpRG.resetRowGroup(0);
    l_tmpRG.getRow(0, &tmpRow);
  }
  else
  {
    outRGData = RGData(l_outputRG);
    l_outputRG.setData(&outRGData);
    l_outputRG.resetRowGroup(0);
    l_outputRG.getRow(0, &outRow);
  }

  try
  {
    it = dl->getIterator();
    more = dl->next(it, &inRGData);

    if (dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    if (fStartTime == -1)
    {
      sts.msg_type = StepTeleStats::ST_START;
      sts.total_units_of_work = 1;
      postStepStartTele(sts);
    }

    while (more && !cancelled())
    {
      /*
          normalize each row
            if distinct flag is set
                  copy the row into the output and test for uniqueness
                    if unique, increment the row count
            else
              copy the row into the output & inc row count
      */
      l_inputRG.setData(&inRGData);
      l_inputRG.getRow(0, &inRow);

      if (distinct)
      {
        memDiff = 0;
        l_tmpRG.resetRowGroup(0);
        l_tmpRG.getRow(0, &tmpRow);
        l_tmpRG.setRowCount(l_inputRG.getRowCount());

        for (uint32_t i = 0; i < l_inputRG.getRowCount(); i++, inRow.nextRow(), tmpRow.nextRow())
          normalize(inRow, &tmpRow);

        l_tmpRG.getRow(0, &tmpRow);
        {
          boost::mutex::scoped_lock lk(uniquerMutex);
          getOutput(&l_outputRG, &outRow, &outRGData);
          memUsageBefore = allocator.getMemUsage();

          for (uint32_t i = 0; i < l_tmpRG.getRowCount(); i++, tmpRow.nextRow())
          {
            pair<Uniquer_t::iterator, bool> inserted;
            inserted = uniquer->insert(RowPosition(which | RowPosition::normalizedFlag, i));

            if (inserted.second)
            {
              copyRow(tmpRow, &outRow);
              const_cast<RowPosition&>(*(inserted.first)) =
                  RowPosition(rowMemory.size() - 1, l_outputRG.getRowCount());
              memDiff += outRow.getRealSize();
              addToOutput(&outRow, &l_outputRG, true, outRGData);
            }
          }

          memUsageAfter = allocator.getMemUsage();
          memDiff += (memUsageAfter - memUsageBefore);
        }

        if (rm->getMemory(memDiff, sessionMemLimit))
        {
          memUsage += memDiff;
        }
        else
        {
          fLogger->logMessage(logging::LOG_TYPE_INFO, logging::ERR_UNION_TOO_BIG);

          if (status() == 0)  // preserve existing error code
          {
            errorMessage(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_UNION_TOO_BIG));
            status(logging::ERR_UNION_TOO_BIG);
          }

          abort();
        }
      }
      else
      {
        for (uint32_t i = 0; i < l_inputRG.getRowCount(); i++, inRow.nextRow())
        {
          normalize(inRow, &outRow);
          addToOutput(&outRow, &l_outputRG, false, outRGData);
        }
      }

      more = dl->next(it, &inRGData);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::unionStepErr, logging::ERR_UNION_TOO_BIG,
                    "TupleUnion::readInput()");
    status(logging::unionStepErr);
    abort();
  }

  /* make sure that the input was drained before exiting.  This can happen if the
  query was aborted */
  if (dl && it != numeric_limits<uint32_t>::max())
    while (more)
      more = dl->next(it, &inRGData);

  {
    boost::mutex::scoped_lock lock1(uniquerMutex);
    boost::mutex::scoped_lock lock2(sMutex);

    if (!distinct && l_outputRG.getRowCount() > 0)
      output->insert(outRGData);

    if (distinct)
    {
      getOutput(&l_outputRG, &outRow, &outRGData);

      if (++distinctDone == distinctCount && l_outputRG.getRowCount() > 0)
        output->insert(outRGData);
    }

    if (++runnersDone == fInputJobStepAssociation.outSize())
    {
      output->endOfInput();

      sts.msg_type = StepTeleStats::ST_SUMMARY;
      sts.total_units_of_work = sts.units_of_work_completed = 1;
      sts.rows = fRowsReturned;
      postStepSummaryTele(sts);

      if (traceOn())
      {
        dlTimes.setLastReadTime();
        dlTimes.setEndOfInputTime();

        time_t t = time(0);
        char timeString[50];
        ctime_r(&t, timeString);
        timeString[strlen(timeString) - 1] = '\0';
        ostringstream logStr;
        logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString
               << "; total rows returned-" << fRowsReturned << endl
               << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
               << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
               << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
               << "\tJob completion status " << status() << endl;
        logEnd(logStr.str().c_str());
        fExtendedInfo += logStr.str();
        formatMiniStats();
      }
    }
  }
}

uint32_t TupleUnion::nextBand(messageqcpp::ByteStream& bs)
{
  RGData mem;
  bool more;
  uint32_t ret = 0;

  bs.restart();
  more = output->next(outputIt, &mem);

  if (more)
    outputRG.setData(&mem);
  else
  {
    mem = RGData(outputRG, 0);
    outputRG.setData(&mem);
    outputRG.resetRowGroup(0);
    outputRG.setStatus(status());
  }

  outputRG.serializeRGData(bs);
  ret = outputRG.getRowCount();

  return ret;
}

void TupleUnion::getOutput(RowGroup* rg, Row* row, RGData* data)
{
  if (UNLIKELY(rowMemory.empty()))
  {
    *data = RGData(*rg);
    rg->setData(data);
    rg->resetRowGroup(0);
    rowMemory.push_back(*data);
  }
  else
  {
    *data = rowMemory.back();
    rg->setData(data);
  }

  rg->getRow(rg->getRowCount(), row);
}

void TupleUnion::addToOutput(Row* r, RowGroup* rg, bool keepit, RGData& data)
{
  r->nextRow();
  rg->incRowCount();
  fRowsReturned++;

  if (rg->getRowCount() == 8192)
  {
    {
      boost::mutex::scoped_lock lock(sMutex);
      output->insert(data);
    }
    data = RGData(*rg);
    rg->setData(&data);
    rg->resetRowGroup(0);
    rg->getRow(0, r);

    if (keepit)
      rowMemory.push_back(data);
  }
}

void TupleUnion::normalize(const Row& in, Row* out)
{
  uint32_t i;

  out->setRid(0);

  for (i = 0; i < out->getColumnCount(); i++)
  {
    if (in.isNullValue(i))
    {
      writeNull(out, i);
      continue;
    }

    switch (in.getColTypes()[i])
    {
      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::BIGINT:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TINYINT:
          case CalpontSystemCatalog::SMALLINT:
          case CalpontSystemCatalog::MEDINT:
          case CalpontSystemCatalog::INT:
          case CalpontSystemCatalog::BIGINT:
            if (out->getScale(i) || in.getScale(i))
              goto dec1;

            out->setIntField(in.getIntField(i), i);
            break;

          case CalpontSystemCatalog::UTINYINT:
          case CalpontSystemCatalog::USMALLINT:
          case CalpontSystemCatalog::UMEDINT:
          case CalpontSystemCatalog::UINT:
          case CalpontSystemCatalog::UBIGINT:
            if (in.getScale(i))
              goto dec1;

            out->setUintField(in.getUintField(i), i);
            break;

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            ostringstream os;

            if (in.getScale(i))
            {
              double d = in.getIntField(i);
              d /= exp10(in.getScale(i));
              os.precision(15);
              os << d;
            }
            else
              os << in.getIntField(i);

            out->setStringField(os.str(), i);
            break;
          }

          case CalpontSystemCatalog::DATE:
          case CalpontSystemCatalog::DATETIME:
          case CalpontSystemCatalog::TIME:
          case CalpontSystemCatalog::TIMESTAMP:
            throw logic_error(
                "TupleUnion::normalize(): tried to normalize an int to a timestamp, time, date or datetime");

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            auto d = in.getScaledSInt64FieldAsXFloat<double>(i);
            out->setFloatField((float)d, i);
            break;
          }

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          {
            auto d = in.getScaledSInt64FieldAsXFloat<double>(i);
            out->setDoubleField(d, i);
            break;
          }

          case CalpontSystemCatalog::LONGDOUBLE:
          {
            auto d = in.getScaledSInt64FieldAsXFloat<long double>(i);
            out->setLongDoubleField(d, i);
            break;
          }

          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
          dec1:
            int diff = out->getScale(i) - in.getScale(i);

            idbassert(diff >= 0);
            /*
               Signed INT to XDecimal
               TODO:
               - This code does not handle overflow that may happen on
                 scale multiplication. Instead of returning a garbage value
                 we should probably apply saturation here. In long terms we
                 should implement DECIMAL(65,x) to avoid overflow completely
                 (so the UNION between DECIMAL and integer can choose a proper
                  DECIMAL(M,N) result data type to guarantee that any incoming
                  integer value can fit into it).
            */
            if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
            {
              int128_t val = datatypes::applySignedScale<int128_t>(in.getIntField(i), diff);
              out->setInt128Field(val, i);
            }
            else
            {
              int64_t val = datatypes::applySignedScale<int64_t>(in.getIntField(i), diff);
              out->setIntField(val, i);
            }

            break;
          }

          default:
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
        }

        break;

      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TINYINT:
          case CalpontSystemCatalog::SMALLINT:
          case CalpontSystemCatalog::MEDINT:
          case CalpontSystemCatalog::INT:
          case CalpontSystemCatalog::BIGINT:
            if (out->getScale(i))
              goto dec2;

            out->setIntField(in.getUintField(i), i);
            break;

          case CalpontSystemCatalog::UTINYINT:
          case CalpontSystemCatalog::USMALLINT:
          case CalpontSystemCatalog::UMEDINT:
          case CalpontSystemCatalog::UINT:
          case CalpontSystemCatalog::UBIGINT: out->setUintField(in.getUintField(i), i); break;

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            ostringstream os;

            if (in.getScale(i))
            {
              double d = in.getUintField(i);
              d /= exp10(in.getScale(i));
              os.precision(15);
              os << d;
            }
            else
              os << in.getUintField(i);

            out->setStringField(os.str(), i);
            break;
          }

          case CalpontSystemCatalog::DATE:
          case CalpontSystemCatalog::DATETIME:
          case CalpontSystemCatalog::TIME:
          case CalpontSystemCatalog::TIMESTAMP:
            throw logic_error(
                "TupleUnion::normalize(): tried to normalize an int to a timestamp, time, date or datetime");

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            auto d = in.getScaledUInt64FieldAsXFloat<double>(i);
            out->setFloatField((float)d, i);
            break;
          }

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          {
            auto d = in.getScaledUInt64FieldAsXFloat<double>(i);
            out->setDoubleField(d, i);
            break;
          }

          case CalpontSystemCatalog::LONGDOUBLE:
          {
            auto d = in.getScaledUInt64FieldAsXFloat<long double>(i);
            out->setLongDoubleField(d, i);
            break;
          }

          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
          dec2:
            int diff = out->getScale(i) - in.getScale(i);

            idbassert(diff >= 0);
            /*
              Unsigned INT to XDecimal
              TODO:
              - The overflow problem mentioned in the code under label "dec1:" is
                also applicable here.
            */
            if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
            {
              int128_t val = datatypes::applySignedScale<int128_t>(in.getUintField(i), diff);
              out->setInt128Field(val, i);
            }
            else
            {
              uint64_t val = datatypes::applySignedScale<uint64_t>(in.getUintField(i), diff);
              out->setUintField(val, i);
            }

            break;
          }

          default:
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
        }

        break;

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::TEXT:
      case CalpontSystemCatalog::VARCHAR:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR: out->setStringField(in.getStringField(i), i); break;

          default:
          {
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: string to " << out->getColTypes()[i];
            throw logic_error(os.str());
          }
        }

        break;

      case CalpontSystemCatalog::DATE:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::DATE: out->setIntField(in.getIntField(i), i); break;

          case CalpontSystemCatalog::DATETIME:
          {
            uint64_t date = in.getUintField(i);
            date &= ~0x3f;  // zero the 'spare' field
            date <<= 32;
            out->setUintField(date, i);
            break;
          }

          case CalpontSystemCatalog::TIMESTAMP:
          {
            dataconvert::Date date(in.getUintField(i));
            dataconvert::MySQLTime m_time;
            m_time.year = date.year;
            m_time.month = date.month;
            m_time.day = date.day;
            m_time.hour = 0;
            m_time.minute = 0;
            m_time.second = 0;
            m_time.second_part = 0;

            dataconvert::TimeStamp timeStamp;
            bool isValid = true;
            int64_t seconds = dataconvert::mySQLTimeToGmtSec(m_time, fTimeZone, isValid);

            if (!isValid)
            {
              timeStamp.reset();
            }
            else
            {
              timeStamp.second = seconds;
              timeStamp.msecond = m_time.second_part;
            }

            uint64_t outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&timeStamp));
            out->setUintField(outValue, i);
            break;
          }

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            string d = DataConvert::dateToString(in.getUintField(i));
            out->setStringField(d, i);
            break;
          }

          default:
          {
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: date to " << out->getColTypes()[i];
            throw logic_error(os.str());
          }
        }

        break;

      case CalpontSystemCatalog::DATETIME:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::DATETIME: out->setIntField(in.getIntField(i), i); break;

          case CalpontSystemCatalog::DATE:
          {
            uint64_t val = in.getUintField(i);
            val >>= 32;
            out->setUintField(val, i);
            break;
          }

          case CalpontSystemCatalog::TIMESTAMP:
          {
            uint64_t val = in.getUintField(i);
            dataconvert::DateTime dtime(val);
            dataconvert::MySQLTime m_time;
            dataconvert::TimeStamp timeStamp;

            m_time.year = dtime.year;
            m_time.month = dtime.month;
            m_time.day = dtime.day;
            m_time.hour = dtime.hour;
            m_time.minute = dtime.minute;
            m_time.second = dtime.second;
            m_time.second_part = dtime.msecond;

            bool isValid = true;
            int64_t seconds = mySQLTimeToGmtSec(m_time, fTimeZone, isValid);

            if (!isValid)
            {
              timeStamp.reset();
            }
            else
            {
              timeStamp.second = seconds;
              timeStamp.msecond = m_time.second_part;
            }

            uint64_t outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&timeStamp));
            out->setUintField(outValue, i);
            break;
          }

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            string d = DataConvert::datetimeToString(in.getUintField(i));
            out->setStringField(d, i);
            break;
          }

          default:
          {
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: datetime to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
          }
        }

        break;

      case CalpontSystemCatalog::TIMESTAMP:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TIMESTAMP: out->setIntField(in.getIntField(i), i); break;

          case CalpontSystemCatalog::DATE:
          case CalpontSystemCatalog::DATETIME:
          {
            uint64_t val = in.getUintField(i);
            dataconvert::TimeStamp timestamp(val);
            int64_t seconds = timestamp.second;
            uint64_t outValue;

            dataconvert::MySQLTime time;
            dataconvert::gmtSecToMySQLTime(seconds, time, fTimeZone);

            if (out->getColTypes()[i] == CalpontSystemCatalog::DATE)
            {
              dataconvert::Date date;
              date.year = time.year;
              date.month = time.month;
              date.day = time.day;
              date.spare = 0;
              outValue = (uint32_t) * (reinterpret_cast<uint32_t*>(&date));
            }
            else
            {
              dataconvert::DateTime datetime;
              datetime.year = time.year;
              datetime.month = time.month;
              datetime.day = time.day;
              datetime.hour = time.hour;
              datetime.minute = time.minute;
              datetime.second = time.second;
              datetime.msecond = timestamp.msecond;
              outValue = (uint64_t) * (reinterpret_cast<uint64_t*>(&datetime));
            }

            out->setUintField(outValue, i);
            break;
          }

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            string d = DataConvert::timestampToString(in.getUintField(i), fTimeZone);
            out->setStringField(d, i);
            break;
          }

          default:
          {
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: timestamp to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
          }
        }

        break;

      case CalpontSystemCatalog::TIME:
        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TIME: out->setIntField(in.getIntField(i), i); break;

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            string d = DataConvert::timeToString(in.getIntField(i));
            out->setStringField(d, i);
            break;
          }

          default:
          {
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: time to " << out->getColTypes()[i];
            throw logic_error(os.str());
          }
        }

        break;

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      {
        double val =
            (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT ? in.getFloatField(i) : in.getDoubleField(i));

        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TINYINT:
          case CalpontSystemCatalog::SMALLINT:
          case CalpontSystemCatalog::MEDINT:
          case CalpontSystemCatalog::INT:
          case CalpontSystemCatalog::BIGINT:
            if (out->getScale(i))
              goto dec3;

            out->setIntField((int64_t)val, i);
            break;

          case CalpontSystemCatalog::UTINYINT:
          case CalpontSystemCatalog::USMALLINT:
          case CalpontSystemCatalog::UMEDINT:
          case CalpontSystemCatalog::UINT:
          case CalpontSystemCatalog::UBIGINT: out->setUintField((uint64_t)val, i); break;

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT: out->setFloatField(val, i); break;

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE: out->setDoubleField(val, i); break;

          case CalpontSystemCatalog::LONGDOUBLE: out->setLongDoubleField(val, i); break;

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            ostringstream os;
            os.precision(15);  // to match mysql's output
            os << val;
            out->setStringField(os.str(), i);
            break;
          }

          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
          dec3: /* have to pick a scale to use for the double. using 5... */
            uint32_t scale = 5;
            uint64_t ival = (uint64_t)(double)(val * datatypes::scaleDivisor<double>(scale));
            int diff = out->getScale(i) - scale;
            // xFLOAT or xDOUBLE to xDECIMAL conversion. Is it really possible?
            // TODO:
            // Perhaps we should add an assert here that this combination is not possible
            // In the current reduction all problems mentioned in the code under
            //  label "dec1:" are also applicable here.
            // TODO: isn't overflow possible below?
            ival = datatypes::applySignedScale<uint64_t>(ival, diff);

            if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
              out->setInt128Field(ival, i);
            else
              out->setIntField(ival, i);

            break;
          }

          default:
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: floating point to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
        }

        break;
      }

      case CalpontSystemCatalog::LONGDOUBLE:
      {
        long double val = in.getLongDoubleField(i);

        switch (out->getColTypes()[i])
        {
          case CalpontSystemCatalog::TINYINT:
          case CalpontSystemCatalog::SMALLINT:
          case CalpontSystemCatalog::MEDINT:
          case CalpontSystemCatalog::INT:
          case CalpontSystemCatalog::BIGINT:
            if (out->getScale(i))
              goto dec4;

            out->setIntField((int64_t)val, i);
            break;

          case CalpontSystemCatalog::UTINYINT:
          case CalpontSystemCatalog::USMALLINT:
          case CalpontSystemCatalog::UMEDINT:
          case CalpontSystemCatalog::UINT:
          case CalpontSystemCatalog::UBIGINT: out->setUintField((uint64_t)val, i); break;

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT: out->setFloatField(val, i); break;

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE: out->setDoubleField(val, i); break;

          case CalpontSystemCatalog::LONGDOUBLE: out->setLongDoubleField(val, i); break;

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          {
            ostringstream os;
            os.precision(15);  // to match mysql's output
            os << val;
            out->setStringField(os.str(), i);
            break;
          }

          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
          dec4: /* have to pick a scale to use for the double. using 5... */
            uint32_t scale = 5;
            uint64_t ival = (uint64_t)(double)(val * datatypes::scaleDivisor<double>(scale));
            int diff = out->getScale(i) - scale;

            // LONGDOUBLE to xDECIMAL conversions: is it really possible?
            // TODO:
            // Perhaps we should add an assert here that this combination is not possible
            // In the current reduction all problems mentioned in the code under
            //  label "dec1:" are also applicable here.
            ival = datatypes::applySignedScale<uint64_t>(ival, diff);

            if (out->getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
              out->setInt128Field(ival, i);
            else
              out->setIntField(ival, i);

            break;
          }

          default:
            ostringstream os;
            os << "TupleUnion::normalize(): tried an illegal conversion: floating point to "
               << out->getColTypes()[i];
            throw logic_error(os.str());
        }

        break;
      }

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        int64_t val = 0;
        int128_t val128 = 0;
        bool isInputWide = false;

        if (in.getColumnWidth(i) == datatypes::MAXDECIMALWIDTH)
        {
          in.getInt128Field(i, val128);
          isInputWide = true;
        }
        else
          val = in.getIntField(i);

        uint32_t scale = in.getScale(i);

        switch (out->getColTypes()[i])
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
          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
            if (datatypes::isWideDecimalType(out->getColTypes()[i], out->getColumnWidth(i)))
            {
              if (out->getScale(i) == scale)
                out->setInt128Field(isInputWide ? val128 : val, i);
              else if (out->getScale(i) > scale)
              {
                int128_t temp = datatypes::applySignedScale<int128_t>(isInputWide ? val128 : val,
                                                                      out->getScale(i) - scale);
                out->setInt128Field(temp, i);
              }
              else  // should not happen, the output's scale is the largest
                throw logic_error("TupleUnion::normalize(): incorrect scale setting");
            }
            // If output type is narrow decimal, input type
            // has to be narrow decimal as well.
            else
            {
              if (out->getScale(i) == scale)
                out->setIntField(val, i);
              else if (out->getScale(i) > scale)
              {
                int64_t temp = datatypes::applySignedScale<int64_t>(val, out->getScale(i) - scale);
                out->setIntField(temp, i);
              }
              else  // should not happen, the output's scale is the largest
                throw logic_error("TupleUnion::normalize(): incorrect scale setting");
            }

            break;
          }

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            float fval = ((float)val) / IDB_pow[scale];
            out->setFloatField(fval, i);
            break;
          }

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          {
            double dval = ((double)val) / IDB_pow[scale];
            out->setDoubleField(dval, i);
            break;
          }

          case CalpontSystemCatalog::LONGDOUBLE:
          {
            long double dval = ((long double)val) / IDB_pow[scale];
            out->setLongDoubleField(dval, i);
            break;
          }

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::VARCHAR:
          default:
          {
            if (LIKELY(isInputWide))
            {
              datatypes::Decimal dec(0, in.getScale(i), in.getPrecision(i), val128);
              out->setStringField(dec.toString(), i);
            }
            else
            {
              datatypes::Decimal dec(val, in.getScale(i), in.getPrecision(i));
              out->setStringField(dec.toString(), i);
            }
            break;
          }
        }

        break;
      }

      case CalpontSystemCatalog::BLOB:
      case CalpontSystemCatalog::VARBINARY:
      {
        // out->setVarBinaryField(in.getVarBinaryStringField(i), i);  // not efficient
        out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), i);
        break;
      }

      default:
      {
        ostringstream os;
        os << "TupleUnion::normalize(): unknown input type (" << in.getColTypes()[i] << ")";
        cout << os.str() << endl;
        throw logic_error(os.str());
      }
    }
  }
}

void TupleUnion::run()
{
  uint32_t i;

  boost::mutex::scoped_lock lk(jlLock);

  if (runRan)
    return;

  runRan = true;
  lk.unlock();

  for (i = 0; i < fInputJobStepAssociation.outSize(); i++)
    inputs.push_back(fInputJobStepAssociation.outAt(i)->rowGroupDL());

  output = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

  if (fDelivery)
  {
    outputIt = output->getIterator();
  }

  outputRG.initRow(&row);
  outputRG.initRow(&row2);

  distinctCount = 0;
  normalizedData.reset(new RGData[inputs.size()]);

  for (i = 0; i < inputs.size(); i++)
  {
    if (distinctFlags[i])
    {
      distinctCount++;
      normalizedData[i].reinit(outputRG);
    }
  }

  runners.reserve(inputs.size());

  for (i = 0; i < inputs.size(); i++)
  {
    runners.push_back(jobstepThreadPool.invoke(Runner(this, i)));
  }
}

void TupleUnion::join()
{
  boost::mutex::scoped_lock lk(jlLock);

  if (joinRan)
    return;

  joinRan = true;
  lk.unlock();

  jobstepThreadPool.join(runners);
  runners.clear();
  uniquer->clear();
  rowMemory.clear();
  rm->returnMemory(memUsage, sessionMemLimit);
  memUsage = 0;
}

const string TupleUnion::toString() const
{
  ostringstream oss;
  oss << "TupleUnion       ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId;
  oss << " st:" << fStepId;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << ((i == 0) ? " " : ", ") << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << ((i == 0) ? " " : ", ") << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleUnion::writeNull(Row* out, uint32_t col)
{
  switch (out->getColTypes()[col])
  {
    case CalpontSystemCatalog::TINYINT: out->setUintField<1>(joblist::TINYINTNULL, col); break;

    case CalpontSystemCatalog::SMALLINT: out->setUintField<1>(joblist::SMALLINTNULL, col); break;

    case CalpontSystemCatalog::UTINYINT: out->setUintField<1>(joblist::UTINYINTNULL, col); break;

    case CalpontSystemCatalog::USMALLINT: out->setUintField<1>(joblist::USMALLINTNULL, col); break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      uint32_t len = out->getColumnWidth(col);

      switch (len)
      {
        case 1: out->setUintField<1>(joblist::TINYINTNULL, col); break;

        case 2: out->setUintField<1>(joblist::SMALLINTNULL, col); break;

        case 4: out->setUintField<4>(joblist::INTNULL, col); break;

        case 8: out->setUintField<8>(joblist::BIGINTNULL, col); break;

        default:
        {
        }
      }

      break;
    }

    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT: out->setUintField<4>(joblist::INTNULL, col); break;

    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT: out->setUintField<4>(joblist::UINTNULL, col); break;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: out->setUintField<4>(joblist::FLOATNULL, col); break;

    case CalpontSystemCatalog::DATE: out->setUintField<4>(joblist::DATENULL, col); break;

    case CalpontSystemCatalog::BIGINT: out->setUintField<8>(joblist::BIGINTNULL, col); break;

    case CalpontSystemCatalog::UBIGINT: out->setUintField<8>(joblist::UBIGINTNULL, col); break;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: out->setUintField<8>(joblist::DOUBLENULL, col); break;

    case CalpontSystemCatalog::DATETIME: out->setUintField<8>(joblist::DATETIMENULL, col); break;

    case CalpontSystemCatalog::TIMESTAMP: out->setUintField<8>(joblist::TIMESTAMPNULL, col); break;

    case CalpontSystemCatalog::TIME: out->setUintField<8>(joblist::TIMENULL, col); break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
    {
      uint32_t len = out->getColumnWidth(col);

      switch (len)
      {
        case 1: out->setUintField<1>(joblist::CHAR1NULL, col); break;

        case 2: out->setUintField<2>(joblist::CHAR2NULL, col); break;

        case 3:
        case 4: out->setUintField<4>(joblist::CHAR4NULL, col); break;

        case 5:
        case 6:
        case 7:
        case 8: out->setUintField<8>(joblist::CHAR8NULL, col); break;

        default: out->setStringField(joblist::CPNULLSTRMARK, col); break;
      }

      break;
    }

    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::VARBINARY:
      // could use below if zero length and NULL are treated the same
      // out->setVarBinaryField("", col); break;
      out->setVarBinaryField(joblist::CPNULLSTRMARK, col);
      break;

    default:
    {
    }
  }
}

void TupleUnion::formatMiniStats()
{
  ostringstream oss;
  oss << "TUS "
      << "UM "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- " << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
      << fRowsReturned << " ";
  fMiniInfo += oss.str();
}

}  // namespace joblist
