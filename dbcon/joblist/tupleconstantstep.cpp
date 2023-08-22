/*
 Copyright (C) 2023 MariaDB Corporation
 This program is free software; you can redistribute it and/or modify it under the terms of the GNU General
 Public License as published by the Free Software Foundation; version 2 of the License. This program is
 distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 You should have received a copy of the GNU General Public License along with this program; if not, write to
 the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

// #define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>

#include <boost/uuid/uuid_io.hpp>
using namespace boost;

#include "messagequeue.h"
using namespace messageqcpp;

#include "loggingid.h"
#include "errorcodes.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "threadnaming.h"
using namespace utils;

#include "querytele.h"
using namespace querytele;

#include "funcexp.h"
#include "jobstep.h"
#include "jlf_common.h"
#include "tupleconstantstep.h"

namespace joblist
{
TupleConstantStep::TupleConstantStep(const JobInfo& jobInfo) : JobStep(jobInfo), jobList_(jobInfo.jobListPtr)
{
  fExtendedInfo = "TCS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TCS;
}

TupleConstantStep::~TupleConstantStep()
{
}

void TupleConstantStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}

void TupleConstantStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
{
  // Initialize structures used by separate workers
  constInitialize(jobInfo, &rgIn);
}

void TupleConstantStep::constInitialize(const JobInfo& jobInfo, const RowGroup* rgIn)
{
  vector<uint32_t> oids, oidsIn = rowGroupIn_.getOIDs();
  vector<uint32_t> keys, keysIn = rowGroupIn_.getKeys();
  vector<uint32_t> scale, scaleIn = rowGroupIn_.getScale();
  vector<uint32_t> precision, precisionIn = rowGroupIn_.getPrecision();
  vector<CalpontSystemCatalog::ColDataType> types, typesIn = rowGroupIn_.getColTypes();
  vector<uint32_t> csNums, csNumsIn = rowGroupIn_.getCharsetNumbers();
  vector<uint32_t> pos;
  pos.push_back(2);

  if (rgIn)
  {
    rowGroupIn_ = *rgIn;
    rowGroupIn_.initRow(&rowIn_);
    oidsIn = rowGroupIn_.getOIDs();
    keysIn = rowGroupIn_.getKeys();
    scaleIn = rowGroupIn_.getScale();
    precisionIn = rowGroupIn_.getPrecision();
    typesIn = rowGroupIn_.getColTypes();
    csNumsIn = rowGroupIn_.getCharsetNumbers();
  }

  for (uint64_t i = 0, j = 0; i < jobInfo.deliveredCols.size(); i++)
  {
    const ConstantColumn* cc = dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get());

    if (cc != NULL)
    {
      CalpontSystemCatalog::ColType ct = cc->resultType();

      if (ct.colDataType == CalpontSystemCatalog::VARCHAR)
        ct.colWidth++;

      // Round colWidth up
      if (ct.colWidth == 3)
        ct.colWidth = 4;
      else if (ct.colWidth == 5 || ct.colWidth == 6 || ct.colWidth == 7)
        ct.colWidth = 8;

      oids.push_back(-1);
      keys.push_back(-1);
      scale.push_back(ct.scale);
      precision.push_back(ct.precision);
      types.push_back(ct.colDataType);
      csNums.push_back(ct.charsetNumber);
      pos.push_back(pos.back() + ct.colWidth);

      indexConst_.push_back(i);
    }
    else
    {
      // select (select a) from region;
      if (j >= oidsIn.size() && jobInfo.tableList.empty())
      {
        throw IDBExcept(ERR_NO_FROM);
      }

      idbassert(j < oidsIn.size());

      oids.push_back(oidsIn[j]);
      keys.push_back(keysIn[j]);
      scale.push_back(scaleIn[j]);
      precision.push_back(precisionIn[j]);
      types.push_back(typesIn[j]);
      csNums.push_back(csNumsIn[j]);
      pos.push_back(pos.back() + rowGroupIn_.getColumnWidth(j));
      j++;

      indexMapping_.push_back(i);
    }
  }

  rowGroupOut_ =
      RowGroup(oids.size(), pos, oids, keys, types, csNums, scale, precision, jobInfo.stringTableThreshold);
  rowGroupOut_.initRow(&rowOut_);
  rowGroupOut_.initRow(&rowConst_, true);

  constructContanstRow(jobInfo);
}

void TupleConstantStep::constructContanstRow(const JobInfo& jobInfo)
{
  // construct a row with only the constant values
  constRowData_.reset(new uint8_t[rowConst_.getSize()]);
  rowConst_.setData(rowgroup::Row::Pointer(constRowData_.get()));
  rowConst_.initToNull();  // make sure every col is init'd to something, because later we copy the whole row
  const vector<CalpontSystemCatalog::ColDataType>& types = rowGroupOut_.getColTypes();

  for (vector<uint64_t>::iterator i = indexConst_.begin(); i != indexConst_.end(); i++)
  {
    const ConstantColumn* cc = dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[*i].get());
    const execplan::Result c = cc->result();

    if (cc->isNull())
    {
      if (types[*i] == CalpontSystemCatalog::CHAR || types[*i] == CalpontSystemCatalog::VARCHAR ||
          types[*i] == CalpontSystemCatalog::TEXT)
      {
        rowConst_.setStringField(nullptr, 0, *i);
      }
      else if (isUnsigned(types[*i]))
      {
        rowConst_.setUintField(rowConst_.getNullValue(*i), *i);
      }
      else
      {
        rowConst_.setIntField(rowConst_.getSignedNullValue(*i), *i);
      }

      continue;
    }

    switch (types[*i])
    {
      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::BIGINT:
      case CalpontSystemCatalog::DATE:
      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIME:
      case CalpontSystemCatalog::TIMESTAMP:
      {
        rowConst_.setIntField(c.intVal, *i);
        break;
      }

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
      {
        if (rowGroupOut_.getColWidths()[*i] > datatypes::MAXLEGACYWIDTH)
          rowConst_.setInt128Field(c.decimalVal.TSInt128::getValue(), *i);
        else
          rowConst_.setIntField(c.decimalVal.value, *i);
        break;
      }

      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
      {
        rowConst_.setFloatField(c.floatVal, *i);
        break;
      }

      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE:
      {
        rowConst_.setDoubleField(c.doubleVal, *i);
        break;
      }

      case CalpontSystemCatalog::LONGDOUBLE:
      {
        rowConst_.setLongDoubleField(c.longDoubleVal, *i);
        break;
      }

      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::TEXT:
      {
        rowConst_.setStringField(c.strVal, *i);
        break;
      }

      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT:
      {
        rowConst_.setUintField(c.uintVal, *i);
        break;
      }

      default:
      {
        throw runtime_error("un-supported constant column type.");
        break;
      }
    }  // switch
  }    // for constant columns
}

void TupleConstantStep::run()
{
  if (fInputJobStepAssociation.outSize() == 0)
    throw logic_error("No input data list for constant step.");

  inputDL_ = fInputJobStepAssociation.outAt(0)->rowGroupDL();

  if (inputDL_ == nullptr)
    throw logic_error("Input is not a RowGroup data list.");

  if (fOutputJobStepAssociation.outSize() == 0)
    throw logic_error("No output data list for annex step.");

  outputDL_ = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

  if (outputDL_ == nullptr)
    throw logic_error("Output is not a RowGroup data list.");

  if (fDelivery == true)
  {
    fOutputIterator = outputDL_->getIterator();
  }

  inputDL_ = fInputJobStepAssociation.outAt(0)->rowGroupDL();
  if (inputDL_ == nullptr)
    throw logic_error("Input is not a RowGroup data list.");

  fInputIterator = inputDL_->getIterator();
  runner_ = jobstepThreadPool.invoke([this]() { this->execute(); });
}

void TupleConstantStep::join()
{
  if (runner_)
  {
    jobstepThreadPool.join(runner_);
  }
}

uint32_t TupleConstantStep::nextBand(messageqcpp::ByteStream& bs)
{
  bool more = false;
  uint32_t rowCount = 0;

  try
  {
    bs.restart();

    more = outputDL_->next(fOutputIterator, &rgDataOut_);

    if (more && !cancelled())
    {
      rowGroupOut_.setData(&rgDataOut_);
      rowGroupOut_.serializeRGData(bs);
      rowCount = rowGroupOut_.getRowCount();
    }
    else
    {
      while (more)
        more = outputDL_->next(fOutputIterator, &rgDataOut_);

      endOfResult_ = true;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_DELIVERY, logging::ERR_ALWAYS_CRITICAL,
                    "TupleConstantStep::nextBand()");
    while (more)
      more = outputDL_->next(fOutputIterator, &rgDataOut_);

    endOfResult_ = true;
  }

  if (endOfResult_)
  {
    // send an empty / error band
    rgDataOut_.reinit(rowGroupOut_, 0);
    rowGroupOut_.setDataAndResetRowGroup(&rgDataOut_, 0);
    rowGroupOut_.setStatus(status());
    rowGroupOut_.serializeRGData(bs);
  }

  return rowCount;
}

void TupleConstantStep::execute()
{
  utils::setThreadName("TCS");
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;

  try
  {
    more = inputDL_->next(fInputIterator, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    StepTeleStats sts(fQueryUuid, fStepUuid, StepTeleStats::ST_START, 1);
    postStepStartTele(sts);

    while (more && !cancelled())
    {
      rowGroupIn_.setData(&rgDataIn);
      rowGroupIn_.getRow(0, &rowIn_);
      // Get a new output rowgroup for each input rowgroup to preserve the rids
      rgDataOut.reinit(rowGroupOut_, rowGroupIn_.getRowCount());
      rowGroupOut_.setDataAndResetRowGroup(&rgDataOut, rowGroupIn_.getBaseRid());
      rowGroupOut_.setDBRoot(rowGroupIn_.getDBRoot());
      rowGroupOut_.getRow(0, &rowOut_);

      uint64_t i = 0;
      for (; i < rowGroupIn_.getRowCount() && !cancelled(); ++i)
      {
        fillInConstants(rowIn_, rowOut_);
        rowGroupOut_.incRowCount();

        rowOut_.nextRow();
        rowIn_.nextRow();
      }

      if (rowGroupOut_.getRowCount() > 0)
      {
        outputDL_->insert(rgDataOut);
      }

      rowsReturned_ += i;

      more = inputDL_->next(fInputIterator, &rgDataIn);
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_PROCESS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleConstantStep::execute()");
  }

  while (more)
    more = inputDL_->next(fInputIterator, &rgDataIn);

  outputDL_->endOfInput();

  StepTeleStats sts(fQueryUuid, fStepUuid, StepTeleStats::ST_SUMMARY, 1, 1, rowsReturned_);
  postStepSummaryTele(sts);

  setTimesAndPrintTrace(dlTimes.FirstReadTime().tv_sec == 0);
}

void TupleConstantStep::fillInConstants(const rowgroup::Row& rowIn, rowgroup::Row& rowOut)
{
  if (indexConst_.size() > 1 || indexConst_[0] != 0)
  {
    copyRow(rowConst_, &rowOut);
    rowOut.setRid(rowIn.getRelRid());

    for (uint64_t j = 0; j < indexMapping_.size(); ++j)
      rowIn.copyField(rowOut, indexMapping_[j], j);
  }
  else  // only first column is constant
  {
    rowOut.setRid(rowIn.getRelRid());
    rowConst_.copyField(rowOut, 0, 0);

    for (uint32_t i = 1; i < rowOut.getColumnCount(); i++)
      rowIn.copyField(rowOut, i, i - 1);
  }
}

const RowGroup& TupleConstantStep::getOutputRowGroup() const
{
  return rowGroupOut_;
}

const RowGroup& TupleConstantStep::getDeliveredRowGroup() const
{
  return rowGroupOut_;
}

void TupleConstantStep::deliverStringTableRowGroup(bool b)
{
  rowGroupOut_.setUseStringTable(b);
}

bool TupleConstantStep::deliverStringTableRowGroup() const
{
  return rowGroupOut_.usesStringTable();
}

const string TupleConstantStep::toString() const
{
  ostringstream oss;
  oss << "ConstantStep ";
  oss << "  ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleConstantStep::printCalTrace()
{
  time_t t = time(0);
  char timeString[50];
  ctime_r(&t, timeString);
  timeString[strlen(timeString) - 1] = '\0';
  ostringstream logStr;
  logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString
         << "; total rows returned-" << rowsReturned_ << endl
         << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
         << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
         << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
         << "\tJob completion status " << status() << endl;
  logEnd(logStr.str().c_str());
  fExtendedInfo += logStr.str();
  formatMiniStats();
}

void TupleConstantStep::formatMiniStats()
{
  ostringstream oss;
  oss << "TCS ";
  oss << "UM "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- " << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
      << rowsReturned_ << " ";
  fMiniInfo += oss.str();
}

}  // namespace joblist
