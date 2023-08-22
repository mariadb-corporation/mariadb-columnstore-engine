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
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
using namespace execplan;

#include "jobstep.h"
#include "rowgroup.h"
using namespace rowgroup;

#include "querytele.h"
using namespace querytele;

#include "jlf_common.h"
#include "tupleconstantonlystep.h"

namespace joblist
{
SJSTEP addConstantStep(const JobInfo& jobInfo, const rowgroup::RowGroup* rg)
{
  TupleConstantOnlyStep* tcos = new TupleConstantOnlyStep(jobInfo);
  tcos->initialize(jobInfo, rg);
  SJSTEP spcs(tcos);
  return spcs;
}

// class TupleConstantOnlyStep
TupleConstantOnlyStep::TupleConstantOnlyStep(const JobInfo& jobInfo) : JobStep(jobInfo)
{
  fExtendedInfo = "TCOS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TCOS;
}

TupleConstantOnlyStep::~TupleConstantOnlyStep()
{
}

const RowGroup& TupleConstantOnlyStep::getOutputRowGroup() const
{
  return rowGroupOut_;
}

void TupleConstantOnlyStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}

const RowGroup& TupleConstantOnlyStep::getDeliveredRowGroup() const
{
  return rowGroupOut_;
}

void TupleConstantOnlyStep::deliverStringTableRowGroup(bool b)
{
  rowGroupOut_.setUseStringTable(b);
}

bool TupleConstantOnlyStep::deliverStringTableRowGroup() const
{
  return rowGroupOut_.usesStringTable();
}

void TupleConstantOnlyStep::join()
{
}

// void TupleConstantOnlyStep::initialize(const RowGroup& rgIn, const JobInfo& jobInfo)
void TupleConstantOnlyStep::initialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn)
{
  vector<uint32_t> oids;
  vector<uint32_t> keys;
  vector<uint32_t> scale;
  vector<uint32_t> precision;
  vector<CalpontSystemCatalog::ColDataType> types;
  vector<uint32_t> csNums;
  vector<uint32_t> pos;
  pos.push_back(2);

  deliverStringTableRowGroup(false);

  for (uint64_t i = 0; i < jobInfo.deliveredCols.size(); i++)
  {
    const ConstantColumn* cc = dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get());

    if (cc == NULL)
      throw runtime_error("none constant column found.");

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

  rowGroupOut_ = RowGroup(oids.size(), pos, oids, keys, types, csNums, scale, precision,
                          jobInfo.stringTableThreshold, false);
  rowGroupOut_.initRow(&rowOut_);
  rowGroupOut_.initRow(&rowConst_, true);

  constructContanstRow(jobInfo);
}

void TupleConstantOnlyStep::constructContanstRow(const JobInfo& jobInfo)
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

void TupleConstantOnlyStep::execute()
{
  RGData rgDataIn;
  RGData rgDataOut;
  bool more = false;
  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;

  try
  {
    more = inputDL_->next(inputIterator_, &rgDataIn);

    if (traceOn())
      dlTimes.setFirstReadTime();

    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    if (!more && cancelled())
    {
      endOfResult_ = true;
    }

    while (more && !endOfResult_)
    {
      rowGroupIn_.setData(&rgDataIn);
      rgDataOut.reinit(rowGroupOut_, rowGroupIn_.getRowCount());
      rowGroupOut_.setData(&rgDataOut);

      fillInConstants();

      more = inputDL_->next(inputIterator_, &rgDataIn);

      if (cancelled())
      {
        endOfResult_ = true;
      }
      else
      {
        outputDL_->insert(rgDataOut);
      }
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::tupleConstantStepErr, logging::ERR_ALWAYS_CRITICAL,
                    "TupleConstantOnlyStep::execute()");
  }

  while (more)
    more = inputDL_->next(inputIterator_, &rgDataIn);

  sts.msg_type = StepTeleStats::ST_SUMMARY;
  sts.total_units_of_work = sts.units_of_work_completed = 1;
  sts.rows = rowsReturned_;
  postStepSummaryTele(sts);

  // Bug 3136, let mini stats to be formatted if traceOn.
  setTimesAndPrintTrace(false);
  endOfResult_ = true;
  outputDL_->endOfInput();
}

void TupleConstantOnlyStep::run()
{
  if (fDelivery == false)
  {
    if (fOutputJobStepAssociation.outSize() == 0)
      throw logic_error("No output data list for non-delivery constant step.");

    outputDL_ = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

    if (outputDL_ == NULL)
      throw logic_error("Output is not a RowGroup data list.");

    try
    {
      RGData rgDataOut(rowGroupOut_, 1);
      rowGroupOut_.setData(&rgDataOut);

      if (traceOn())
        dlTimes.setFirstReadTime();

      fillInConstants();

      outputDL_->insert(rgDataOut);
    }
    catch (...)
    {
      handleException(std::current_exception(), logging::tupleConstantStepErr, logging::ERR_ALWAYS_CRITICAL,
                      "TupleConstantOnlyStep::run()");
    }

    if (traceOn())
    {
      dlTimes.setLastReadTime();
      dlTimes.setEndOfInputTime();
      printCalTrace();
    }

    // Bug 3136, let mini stats to be formatted if traceOn.
    endOfResult_ = true;
    outputDL_->endOfInput();
  }
}

uint32_t TupleConstantOnlyStep::nextBand(messageqcpp::ByteStream& bs)
{
  RGData rgDataOut;
  uint32_t rowCount = 0;

  if (!endOfResult_)
  {
    try
    {
      bs.restart();

      if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
        dlTimes.setFirstReadTime();

      rgDataOut.reinit(rowGroupOut_, 1);
      rowGroupOut_.setData(&rgDataOut);

      fillInConstants();
      rowGroupOut_.serializeRGData(bs);
      rowCount = rowGroupOut_.getRowCount();
    }
    catch (...)
    {
      handleException(std::current_exception(), logging::tupleConstantStepErr, logging::ERR_ALWAYS_CRITICAL,
                      "TupleConstantOnlyStep::nextBand()");
    }

    endOfResult_ = true;
  }
  else
  {
    // send an empty / error band
    RGData rgData(rowGroupOut_, 0);
    rowGroupOut_.setDataAndResetRowGroup(&rgData, 0);
    rowGroupOut_.setStatus(status());
    rowGroupOut_.serializeRGData(bs);

    setTimesAndPrintTrace(false);
  }

  return rowCount;
}

void TupleConstantOnlyStep::fillInConstants()
{
  rowGroupOut_.getRow(0, &rowOut_);
  idbassert(rowConst_.getColumnCount() == rowOut_.getColumnCount());
  rowOut_.usesStringTable(rowConst_.usesStringTable());
  copyRow(rowConst_, &rowOut_);
  rowGroupOut_.resetRowGroup(0);
  rowGroupOut_.setRowCount(1);
  rowsReturned_ = 1;
}

const string TupleConstantOnlyStep::toString() const
{
  ostringstream oss;
  oss << "ConstantOnlyStep ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleConstantOnlyStep::printCalTrace()
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

void TupleConstantOnlyStep::formatMiniStats()
{
  ostringstream oss;
  oss << "TCOS "
      << "UM "
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