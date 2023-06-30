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
#include "tupleconstantbooleanstep.h"

namespace joblist
{
// class TupleConstantBooleanStep
TupleConstantBooleanStep::TupleConstantBooleanStep(const JobInfo& jobInfo, bool value)
 : JobStep(jobInfo), value_(value)
{
  fExtendedInfo = "TCBS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_TCBS;
}

TupleConstantBooleanStep::~TupleConstantBooleanStep()
{
}

const RowGroup& TupleConstantBooleanStep::getOutputRowGroup() const
{
  return rowGroupOut_;
}

void TupleConstantBooleanStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  throw runtime_error("Disabled, use initialize() to set output RowGroup.");
}

const RowGroup& TupleConstantBooleanStep::getDeliveredRowGroup() const
{
  return rowGroupOut_;
}

void TupleConstantBooleanStep::deliverStringTableRowGroup(bool b)
{
  rowGroupOut_.setUseStringTable(b);
}

bool TupleConstantBooleanStep::deliverStringTableRowGroup() const
{
  return rowGroupOut_.usesStringTable();
}

void TupleConstantBooleanStep::join()
{
}

void TupleConstantBooleanStep::initialize(const RowGroup& rgIn, const JobInfo&)
{
  rowGroupOut_ = rgIn;
  rowGroupOut_.initRow(&rowOut_);
  rowGroupOut_.initRow(&rowConst_, true);
}

void TupleConstantBooleanStep::run()
{
  if (fDelivery == false)
  {
    if (fOutputJobStepAssociation.outSize() == 0)
      throw logic_error("No output data list for non-delivery constant step.");

    outputDL_ = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

    if (outputDL_ == NULL)
      throw logic_error("Output is not a RowGroup data list.");

    if (traceOn())
    {
      dlTimes.setFirstReadTime();
      dlTimes.setLastReadTime();
      dlTimes.setEndOfInputTime();
      printCalTrace();
    }

    // Bug 3136, let mini stats to be formatted if traceOn.
    outputDL_->endOfInput();
  }
}

uint32_t TupleConstantBooleanStep::nextBand(messageqcpp::ByteStream& bs)
{
  // send an empty band
  RGData rgData(rowGroupOut_, 0);
  rowGroupOut_.setDataAndResetRowGroup(&rgData, 0);
  rowGroupOut_.setStatus(status());
  rowGroupOut_.serializeRGData(bs);

  if (traceOn())
  {
    dlTimes.setFirstReadTime();
    dlTimes.setLastReadTime();
    dlTimes.setEndOfInputTime();
    printCalTrace();
  }

  return 0;
}

const string TupleConstantBooleanStep::toString() const
{
  ostringstream oss;
  oss << "ConstantBooleanStep ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void TupleConstantBooleanStep::printCalTrace()
{
  time_t t = time(0);
  char timeString[50];
  ctime_r(&t, timeString);
  timeString[strlen(timeString) - 1] = '\0';
  ostringstream logStr;
  logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString << endl
         << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
         << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
         << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
         << "\tJob completion status " << status() << endl;
  logEnd(logStr.str().c_str());
  fExtendedInfo += logStr.str();
  formatMiniStats();
}

void TupleConstantBooleanStep::formatMiniStats()
{
  ostringstream oss;
  oss << "TCBS "
      << "UM "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- "
      << "- " << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " ";
  fMiniInfo += oss.str();
}

}  // namespace joblist
