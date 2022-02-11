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

//  $Id: crossenginestep.cpp 9709 2013-07-20 06:08:46Z xlou $

#define PREFER_MY_CONFIG_H
#include "crossenginestep.h"
#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
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

#include "rowgroup.h"
using namespace rowgroup;

#include "querytele.h"
using namespace querytele;

#include "funcexp.h"

#include "jobstep.h"
#include "jlf_common.h"

#include "libmysql_client.h"

namespace joblist
{
CrossEngineStep::CrossEngineStep(const std::string& schema, const std::string& table,
                                 const std::string& alias, const JobInfo& jobInfo)
 : BatchPrimitive(jobInfo)
 , fRowsRetrieved(0)
 , fRowsReturned(0)
 , fRowsPerGroup(256)
 , fOutputDL(NULL)
 , fOutputIterator(0)
 , fRunner(0)
 , fEndOfResult(false)
 , fSchema(schema)
 , fTable(table)
 , fAlias(alias)
 , fColumnCount(0)
 , fFeInstance(funcexp::FuncExp::instance())
{
  fExtendedInfo = "CES: ";
  getMysqldInfo(jobInfo);
  fQtc.stepParms().stepType = StepTeleStats::T_CES;
  mysql = new utils::LibMySQL();
}

CrossEngineStep::~CrossEngineStep()
{
  delete mysql;
}

void CrossEngineStep::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  fRowGroupOut = fRowGroupDelivered = fRowGroupAdded = rg;
}

void CrossEngineStep::deliverStringTableRowGroup(bool b)
{
  // results are either using setField, or mapping to delivered row group
  fRowGroupDelivered.setUseStringTable(b);
}

bool CrossEngineStep::deliverStringTableRowGroup() const
{
  return fRowGroupDelivered.usesStringTable();
}

void CrossEngineStep::addFcnJoinExp(const std::vector<execplan::SRCP>& fe)
{
  fFeFcnJoin = fe;
}

void CrossEngineStep::addFcnExpGroup1(const boost::shared_ptr<ParseTree>& fe)
{
  fFeFilters.push_back(fe);
}

void CrossEngineStep::setFE1Input(const rowgroup::RowGroup& rg)
{
  fRowGroupFe1 = rg;
}

void CrossEngineStep::setFcnExpGroup3(const std::vector<execplan::SRCP>& fe)
{
  fFeSelects = fe;
}

void CrossEngineStep::setFE23Output(const rowgroup::RowGroup& rg)
{
  fRowGroupFe3 = fRowGroupDelivered = fRowGroupAdded = rg;
}

void CrossEngineStep::makeMappings()
{
  fFe1Column.reset(new int[fColumnCount]);

  for (uint64_t i = 0; i < fColumnCount; ++i)
    fFe1Column[i] = -1;

  if (fFeFilters.size() > 0 || fFeFcnJoin.size() > 0)
  {
    const std::vector<uint32_t>& colInFe1 = fRowGroupFe1.getKeys();

    for (uint64_t i = 0; i < colInFe1.size(); i++)
    {
      map<uint32_t, uint32_t>::iterator it = fColumnMap.find(colInFe1[i]);

      if (it != fColumnMap.end())
        fFe1Column[it->second] = i;
    }

    fFeMapping1 = makeMapping(fRowGroupFe1, fRowGroupOut);
  }

  if (!fFeSelects.empty())
    fFeMapping3 = makeMapping(fRowGroupOut, fRowGroupFe3);
}

void CrossEngineStep::setField(int i, const char* value, unsigned long length, MYSQL_FIELD* field, Row& row)
{
  CalpontSystemCatalog::ColDataType colType = row.getColType(i);

  if ((colType == CalpontSystemCatalog::CHAR || colType == CalpontSystemCatalog::VARCHAR) &&
      row.getColumnWidth(i) > 8)
  {
    if (value != NULL)
      row.setStringField(value, i);
    else
      row.setStringField("", i);
  }
  else if ((colType == CalpontSystemCatalog::BLOB) || (colType == CalpontSystemCatalog::TEXT) ||
           (colType == CalpontSystemCatalog::VARBINARY))
  {
    if (value != NULL)
      row.setVarBinaryField((const uint8_t*)value, length, i);
    else
      row.setVarBinaryField(NULL, 0, i);
  }
  else
  {
    CalpontSystemCatalog::ColType ct;
    ct.colDataType = colType;
    ct.colWidth = row.getColumnWidth(i);

    if (colType == CalpontSystemCatalog::DECIMAL || colType == CalpontSystemCatalog::UDECIMAL)
    {
      ct.scale = field->decimals;
      ct.precision = field->length;

      if (ct.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        row.setInt128Field(convertValueNum<int128_t>(value, ct), i);
        return;
      }
    }
    else
    {
      ct.scale = row.getScale(i);
      ct.precision = row.getPrecision(i);
    }

    row.setIntField(convertValueNum<int64_t>(value, ct), i);
  }
}

inline void CrossEngineStep::addRow(RGData& data)
{
  fRowDelivered.setRid(fRowsReturned % fRowsPerGroup);
  fRowDelivered.nextRow();
  fRowGroupAdded.incRowCount();

  if (++fRowsReturned % fRowsPerGroup == 0)
  {
    fOutputDL->insert(data);
    data.reinit(fRowGroupAdded, fRowsPerGroup);
    fRowGroupAdded.setData(&data);
    fRowGroupAdded.resetRowGroup(fRowsReturned);
    fRowGroupAdded.getRow(0, &fRowDelivered);
  }
}

// simplified version of convertValueNum() in jlf_execplantojoblist.cpp.
template <typename T>
T CrossEngineStep::convertValueNum(const char* str, const CalpontSystemCatalog::ColType& ct)
{
  T rv = 0;
  bool pushWarning = false;
  bool nullFlag = (str == NULL);
  boost::any anyVal =
      ct.convertColumnData((nullFlag ? "" : str), pushWarning, fTimeZone, nullFlag, true, false);

  // Out of range values are treated as NULL as discussed during design review.
  if (pushWarning)
  {
    anyVal = ct.getNullValueForType();
  }

  switch (ct.colDataType)
  {
    case CalpontSystemCatalog::BIT: rv = boost::any_cast<bool>(anyVal); break;

    case CalpontSystemCatalog::TINYINT:
#if BOOST_VERSION >= 105200
      rv = boost::any_cast<char>(anyVal);
#else
      rv = boost::any_cast<int8_t>(anyVal);
#endif
      break;

    case CalpontSystemCatalog::UTINYINT: rv = boost::any_cast<uint8_t>(anyVal); break;

    case CalpontSystemCatalog::SMALLINT: rv = boost::any_cast<int16_t>(anyVal); break;

    case CalpontSystemCatalog::USMALLINT: rv = boost::any_cast<uint16_t>(anyVal); break;

    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT:
#ifdef _MSC_VER
      rv = boost::any_cast<int>(anyVal);
#else
      rv = boost::any_cast<int32_t>(anyVal);
#endif
      break;

    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
#ifdef _MSC_VER
      rv = boost::any_cast<unsigned int>(anyVal);
#else
      rv = boost::any_cast<uint32_t>(anyVal);
#endif
      break;

    case CalpontSystemCatalog::BIGINT: rv = boost::any_cast<long long>(anyVal); break;

    case CalpontSystemCatalog::UBIGINT: rv = boost::any_cast<uint64_t>(anyVal); break;

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      float f = boost::any_cast<float>(anyVal);

      // N.B. There is a bug in boost::any or in gcc where, if you store a nan,
      //     you will get back a nan, but not necessarily the same bits that you put in.
      //     This only seems to be for float (double seems to work).
      if (isnan(f))
      {
        uint32_t ti = joblist::FLOATNULL;
        float* tfp = (float*)&ti;
        f = *tfp;
      }

      float* fp = &f;
      int32_t* ip = reinterpret_cast<int32_t*>(fp);
      rv = *ip;
    }
    break;

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      double d = boost::any_cast<double>(anyVal);
      double* dp = &d;
      int64_t* ip = reinterpret_cast<int64_t*>(dp);
      rv = *ip;
    }
    break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::CLOB:
    {
      std::string i = boost::any_cast<std::string>(anyVal);
      // bug 1932, pad nulls up to the size of v
      i.resize(sizeof(rv), 0);
      rv = *((uint64_t*)i.data());
    }
    break;

    case CalpontSystemCatalog::DATE: rv = boost::any_cast<uint32_t>(anyVal); break;

    case CalpontSystemCatalog::DATETIME: rv = boost::any_cast<uint64_t>(anyVal); break;

    case CalpontSystemCatalog::TIMESTAMP: rv = boost::any_cast<uint64_t>(anyVal); break;

    case CalpontSystemCatalog::TIME: rv = boost::any_cast<int64_t>(anyVal); break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (LIKELY(ct.colWidth == datatypes::MAXDECIMALWIDTH))
        rv = boost::any_cast<int128_t>(anyVal);
      else if (ct.colWidth == execplan::CalpontSystemCatalog::EIGHT_BYTE)
        rv = boost::any_cast<long long>(anyVal);
      else if (ct.colWidth == execplan::CalpontSystemCatalog::FOUR_BYTE)
#ifdef _MSC_VER
        rv = boost::any_cast<int>(anyVal);

#else
        rv = boost::any_cast<int32_t>(anyVal);
#endif
      else if (ct.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
        rv = boost::any_cast<int16_t>(anyVal);
      else if (ct.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
        rv = boost::any_cast<char>(anyVal);
      break;

    default: break;
  }

  return rv;
}

void CrossEngineStep::getMysqldInfo(const JobInfo& jobInfo)
{
  if (jobInfo.rm->getMysqldInfo(fHost, fUser, fPasswd, fPort) == false)
    throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_CROSS_ENGINE_CONFIG), ERR_CROSS_ENGINE_CONFIG);
}

void CrossEngineStep::run()
{
  //	idbassert(!fDelivery);

  if (fOutputJobStepAssociation.outSize() == 0)
    throw logic_error("No output data list for cross engine step.");

  fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();

  if (fOutputDL == NULL)
    throw logic_error("Output is not a RowGroup data list.");

  if (fDelivery == true)
  {
    fOutputIterator = fOutputDL->getIterator();
  }

  fRunner = jobstepThreadPool.invoke(Runner(this));
}

void CrossEngineStep::join()
{
  if (fRunner)
    jobstepThreadPool.join(fRunner);
}

void CrossEngineStep::execute()
{
  int ret = 0;
  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;

  try
  {
    sts.msg_type = StepTeleStats::ST_START;
    sts.total_units_of_work = 1;
    postStepStartTele(sts);

    ret = mysql->init(fHost.c_str(), fPort, fUser.c_str(), fPasswd.c_str(), fSchema.c_str());

    if (ret != 0)
      mysql->handleMySqlError(mysql->getError().c_str(), ret);

    std::string query(makeQuery());
    fLogger->logMessage(logging::LOG_TYPE_INFO, "QUERY to foreign engine: " + query);

    if (traceOn())
      cout << "QUERY: " << query << endl;

    ret = mysql->run(query.c_str());

    if (ret != 0)
      mysql->handleMySqlError(mysql->getError().c_str(), ret);

    int num_fields = mysql->getFieldCount();

    char** rowIn;  // input
    // shared_array<uint8_t> rgDataDelivered;      // output
    RGData rgDataDelivered;
    fRowGroupAdded.initRow(&fRowDelivered);
    // use getDataSize() i/o getMaxDataSize() to make sure there are 8192 rows.
    rgDataDelivered.reinit(fRowGroupAdded, fRowsPerGroup);
    fRowGroupAdded.setData(&rgDataDelivered);
    fRowGroupAdded.resetRowGroup(0);
    fRowGroupAdded.getRow(0, &fRowDelivered);

    if (traceOn())
      dlTimes.setFirstReadTime();

    // Any functions to evaluate
    makeMappings();
    bool doFE1 = ((fFeFcnJoin.size() > 0) || (fFeFilters.size() > 0));
    bool doFE3 = (fFeSelects.size() > 0);

    if (!doFE1 && !doFE3)
    {
      while ((rowIn = mysql->nextRow()) && !cancelled())
      {
        for (int i = 0; i < num_fields; i++)
          setField(i, rowIn[i], mysql->getFieldLength(i), mysql->getField(i), fRowDelivered);

        addRow(rgDataDelivered);
      }
    }

    else if (doFE1 && !doFE3)  // FE in WHERE clause only
    {
      shared_array<uint8_t> rgDataFe1;  // functions in where clause
      Row rowFe1;                       // row for fe evaluation
      fRowGroupFe1.initRow(&rowFe1, true);
      rgDataFe1.reset(new uint8_t[rowFe1.getSize()]);
      rowFe1.setData(rgDataFe1.get());

      while ((rowIn = mysql->nextRow()) && !cancelled())
      {
        // Parse the columns used in FE1 first, the other column may not need be parsed.
        for (int i = 0; i < num_fields; i++)
        {
          if (fFe1Column[i] != -1)
            setField(fFe1Column[i], rowIn[i], mysql->getFieldLength(i), mysql->getField(i), rowFe1);
        }

        if (fFeFilters.size() > 0)
        {
          bool feBreak = false;

          for (std::vector<boost::shared_ptr<execplan::ParseTree> >::iterator it = fFeFilters.begin();
               it != fFeFilters.end(); it++)
          {
            if (fFeInstance->evaluate(rowFe1, (*it).get()) == false)
            {
              feBreak = true;
              break;
            }
          }

          if (feBreak)
            continue;
        }

        // evaluate the FE join column
        fFeInstance->evaluate(rowFe1, fFeFcnJoin);

        // Pass throug the parsed columns, and parse the remaining columns.
        applyMapping(fFeMapping1, rowFe1, &fRowDelivered);

        for (int i = 0; i < num_fields; i++)
        {
          if (fFe1Column[i] == -1)
            setField(i, rowIn[i], mysql->getFieldLength(i), mysql->getField(i), fRowDelivered);
        }

        addRow(rgDataDelivered);
      }
    }

    else if (!doFE1 && doFE3)  // FE in SELECT clause only
    {
      shared_array<uint8_t> rgDataFe3;  // functions in select clause
      Row rowFe3;                       // row for fe evaluation
      fRowGroupOut.initRow(&rowFe3, true);
      rgDataFe3.reset(new uint8_t[rowFe3.getSize()]);
      rowFe3.setData(rgDataFe3.get());

      while ((rowIn = mysql->nextRow()) && !cancelled())
      {
        for (int i = 0; i < num_fields; i++)
          setField(i, rowIn[i], mysql->getFieldLength(i), mysql->getField(i), rowFe3);

        fFeInstance->evaluate(rowFe3, fFeSelects);

        applyMapping(fFeMapping3, rowFe3, &fRowDelivered);

        addRow(rgDataDelivered);
      }
    }

    else  // FE in SELECT clause, FE join and WHERE clause
    {
      shared_array<uint8_t> rgDataFe1;  // functions in where clause
      Row rowFe1;                       // row for fe1 evaluation
      fRowGroupFe1.initRow(&rowFe1, true);
      rgDataFe1.reset(new uint8_t[rowFe1.getSize()]);
      rowFe1.setData(rgDataFe1.get());

      shared_array<uint8_t> rgDataFe3;  // functions in select clause
      Row rowFe3;                       // row for fe3 evaluation
      fRowGroupOut.initRow(&rowFe3, true);
      rgDataFe3.reset(new uint8_t[rowFe3.getSize()]);
      rowFe3.setData(rgDataFe3.get());

      while ((rowIn = mysql->nextRow()) && !cancelled())
      {
        // Parse the columns used in FE1 first, the other column may not need be parsed.
        for (int i = 0; i < num_fields; i++)
        {
          if (fFe1Column[i] != -1)
            setField(fFe1Column[i], rowIn[i], mysql->getFieldLength(i), mysql->getField(i), rowFe1);
        }

        if (fFeFilters.size() > 0)
        {
          bool feBreak = false;

          for (std::vector<boost::shared_ptr<execplan::ParseTree> >::iterator it = fFeFilters.begin();
               it != fFeFilters.end(); it++)
          {
            if (fFeInstance->evaluate(rowFe1, (*it).get()) == false)
            {
              feBreak = true;
              break;
            }
          }

          if (feBreak)
            continue;
        }

        // evaluate the FE join column
        fFeInstance->evaluate(rowFe1, fFeFcnJoin);

        // Pass throug the parsed columns, and parse the remaining columns.
        applyMapping(fFeMapping1, rowFe1, &rowFe3);

        for (int i = 0; i < num_fields; i++)
        {
          if (fFe1Column[i] == -1)
            setField(i, rowIn[i], mysql->getFieldLength(i), mysql->getField(i), rowFe3);
        }

        fFeInstance->evaluate(rowFe3, fFeSelects);
        applyMapping(fFeMapping3, rowFe3, &fRowDelivered);

        addRow(rgDataDelivered);
      }
    }

    // INSERT_ADAPTER(fOutputDL, rgDataDelivered);
    fOutputDL->insert(rgDataDelivered);
    fRowsRetrieved = mysql->getRowCount();
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_CROSS_ENGINE_CONNECT, logging::ERR_ALWAYS_CRITICAL,
                    "CrossEngineStep::execute()");
  }

  sts.msg_type = StepTeleStats::ST_SUMMARY;
  sts.total_units_of_work = sts.units_of_work_completed = 1;
  sts.rows = fRowsReturned;
  postStepSummaryTele(sts);

  fEndOfResult = true;
  fOutputDL->endOfInput();

  // Bug 3136, let mini stats to be formatted if traceOn.
  if (traceOn())
  {
    dlTimes.setLastReadTime();
    dlTimes.setEndOfInputTime();
    printCalTrace();
  }
}

void CrossEngineStep::setBPP(JobStep* jobStep)
{
  pColStep* pcs = dynamic_cast<pColStep*>(jobStep);
  pColScanStep* pcss = NULL;
  pDictionaryStep* pds = NULL;
  pDictionaryScan* pdss = NULL;
  FilterStep* fs = NULL;
  std::string bop = " AND ";

  if (pcs != 0)
  {
    if (dynamic_cast<PseudoColStep*>(pcs) != NULL)
      throw logic_error("No Psedo Column for foreign engine.");

    if (pcs->BOP() == BOP_OR)
      bop = " OR ";

    addFilterStr(pcs->getFilters(), bop);
  }
  else if ((pcss = dynamic_cast<pColScanStep*>(jobStep)) != NULL)
  {
    if (pcss->BOP() == BOP_OR)
      bop = " OR ";

    addFilterStr(pcss->getFilters(), bop);
  }
  else if ((pds = dynamic_cast<pDictionaryStep*>(jobStep)) != NULL)
  {
    if (pds->BOP() == BOP_OR)
      bop = " OR ";

    addFilterStr(pds->getFilters(), bop);
  }
  else if ((pdss = dynamic_cast<pDictionaryScan*>(jobStep)) != NULL)
  {
    if (pds->BOP() == BOP_OR)
      bop = " OR ";

    addFilterStr(pdss->getFilters(), bop);
  }
  else if ((fs = dynamic_cast<FilterStep*>(jobStep)) != NULL)
  {
    addFilterStr(fs->getFilters(), bop);
  }
}

void CrossEngineStep::addFilterStr(const std::vector<const Filter*>& f, const std::string& bop)
{
  if (f.size() == 0)
    return;

  std::string filterStr;

  for (uint64_t i = 0; i < f.size(); i++)
  {
    if (f[i]->data().empty())
      continue;

    if (!filterStr.empty())
      filterStr += bop;

    filterStr += f[i]->data();
  }

  if (!filterStr.empty())
  {
    if (!fWhereClause.empty())
      fWhereClause += " AND (" + filterStr + ")";
    else
      fWhereClause += " WHERE (" + filterStr + ")";
  }
}

void CrossEngineStep::setProjectBPP(JobStep* jobStep1, JobStep*)
{
  fColumnMap[jobStep1->tupleId()] = fColumnCount++;

  if (!fSelectClause.empty())
    fSelectClause += ", ";
  else
    fSelectClause += "SELECT ";

  fSelectClause += "`" + jobStep1->name() + "`";
}

std::string CrossEngineStep::makeQuery()
{
  ostringstream oss;
  oss << fSelectClause << " FROM `" << fTable << "`";

  if (fTable.compare(fAlias) != 0)
    oss << " `" << fAlias << "`";

  if (!fWhereClause.empty())
    oss << fWhereClause;

  // the std::string must consist of a single SQL statement without a terminating semicolon ; or \g.
  // oss << ";";
  return oss.str();
}

const RowGroup& CrossEngineStep::getOutputRowGroup() const
{
  return fRowGroupOut;
}

const RowGroup& CrossEngineStep::getDeliveredRowGroup() const
{
  return fRowGroupDelivered;
}

uint32_t CrossEngineStep::nextBand(messageqcpp::ByteStream& bs)
{
  // shared_array<uint8_t> rgDataOut;
  RGData rgDataOut;
  bool more = false;
  uint32_t rowCount = 0;

  try
  {
    bs.restart();

    more = fOutputDL->next(fOutputIterator, &rgDataOut);

    if (traceOn() && dlTimes.FirstReadTime().tv_sec == 0)
      dlTimes.setFirstReadTime();

    if (more && !cancelled())
    {
      fRowGroupDelivered.setData(&rgDataOut);
      fRowGroupDelivered.serializeRGData(bs);
      rowCount = fRowGroupDelivered.getRowCount();
    }
    else
    {
      while (more)
        more = fOutputDL->next(fOutputIterator, &rgDataOut);

      fEndOfResult = true;
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_IN_DELIVERY, logging::ERR_ALWAYS_CRITICAL,
                    "CrossEngineStep::nextBand()");
    while (more)
      more = fOutputDL->next(fOutputIterator, &rgDataOut);

    fEndOfResult = true;
  }

  if (fEndOfResult)
  {
    // send an empty / error band
    rgDataOut.reinit(fRowGroupDelivered, 0);
    fRowGroupDelivered.setData(&rgDataOut);
    fRowGroupDelivered.resetRowGroup(0);
    fRowGroupDelivered.setStatus(status());
    fRowGroupDelivered.serializeRGData(bs);

    if (traceOn())
    {
      dlTimes.setLastReadTime();
      dlTimes.setEndOfInputTime();
    }

    if (traceOn())
      printCalTrace();
  }

  return rowCount;
}

const std::string CrossEngineStep::toString() const
{
  ostringstream oss;
  oss << "CrossEngineStep ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
    oss << fInputJobStepAssociation.outAt(i);

  oss << " out:";

  for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
    oss << fOutputJobStepAssociation.outAt(i);

  oss << endl;

  return oss.str();
}

void CrossEngineStep::printCalTrace()
{
  time_t t = time(0);
  char timeString[50];
  ctime_r(&t, timeString);
  timeString[strlen(timeString) - 1] = '\0';
  ostringstream logStr;
  logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << timeString << "; rows retrieved-"
         << fRowsRetrieved << "; total rows returned-" << fRowsReturned << endl
         << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
         << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
         << "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
         << "\tJob completion status " << status() << endl;
  logEnd(logStr.str().c_str());
  fExtendedInfo += logStr.str();
  formatMiniStats();
}

void CrossEngineStep::formatMiniStats()
{
  ostringstream oss;
  oss << "CES "
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
// vim:ts=4 sw=4:
