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

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#ifndef _MSC_VER
#include <unistd.h>
#endif
#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#include <stdio.h>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <time.h>
#include <cassert>
#include <vector>
#include <map>
#include <limits>
#if defined(__linux__)
#include <wait.h>  //wait()
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/stat.h>  // For stat().
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>

#include "mcs_basic_types.h"
#include "idb_mysql.h"

#define NEED_CALPONT_INTERFACE
#include "ha_mcs_impl.h"

#include "ha_mcs_impl_if.h"
using namespace cal_impl_if;

#include "calpontselectexecutionplan.h"
#include "logicoperator.h"
#include "parsetree.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "sm.h"
#include "ha_mcs_pushdown.h"
#include "ha_mcs_sysvars.h"

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "dmlpackage.h"
#include "calpontdmlpackage.h"
#include "insertdmlpackage.h"
#include "vendordmlstatement.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "configcpp.h"
using namespace config;

#include "rowgroup.h"
using namespace rowgroup;

#include "brmtypes.h"
using namespace BRM;

#include "querystats.h"
using namespace querystats;

#include "calpontselectexecutionplan.h"
#include "mcsanalyzetableexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn_int.h"
#include "simplecolumn_decimal.h"
#include "aggregatecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "rowcolumn.h"
#include "selectfilter.h"
using namespace execplan;

#include "joblisttypes.h"
using namespace joblist;

#include "cacheutils.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "resourcemanager.h"

#include "funcexp.h"
#include "functor.h"
using namespace funcexp;

#include "installdir.h"
#include "columnstoreversion.h"
#include "ha_mcs_sysvars.h"

#include "ha_mcs_datatype.h"
#include "statistics.h"
#include "ha_mcs_logging.h"

namespace cal_impl_if
{
extern bool nonConstFunc(Item_func* ifp);
}

namespace
{
// Calpont vtable non-support error message
const string infinidb_autoswitch_warning =
    "The query includes syntax that is not supported by MariaDB Columnstore distributed mode. The execution "
    "was switched to standard mode with downgraded performance.";

// copied from item_timefunc.cc
static const string interval_names[] = {"year",
                                        "quarter",
                                        "month",
                                        "week",
                                        "day",
                                        "hour",
                                        "minute",
                                        "second",
                                        "microsecond",
                                        "year_month",
                                        "day_hour",
                                        "day_minute",
                                        "day_second",
                                        "hour_minute",
                                        "hour_second",
                                        "minute_second",
                                        "day_microsecond",
                                        "hour_microsecond",
                                        "minute_microsecond",
                                        "second_microsecond"};

// HDFS is never used nowadays, so don't bother
bool useHdfs = false;  // ResourceManager::instance()->useHdfs();

// convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
  return CalpontSystemCatalog::idb_tid2sid(tid);
}

/**
  @brief
  Forcely close a FEP connection.

  @details
  Plugin code opens network connection with ExMgr to
  get:
    the result of meta-data queries
    the result of DML or DQL query in any mode
    statistics
  This code allows to explicitly close the connection
  if any error happens using a non-existing protocol
  code 0. This causes ExeMgr loop to drop the
  connection.

  Called from many places in ha_mcs_impl.cpp().
*/
void force_close_fep_conn(THD* thd, cal_connection_info* ci, bool check_prev_rc = false)
{
  if (!ci->cal_conn_hndl)
  {
    return;
  }

  if (check_prev_rc && !ci->rc)
  {
    return;
  }

  // send ExeMgr an unknown signal to force him to close
  // the connection
  ByteStream msg;
  ByteStream::quadbyte qb = 0;
  msg << qb;

  try
  {
    ci->cal_conn_hndl->exeMgr->write(msg);
  }
  catch (...)
  {
    // Add details into the message.
    ha_mcs_impl::log_this(thd, "Exception in force_close_fep_conn().", logging::LOG_TYPE_DEBUG,
                          tid2sid(thd->thread_id));
  }

  sm::sm_cleanup(ci->cal_conn_hndl);
  ci->cal_conn_hndl = 0;
}

//
// @bug 2244. Log exception related to lost connection to ExeMgr.
// Log exception error from calls to sm::tpl_scan_fetch in fetchNextRow()
//
void tpl_scan_fetch_LogException(cal_table_info& ti, cal_connection_info* ci, std::exception* ex)
{
  time_t t = time(0);
  char datestr[50];
  ctime_r(&t, datestr);
  datestr[strlen(datestr) - 1] = '\0';  // strip off trailing newline

  uint32_t sesID = 0;
  string connHndl("No connection handle to use");

  if (ti.conn_hndl)
  {
    connHndl = "ti connection used";
    sesID = ti.conn_hndl->sessionID;
  }

  else if (ci->cal_conn_hndl)
  {
    connHndl = "ci connection used";
    sesID = ci->cal_conn_hndl->sessionID;
  }

  int64_t rowsRet = -1;

  if (ti.tpl_scan_ctx)
    rowsRet = ti.tpl_scan_ctx->rowsreturned;

  if (ex)
    cerr << datestr << ": sm::tpl_scan_fetch error getting rows for sessionID: " << sesID << "; " << connHndl
         << "; rowsReturned: " << rowsRet << "; reason-" << ex->what() << endl;
  else
    cerr << datestr << ": sm::tpl_scan_fetch unknown error getting rows for sessionID: " << sesID << "; "
         << connHndl << "; rowsReturned: " << rowsRet << endl;
}

// Table Map is used by both cond_push and table mode processing
// Entries made by cond_push don't have csep though.
// When
bool onlyOneTableinTM(cal_impl_if::cal_connection_info* ci)
{
  size_t counter = 0;
  for (auto& tableMapEntry : ci->tableMap)
  {
    if (tableMapEntry.second.csep)
      counter++;
    if (counter >= 1)
      return false;
  }

  return true;
}

int fetchNextRow(uchar* buf, cal_table_info& ti, cal_connection_info* ci, long timeZone,
                 bool handler_flag = false)
{
  int rc = HA_ERR_END_OF_FILE;
  int num_attr = ti.msTablePtr->s->fields;
  sm::status_t sm_stat;

  try
  {
    if (ti.conn_hndl)
    {
      sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, ti.conn_hndl);
    }
    else if (ci->cal_conn_hndl)
    {
      sm_stat = sm::tpl_scan_fetch(ti.tpl_scan_ctx, ci->cal_conn_hndl, (int*)(&current_thd->killed));
    }
    else
      throw runtime_error("internal error");
  }
  catch (std::exception& ex)
  {
    // @bug 2244. Always log this msg for now, as we try to track down when/why we are
    //			losing socket connection with ExeMgr
    //#ifdef INFINIDB_DEBUG
    tpl_scan_fetch_LogException(ti, ci, &ex);
    //#endif
    sm_stat = sm::CALPONT_INTERNAL_ERROR;
  }
  catch (...)
  {
    // @bug 2244. Always log this msg for now, as we try to track down when/why we are
    //			losing socket connection with ExeMgr
    //#ifdef INFINIDB_DEBUG
    tpl_scan_fetch_LogException(ti, ci, 0);
    //#endif
    sm_stat = sm::CALPONT_INTERNAL_ERROR;
  }

  if (sm_stat == sm::STATUS_OK)
  {
    Field** f;
    f = ti.msTablePtr->field;

    // set all fields to null in null col bitmap
    if (!handler_flag)
      memset(buf, -1, ti.msTablePtr->s->null_bytes);
    else
    {
      memset(ti.msTablePtr->null_flags, -1, ti.msTablePtr->s->null_bytes);
    }

    std::vector<CalpontSystemCatalog::ColType>& colTypes = ti.tpl_scan_ctx->ctp;

    RowGroup* rowGroup = ti.tpl_scan_ctx->rowGroup;

    // table mode mysql expects all columns of the table. mapping between columnoid and position in rowgroup
    // set coltype.position to be the position in rowgroup. only set once.
    if (ti.tpl_scan_ctx->rowsreturned == 0 &&
        (ti.tpl_scan_ctx->traceFlags & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF))
    {
      for (uint32_t i = 0; i < rowGroup->getColumnCount(); i++)
      {
        int oid = rowGroup->getOIDs()[i];
        int j = 0;

        for (; j < num_attr; j++)
        {
          // mysql should haved eliminated duplicate projection columns
          if (oid == colTypes[j].columnOID || oid == colTypes[j].ddn.dictOID)
          {
            colTypes[j].colPosition = i;
            break;
          }
        }
      }
    }

    rowgroup::Row row;
    rowGroup->initRow(&row);
    rowGroup->getRow(ti.tpl_scan_ctx->rowsreturned, &row);
    int s;

    for (int p = 0; p < num_attr; p++, f++)
    {
      // This col is going to be written
      bitmap_set_bit(ti.msTablePtr->write_set, (*f)->field_index);

      // get coltype if not there yet
      if (colTypes[0].colWidth == 0)
      {
        for (short c = 0; c < num_attr; c++)
        {
          colTypes[c].colPosition = c;
          colTypes[c].colWidth = rowGroup->getColumnWidth(c);
          colTypes[c].colDataType = rowGroup->getColTypes()[c];
          colTypes[c].columnOID = rowGroup->getOIDs()[c];
          colTypes[c].scale = rowGroup->getScale()[c];
          colTypes[c].precision = rowGroup->getPrecision()[c];
        }
      }

      CalpontSystemCatalog::ColType colType(colTypes[p]);

      // table mode handling
      if (ti.tpl_scan_ctx->traceFlags & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF)
      {
        if (colType.colPosition == -1)  // not projected by tuplejoblist
          continue;
        else
          s = colType.colPosition;
      }
      else
      {
        s = p;
      }

      // precision == -16 is borrowed as skip null check indicator for bit ops.
      if (row.isNullValue(s) && colType.precision != -16)
      {
        // @2835. Handle empty string and null confusion. store empty string for string column
        if (colType.colDataType == CalpontSystemCatalog::CHAR ||
            colType.colDataType == CalpontSystemCatalog::VARCHAR ||
            colType.colDataType == CalpontSystemCatalog::VARBINARY)
        {
          (*f)->store("", 0, (*f)->charset());
        }

        continue;
      }

      const datatypes::TypeHandler* h = colType.typeHandler();
      if (!h)
      {
        idbassert(0);
        (*f)->reset();
        (*f)->set_null();
      }
      else
      {
        // fetch and store data
        (*f)->set_notnull();
        datatypes::StoreFieldMariaDB mf(*f, colType, timeZone);
        h->storeValueToField(row, s, &mf);
      }
    }

    ti.tpl_scan_ctx->rowsreturned++;
    ti.c++;
#ifdef INFINIDB_DEBUG

    if ((ti.c % 1000000) == 0)
      cerr << "fetchNextRow so far table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c << endl;

#endif
    ti.moreRows = true;
    rc = 0;
  }
  else if (sm_stat == sm::SQL_NOT_FOUND)
  {
    IDEBUG(cerr << "fetchNextRow done for table " << ti.msTablePtr->s->table_name.str << " rows = " << ti.c
                << endl);
    ti.c = 0;
    ti.moreRows = false;
    rc = HA_ERR_END_OF_FILE;
  }
  else if (sm_stat == sm::CALPONT_INTERNAL_ERROR)
  {
    ti.moreRows = false;
    rc = ER_INTERNAL_ERROR;
    ci->rc = rc;
  }
  else if ((uint32_t)sm_stat == logging::ERR_LOST_CONN_EXEMGR)
  {
    ti.moreRows = false;
    rc = logging::ERR_LOST_CONN_EXEMGR;
    sm::sm_init(tid2sid(current_thd->thread_id), &ci->cal_conn_hndl, get_local_query(current_thd));
    idbassert(ci->cal_conn_hndl != 0);
    ci->rc = rc;
  }
  else if (sm_stat == sm::SQL_KILLED)
  {
    // query was aborted by the user. treat it the same as limit query. close
    // connection after rnd_close.
    ti.c = 0;
    ti.moreRows = false;
    rc = HA_ERR_END_OF_FILE;
    ci->rc = rc;
  }
  else
  {
    ti.moreRows = false;
    rc = sm_stat;
    ci->rc = rc;
  }

  return rc;
}

void makeUpdateScalarJoin(const ParseTree* n, void* obj)
{
  TreeNode* tn = n->data();
  SimpleFilter* sf = dynamic_cast<SimpleFilter*>(tn);

  if (!sf)
    return;

  SimpleColumn* scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
  SimpleColumn* scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
  CalpontSystemCatalog::TableAliasName* updatedTables =
      reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);

  if (scLeft && scRight)
  {
    if ((strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0) &&
        (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0) &&
        (strcasecmp(scLeft->tableAlias().c_str(), updatedTables->alias.c_str()) == 0))
    {
      uint64_t lJoinInfo = sf->lhs()->joinInfo();
      lJoinInfo |= JOIN_SCALAR;
      // lJoinInfo |= JOIN_OUTER_SELECT;
      // lJoinInfo |= JOIN_CORRELATED;
      sf->lhs()->joinInfo(lJoinInfo);
    }
    else if ((strcasecmp(scRight->tableName().c_str(), updatedTables->table.c_str()) == 0) &&
             (strcasecmp(scRight->schemaName().c_str(), updatedTables->schema.c_str()) == 0) &&
             (strcasecmp(scRight->tableAlias().c_str(), updatedTables->alias.c_str()) == 0))
    {
      uint64_t rJoinInfo = sf->rhs()->joinInfo();
      rJoinInfo |= JOIN_SCALAR;
      // rJoinInfo |= JOIN_OUTER_SELECT;
      // rJoinInfo |= JOIN_CORRELATED;
      sf->rhs()->joinInfo(rJoinInfo);
    }
    else
      return;
  }
  else
    return;
}

void makeUpdateSemiJoin(const ParseTree* n, void* obj)
{
  TreeNode* tn = n->data();
  SimpleFilter* sf = dynamic_cast<SimpleFilter*>(tn);

  if (!sf)
    return;

  SimpleColumn* scLeft = dynamic_cast<SimpleColumn*>(sf->lhs());
  SimpleColumn* scRight = dynamic_cast<SimpleColumn*>(sf->rhs());
  CalpontSystemCatalog::TableAliasName* updatedTables =
      reinterpret_cast<CalpontSystemCatalog::TableAliasName*>(obj);

  //@Bug 3279. Added a check for column filters.
  if (scLeft && scRight && (strcasecmp(scRight->tableAlias().c_str(), scLeft->tableAlias().c_str()) != 0))
  {
    if ((strcasecmp(scLeft->tableName().c_str(), updatedTables->table.c_str()) == 0) &&
        (strcasecmp(scLeft->schemaName().c_str(), updatedTables->schema.c_str()) == 0) &&
        (strcasecmp(scLeft->tableAlias().c_str(), updatedTables->alias.c_str()) == 0))
    {
      uint64_t lJoinInfo = sf->lhs()->joinInfo();
      lJoinInfo |= JOIN_SEMI;
      // lJoinInfo |= JOIN_OUTER_SELECT;
      // lJoinInfo |= JOIN_CORRELATED;
      sf->lhs()->joinInfo(lJoinInfo);
    }
    else if ((strcasecmp(scRight->tableName().c_str(), updatedTables->table.c_str()) == 0) &&
             (strcasecmp(scRight->schemaName().c_str(), updatedTables->schema.c_str()) == 0) &&
             (strcasecmp(scRight->tableAlias().c_str(), updatedTables->alias.c_str()) == 0))
    {
      uint64_t rJoinInfo = sf->rhs()->joinInfo();
      rJoinInfo |= JOIN_SEMI;
      // rJoinInfo |= JOIN_OUTER_SELECT;
      // rJoinInfo |= JOIN_CORRELATED;
      sf->rhs()->joinInfo(rJoinInfo);
    }
    else
      return;
  }
  else
    return;
}

vector<string> getOnUpdateTimestampColumns(string& schema, string& tableName, int sessionID)
{
  vector<string> returnVal;
  typedef CalpontSelectExecutionPlan::ColumnMap::value_type CMVT_;
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(execplan::CalpontSystemCatalog::FE);
  CalpontSystemCatalog::TableName aTableName;

  // select columnname from calpontsys.syscolumn
  // where schema = schema and tablename = tableName
  // and datatype = 'timestamp'
  // and defaultvalue = 'current_timestamp() ON UPDATE current_timestamp()'
  CalpontSelectExecutionPlan csep;
  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
  CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
  CalpontSelectExecutionPlan::ColumnMap colMap;

  SessionManager sm;
  BRM::TxnID txnID;
  txnID = sm.getTxnID(sessionID);

  if (!txnID.valid)
  {
    txnID.id = 0;
    txnID.valid = true;
  }

  QueryContext verID;
  verID = sm.verID();
  csep.txnID(txnID.id);
  csep.verID(verID);
  csep.sessionID(sessionID);

  string sysTable = "calpontsys.syscolumn.";
  string firstCol = sysTable + "columnname";
  SimpleColumn* c1 = new SimpleColumn(firstCol, sessionID);
  string secondCol = sysTable + "schema";
  SimpleColumn* c2 = new SimpleColumn(secondCol, sessionID);
  string thirdCol = sysTable + "tablename";
  SimpleColumn* c3 = new SimpleColumn(thirdCol, sessionID);
  string fourthCol = sysTable + "datatype";
  SimpleColumn* c4 = new SimpleColumn(fourthCol, sessionID);
  string fifthCol = sysTable + "defaultvalue";
  SimpleColumn* c5 = new SimpleColumn(fifthCol, sessionID);
  SRCP srcp;
  srcp.reset(c1);
  colMap.insert(CMVT_(firstCol, srcp));
  srcp.reset(c2);
  colMap.insert(CMVT_(secondCol, srcp));
  srcp.reset(c3);
  colMap.insert(CMVT_(thirdCol, srcp));
  srcp.reset(c4);
  colMap.insert(CMVT_(fourthCol, srcp));
  srcp.reset(c5);
  colMap.insert(CMVT_(fifthCol, srcp));
  csep.columnMapNonStatic(colMap);
  srcp.reset(c1->clone());
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  // Filters
  const SOP opeq(new Operator("="));
  SimpleFilter* f1 = new SimpleFilter(opeq, c2->clone(), new ConstantColumn(schema, ConstantColumn::LITERAL));
  filterTokenList.push_back(f1);
  filterTokenList.push_back(new Operator("and"));

  SimpleFilter* f2 =
      new SimpleFilter(opeq, c3->clone(), new ConstantColumn(tableName, ConstantColumn::LITERAL));
  filterTokenList.push_back(f2);
  filterTokenList.push_back(new Operator("and"));

  SimpleFilter* f3 = new SimpleFilter(
      opeq, c4->clone(),
      new ConstantColumn((uint64_t)execplan::CalpontSystemCatalog::TIMESTAMP, ConstantColumn::NUM));
  filterTokenList.push_back(f3);
  filterTokenList.push_back(new Operator("and"));

  string defaultValue = "current_timestamp() ON UPDATE current_timestamp()";
  SimpleFilter* f4 =
      new SimpleFilter(opeq, c5->clone(), new ConstantColumn(defaultValue, ConstantColumn::LITERAL));
  filterTokenList.push_back(f4);
  csep.filterTokenList(filterTokenList);

  CalpontSelectExecutionPlan::TableList tablelist;
  tablelist.push_back(make_aliastable("calpontsys", "syscolumn", ""));
  csep.tableList(tablelist);

  boost::shared_ptr<messageqcpp::MessageQueueClient> exemgrClient(
      new messageqcpp::MessageQueueClient("ExeMgr1"));
  ByteStream msg, emsgBs;
  rowgroup::RGData rgData;
  ByteStream::quadbyte qb = 4;
  msg << qb;
  rowgroup::RowGroup* rowGroup = 0;
  uint32_t rowCount;

  exemgrClient->write(msg);
  ByteStream msgPlan;
  csep.serialize(msgPlan);
  exemgrClient->write(msgPlan);
  msg.restart();
  msg = exemgrClient->read();  // error handling
  emsgBs = exemgrClient->read();
  ByteStream::quadbyte qb1;

  if (emsgBs.length() == 0)
  {
    // exemgrClient->shutdown();
    // delete exemgrClient;
    // exemgrClient = 0;
    throw runtime_error("Lost conection to ExeMgr.");
  }

  string emsgStr;
  emsgBs >> emsgStr;

  if (msg.length() == 4)
  {
    msg >> qb1;

    if (qb1 != 0)
    {
      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
      throw runtime_error(emsgStr);
    }
  }

  while (true)
  {
    msg.restart();
    msg = exemgrClient->read();

    if (msg.length() == 0)
    {
      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
      throw runtime_error("Lost conection to ExeMgr.");
    }
    else
    {
      if (!rowGroup)
      {
        // This is mete data
        rowGroup = new rowgroup::RowGroup();
        rowGroup->deserialize(msg);
        qb = 100;
        msg.restart();
        msg << qb;
        exemgrClient->write(msg);
        continue;
      }

      rgData.deserialize(msg);
      rowGroup->setData(&rgData);

      if (rowGroup->getStatus() != 0)
      {
        // msg.advance(rowGroup->getDataSize());
        msg >> emsgStr;
        // exemgrClient->shutdown();
        // delete exemgrClient;
        // exemgrClient = 0;
        throw runtime_error(emsgStr);
      }

      rowCount = rowGroup->getRowCount();
      if (rowCount > 0)
      {
        rowgroup::Row row;
        rowGroup->initRow(&row);
        for (uint32_t i = 0; i < rowCount; i++)
        {
          rowGroup->getRow(i, &row);
          // we are only fetching a single column
          returnVal.push_back(row.getStringField(0));
        }
      }
      else
      {
        break;
      }

      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
    }
  }

  return returnVal;
}

uint32_t doUpdateDelete(THD* thd, gp_walk_info& gwi, const std::vector<COND*>& condStack)
{
  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (ci->isSlaveNode && !thd->slave_thread)
  {
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
    setError(thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  //@Bug 4387. Check BRM status before start statement.
  boost::scoped_ptr<DBRM> dbrmp(new DBRM());
  int rc = dbrmp->isReadWrite();

  if (rc != 0)
  {
    setError(current_thd, ER_READ_ONLY_MODE, "Cannot execute the statement. DBRM is read only!");
    return ER_READ_ONLY_MODE;
  }

  // stats start
  ci->stats.reset();
  ci->stats.setStartTime();
  if (thd->main_security_ctx.user)
  {
    ci->stats.fUser = thd->main_security_ctx.user;
  }
  else
  {
    ci->stats.fUser = "";
  }

  if (thd->main_security_ctx.host)
    ci->stats.fHost = thd->main_security_ctx.host;
  else if (thd->main_security_ctx.host_or_ip)
    ci->stats.fHost = thd->main_security_ctx.host_or_ip;
  else
    ci->stats.fHost = "unknown";

  try
  {
    ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
  }
  catch (std::exception& e)
  {
    string msg = string("Columnstore User Priority - ") + e.what();
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
  }

  ci->stats.fSessionID = tid2sid(thd->thread_id);

  LEX* lex = thd->lex;
  idbassert(lex != 0);

  // Error out DELETE on VIEW. It's currently not supported.
  // @note DELETE on VIEW works natually (for simple cases at least), but we choose to turn it off
  // for now - ZZ.
  TABLE_LIST* tables = thd->lex->query_tables;

  for (; tables; tables = tables->next_local)
  {
    if (tables->view)
    {
      Message::Args args;

      if (isUpdateStatement(thd->lex->sql_command))
        args.add("Update");
#if 0
            else if (thd->rgi_slave && thd->rgi_slave->m_table_map.count() != 0)
                args.add("Row based replication event");
#endif
      else
        args.add("Delete");

      string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
      setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
      return ER_CHECK_NOT_IMPLEMENTED;
    }

    /*
    #if (defined(_MSC_VER) && defined(_DEBUG)) || defined(SAFE_MUTEX)
                    if ((strcmp((*tables->table->s->db_plugin)->name.str, "InfiniDB") != 0) &&
    (strcmp((*tables->table->s->db_plugin)->name.str, "MEMORY") != 0) && (tables->table->s->table_category !=
    TABLE_CATEGORY_TEMPORARY) ) #else if ((strcmp(tables->table->s->db_plugin->name.str, "InfiniDB") != 0) &&
    (strcmp(tables->table->s->db_plugin->name.str, "MEMORY") != 0) && (tables->table->s->table_category !=
    TABLE_CATEGORY_TEMPORARY) ) #endif
                    {
                            Message::Args args;
                            args.add("Non Calpont table(s)");
                            string emsg(IDBErrorInfo::instance()->errorMsg(ERR_DML_NOT_SUPPORT_FEATURE,
    args)); setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg); return ER_CHECK_NOT_IMPLEMENTED;
                    }
    */
  }

  // @bug 1127. Re-construct update stmt using lex instead of using the original query.
  char* query_char = idb_mysql_query_str(thd);
  std::string dmlStmt;
  if (!query_char)
  {
    dmlStmt = "<Replication event>";
  }
  else
  {
    dmlStmt = query_char;
  }
  string schemaName;
  string tableName("");
  string aliasName("");
  UpdateSqlStatement updateSqlStmt;
  ColumnAssignmentList* colAssignmentListPtr = new ColumnAssignmentList();
  bool isFromCol = false;
  bool isFromSameTable = true;
  execplan::SCSEP updateCP(new execplan::CalpontSelectExecutionPlan());

  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);

  updateCP->isDML(true);

  //@Bug 2753. the memory already freed by destructor of UpdateSqlStatement
  if (isUpdateStatement(thd->lex->sql_command))
  {
    ColumnAssignment* columnAssignmentPtr;
    Item_field* item;
    List_iterator_fast<Item> field_it(thd->lex->first_select_lex()->item_list);
    List_iterator_fast<Item> value_it(thd->lex->value_list);
    updateCP->queryType(CalpontSelectExecutionPlan::UPDATE);
    ci->stats.fQueryType = updateCP->queryType();
    uint32_t cnt = 0;
    tr1::unordered_set<string> timeStampColumnNames;

    while ((item = (Item_field*)field_it++))
    {
      cnt++;

      string tmpTableName = bestTableName(item);

      //@Bug 5312 populate aliasname with tablename if it is empty
      if (!item->table_name.str)
        aliasName = tmpTableName;
      else
        aliasName = item->table_name.str;

      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(aliasName);
        boost::algorithm::to_lower(tableName);
        boost::algorithm::to_lower(tmpTableName);
      }

      if (strcasecmp(tableName.c_str(), "") == 0)
      {
        tableName = tmpTableName;
      }
      else if (strcmp(tableName.c_str(), tmpTableName.c_str()) != 0)
      {
        //@ Bug3326 error out for multi table update
        string emsg(IDBErrorInfo::instance()->errorMsg(ERR_UPDATE_NOT_SUPPORT_FEATURE));
        thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED, emsg.c_str());
        ci->rc = -1;
        thd->set_row_count_func(0);
        return -1;
      }

      if (!item->db_name.str)
      {
        //@Bug 5312. if subselect, wait until the schema info is available.
        if (thd->derived_tables_processing)
          return 0;
        else
        {
          thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                  "The statement cannot be processed without schema.");
          ci->rc = -1;
          thd->set_row_count_func(0);
          return -1;
        }
      }
      else
      {
        schemaName = string(item->db_name.str);
        if (lower_case_table_names)
        {
          boost::algorithm::to_lower(schemaName);
        }
      }
      columnAssignmentPtr = new ColumnAssignment(item->name.str, "=", "");
      if (item->field_type() == MYSQL_TYPE_TIMESTAMP || item->field_type() == MYSQL_TYPE_TIMESTAMP2)
      {
        timeStampColumnNames.insert(string(item->name.str));
      }

      Item* value = value_it++;

      if (value->type() == Item::CONST_ITEM)
      {
        if (value->cmp_type() == STRING_RESULT || value->cmp_type() == DECIMAL_RESULT ||
            value->cmp_type() == REAL_RESULT)
        {
          //@Bug 2587 use val_str to replace value->name to get rid of 255 limit
          String val, *str;
          str = value->val_str(&val);
          columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
          columnAssignmentPtr->fFromCol = false;
        }
        else if (value->cmp_type() == INT_RESULT)
        {
          std::ostringstream oss;

          if (value->unsigned_flag)
          {
            oss << value->val_uint();
          }
          else
          {
            oss << value->val_int();
          }

          columnAssignmentPtr->fScalarExpression = oss.str();
          columnAssignmentPtr->fFromCol = false;
        }
      }
      else if (value->type() == Item::FUNC_ITEM)
      {
        // Bug 2092 handle negative values
        Item_func* ifp = (Item_func*)value;

        if (ifp->result_type() == DECIMAL_RESULT)
          columnAssignmentPtr->fFuncScale = ifp->decimals;  // decimal scale

        vector<Item_field*> tmpVec;
        bool hasNonSupportItem = false;
        uint16_t parseInfo = 0;
        parse_item(ifp, tmpVec, hasNonSupportItem, parseInfo);

        // const f&e evaluate here. @bug3513. Rule out special functions that takes
        // no argument but needs to be sent to the back end to process. Like rand(),
        // sysdate() etc.
        if (!hasNonSupportItem && !cal_impl_if::nonConstFunc(ifp) && tmpVec.size() == 0)
        {
          gp_walk_info gwi2(gwi.timeZone);
          gwi2.thd = thd;
          SRCP srcp(buildReturnedColumn(value, gwi2, gwi2.fatalParseError));
          ConstantColumn* constCol = dynamic_cast<ConstantColumn*>(srcp.get());

          if (constCol)
          {
            columnAssignmentPtr->fScalarExpression = constCol->constval();
            isFromCol = false;
            columnAssignmentPtr->fFromCol = false;
          }
          else
          {
            isFromCol = true;
            columnAssignmentPtr->fFromCol = true;
          }
        }
        else
        {
          isFromCol = true;
          columnAssignmentPtr->fFromCol = true;
        }

        if (isFromCol)
        {
          string sectableName("");
          string secschemaName("");

          for (unsigned i = 0; i < tmpVec.size(); i++)
          {
            sectableName = bestTableName(tmpVec[i]);

            if (tmpVec[i]->db_name.str)
            {
              secschemaName = string(tmpVec[i]->db_name.str);
            }

            if ((strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) ||
                (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
            {
              isFromSameTable = false;
              break;
            }
          }
        }
      }
      else if (value->type() == Item::FIELD_ITEM)
      {
        isFromCol = true;
        columnAssignmentPtr->fFromCol = true;
        Item_field* setIt = reinterpret_cast<Item_field*>(value);

        // Minor optimization:
        // do not perform updates of the form "update t1 set a = a;"
        if (!strcmp(item->name.str, setIt->name.str) && item->table_name.str && setIt->table_name.str &&
            !strcmp(item->table_name.str, setIt->table_name.str) && item->db_name.str && setIt->db_name.str &&
            !strcmp(item->db_name.str, setIt->db_name.str))
        {
          delete columnAssignmentPtr;
          continue;
        }

        string sectableName = string(setIt->table_name.str);

        if (setIt->db_name.str)  // derived table
        {
          string secschemaName = string(setIt->db_name.str);

          if ((strcasecmp(tableName.c_str(), sectableName.c_str()) != 0) ||
              (strcasecmp(schemaName.c_str(), secschemaName.c_str()) != 0))
          {
            isFromSameTable = false;
          }
        }
        else
        {
          isFromSameTable = false;
        }
      }
      else if (value->type() == Item::NULL_ITEM)
      {
        columnAssignmentPtr->fScalarExpression = "";
        columnAssignmentPtr->fFromCol = false;
        columnAssignmentPtr->fIsNull = true;
      }
      else if (value->type() == Item::SUBSELECT_ITEM)
      {
        isFromCol = true;
        columnAssignmentPtr->fFromCol = true;
        isFromSameTable = false;
      }
      //@Bug 4449 handle default value
      else if (value->type() == Item::DEFAULT_VALUE_ITEM)
      {
        Item_field* tmp = (Item_field*)value;

        if (!tmp->field_name.length)  // null
        {
          columnAssignmentPtr->fScalarExpression = "NULL";
          columnAssignmentPtr->fFromCol = false;
        }
        else
        {
          String val, *str;
          str = value->val_str(&val);
          columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
          columnAssignmentPtr->fFromCol = false;
        }
      }
      else if (value->type() == Item::WINDOW_FUNC_ITEM)
      {
        setError(thd, ER_INTERNAL_ERROR, logging::IDBErrorInfo::instance()->errorMsg(ERR_WF_UPDATE));
        return ER_CHECK_NOT_IMPLEMENTED;
      }
      else
      {
        String val, *str;
        str = value->val_str(&val);

        if (str)
        {
          columnAssignmentPtr->fScalarExpression.assign(str->ptr(), str->length());
          columnAssignmentPtr->fFromCol = false;
        }
        else
        {
          columnAssignmentPtr->fScalarExpression = "NULL";
          columnAssignmentPtr->fFromCol = false;
        }
      }

      colAssignmentListPtr->push_back(columnAssignmentPtr);
    }

    // Support for on update current_timestamp() for timestamp fields
    // Query calpontsys.syscolumn to get all timestamp columns with
    // ON UPDATE current_timestamp() property
    vector<string> onUpdateTimeStampColumns =
        getOnUpdateTimestampColumns(schemaName, tableName, tid2sid(thd->thread_id));
    for (size_t i = 0; i < onUpdateTimeStampColumns.size(); i++)
    {
      if (timeStampColumnNames.find(onUpdateTimeStampColumns[i]) == timeStampColumnNames.end())
      {
        // DRRTUY * That is far from ideal.
        columnAssignmentPtr = new ColumnAssignment(string(onUpdateTimeStampColumns[i]), "=", "");
        struct timeval tv;
        char buf[64];
        gettimeofday(&tv, 0);
        MySQLTime time;
        gmtSecToMySQLTime(tv.tv_sec, time, timeZoneOffset);
        sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06ld", time.year, time.month, time.day, time.hour,
                time.minute, time.second, tv.tv_usec);
        columnAssignmentPtr->fScalarExpression = buf;
        colAssignmentListPtr->push_back(columnAssignmentPtr);
      }
    }
  }
#if 0
    else if (thd->rgi_slave && thd->rgi_slave->m_table_map.count() != 0)
    {
        string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_RBR_EVENT);
        setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
        return ER_CHECK_NOT_IMPLEMENTED;
    }
#endif
  else
  {
    updateCP->queryType(CalpontSelectExecutionPlan::DELETE);
    ci->stats.fQueryType = updateCP->queryType();
  }

  // Exit early if there is nothing to update
  if (colAssignmentListPtr->empty() && isUpdateStatement(thd->lex->sql_command))
  {
    ci->affectedRows = 0;
    return 0;
  }

  // save table oid for commit/rollback to use
  uint32_t sessionID = tid2sid(thd->thread_id);
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(execplan::CalpontSystemCatalog::FE);
  CalpontSystemCatalog::TableName aTableName;
  TABLE_LIST* first_table = 0;

  if (isUpdateStatement(thd->lex->sql_command))
  {
    aTableName.schema = schemaName;
    aTableName.table = tableName;
  }
  else
  {
    first_table = (TABLE_LIST*)thd->lex->first_select_lex()->table_list.first;
    aTableName.schema = first_table->table->s->db.str;
    aTableName.table = first_table->table->s->table_name.str;
  }
  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(aTableName.schema);
    boost::algorithm::to_lower(aTableName.table);
  }

  CalpontDMLPackage* pDMLPackage = 0;
  //	dmlStmt += ";";
  IDEBUG(cout << "STMT: " << dmlStmt << " and sessionID " << thd->thread_id << endl);
  VendorDMLStatement dmlStatement(dmlStmt, sessionID);

  if (isUpdateStatement(thd->lex->sql_command))
    dmlStatement.set_DMLStatementType(DML_UPDATE);
  else
    dmlStatement.set_DMLStatementType(DML_DELETE);

  TableName* qualifiedTablName = new TableName();

  UpdateSqlStatement updateStmt;
  //@Bug 2753. To make sure the momory is freed.
  updateStmt.fColAssignmentListPtr = colAssignmentListPtr;

  if (isUpdateStatement(thd->lex->sql_command))
  {
    qualifiedTablName->fName = tableName;
    qualifiedTablName->fSchema = schemaName;
    updateStmt.fNamePtr = qualifiedTablName;
    pDMLPackage = CalpontDMLFactory::makeCalpontUpdatePackageFromMysqlBuffer(dmlStatement, updateStmt);
  }
  else if ((thd->lex)->sql_command == SQLCOM_DELETE_MULTI)  //@Bug 6121 error out on multi tables delete.
  {
    if ((thd->lex->first_select_lex()->join) != 0)
    {
      multi_delete* deleteTable = (multi_delete*)((thd->lex->first_select_lex()->join)->result);
      first_table = (TABLE_LIST*)deleteTable->get_tables();

      if (deleteTable->get_num_of_tables() == 1)
      {
        schemaName = first_table->db.str;
        tableName = first_table->table_name.str;
        aliasName = first_table->alias.str;
        if (lower_case_table_names)
        {
          boost::algorithm::to_lower(schemaName);
          boost::algorithm::to_lower(tableName);
          boost::algorithm::to_lower(aliasName);
        }
        qualifiedTablName->fName = tableName;
        qualifiedTablName->fSchema = schemaName;
        pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
      }
      else
      {
        string emsg("Deleting rows from multiple tables in a single statement is currently not supported.");
        thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
        ci->rc = 1;
        thd->set_row_count_func(0);
        return 1;
      }
    }
    else
    {
      first_table = (TABLE_LIST*)thd->lex->first_select_lex()->table_list.first;
      schemaName = first_table->table->s->db.str;
      tableName = first_table->table->s->table_name.str;
      aliasName = first_table->alias.str;
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(schemaName);
        boost::algorithm::to_lower(tableName);
        boost::algorithm::to_lower(aliasName);
      }
      qualifiedTablName->fName = tableName;
      qualifiedTablName->fSchema = schemaName;
      pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
    }
  }
  else
  {
    first_table = (TABLE_LIST*)thd->lex->first_select_lex()->table_list.first;
    schemaName = first_table->table->s->db.str;
    tableName = first_table->table->s->table_name.str;
    aliasName = first_table->alias.str;
    if (lower_case_table_names)
    {
      boost::algorithm::to_lower(schemaName);
      boost::algorithm::to_lower(tableName);
      boost::algorithm::to_lower(aliasName);
    }
    qualifiedTablName->fName = tableName;
    qualifiedTablName->fSchema = schemaName;
    pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStatement);
  }

  if (!pDMLPackage)
  {
    string emsg("Fatal parse error in vtable mode in DMLParser ");
    setError(thd, ER_INTERNAL_ERROR, emsg);
    return ER_INTERNAL_ERROR;
  }

  pDMLPackage->set_TableName(tableName);

  pDMLPackage->set_SchemaName(schemaName);
  pDMLPackage->set_TimeZone(timeZoneOffset);

  pDMLPackage->set_IsFromCol(true);
  // cout << " setting 	isFromCol to " << isFromCol << endl;
  std::string origStmt = dmlStmt;
  origStmt += ";";
  pDMLPackage->set_SQLStatement(origStmt);

  // Save the item list
  List<Item> items;
  SELECT_LEX select_lex;

  if (isUpdateStatement(thd->lex->sql_command))
  {
    items = (thd->lex->first_select_lex()->item_list);
    thd->lex->first_select_lex()->item_list = thd->lex->value_list;
  }

  select_lex = *lex->first_select_lex();

  //@Bug 2808 Error out on order by or limit clause
  //@bug5096. support dml limit.
  if ((thd->lex->first_select_lex()->order_list.elements != 0))
  {
    string emsg("DML Statement with order by clause is not currently supported.");
    thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
    ci->rc = 1;
    thd->set_row_count_func(0);
    return 1;
  }

  {
    updateCP->subType(CalpontSelectExecutionPlan::SELECT_SUBS);
    //@Bug 2975.
    SessionManager sm;
    BRM::TxnID txnID;
    txnID = sm.getTxnID(sessionID);

    if (!txnID.valid)
    {
      txnID.id = 0;
      txnID.valid = true;
    }

    QueryContext verID;
    verID = sm.verID();

    updateCP->txnID(txnID.id);
    updateCP->verID(verID);
    updateCP->sessionID(sessionID);
    char* query_char = idb_mysql_query_str(thd);
    std::string query_str;
    if (!query_char)
    {
      query_str = "<Replication event>";
    }
    else
    {
      query_str = query_char;
    }
    updateCP->data(query_str);

    try
    {
      updateCP->priority(ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore User Priority - ") + e.what();
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
    }

    gwi.clauseType = WHERE;

    if (getSelectPlan(gwi, select_lex, updateCP, false, false, condStack) !=
        0)  //@Bug 3030 Modify the error message for unsupported functions
    {
      if (gwi.cs_vtable_is_update_with_derive)
      {
        // @bug 4457. MySQL inconsistence! for some queries, some structures are only available
        // in the derived_tables_processing phase. So by pass the phase for DML only when the
        // execution plan can not be successfully generated. recover lex before returning;
        thd->lex->first_select_lex()->item_list = items;
        return 0;
      }

      // check different error code
      // avoid double set IDB error
      string emsg;

      if (gwi.parseErrorText.find("MCS-") == string::npos)
      {
        Message::Args args;
        args.add(gwi.parseErrorText);
        emsg = IDBErrorInfo::instance()->errorMsg(ER_INTERNAL_ERROR, args);
      }
      else
      {
        emsg = gwi.parseErrorText;
      }

      thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
      ci->rc = -1;
      thd->set_row_count_func(0);
      return -1;
    }

    // cout<< "Plan before is " << endl << *updateCP << endl;
    // set the large side by putting the updated table at the first
    CalpontSelectExecutionPlan::TableList tbList = updateCP->tableList();

    CalpontSelectExecutionPlan::TableList::iterator iter = tbList.begin();
    bool notFirst = false;

    while (iter != tbList.end())
    {
      if ((iter != tbList.begin()) && (iter->schema == schemaName) && (iter->table == tableName) &&
          (iter->alias == aliasName))
      {
        notFirst = true;
        tbList.erase(iter);
        break;
      }

      iter++;
    }

    if (notFirst)
    {
      CalpontSystemCatalog::TableAliasName tn = make_aliastable(schemaName, tableName, aliasName);
      iter = tbList.begin();
      tbList.insert(iter, 1, tn);
    }

    updateCP->tableList(tbList);
    // DRRTUY * this is very optimisic assumption
    updateCP->overrideLargeSideEstimate(true);
    // loop through returnedcols to find out constant columns
    CalpontSelectExecutionPlan::ReturnedColumnList returnedCols = updateCP->returnedCols();
    CalpontSelectExecutionPlan::ReturnedColumnList::iterator coliter = returnedCols.begin();

    while (coliter != returnedCols.end())
    {
      ConstantColumn* returnCol = dynamic_cast<ConstantColumn*>((*coliter).get());

      if (returnCol)
      {
        returnedCols.erase(coliter);
        coliter = returnedCols.begin();
      }
      else
        coliter++;
    }

    if ((updateCP->columnMap()).empty())
      throw runtime_error("column map is empty!");

    if (returnedCols.empty())
      returnedCols.push_back((updateCP->columnMap()).begin()->second);

    //@Bug 6123. get the correct returned columnlist
    if (isDeleteStatement(thd->lex->sql_command))
    {
      returnedCols.clear();
      // choose the smallest column to project
      CalpontSystemCatalog::TableName deleteTableName;
      deleteTableName.schema = schemaName;
      deleteTableName.table = tableName;
      CalpontSystemCatalog::RIDList colrids;

      try
      {
        colrids = csc->columnRIDs(deleteTableName, false, lower_case_table_names);
      }
      catch (IDBExcept& ie)
      {
        thd->raise_error_printf(ER_INTERNAL_ERROR, ie.what());
        ci->rc = -1;
        thd->set_row_count_func(0);
        return -1;
      }

      int minColWidth = -1;
      int minWidthColOffset = 0;

      for (unsigned int j = 0; j < colrids.size(); j++)
      {
        CalpontSystemCatalog::ColType ct = csc->colType(colrids[j].objnum);

        if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
          continue;

        if (minColWidth == -1 || ct.colWidth < minColWidth)
        {
          minColWidth = ct.colWidth;
          minWidthColOffset = j;
        }
      }

      CalpontSystemCatalog::TableColName tcn = csc->colName(colrids[minWidthColOffset].objnum);
      SimpleColumn* sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, csc->sessionID());
      sc->tableAlias(aliasName);
      sc->timeZone(timeZoneOffset);
      sc->resultType(csc->colType(colrids[minWidthColOffset].objnum));
      SRCP srcp;
      srcp.reset(sc);
      returnedCols.push_back(srcp);
      // cout << "tablename:alias = " << tcn.table<<":"<<aliasName<<endl;
    }

    updateCP->returnedCols(returnedCols);

    if (isUpdateStatement(thd->lex->sql_command))
    {
      const ParseTree* ptsub = updateCP->filters();

      if (!isFromSameTable)
      {
        // cout << "set scalar" << endl;
        // walk tree to set scalar
        if (ptsub)
          ptsub->walk(makeUpdateScalarJoin, &tbList[0]);
      }
      else
      {
        // cout << "set semi" << endl;
        if (ptsub)
          ptsub->walk(makeUpdateSemiJoin, &tbList[0]);
      }

      thd->lex->first_select_lex()->item_list = items;
    }
  }

  // cout<< "Plan is " << endl << *updateCP << endl;
  // updateCP->traceFlags(1);
  pDMLPackage->HasFilter(true);
  pDMLPackage->uuid(updateCP->uuid());

  ByteStream bytestream, bytestream1;
  bytestream << sessionID;
  boost::shared_ptr<messageqcpp::ByteStream> plan = pDMLPackage->get_ExecutionPlan();
  updateCP->rmParms(ci->rmParms);
  updateCP->serialize(*plan);
  pDMLPackage->write(bytestream);

  delete pDMLPackage;

  ByteStream::byte b = 0;
  ByteStream::octbyte rows = 0;
  std::string errorMsg;
  long long dmlRowCount = 0;

  if (thd->killed > 0)
  {
    return 0;
  }

  // querystats::QueryStats stats;
  string tableLockInfo;

  // Save the tableOid for the COMMIT | ROLLBACK
  CalpontSystemCatalog::ROPair roPair;
  try
  {
    roPair = csc->tableRID(aTableName);
  }
  catch (IDBExcept& ie)
  {
    setError(thd, ER_INTERNAL_ERROR, ie.what());
    return ER_INTERNAL_ERROR;
  }
  catch (std::exception& ex)
  {
    setError(thd, ER_INTERNAL_ERROR,
             logging::IDBErrorInfo::instance()->errorMsg(ERR_SYSTEM_CATALOG) + ex.what());
    return ER_INTERNAL_ERROR;
  }
  ci->tableOid = roPair.objnum;

  // Send the request to DMLProc and wait for a response.
  try
  {
    timespec* tsp = 0;
#ifndef _MSC_VER
    timespec ts;
    ts.tv_sec = 3L;
    ts.tv_nsec = 0L;
    tsp = &ts;
#else
    // FIXME: @#$%^&! mysql has buggered up timespec!
    // The definition in my_pthread.h isn't the same as in winport/unistd.h...
    struct timespec_foo
    {
      long tv_sec;
      long tv_nsec;
    } ts_foo;
    ts_foo.tv_sec = 3;
    ts_foo.tv_nsec = 0;
    // This is only to get the compiler to not carp below at the read() call.
    // The messagequeue lib uses the correct struct
    tsp = reinterpret_cast<timespec*>(&ts_foo);
#endif
    bool isTimeOut = true;
    int maxRetries = 2;
    std::string exMsg;

    // We try twice to get a response from dmlproc.
    // Every (3) seconds, check for ctrl+c
    for (int retry = 0; bytestream1.length() == 0 && retry < maxRetries; ++retry)
    {
      try
      {
        if (!ci->dmlProc)
        {
          ci->dmlProc = new MessageQueueClient("DMLProc");
          // cout << "doUpdateDelete start new DMLProc client " << ci->dmlProc << " for session " << sessionID
          // << endl;
        }
        else
        {
          delete ci->dmlProc;
          ci->dmlProc = nullptr;
          ci->dmlProc = new MessageQueueClient("DMLProc");
        }

        // Send the request to DMLProc
        ci->dmlProc->write(bytestream);

        // Get an answer from DMLProc
        while (isTimeOut)
        {
          isTimeOut = false;
          bytestream1 = ci->dmlProc->read(tsp, &isTimeOut);

          if (b == 0 && thd->killed > 0 && isTimeOut)
          {
            // We send the CTRL+C command to DMLProc out of band
            // (on a different connection) This will cause
            // DMLProc to stop processing and return an error on the
            // original connection which will cause a rollback.
            messageqcpp::MessageQueueClient ctrlCProc("DMLProc");
            // cout << "doUpdateDelete start new DMLProc client for ctrl-c " <<  " for session " << sessionID
            // << endl;
            VendorDMLStatement cmdStmt("CTRL+C", DML_COMMAND, sessionID);
            CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
            pDMLPackage->set_TimeZone(timeZoneOffset);
            ByteStream bytestream;
            bytestream << static_cast<uint32_t>(sessionID);
            pDMLPackage->write(bytestream);
            delete pDMLPackage;
            b = 1;
            retry = maxRetries;
            errorMsg = "Command canceled by user";

            try
            {
              ctrlCProc.write(bytestream);
            }
            catch (runtime_error&)
            {
              errorMsg = "Lost connection to DMLProc while doing ctrl+c";
            }
            catch (...)
            {
              errorMsg = "Unknown error caught while doing ctrl+c";
            }

            //						break;
          }
        }
      }
      catch (runtime_error& ex)
      {
        // An exception causes a retry, so fall thru
        exMsg = ex.what();
      }

      if (bytestream1.length() == 0 && thd->killed <= 0)
      {
        // cout << "line 1442. received 0 byte from DMLProc and retry = "<< retry << endl;
        // Seems dmlProc isn't playing. Reset it and try again.
        delete ci->dmlProc;
        ci->dmlProc = nullptr;
        isTimeOut = true;  //@Bug 4742
      }
    }

    if (bytestream1.length() == 0)
    {
      // If we didn't get anything, error
      b = 1;

      if (exMsg.length() > 0)
      {
        errorMsg = exMsg;
      }
      else
      {
        errorMsg = "Lost connection to DMLProc";
      }
    }
    else
    {
      bytestream1 >> b;
      bytestream1 >> rows;
      bytestream1 >> errorMsg;

      if (b == 0)
      {
        bytestream1 >> tableLockInfo;
        bytestream1 >> ci->queryStats;
        bytestream1 >> ci->extendedStats;
        bytestream1 >> ci->miniStats;
        ci->stats.unserialize(bytestream1);
      }
    }

    dmlRowCount = rows;

    if (thd->killed && b == 0)
    {
      b = dmlpackageprocessor::DMLPackageProcessor::JOB_CANCELED;
      errorMsg = "Command canceled by user";
    }
  }
  catch (runtime_error& ex)
  {
    cout << ex.what() << endl;
    b = 1;
    delete ci->dmlProc;
    ci->dmlProc = nullptr;
    errorMsg = ex.what();
  }
  catch (...)
  {
    // cout << "... exception while writing to DMLProc" << endl;
    b = 1;
    delete ci->dmlProc;
    ci->dmlProc = nullptr;
    errorMsg = "Unknown error caught";
  }

  // If autocommit is on then go ahead and tell the engine to commit the transaction
  //@Bug 1960 If error occurs, the commit is just to release the active transaction.
  //@Bug 2241. Rollback transaction when it failed
  //@Bug 4605. If error, always rollback.
  if (b != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR)
  {
    std::string command;

    if ((useHdfs) && (b == 0))
      command = "COMMIT";
    else if ((useHdfs) && (b != 0))
      command = "ROLLBACK";
    else if ((b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING) && thd->is_strict_mode())
      command = "ROLLBACK";
    else if ((!(current_thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) &&
             ((b == 0) || (b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)))
      command = "COMMIT";
    else if ((b != 0) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
      command = "ROLLBACK";
    else
      command = "";

    if (command != "")
    {
      VendorDMLStatement cmdStmt(command, DML_COMMAND, sessionID);
      CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
      pDMLPackage->set_TimeZone(timeZoneOffset);
      pDMLPackage->setTableOid(ci->tableOid);
      ByteStream bytestream;
      bytestream << static_cast<uint32_t>(sessionID);
      pDMLPackage->write(bytestream);
      delete pDMLPackage;

      ByteStream::byte bc;
      std::string errMsg;

      try
      {
        if (!ci->dmlProc)
        {
          ci->dmlProc = new MessageQueueClient("DMLProc");
          // cout << " doupdateDelete command use a new dml client " << ci->dmlProc << endl;
        }

        ci->dmlProc->write(bytestream);
        bytestream1 = ci->dmlProc->read();
        bytestream1 >> bc;
        bytestream1 >> rows;
        bytestream1 >> errMsg;

        if (b == 0)
        {
          b = bc;
          errorMsg = errMsg;
        }
      }
      catch (runtime_error&)
      {
        errorMsg = "Lost connection to DMLProc";
        b = 1;
        delete ci->dmlProc;
        ci->dmlProc = nullptr;
      }
      catch (...)
      {
        errorMsg = "Unknown error caught";
        b = 1;
      }

      // Clear tableOid for the next SQL statement
      ci->tableOid = 0;
    }
  }

  //@Bug 2241 Display an error message to user

  if ((b != 0) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
  {
    //@Bug 2540. Set error status instead of warning
    thd->raise_error_printf(ER_INTERNAL_ERROR, errorMsg.c_str());
    ci->rc = b;
    rc = ER_INTERNAL_ERROR;
  }

  if (b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
  {
    if (thd->is_strict_mode())
    {
      thd->set_row_count_func(0);
      ci->rc = b;
      // Turn this on as MariaDB doesn't do it until the next phase
      thd->abort_on_warning = thd->is_strict_mode();
      rc = ER_INTERNAL_ERROR;
    }
    else
    {
      ci->affectedRows = dmlRowCount;
    }

    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, ER_WARN_DATA_OUT_OF_RANGE, errorMsg.c_str());
  }
  else
  {
    //		if (dmlRowCount != 0) //Bug 5117. Handling self join.
    ci->affectedRows = dmlRowCount;
    // cout << " error status " << ci->rc << " and rowcount = " << dmlRowCount << endl;
  }

  // insert query stats
  ci->stats.setEndTime();

  try
  {
    ci->stats.insert();
  }
  catch (std::exception& e)
  {
    string msg = string("Columnstore Query Stats - ") + e.what();
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
  }

  delete ci->dmlProc;
  ci->dmlProc = nullptr;
  return rc;
}

inline bool isSupportedToAnalyze(const execplan::CalpontSystemCatalog::ColType& colType)
{
  return colType.isUnsignedInteger() || colType.isSignedInteger();
}

// Initializes `cal_connection_info` using given `thd` and `sessionID`.
bool initializeCalConnectionInfo(cal_connection_info* ci, THD* thd,
                                 boost::shared_ptr<execplan::CalpontSystemCatalog> csc, uint32_t sessionID,
                                 bool localQuery)
{
  ci->stats.reset();
  ci->stats.setStartTime();

  if (thd->main_security_ctx.user)
  {
    ci->stats.fUser = thd->main_security_ctx.user;
  }
  else
  {
    ci->stats.fUser = "";
  }

  if (thd->main_security_ctx.host)
    ci->stats.fHost = thd->main_security_ctx.host;
  else if (thd->main_security_ctx.host_or_ip)
    ci->stats.fHost = thd->main_security_ctx.host_or_ip;
  else
    ci->stats.fHost = "unknown";

  try
  {
    ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
  }
  catch (std::exception& e)
  {
    string msg = string("Columnstore User Priority - ") + e.what();
    ci->warningMsg = msg;
  }

  if (ci->queryState != 0)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  sm::sm_init(sessionID, &ci->cal_conn_hndl, localQuery);
  idbassert(ci->cal_conn_hndl != 0);
  ci->cal_conn_hndl->csc = csc;
  idbassert(ci->cal_conn_hndl->exeMgr != 0);

  try
  {
    ci->cal_conn_hndl->connect();
  }
  catch (...)
  {
    setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return false;
  }

  return true;
}

bool sendExecutionPlanToExeMgr(sm::cpsm_conhdl_t* hndl, ByteStream::quadbyte qb,
                               std::shared_ptr<execplan::MCSAnalyzeTableExecutionPlan> caep,
                               cal_connection_info* ci, THD* thd)
{
  ByteStream msg;
  try
  {
    msg << qb;
    hndl->exeMgr->write(msg);
    msg.restart();
    caep->rmParms(ci->rmParms);

    // Send the execution plan.
    caep->serialize(msg);
    hndl->exeMgr->write(msg);

    // Get the status from ExeMgr.
    msg.restart();
    msg = hndl->exeMgr->read();

    // Any return code is ok for now.
    if (msg.length() == 0)
    {
      auto emsg = "Lost connection to ExeMgr. Please contact your administrator";
      setError(thd, ER_INTERNAL_ERROR, emsg);
      return false;
    }
  }
  catch (...)
  {
    return false;
  }

  return true;
}

}  // namespace

int ha_mcs_impl_analyze(THD* thd, TABLE* table)
{
  uint32_t sessionID = execplan::CalpontSystemCatalog::idb_tid2sid(thd->thread_id);
  boost::shared_ptr<execplan::CalpontSystemCatalog> csc =
      execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);

  csc->identity(execplan::CalpontSystemCatalog::FE);

  auto table_name = execplan::make_table(table->s->db.str, table->s->table_name.str, lower_case_table_names);

  // Skip for now.
  if (table->s->db.length && strcmp(table->s->db.str, "information_schema") == 0)
    return 0;

  bool columnStore = (table ? isMCSTable(table) : true);
  // Skip non columnstore tables.
  if (!columnStore)
    return 0;

  execplan::CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(table_name, true);
  execplan::MCSAnalyzeTableExecutionPlan::ReturnedColumnList returnedColumnList;
  execplan::MCSAnalyzeTableExecutionPlan::ColumnMap columnMap;

  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);

  // Iterate over table oid list and create a `SimpleColumn` for every column with supported type.
  for (uint32_t i = 0, e = oidlist.size(); i < e; ++i)
  {
    execplan::SRCP returnedColumn;
    const auto objNum = oidlist[i].objnum;
    auto tableColName = csc->colName(objNum);
    auto colType = csc->colType(objNum);

    if (!isSupportedToAnalyze(colType))
      continue;

    execplan::SimpleColumn* simpleColumn = new execplan::SimpleColumn();
    simpleColumn->columnName(tableColName.column);
    simpleColumn->tableName(tableColName.table, lower_case_table_names);
    simpleColumn->schemaName(tableColName.schema, lower_case_table_names);
    simpleColumn->oid(objNum);
    simpleColumn->alias(tableColName.column);
    simpleColumn->resultType(colType);
    simpleColumn->timeZone(timeZoneOffset);

    returnedColumn.reset(simpleColumn);
    returnedColumnList.push_back(returnedColumn);
    columnMap.insert(execplan::MCSAnalyzeTableExecutionPlan::ColumnMap::value_type(simpleColumn->columnName(),
                                                                                   returnedColumn));
  }

  // Create execution plan and initialize it with `returned columns` and `column map`.
  std::shared_ptr<execplan::MCSAnalyzeTableExecutionPlan> caep(
      new execplan::MCSAnalyzeTableExecutionPlan(returnedColumnList, columnMap));

  caep->schemaName(table->s->db.str, lower_case_table_names);
  caep->tableName(table->s->table_name.str, lower_case_table_names);
  caep->timeZone(timeZoneOffset);

  SessionManager sm;
  BRM::TxnID txnID;
  txnID = sm.getTxnID(sessionID);

  if (!txnID.valid)
  {
    txnID.id = 0;
    txnID.valid = true;
  }

  QueryContext verID;
  verID = sm.verID();

  caep->txnID(txnID.id);
  caep->verID(verID);
  caep->sessionID(sessionID);

  string query;
  query.assign(idb_mysql_query_str(thd));
  caep->data(query);

  if (!get_fe_conn_info_ptr())
    set_fe_conn_info_ptr(reinterpret_cast<void*>(new cal_connection_info(), thd));

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  idbassert(ci != 0);

  try
  {
    caep->priority(ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
  }
  catch (std::exception& e)
  {
    string msg = string("Columnstore User Priority - ") + e.what();
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
  }

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  caep->traceFlags(ci->traceFlags);

  cal_table_info ti;
  sm::cpsm_conhdl_t* hndl;

  bool localQuery = (get_local_query(thd) > 0 ? true : false);
  caep->localQuery(localQuery);

  // Try to initialize connection.
  if (!initializeCalConnectionInfo(ci, thd, csc, sessionID, localQuery))
    goto error;

  hndl = ci->cal_conn_hndl;

  if (caep->traceOn())
    std::cout << caep->toString() << std::endl;
  {
    ByteStream::quadbyte qb = ANALYZE_TABLE_EXECUTE;
    // Serialize and the send the `anlyze table` execution plan.
    if (!sendExecutionPlanToExeMgr(hndl, qb, caep, ci, thd))
      goto error;
  }

  ci->rmParms.clear();
  ci->tableMap[table] = ti;

  return 0;

error:

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;
}

int ha_mcs_impl_open(const char* name, int mode, uint32_t test_if_locked)
{
  IDEBUG(cout << "ha_mcs_impl_open: " << name << ", " << mode << ", " << test_if_locked << endl);
  Config::makeConfig();
  return 0;
}

int ha_mcs_impl_close(void)
{
  IDEBUG(cout << "ha_mcs_impl_close" << endl);
  return 0;
}

int ha_mcs_impl_discover_existence(const char* schema, const char* name)
{
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog();

  try
  {
    const CalpontSystemCatalog::OID oid =
        csc->lookupTableOID(make_table(schema, name, lower_case_table_names));

    if (oid)
      return 1;
  }
  catch (...)
  {
  }

  return 0;
}

int ha_mcs_impl_direct_update_delete_rows(bool execute, ha_rows* affected_rows,
                                          const std::vector<COND*>& condStack)
{
  THD* thd = current_thd;
  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
  cal_impl_if::gp_walk_info gwi(timeZoneOffset);
  gwi.thd = thd;
  int rc = 0;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
  {
    if (affected_rows)
      *affected_rows = 0;
    return 0;
  }

  if (execute)
  {
    rc = doUpdateDelete(thd, gwi, condStack);
  }

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  if (ci)
  {
    *affected_rows = ci->affectedRows;
  }

  return rc;
}

int ha_mcs::impl_rnd_init(TABLE* table, const std::vector<COND*>& condStack)
{
  IDEBUG(cout << "rnd_init for table " << table->s->table_name.str << endl);
  THD* thd = current_thd;
  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
  gp_walk_info gwi(timeZoneOffset);
  gwi.thd = thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return 0;

    // check whether the system is ready to process statement.
#ifndef _MSC_VER
  static DBRM dbrm(true);
  int bSystemQueryReady = dbrm.getSystemQueryReady();

  if (bSystemQueryReady == 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "The system is not yet ready to accept queries");
    return ER_INTERNAL_ERROR;
  }
  else if (bSystemQueryReady < 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "DBRM is not responding. Cannot accept queries");
    return ER_INTERNAL_ERROR;
  }
#endif

  // Set this to close all outstanding FEP connections on
  // client disconnect in handlerton::closecon_handlerton().
  if (!thd_get_ha_data(thd, mcs_hton))
  {
    thd_set_ha_data(thd, mcs_hton, reinterpret_cast<void*>(0x42));
  }

#if 0
    if (thd->rgi_slave && thd->rgi_slave->m_table_map.count() != 0)
    {
        string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_RBR_EVENT);
        setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
        return ER_CHECK_NOT_IMPLEMENTED;
    }
#endif

  // If ALTER TABLE and not ENGINE= we don't need rnd_init (gets us in a bad state)
  if ((thd->lex->sql_command == SQLCOM_ALTER_TABLE) &&
      !(thd->lex->create_info.used_fields & HA_CREATE_USED_ENGINE))
  {
    return 0;
  }

  /*
    Update and delete code.
    Note, we may be updating/deleting a different table,
    and the current one is only needed for reading,
    e.g. cstab1 is needed for reading in this example:

    UPDATE innotab1 SET a=100 WHERE a NOT IN (SELECT a FROM cstab1 WHERE a=1);
  */
  if (!isReadOnly() &&  // make sure the current table is being modified
      isUpdateOrDeleteStatement(thd->lex->sql_command))
    return doUpdateDelete(thd, gwi, condStack);

  uint32_t sessionID = tid2sid(thd->thread_id);
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  idbassert(ci != 0);

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  sm::tableid_t tableid = 0;
  cal_table_info ti;
  sm::cpsm_conhdl_t* hndl;
  SCSEP csep;

  // update traceFlags according to the autoswitch state.
  ci->traceFlags |= CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;

  bool localQuery = get_local_query(thd);

  // table mode
  {
    ti = ci->tableMap[table];

    // get connection handle for this table handler
    // re-establish table handle
    if (ti.conn_hndl)
    {
      sm::sm_cleanup(ti.conn_hndl);
      ti.conn_hndl = 0;
    }

    sm::sm_init(sessionID, &ti.conn_hndl, localQuery);
    ti.conn_hndl->csc = csc;
    hndl = ti.conn_hndl;

    try
    {
      ti.conn_hndl->connect();
    }
    catch (...)
    {
      setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto error;
    }

    // get filter plan for table
    if (ti.csep.get() == 0)
    {
      ti.csep.reset(new CalpontSelectExecutionPlan());

      SessionManager sm;
      BRM::TxnID txnID;
      txnID = sm.getTxnID(sessionID);

      if (!txnID.valid)
      {
        txnID.id = 0;
        txnID.valid = true;
      }

      QueryContext verID;
      verID = sm.verID();

      ti.csep->txnID(txnID.id);
      ti.csep->verID(verID);
      ti.csep->sessionID(sessionID);

      if (thd->db.length)
        ti.csep->schemaName(thd->db.str, lower_case_table_names);

      ti.csep->traceFlags(ci->traceFlags);
      ti.msTablePtr = table;

      // send plan whenever rnd_init is called
      cp_get_table_plan(thd, ti.csep, ti, timeZoneOffset);
    }

    IDEBUG(cerr << table->s->table_name.str << " send plan:" << endl);
    IDEBUG(cerr << *ti.csep << endl);
    csep = ti.csep;

    // for ExeMgr logging sqltext. only log once for the query although multi plans may be sent
    // CS adds the ti into TM in the end of rnd_init thus we log the SQL
    // only once when there is no ti with csep.
    if (onlyOneTableinTM(ci))
    {
      ti.csep->data(idb_mysql_query_str(thd));
    }
    else
    {
      ti.csep->data("<part of the query executed in table mode>");
    }
  }

  {
    ByteStream msg;
    ByteStream emsgBs;

    while (true)
    {
      try
      {
        ByteStream::quadbyte qb = 4;
        msg << qb;
        hndl->exeMgr->write(msg);
        msg.restart();
        csep->rmParms(ci->rmParms);

        // send plan
        csep->serialize(msg);
        hndl->exeMgr->write(msg);

        // get ExeMgr status back to indicate a vtable joblist success or not
        msg.restart();
        emsgBs.restart();
        msg = hndl->exeMgr->read();
        emsgBs = hndl->exeMgr->read();
        string emsg;

        if (msg.length() == 0 || emsgBs.length() == 0)
        {
          emsg = "Lost connection to ExeMgr. Please contact your administrator";
          setError(thd, ER_INTERNAL_ERROR, emsg);
          return ER_INTERNAL_ERROR;
        }

        string emsgStr;
        emsgBs >> emsgStr;
        bool err = false;

        if (msg.length() == 4)
        {
          msg >> qb;

          if (qb != 0)
          {
            err = true;
            // for makejoblist error, stats contains only error code and insert from here
            // because table fetch is not started
            ci->stats.setEndTime();
            ci->stats.fQuery = csep->data();
            ci->stats.fQueryType = csep->queryType();
            ci->stats.fErrorNo = qb;

            try
            {
              ci->stats.insert();
            }
            catch (std::exception& e)
            {
              string msg = string("Columnstore Query Stats - ") + e.what();
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
            }
          }
        }
        else
        {
          err = true;
        }

        if (err)
        {
          setError(thd, ER_INTERNAL_ERROR, emsgStr);
          return ER_INTERNAL_ERROR;
        }

        ci->rmParms.clear();

        ci->tableMap[table] = ti;

        break;
      }
      catch (...)
      {
        sm::sm_cleanup(hndl);
        hndl = 0;

        sm::sm_init(sessionID, &hndl, localQuery);
        idbassert(hndl != 0);
        hndl->csc = csc;

        ti.conn_hndl = hndl;

        try
        {
          hndl->connect();
        }
        catch (...)
        {
          setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
          CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
          goto error;
        }

        msg.restart();
      }
    }
  }

  // common path for both vtable select phase and table mode -- open scan handle
  ti = ci->tableMap[table];
  ti.msTablePtr = table;

  if (ti.tpl_ctx == nullptr)
  {
    ti.tpl_ctx = new sm::cpsm_tplh_t();
    ti.tpl_scan_ctx = sm::sp_cpsm_tplsch_t(new sm::cpsm_tplsch_t());
  }

  // make sure rowgroup is null so the new meta data can be taken. This is for some case mysql
  // call rnd_init for a table more than once.
  ti.tpl_scan_ctx->rowGroup = nullptr;

  try
  {
    tableid = execplan::IDB_VTABLE_ID;
  }
  catch (...)
  {
    string emsg = "No table ID found for table " + string(table->s->table_name.str);
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    goto internal_error;
  }

  try
  {
    sm::tpl_open(tableid, ti.tpl_ctx, hndl);
    sm::tpl_scan_open(tableid, ti.tpl_scan_ctx, hndl);
  }
  catch (std::exception& e)
  {
    string emsg = "table can not be opened: " + string(e.what());
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    goto internal_error;
  }
  catch (...)
  {
    string emsg = "table can not be opened";
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    goto internal_error;
  }

  ti.tpl_scan_ctx->traceFlags = ci->traceFlags;

  if ((ti.tpl_scan_ctx->ctp).size() == 0)
  {
    uint32_t num_attr = table->s->fields;

    for (uint32_t i = 0; i < num_attr; i++)
    {
      CalpontSystemCatalog::ColType ctype;
      ti.tpl_scan_ctx->ctp.push_back(ctype);
    }

    // populate coltypes here for table mode because tableband gives treeoid for dictionary column
    {
      CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(
          make_table(table->s->db.str, table->s->table_name.str, lower_case_table_names), true);

      if (oidlist.size() != num_attr)
      {
        string emsg = "Size mismatch probably caused by front end out of sync";
        setError(thd, ER_INTERNAL_ERROR, emsg);
        CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
        goto internal_error;
      }

      for (unsigned int j = 0; j < oidlist.size(); j++)
      {
        CalpontSystemCatalog::ColType ctype = csc->colType(oidlist[j].objnum);
        ti.tpl_scan_ctx->ctp[ctype.colPosition] = ctype;
        ti.tpl_scan_ctx->ctp[ctype.colPosition].colPosition = -1;
      }
    }
  }

  ci->tableMap[table] = ti;
  return 0;

error:
  // CS doesn't need to close the actual sockets
  // b/c it tries to reuse it running next query.
  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;

internal_error:

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;
}

int ha_mcs_impl_rnd_next(uchar* buf, TABLE* table, long timeZone)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return HA_ERR_END_OF_FILE;

  if (isMCSTableUpdate(thd) || isMCSTableDelete(thd))
    return HA_ERR_END_OF_FILE;

  // @bug 2547
  // MCOL-2178 This variable can never be true in the scope of this function
  //    if (MIGR::infinidb_vtable.impossibleWhereOnUnion)
  //        return HA_ERR_END_OF_FILE;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  if (ci->alterTableState > 0)
    return HA_ERR_END_OF_FILE;

  cal_table_info ti;
  ti = ci->tableMap[table];
  int rc = HA_ERR_END_OF_FILE;

  if (!ti.tpl_ctx || !ti.tpl_scan_ctx)
  {
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    return ER_INTERNAL_ERROR;
  }

  idbassert(ti.msTablePtr == table);

  try
  {
    rc = fetchNextRow(buf, ti, ci, timeZone);
  }
  catch (std::exception& e)
  {
    string emsg = string("Error while fetching from ExeMgr: ") + e.what();
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    return ER_INTERNAL_ERROR;
  }

  ci->tableMap[table] = ti;

  if (rc != 0 && rc != HA_ERR_END_OF_FILE)
  {
    string emsg;

    // remove this check when all error handling migrated to the new framework.
    if (rc >= 1000)
      emsg = ti.tpl_scan_ctx->errMsg;
    else
    {
      logging::ErrorCodes errorcodes;
      emsg = errorcodes.errorString(rc);
    }

    setError(thd, ER_INTERNAL_ERROR, emsg);
    // setError(thd, ER_INTERNAL_ERROR, "testing");
    ci->stats.fErrorNo = rc;
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    rc = ER_INTERNAL_ERROR;
  }

  return rc;
}

int ha_mcs_impl_rnd_end(TABLE* table, bool is_pushdown_hand)
{
  int rc = 0;
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return 0;

  cal_connection_info* ci = nullptr;

  if (get_fe_conn_info_ptr() != NULL)
    ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if ((thd->lex)->sql_command == SQLCOM_ALTER_TABLE)
    return rc;

  if (isMCSTableUpdate(thd) || isMCSTableDelete(thd))
    return rc;

  if (!ci)
  {
    set_fe_conn_info_ptr((void*)new cal_connection_info());
    ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  }

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    // clear querystats because no query stats available for cancelled query
    ci->queryStats = "";
    return 0;
  }

  IDEBUG(cerr << "rnd_end for table " << table->s->table_name.str << endl);

  cal_table_info ti = ci->tableMap[table];
  sm::cpsm_conhdl_t* hndl;

  if (!is_pushdown_hand)
    hndl = ti.conn_hndl;
  else
    hndl = ci->cal_conn_hndl;

  if (ti.tpl_ctx)
  {
    if (ti.tpl_scan_ctx.get())
    {
      try
      {
        sm::tpl_scan_close(ti.tpl_scan_ctx);
      }
      catch (...)
      {
        rc = ER_INTERNAL_ERROR;
      }
    }

    ti.tpl_scan_ctx.reset();

    try
    {
      {
        bool ask_4_stats = (ci->traceFlags) ? true : false;
        sm::tpl_close(ti.tpl_ctx, &hndl, ci->stats, ask_4_stats);
      }

      // set conn hndl back. could be changed in tpl_close
      if (!is_pushdown_hand)
        ti.conn_hndl = hndl;
      else
        ci->cal_conn_hndl = hndl;

      ti.tpl_ctx = 0;
    }
    catch (IDBExcept& e)
    {
      if (e.errorCode() == ERR_CROSS_ENGINE_CONNECT || e.errorCode() == ERR_CROSS_ENGINE_CONFIG)
      {
        string msg = string("Columnstore Query Stats - ") + e.what();
        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
      }
      else
      {
        setError(thd, ER_INTERNAL_ERROR, e.what());
        rc = ER_INTERNAL_ERROR;
      }
    }
    catch (std::exception& e)
    {
      setError(thd, ER_INTERNAL_ERROR, e.what());
      rc = ER_INTERNAL_ERROR;
    }
    catch (...)
    {
      setError(thd, ER_INTERNAL_ERROR, "Internal error throwed in rnd_end");
      rc = ER_INTERNAL_ERROR;
    }
  }

  ti.tpl_ctx = 0;

  ci->tableMap[table] = ti;

  // push warnings from CREATE phase
  if (!ci->warningMsg.empty())
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, ci->warningMsg.c_str());

  ci->warningMsg.clear();
  // reset expressionId just in case
  ci->expressionId = 0;

  thd_set_ha_data(thd, mcs_hton, reinterpret_cast<void*>(ci));

  return rc;
}

int ha_mcs_impl_create(const char* name, TABLE* table_arg, HA_CREATE_INFO* create_info)
{
  THD* thd = current_thd;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  //@Bug 1948. Mysql calls create table to create a new table with new signature.
  if (ci->alterTableState > 0)
    return 0;

  // Just to be sure
  if (!table_arg)
  {
    setError(thd, ER_INTERNAL_ERROR, "ha_mcs_impl_create_: table_arg is NULL");
    return 1;
  }

  if (!table_arg->s)
  {
    setError(thd, ER_INTERNAL_ERROR, "ha_mcs_impl_create_: table_arg->s is NULL");
    return 1;
  }

  int rc = ha_mcs_impl_create_(name, table_arg, create_info, *ci);

  return rc;
}

int ha_mcs_impl_delete_table(const char* name)
{
  THD* thd = current_thd;
  char* dbName = nullptr;

  if (!name)
  {
    setError(thd, ER_INTERNAL_ERROR, "Drop Table with NULL name not permitted");
    return 1;
  }

  // if this is an InfiniDB tmp table ('#sql*.frm') just leave...
  if (!memcmp((uchar*)name, tmp_file_prefix, tmp_file_prefix_length))
    return 0;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (!thd)
    return 0;

  if (!thd->lex)
    return 0;

  if (!idb_mysql_query_str(thd))
    return 0;

  if (thd->lex->sql_command == SQLCOM_DROP_DB)
  {
    dbName = const_cast<char*>(thd->lex->name.str);
  }
  else
  {
    TABLE_LIST* first_table = (TABLE_LIST*)thd->lex->first_select_lex()->table_list.first;
    dbName = const_cast<char*>(first_table->db.str);
  }

  if (!dbName)
  {
    setError(thd, ER_INTERNAL_ERROR, "Drop Table with NULL schema not permitted");
    return 1;
  }

  if (!ci)
    return 0;

  //@Bug 1948,2306. if alter table want to drop the old table, InfiniDB does not need to drop.
  if (ci->isAlter)
  {
    ci->isAlter = false;
    return 0;
  }

  int rc = ha_mcs_impl_delete_table_(dbName, name, *ci);
  return rc;
}
int ha_mcs_impl_write_row(const uchar* buf, TABLE* table, uint64_t rows_changed, long timeZone)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return 0;

  // Error out INSERT on VIEW. It's currently not supported.
  // @note INSERT on VIEW works natually (for simple cases at least), but we choose to turn it off
  // for now - ZZ.

  if (thd->lex->query_tables->view)
  {
    Message::Args args;
    args.add("Insert");
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_VIEW, args);
    setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  // At the beginning of insert, make sure there are no
  // left-over values from a previously possibly failed insert.
  if (rows_changed == 0)
    ci->tableValuesMap.clear();

  if (ci->alterTableState > 0)
    return 0;

  ha_rows rowsInserted = 0;
  int rc = 0;

  // ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::ALWAYS means ALWAYS use
  // cpimport, whether it's in a transaction or not. User should use this option
  // very carefully since cpimport currently does not support rollbacks
  if (((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ALWAYS) ||
       ((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ON) &&
        (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))) &&
      (!ci->singleInsert) &&
      ((ci->isLoaddataInfile) || ((thd->lex)->sql_command == SQLCOM_INSERT) ||
       ((thd->lex)->sql_command == SQLCOM_LOAD) || ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) ||
       ci->isCacheInsert))
  {
    rc = ha_mcs_impl_write_batch_row_(buf, table, *ci, timeZone);
  }
  else
  {
    if (!ci->dmlProc)
    {
      ci->dmlProc = new MessageQueueClient("DMLProc");
      // cout << "write_row starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
    }

    rc = ha_mcs_impl_write_row_(buf, table, *ci, rowsInserted);
  }

  //@Bug 2438 Added a variable rowsHaveInserted to keep track of how many rows have been inserted already.
  if (!ci->singleInsert && (rc == 0) && (rowsInserted > 0))
  {
    ci->rowsHaveInserted += rowsInserted;
  }

  return rc;
}

int ha_mcs_impl_update_row()
{
  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  int rc = ci->rc;

  if (rc != 0)
    ci->rc = 0;

  return (rc);
}

int ha_mcs_impl_delete_row()
{
  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  int rc = ci->rc;

  if (rc != 0)
    ci->rc = 0;

  return (rc);
}

void ha_mcs_impl_start_bulk_insert(ha_rows rows, TABLE* table, bool is_cache_insert)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  // clear rows variable
  ci->rowsHaveInserted = 0;

  if (ci->alterTableState > 0)
    return;

  //@bug 5660. Error out DDL/DML on slave node, or on local query node
  if (ci->isSlaveNode)
  {
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
    setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return;
  }

  //@bug 4771. reject REPLACE key word
  if ((thd->lex)->sql_command == SQLCOM_REPLACE_SELECT)
  {
    setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, "REPLACE statement is not supported in Columnstore.");
  }

  boost::shared_ptr<CalpontSystemCatalog> csc =
      CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(thd->thread_id));
  csc->identity(execplan::CalpontSystemCatalog::FE);

  //@Bug 2515.
  // Check command instead of vtable state
  if ((thd->lex)->sql_command == SQLCOM_INSERT)
  {
    string insertStmt = idb_mysql_query_str(thd);
    boost::algorithm::to_lower(insertStmt);
    string intoStr("into");
    size_t found = insertStmt.find(intoStr);

    if (found != string::npos)
      insertStmt.erase(found);

    found = insertStmt.find("ignore");

    if (found != string::npos)
    {
      setError(current_thd, ER_CHECK_NOT_IMPLEMENTED,
               "IGNORE option in insert statement is not supported in Columnstore.");
    }

    if (rows > 1)
    {
      ci->singleInsert = false;
    }
  }
  else if ((thd->lex)->sql_command == SQLCOM_LOAD || (thd->lex)->sql_command == SQLCOM_INSERT_SELECT)
  {
    ci->singleInsert = false;
    ci->isLoaddataInfile = true;
  }

  if (is_cache_insert && (thd->lex)->sql_command != SQLCOM_INSERT_SELECT)
  {
    ci->isCacheInsert = true;

    if (rows > 1)
      ci->singleInsert = false;
  }

  ci->bulkInsertRows = rows;

  if ((((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
       (thd->lex)->sql_command == SQLCOM_INSERT_SELECT || ci->isCacheInsert) &&
      !ci->singleInsert)
  {
    ci->useCpimport = get_use_import_for_batchinsert_mode(thd);

    if (((thd->lex)->sql_command == SQLCOM_INSERT) && (rows > 0))
      ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::OFF;

    if (ci->isCacheInsert)
    {
      if (get_cache_use_import(thd))
        ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::ALWAYS;
      else
        ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::OFF;
    }

    // ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::ALWAYS means ALWAYS use
    // cpimport, whether it's in a transaction or not. User should use this option
    // very carefully since cpimport currently does not support rollbacks
    if ((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ALWAYS) ||
        ((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ON) &&
         (!(thd->variables.option_bits &
            (OPTION_NOT_AUTOCOMMIT |
             OPTION_BEGIN)))))  // If autocommit on batch insert will use cpimport to load data
    {
      // store table info to connection info
      CalpontSystemCatalog::TableName tableName;
      tableName.schema = table->s->db.str;
      tableName.table = table->s->table_name.str;
      ci->useXbit = false;
      CalpontSystemCatalog::RIDList colrids;

      try
      {
        colrids = csc->columnRIDs(tableName, false, lower_case_table_names);
      }
      catch (IDBExcept& ie)
      {
        // TODO Can't use ERR_UNKNOWN_TABLE because it needs two
        // arguments to format. Update setError to take vararg.
        //				setError(thd, ER_UNKNOWN_TABLE, ie.what());
        setError(thd, ER_INTERNAL_ERROR, ie.what());
        ci->rc = 5;
        ci->singleInsert = true;
        return;
      }

      ci->useXbit = table->s->db_options_in_use & HA_OPTION_PACK_RECORD;

      // TODO: This needs a proper fix.
      if (is_cache_insert)
        ci->useXbit = false;

      //@bug 6122 Check how many columns have not null constraint. columnn with not null constraint will not
      //show up in header.
      unsigned int numberNotNull = 0;

      for (unsigned int j = 0; j < colrids.size(); j++)
      {
        CalpontSystemCatalog::ColType ctype = csc->colType(colrids[j].objnum);
        ci->columnTypes.push_back(ctype);

        if (((ctype.colDataType == CalpontSystemCatalog::VARCHAR) ||
             (ctype.colDataType == CalpontSystemCatalog::VARBINARY)) &&
            !ci->useXbit)
          ci->useXbit = true;

        if (ctype.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT)
          numberNotNull++;
      }

      // The length of the record header is:(1 + number of columns + 7) / 8 bytes
      if (ci->useXbit)
        ci->headerLength = (1 + colrids.size() + 7 - 1 - numberNotNull) / 8;  // xbit is used
      else
        ci->headerLength = (1 + colrids.size() + 7 - numberNotNull) / 8;

      // Log the statement to debug.log
      {
        ostringstream oss;
        oss << "Start SQL statement: " << idb_mysql_query_str(thd) << "; |" << table->s->db.str << "|";
        ha_mcs_impl::log_this(thd, oss.str().c_str(), logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
      }

      // start process cpimport mode 1
      ci->mysqld_pid = getpid();

      // get delimiter
      if (char(get_import_for_batchinsert_delimiter(thd)) != '\007')
        ci->delimiter = char(get_import_for_batchinsert_delimiter(thd));
      else
        ci->delimiter = '\007';

      // get enclosed by
      if (char(get_import_for_batchinsert_enclosed_by(thd)) != 8)
        ci->enclosed_by = char(get_import_for_batchinsert_enclosed_by(thd));
      else
        ci->enclosed_by = 8;

      // cout << "current set up is usecpimport:delimiter = " << (int)ci->useCpimport<<":"<< ci->delimiter
      // <<endl; set up for cpimport
      std::vector<char*> Cmds;
      std::string aCmdLine;
      std::string aTmpDir(startup::StartUp::tmpDir());

      // If local module type is not PM and Local PM query is set, error out
      char escapechar[2] = "";

      if (ci->enclosed_by == 34)  // Double quotes
        strcat(escapechar, "\\");

      if (get_local_query(thd))
      {
        const auto oamcache = oam::OamCache::makeOamCache();
        int localModuleId = oamcache->getLocalPMId();

        if (localModuleId == 0)
        {
          setError(current_thd, ER_INTERNAL_ERROR,
                   logging::IDBErrorInfo::instance()->errorMsg(ERR_LOCAL_QUERY_UM));
          ci->singleInsert = true;
          ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
          return;
        }
        else
        {
#ifdef _MSC_VER
          aCmdLine = "cpimport.exe -N -P " + to_string(localModuleId) + " -s " + ci->delimiter + " -e 0" +
                     " -E " + escapechar + ci->enclosed_by + " ";
#else
          aCmdLine = "cpimport -m 1 -N -P " + boost::to_string(localModuleId) + " -s " + ci->delimiter +
                     " -e 0" + " -T " + thd->variables.time_zone->get_name()->ptr() + " -E " + escapechar +
                     ci->enclosed_by + " ";
#endif
        }
      }
      else
      {
#ifdef _MSC_VER
        aCmdLine =
            "cpimport.exe -N -s " + ci->delimiter + " -e 0" + " -E " + escapechar + ci->enclosed_by + " ";
#else
        aCmdLine = std::string("cpimport -m 1 -N -s ") + ci->delimiter + " -e 0" + " -T " +
                   thd->variables.time_zone->get_name()->ptr() + " -E " + escapechar + ci->enclosed_by + " ";
#endif
      }

      aCmdLine = aCmdLine + table->s->db.str + " " + table->s->table_name.str;

      std::istringstream ss(aCmdLine);
      std::string arg;
      std::vector<std::string> v2(20, "");
      unsigned int i = 0;

      while (ss >> arg)
      {
        v2[i++] = arg;
      }

      for (unsigned int j = 0; j < i; ++j)
      {
        Cmds.push_back(const_cast<char*>(v2[j].c_str()));
      }

      Cmds.push_back(0);  // null terminate

#ifdef _MSC_VER
      BOOL bSuccess = false;
      BOOL bInitialized = false;
      SECURITY_ATTRIBUTES saAttr;
      saAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
      saAttr.bInheritHandle = TRUE;
      saAttr.lpSecurityDescriptor = nullptr;
      HANDLE handleList[2];
      const char* pSectionMsg;
      bSuccess = true;

      // Create a pipe for the child process's STDIN.
      if (bSuccess)
      {
        pSectionMsg = "Create Stdin";
        bSuccess = CreatePipe(&ci->cpimport_stdin_Rd, &ci->cpimport_stdin_Wr, &saAttr, 65536);

        // Ensure the write handle to the pipe for STDIN is not inherited.
        if (bSuccess)
        {
          pSectionMsg = "SetHandleInformation(stdin)";
          bSuccess = SetHandleInformation(ci->cpimport_stdin_Wr, HANDLE_FLAG_INHERIT, 0);
        }
      }

      // Launch cpimport
      LPPROC_THREAD_ATTRIBUTE_LIST lpAttributeList = nullptr;
      SIZE_T attrSize = 0;
      STARTUPINFOEX siStartInfo;

      // To ensure the child only inherits the STDIN and STDOUT Handles, we add a list of
      // Handles that can be inherited to the call to CreateProcess
      if (bSuccess)
      {
        pSectionMsg = "InitializeProcThreadAttributeList(NULL)";
        bSuccess = InitializeProcThreadAttributeList(NULL, 1, 0, &attrSize) ||
                   GetLastError() == ERROR_INSUFFICIENT_BUFFER;  // Asks how much buffer to alloc
      }

      if (bSuccess)
      {
        pSectionMsg = "HeapAlloc for AttrList";
        lpAttributeList =
            reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(HeapAlloc(GetProcessHeap(), 0, attrSize));
        bSuccess = lpAttributeList != nullptr;
      }

      if (bSuccess)
      {
        pSectionMsg = "InitializeProcThreadAttributeList";
        bSuccess = InitializeProcThreadAttributeList(lpAttributeList, 1, 0, &attrSize);
      }

      if (bSuccess)
      {
        pSectionMsg = "UpdateProcThreadAttribute";
        bInitialized = true;
        handleList[0] = ci->cpimport_stdin_Rd;
        bSuccess = UpdateProcThreadAttribute(lpAttributeList, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                                             handleList, sizeof(HANDLE), NULL, NULL);
      }

      if (bSuccess)
      {
        pSectionMsg = "CreateProcess";
        // In order for GenerateConsoleCtrlEvent (used when job is canceled) to work,
        // this process must have a Console, which Services don't have. We create this
        // when we create the child process. Once created, we leave it around for next time.
        // AllocConsole will silently fail if it already exists, so no pain.
        AllocConsole();
        // Set up members of the PROCESS_INFORMATION structure.
        memset(&ci->cpimportProcInfo, 0, sizeof(PROCESS_INFORMATION));

        // Set up members of the STARTUPINFOEX structure.
        // This structure specifies the STDIN and STDOUT handles for redirection.
        memset(&siStartInfo, 0, sizeof(STARTUPINFOEX));
        siStartInfo.StartupInfo.cb = sizeof(STARTUPINFOEX);
        siStartInfo.lpAttributeList = lpAttributeList;
        siStartInfo.StartupInfo.hStdError = nullptr;
        siStartInfo.StartupInfo.hStdOutput = nullptr;
        siStartInfo.StartupInfo.hStdInput = ci->cpimport_stdin_Rd;
        siStartInfo.StartupInfo.dwFlags |= STARTF_USESTDHANDLES;
        // Create the child process.
        bSuccess = CreateProcess(NULL,                                 // program. NULL means use command line
                                 const_cast<LPSTR>(aCmdLine.c_str()),  // command line
                                 NULL,                                 // process security attributes
                                 NULL,                                 // primary thread security attributes
                                 TRUE,                                 // handles are inherited
                                 EXTENDED_STARTUPINFO_PRESENT | CREATE_NEW_PROCESS_GROUP,  // creation flags
                                 NULL,                      // use parent's environment
                                 NULL,                      // use parent's current directory
                                 &siStartInfo.StartupInfo,  // STARTUPINFO pointer
                                 &ci->cpimportProcInfo);    // receives PROCESS_INFORMATION
      }

      // We need to clean up the memory created by InitializeProcThreadAttributeList
      // and HeapAlloc
      if (bInitialized)
        DeleteProcThreadAttributeList(lpAttributeList);

      if (lpAttributeList)
        HeapFree(GetProcessHeap(), 0, lpAttributeList);

      if (!bSuccess)
      {
        // If an error occurs, Log and return.
        int errnum = GetLastError();
        char errmsg[512];
        FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, errnum, 0, errmsg, 512, NULL);
        ostringstream oss;
        oss << " : Error in " << pSectionMsg << " (errno-" << errnum << "); " << errmsg;
        setError(current_thd, ER_INTERNAL_ERROR, oss.str());
        ci->singleInsert = true;
        ha_mcs_impl::log_this(thd, oss.str(), logging::LOG_TYPE_ERROR, tid2sid(thd->thread_id));
        ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
        return;
      }

      // Close the read handle that the child is using. We won't be needing this.
      CloseHandle(ci->cpimport_stdin_Rd);
      // The write functions all want a FILE*
      ci->fdt[1] = _open_osfhandle((intptr_t)ci->cpimport_stdin_Wr, _O_APPEND);
      ci->filePtr = _fdopen(ci->fdt[1], "w");
#else
      long maxFD = -1;
      maxFD = sysconf(_SC_OPEN_MAX);

      if (pipe(ci->fdt) == -1)
      {
        int errnum = errno;
        ostringstream oss;
        oss << " : Error in creating pipe (errno-" << errnum << "); " << strerror(errnum);
        setError(current_thd, ER_INTERNAL_ERROR, oss.str());
        ci->singleInsert = true;
        ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
        return;
      }

      // cout << "maxFD = " << maxFD <<endl;
      errno = 0;
      pid_t aChPid = fork();

      if (aChPid == -1)  // an error caused
      {
        int errnum = errno;
        ostringstream oss;
        oss << " : Error in forking cpimport.bin (errno-" << errnum << "); " << strerror(errnum);
        setError(current_thd, ER_INTERNAL_ERROR, oss.str());
        ci->singleInsert = true;
        ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
        return;
      }
      else if (aChPid == 0)  // we are in child
      {
        for (int i = 0; i < maxFD; i++)
        {
          if (i != ci->fdt[0])
            close(i);
        }

        errno = 0;

        if (dup2(ci->fdt[0], 0) < 0)  // make stdin be the reading end of the pipe
        {
          setError(current_thd, ER_INTERNAL_ERROR, "dup2 failed");
          ci->singleInsert = true;
          exit(1);
        }

        close(ci->fdt[0]);  // will trigger an EOF on stdin
        ci->fdt[0] = -1;
        open("/dev/null", O_WRONLY);
        open("/dev/null", O_WRONLY);
        errno = 0;
        execvp(Cmds[0], &Cmds[0]);  // NOTE - works with full Path

        int execvErrno = errno;

        ostringstream oss;
        oss << " : execvp error: cpimport.bin invocation failed; "
            << "(errno-" << errno << "); " << strerror(execvErrno)
            << "; Check file and try invoking locally.";
        cout << oss.str();

        setError(current_thd, ER_INTERNAL_ERROR, "Forking process cpimport failed.");
        ci->singleInsert = true;
        ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
        exit(1);
      }
      else  // parent
      {
        ci->filePtr = fdopen(ci->fdt[1], "w");
        ci->cpimport_pid = aChPid;  // This is the child PID
        close(ci->fdt[0]);          // close the READER of PARENT
        ci->fdt[0] = -1;
        // now we can send all the data thru FIFO[1], writer of PARENT
      }
      // Set read_set used for bulk insertion of Fields inheriting
      // from Field_blob|Field_varstring. Used in ColWriteBatchString()
      bitmap_set_all(table->read_set);

#endif
    }
    else
    {
      if (!ci->dmlProc)
      {
        ci->dmlProc = new MessageQueueClient("DMLProc");
      }
    }
  }

  // Save table oid for commit to use
  if ((((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
       (thd->lex)->sql_command == SQLCOM_INSERT_SELECT) ||
      ci->isCacheInsert)
  {
    // query stats. only collect execution time and rows inserted for insert/load_data_infile
    ci->stats.reset();
    ci->stats.setStartTime();
    if (thd->main_security_ctx.user)
    {
      ci->stats.fUser = thd->main_security_ctx.user;
    }
    else
    {
      ci->stats.fUser = "";
    }

    if (thd->main_security_ctx.host)
      ci->stats.fHost = thd->main_security_ctx.host;
    else if (thd->main_security_ctx.host_or_ip)
      ci->stats.fHost = thd->main_security_ctx.host_or_ip;
    else
      ci->stats.fHost = "unknown";

    ci->stats.fSessionID = thd->thread_id;
    ci->stats.fQuery = idb_mysql_query_str(thd);

    try
    {
      ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore User Priority - ") + e.what();
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
    }

    if ((thd->lex)->sql_command == SQLCOM_INSERT)
      ci->stats.fQueryType =
          CalpontSelectExecutionPlan::queryTypeToString(CalpontSelectExecutionPlan::INSERT);
    else if ((thd->lex)->sql_command == SQLCOM_LOAD)
      ci->stats.fQueryType =
          CalpontSelectExecutionPlan::queryTypeToString(CalpontSelectExecutionPlan::LOAD_DATA_INFILE);

    //@Bug 4387. Check BRM status before start statement.
    boost::scoped_ptr<DBRM> dbrmp(new DBRM());
    int rc = dbrmp->isReadWrite();

    if (rc != 0)
    {
      setError(current_thd, ER_READ_ONLY_MODE, "Cannot execute the statement. DBRM is read only!");
      ci->rc = rc;
      ci->singleInsert = true;
      bitmap_clear_all(table->read_set);
      return;
    }

    uint32_t stateFlags;
    dbrmp->getSystemState(stateFlags);

    if (stateFlags & SessionManagerServer::SS_SUSPENDED)
    {
      setError(current_thd, ER_INTERNAL_ERROR, "Writing to the database is disabled.");
      bitmap_clear_all(table->read_set);
      return;
    }

    CalpontSystemCatalog::TableName tableName;
    tableName.schema = table->s->db.str;
    tableName.table = table->s->table_name.str;

    try
    {
      CalpontSystemCatalog::ROPair roPair = csc->tableRID(tableName, lower_case_table_names);
      ci->tableOid = roPair.objnum;
    }
    catch (IDBExcept& ie)
    {
      bitmap_clear_all(table->read_set);
      setError(thd, ER_INTERNAL_ERROR, ie.what());
    }
    catch (std::exception& ex)
    {
      bitmap_clear_all(table->read_set);
      setError(thd, ER_INTERNAL_ERROR,
               logging::IDBErrorInfo::instance()->errorMsg(ERR_SYSTEM_CATALOG) + ex.what());
    }
  }

  if (ci->rc != 0)
    ci->rc = 0;
}

int ha_mcs_impl_end_bulk_insert(bool abort, TABLE* table)
{
  // Clear read_set used for bulk insertion of Fields inheriting
  // from Field_blob|Field_varstring
  bitmap_clear_all(table->read_set);
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return 0;

  std::string aTmpDir(startup::StartUp::tmpDir());

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  int rc = 0;

  if (ci->rc == 5)  // read only dbrm
    return rc;

  // @bug 2378. do not enter for select, reset singleInsert flag after multiple insert.
  // @bug 2515. Check command intead of vtable state
  if ((((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
       (thd->lex)->sql_command == SQLCOM_INSERT_SELECT || ci->isCacheInsert) &&
      !ci->singleInsert)
  {
    if (((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ALWAYS) ||
         ((ci->useCpimport == mcs_use_import_for_batchinsert_mode_t::ON) &&
          (!(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))) &&
        (!ci->singleInsert) &&
        ((ci->isLoaddataInfile) || ((thd->lex)->sql_command == SQLCOM_INSERT) ||
         ((thd->lex)->sql_command == SQLCOM_LOAD) || ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) ||
         ci->isCacheInsert))
    {
#ifdef _MSC_VER

      if (thd->killed > 0)
      {
        errno = 0;
        // GenerateConsoleCtrlEvent sends a signal to cpimport
        BOOL brtn = GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, ci->cpimportProcInfo.dwProcessId);

        if (!brtn)
        {
          int errnum = GetLastError();
          char errmsg[512];
          FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, errnum, 0, errmsg, 512, NULL);
          ostringstream oss;
          oss << "GenerateConsoleCtrlEvent: (errno-" << errnum << "); " << errmsg;
          ha_mcs_impl::log_this(thd, oss.str(), logging::LOG_TYPE_DEBUG, 0);
        }

        // Close handles to the cpimport process and its primary thread.
        fclose(ci->filePtr);
        ci->filePtr = 0;
        ci->fdt[1] = -1;
        CloseHandle(ci->cpimportProcInfo.hProcess);
        CloseHandle(ci->cpimportProcInfo.hThread);
        WaitForSingleObject(ci->cpimportProcInfo.hProcess, INFINITE);
      }

#else

      if ((thd->killed > 0) && (ci->cpimport_pid > 0))  // handle CTRL-C
      {
        // cout << "sending ctrl-c to cpimport" << endl;
        errno = 0;
        kill(ci->cpimport_pid, SIGUSR1);
        fclose(ci->filePtr);
        ci->filePtr = 0;
        ci->fdt[1] = -1;
        int aStatus;
        waitpid(ci->cpimport_pid, &aStatus, 0);  // wait until cpimport finishs
      }

#endif
      else
      {
        // tear down cpimport
#ifdef _MSC_VER
        fclose(ci->filePtr);
        ci->filePtr = 0;
        ci->fdt[1] = -1;
        DWORD exitCode;
        WaitForSingleObject(ci->cpimportProcInfo.hProcess, INFINITE);
        GetExitCodeProcess(ci->cpimportProcInfo.hProcess, &exitCode);

        if (exitCode != 0)
        {
          rc = 1;
          setError(thd, ER_INTERNAL_ERROR,
                   "load failed. The detailed error information is listed in InfiniDBLog.txt.");
        }

        // Close handles to the cpimport process and its primary thread.
        CloseHandle(ci->cpimportProcInfo.hProcess);
        CloseHandle(ci->cpimportProcInfo.hThread);
#else
        fclose(ci->filePtr);
        ci->filePtr = 0;
        ci->fdt[1] = -1;
        int aStatus;
        pid_t aPid = waitpid(ci->cpimport_pid, &aStatus, 0);  // wait until cpimport finishs

        if ((aPid == ci->cpimport_pid) || (aPid == -1))
        {
          ci->cpimport_pid = 0;

          if ((WIFEXITED(aStatus)) && (WEXITSTATUS(aStatus) == 0))
          {
            // cout << "\tCpimport exit on success" << endl;
          }
          else
          {
            if (WEXITSTATUS(aStatus) == 2)
            {
              rc = 1;
              ifstream dmlFile;
              ostringstream oss;
              oss << aTmpDir << ci->tableOid << ".txt";
              dmlFile.open(oss.str().c_str());

              if (dmlFile.is_open())
              {
                string line;
                getline(dmlFile, line);
                setError(thd, ER_INTERNAL_ERROR, line);
                dmlFile.close();
                remove(oss.str().c_str());
              }
            }
            else
            {
              rc = 1;
              ifstream dmlFile;
              ostringstream oss;
              oss << aTmpDir << ci->tableOid << ".txt";
              dmlFile.open(oss.str().c_str());

              if (dmlFile.is_open())
              {
                string line;
                getline(dmlFile, line);
                setError(thd, ER_INTERNAL_ERROR, line);
                dmlFile.close();
                remove(oss.str().c_str());
              }
              else
                setError(thd, ER_INTERNAL_ERROR,
                         "load failed. The detailed error information is listed in err.log.");
            }
          }
        }

#endif
        if (rc == 0)
        {
          ha_mcs_impl::log_this(thd, "End SQL statement", logging::LOG_TYPE_DEBUG, tid2sid(thd->thread_id));
        }
        else
        {
          ha_mcs_impl::log_this(thd, "End SQL statement with error", logging::LOG_TYPE_DEBUG,
                                tid2sid(thd->thread_id));
        }

        ci->columnTypes.clear();
        // get extra warning count if any
        ifstream dmlFile;
        ostringstream oss;
        oss << aTmpDir << ci->tableOid << ".txt";
        dmlFile.open(oss.str().c_str());
        int totalWarnCount = 0;
        int colWarns = 0;
        string line;

        if (dmlFile.is_open())
        {
          while (getline(dmlFile, line))
          {
            colWarns = atoi(line.c_str());
            totalWarnCount += colWarns;
          }

          dmlFile.close();
          remove(oss.str().c_str());

          for (int i = 0; i < totalWarnCount; i++)
            push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, "Values saturated");
        }
      }
    }
    else
    {
      if (thd->killed > 0)
        abort = true;

      if (!ci->dmlProc)
      {
        ci->dmlProc = new MessageQueueClient("DMLProc");
        // cout << "end_bulk_insert starts a client " << ci->dmlProc << " for session " << thd->thread_id <<
        // endl;
      }

      if (((thd->lex)->sql_command == SQLCOM_INSERT_SELECT) || ((thd->lex)->sql_command == SQLCOM_LOAD))
        rc = ha_mcs_impl_write_last_batch(table, *ci, abort);
    }
  }

  // populate query stats for insert and load data infile. insert select has
  // stats entered in sm already
  if (((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_LOAD) ||
      ci->isCacheInsert)
  {
    ci->stats.setEndTime();
    ci->stats.fErrorNo = rc;

    if (ci->singleInsert)
      ci->stats.fRows = 1;
    else
      ci->stats.fRows = ci->rowsHaveInserted;

    try
    {
      ci->stats.insert();
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore Query Stats - ") + e.what();
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
    }
  }

  // MCOL-4002 We earlier had these re-initializations set only for
  // non-transactions, i.e.:
  // !(thd->variables.option_bits & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
  // However, we should be resetting these members anyways.
  ci->singleInsert = true;  // reset the flag
  ci->isLoaddataInfile = false;
  ci->isCacheInsert = false;
  ci->tableOid = 0;
  ci->rowsHaveInserted = 0;
  ci->useCpimport = mcs_use_import_for_batchinsert_mode_t::ON;

  return rc;
}

int ha_mcs_impl_commit(handlerton* hton, THD* thd, bool all)
{
  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (ci->isAlter)
    return 0;

  //@Bug 5823 check if any active transaction for this session
  boost::scoped_ptr<DBRM> dbrmp(new DBRM());
  BRM::TxnID txnId = dbrmp->getTxnID(tid2sid(thd->thread_id));

  if (!txnId.valid)
    return 0;

  if (!ci->dmlProc)
  {
    ci->dmlProc = new MessageQueueClient("DMLProc");
    // cout << "commit starts a client " << ci->dmlProc << " for session " << thd->thread_id << endl;
  }

  int rc = ha_mcs_impl_commit_(hton, thd, all, *ci);
  thd->server_status &= ~SERVER_STATUS_IN_TRANS;
  ci->singleInsert = true;  // reset the flag
  ci->isLoaddataInfile = false;
  ci->isCacheInsert = false;
  ci->tableOid = 0;
  ci->rowsHaveInserted = 0;
  return rc;
}

int ha_mcs_impl_rollback(handlerton* hton, THD* thd, bool all)
{
  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (!ci->dmlProc)
  {
    ci->dmlProc = new MessageQueueClient("DMLProc");
  }

  int rc = ha_mcs_impl_rollback_(hton, thd, all, *ci);
  ci->singleInsert = true;  // reset the flag
  ci->isLoaddataInfile = false;
  ci->isCacheInsert = false;
  ci->tableOid = 0;
  ci->rowsHaveInserted = 0;
  thd->server_status &= ~SERVER_STATUS_IN_TRANS;
  return rc;
}

int ha_mcs_impl_close_connection(handlerton* hton, THD* thd)
{
  if (!thd)
    return 0;

  if (thd->thread_id == 0)
    return 0;

  execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));

  // MCOL-3247 Use THD::ha_data as a per-plugin per-session
  // storage. Filled in external_lock when we remove a lock
  // from vtable(lock_type = 2)
  // An ugly way. I will use ha_data w/o external_lock.
  // This in MCOL-2178
  cal_connection_info* ci = nullptr;
  if (thd_get_ha_data(thd, hton) != (void*)0x42)  // 0x42 is the magic CS sets when setup hton
  {
    ci = reinterpret_cast<cal_connection_info*>(thd_get_ha_data(thd, hton));
  }

  if (!ci)
    return 0;

  int rc = 0;

  if (ci->dmlProc)
  {
    rc = ha_mcs_impl_close_connection_(hton, thd, *ci);
    delete ci->dmlProc;
    ci->dmlProc = nullptr;
  }

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return rc;
}

int ha_mcs_impl_rename_table(const char* from, const char* to)
{
  IDEBUG(cout << "ha_mcs_impl_rename_table: " << from << " => " << to << endl);

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  //@Bug 1948. Alter table call rename table twice
  if (ci->alterTableState == cal_connection_info::ALTER_FIRST_RENAME)
  {
    ci->alterTableState = cal_connection_info::ALTER_SECOND_RENAME;
    IDEBUG(cout << "ha_mcs_impl_rename_table: was in state ALTER_FIRST_RENAME, now in ALTER_SECOND_RENAME"
                << endl);
    return 0;
  }
  else if (ci->alterTableState == cal_connection_info::ALTER_SECOND_RENAME)
  {
    ci->alterTableState = cal_connection_info::NOT_ALTER;
    IDEBUG(cout << "ha_mcs_impl_rename_table: was in state ALTER_SECOND_RENAME, now in NOT_ALTER" << endl);
    return 0;
  }

  int rc = ha_mcs_impl_rename_table_(from, to, *ci);
  return rc;
}

int ha_mcs_impl_delete_row(const uchar* buf)
{
  IDEBUG(cout << "ha_mcs_impl_delete_row" << endl);
  return 0;
}

COND* ha_mcs_impl_cond_push(COND* cond, TABLE* table, std::vector<COND*>& condStack)
{
  THD* thd = current_thd;

  if (isUpdateOrDeleteStatement(thd->lex->sql_command))
  {
    condStack.push_back(cond);
    return nullptr;
  }

  string alias;
  alias.assign(table->alias.ptr(), table->alias.length());
  IDEBUG(cout << "ha_mcs_impl_cond_push: " << alias << endl);

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  cal_table_info ti = ci->tableMap[table];

#ifdef DEBUG_WALK_COND
  {
    const char* timeZone = thd->variables.time_zone->get_name()->ptr();
    long timeZoneOffset;
    dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
    gp_walk_info gwi(timeZoneOffset);
    gwi.condPush = true;
    gwi.sessionid = tid2sid(thd->thread_id);
    cout << "------------------ cond push -----------------------" << endl;
    cond->traverse_cond(debug_walk, &gwi, Item::POSTFIX);
    cout << "------------------------------------------------\n" << endl;
  }
#endif

  if (!ti.csep)
  {
    if (!ti.condInfo)
    {
      const char* timeZone = thd->variables.time_zone->get_name()->ptr();
      long timeZoneOffset;
      dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
      ti.condInfo = new gp_walk_info(timeZoneOffset);
    }

    gp_walk_info* gwi = ti.condInfo;
    gwi->dropCond = false;
    gwi->fatalParseError = false;
    gwi->condPush = true;
    gwi->thd = thd;
    gwi->sessionid = tid2sid(thd->thread_id);
    cond->traverse_cond(gp_walk, gwi, Item::POSTFIX);
    ci->tableMap[table] = ti;

    if (gwi->fatalParseError)
    {
      IDEBUG(cout << gwi->parseErrorText << endl);

      if (ti.condInfo)
      {
        delete ti.condInfo;
        ti.condInfo = nullptr;
        ci->tableMap[table] = ti;
      }

      return cond;
    }

    if (gwi->dropCond)
    {
      return cond;
    }
    else
    {
      return nullptr;
    }
  }

  return cond;
}

inline void disableBinlogForDML(THD* thd)
{
  if (isDMLStatement(thd->lex->sql_command) && (thd->variables.option_bits & OPTION_BIN_LOG))
  {
    set_original_option_bits(thd->variables.option_bits, thd);
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
    thd->variables.option_bits |= OPTION_BIN_TMP_LOG_OFF;
  }
}

inline void restoreBinlogForDML(THD* thd)
{
  if (isDMLStatement(thd->lex->sql_command))
  {
    ulonglong orig_option_bits = get_original_option_bits(thd);

    if (orig_option_bits)
    {
      thd->variables.option_bits = orig_option_bits;
      set_original_option_bits(0, thd);
    }
  }
}

int ha_mcs::impl_external_lock(THD* thd, TABLE* table, int lock_type)
{
  // @bug 3014. Error out locking table command. IDB does not support it now.
  if (thd->lex->sql_command == SQLCOM_LOCK_TABLES)
  {
    setError(current_thd, ER_CHECK_NOT_IMPLEMENTED,
             logging::IDBErrorInfo::instance()->errorMsg(ERR_LOCK_TABLE));
    return ER_CHECK_NOT_IMPLEMENTED;
  }

  // @info called for every table at the beginning and at the end of a query.
  // used for cleaning up the tableinfo.
  string alias;
  alias.assign(table->alias.ptr(), table->alias.length());
  IDEBUG(cout << "external_lock for " << alias << endl);

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    ci->physTablesList.clear();
    ci->tableMap.clear();
    force_close_fep_conn(thd, ci);
    return 0;
  }

  m_lock_type = lock_type;

  CalTableMap::iterator mapiter = ci->tableMap.find(table);
  // make sure this is a release lock (2nd) call called in
  // the table mode.
  if (mapiter != ci->tableMap.end() && (mapiter->second.condInfo || mapiter->second.csep) && lock_type == 2)
  {
    // CS ends up processing query with handlers
    // table mode
    if (mapiter->second.conn_hndl)
    {
      if (ci->traceFlags & 1)
        push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, 9999,
                     mapiter->second.conn_hndl->queryStats.c_str());

      ci->queryStats = mapiter->second.conn_hndl->queryStats;
      ci->extendedStats = mapiter->second.conn_hndl->extendedStats;
      ci->miniStats = mapiter->second.conn_hndl->miniStats;
      sm::sm_cleanup(mapiter->second.conn_hndl);
      mapiter->second.conn_hndl = nullptr;
    }

    if (mapiter->second.condInfo)
    {
      delete mapiter->second.condInfo;
      mapiter->second.condInfo = nullptr;
    }

    // MCOL-2178 Check for tableMap size to set this only once.
    ci->queryState = 0;
    // Clean up the tableMap and physTablesList
    ci->tableMap.erase(table);
    ci->physTablesList.erase(table);
    thd->variables.in_subquery_conversion_threshold = IN_SUBQUERY_CONVERSION_THRESHOLD;
    restore_optimizer_flags(thd);
    restoreBinlogForDML(thd);
  }
  else
  {
    if ((lock_type == 0) || (lock_type == 1))
    {
      ci->physTablesList.insert(table);
      // MCOL-2178 Disable Conversion of Big IN Predicates Into Subqueries
      thd->variables.in_subquery_conversion_threshold = ~0;
      // Early optimizer_switch changes to avoid unsupported opt-s.
      mutate_optimizer_flags(thd);

      // MCOL-4936 Disable binlog for DMLs
      if (lock_type == 1)
      {
        disableBinlogForDML(thd);
      }
    }
    else if (lock_type == 2)
    {
      restoreBinlogForDML(thd);
      std::set<TABLE*>::iterator iter = ci->physTablesList.find(table);
      if (iter != ci->physTablesList.end())
      {
        ci->physTablesList.erase(table);
      }

      // CS ends up processing query with handlers
      if (iter != ci->physTablesList.end() && ci->physTablesList.empty())
      {
        if (!ci->cal_conn_hndl)
          return 0;

        if (ci->traceFlags & 1)
          push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, 9999, ci->cal_conn_hndl->queryStats.c_str());

        ci->queryStats = ci->cal_conn_hndl->queryStats;
        ci->extendedStats = ci->cal_conn_hndl->extendedStats;
        ci->miniStats = ci->cal_conn_hndl->miniStats;
        ci->queryState = 0;
        // MCOL-3247 Use THD::ha_data as a per-plugin per-session
        // storage for cal_conn_hndl to use it later in close_connection
        thd_set_ha_data(thd, mcs_hton, get_fe_conn_info_ptr());
        // Clean up all tableMap entries made by cond_push
        for (auto& tme : ci->tableMap)
        {
          if (tme.second.condInfo)
          {
            delete tme.second.condInfo;
            tme.second.condInfo = nullptr;
          }
        }
        ci->tableMap.clear();
        // MCOL-2178 Enable Conversion of Big IN Predicates Into Subqueries
        thd->variables.in_subquery_conversion_threshold = IN_SUBQUERY_CONVERSION_THRESHOLD;
        restore_optimizer_flags(thd);
      }
    }
  }

  return 0;
}

// for sorting length exceeds blob limit. Just error out for now.
int ha_mcs_impl_rnd_pos(uchar* buf, uchar* pos)
{
  IDEBUG(cout << "ha_mcs_impl_rnd_pos" << endl);
  string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_ORDERBY_TOO_BIG);
  setError(current_thd, ER_INTERNAL_ERROR, emsg);
  return ER_INTERNAL_ERROR;
}

/*@brief ha_mcs_impl_group_by_init - Get data for MariaDB group_by
    pushdown handler */
/***********************************************************
 * DESCRIPTION:
 * Prepares data for group_by_handler::next_row() calls.
 * PARAMETERS:
 *    group_hand - group by handler, that preserves initial table and items lists. .
 *    table - TABLE pointer The table to save the result set into.
 * RETURN:
 *    0 if success
 *    others if something went wrong whilst getting the result set
 ***********************************************************/
int ha_mcs_impl_group_by_init(mcs_handler_info* handler_info, TABLE* table)
{
  ha_mcs_group_by_handler* group_hand = reinterpret_cast<ha_mcs_group_by_handler*>(handler_info->hndl_ptr);
  string tableName = group_hand->table_list->table->s->table_name.str;
  IDEBUG(cout << "group_by_init for table " << tableName << endl);
  THD* thd = current_thd;

  // check whether the system is ready to process statement.
#ifndef _MSC_VER
  static DBRM dbrm(true);
  int bSystemQueryReady = dbrm.getSystemQueryReady();

  if (bSystemQueryReady == 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "The system is not yet ready to accept queries");
    return ER_INTERNAL_ERROR;
  }
  else if (bSystemQueryReady < 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "DBRM is not responding. Cannot accept queries");
    return ER_INTERNAL_ERROR;
  }

#endif

  uint32_t sessionID = tid2sid(thd->thread_id);
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  idbassert(ci != 0);

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  sm::tableid_t tableid = 0;
  cal_table_info ti;
  cal_group_info gi;
  sm::cpsm_conhdl_t* hndl;
  SCSEP csep;

  bool localQuery = get_local_query(thd);

  {
    ci->stats.reset();  // reset query stats
    ci->stats.setStartTime();
    if (thd->main_security_ctx.user)
    {
      ci->stats.fUser = thd->main_security_ctx.user;
    }
    else
    {
      ci->stats.fUser = "";
    }

    if (thd->main_security_ctx.host)
      ci->stats.fHost = thd->main_security_ctx.host;
    else if (thd->main_security_ctx.host_or_ip)
      ci->stats.fHost = thd->main_security_ctx.host_or_ip;
    else
      ci->stats.fHost = "unknown";

    try
    {
      ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore User Priority - ") + e.what();
      ci->warningMsg = msg;
    }

    // If the previous query has error and
    // this is not a subquery run by the server(MCOL-1601)
    // re-establish the connection
    if (ci->queryState != 0)
    {
      if (ci->cal_conn_hndl_st.size() == 0)
        sm::sm_cleanup(ci->cal_conn_hndl);
      ci->cal_conn_hndl = 0;
    }

    sm::sm_init(sessionID, &ci->cal_conn_hndl, localQuery);
    idbassert(ci->cal_conn_hndl != 0);
    ci->cal_conn_hndl->csc = csc;
    idbassert(ci->cal_conn_hndl->exeMgr != 0);

    try
    {
      ci->cal_conn_hndl->connect();
    }
    catch (...)
    {
      setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto error;
    }

    hndl = ci->cal_conn_hndl;

    ci->cal_conn_hndl_st.push(ci->cal_conn_hndl);
    if (!csep)
      csep.reset(new CalpontSelectExecutionPlan());

    SessionManager sm;
    BRM::TxnID txnID;
    txnID = sm.getTxnID(sessionID);

    if (!txnID.valid)
    {
      txnID.id = 0;
      txnID.valid = true;
    }

    QueryContext verID;
    verID = sm.verID();

    csep->txnID(txnID.id);
    csep->verID(verID);
    csep->sessionID(sessionID);

    if (group_hand->table_list->db.length)
      csep->schemaName(group_hand->table_list->db.str, lower_case_table_names);

    csep->traceFlags(ci->traceFlags);

    // MCOL-1052 Send Items lists down to the optimizer.
    gi.groupByTables = group_hand->table_list;
    gi.groupByFields = group_hand->select;
    gi.groupByWhere = group_hand->where;
    gi.groupByGroup = group_hand->group_by;
    gi.groupByOrder = group_hand->order_by;
    gi.groupByHaving = group_hand->having;
    gi.groupByDistinct = group_hand->distinct;

    // MCOL-1052 Send pushed conditions here, since server could omit GROUP BY
    // items in case of = or IN functions used on GROUP BY columns.
    {
      CalTableMap::iterator mapiter;
      execplan::CalpontSelectExecutionPlan::ColumnMap::iterator colMapIter;
      execplan::CalpontSelectExecutionPlan::ColumnMap::iterator condColMapIter;
      execplan::ParseTree* ptIt;

      for (TABLE_LIST* tl = gi.groupByTables; tl; tl = tl->next_local)
      {
        mapiter = ci->tableMap.find(tl->table);

        if (mapiter != ci->tableMap.end() && mapiter->second.condInfo != NULL &&
            mapiter->second.condInfo->condPush)
        {
          while (!mapiter->second.condInfo->ptWorkStack.empty())
          {
            ptIt = mapiter->second.condInfo->ptWorkStack.top();
            mapiter->second.condInfo->ptWorkStack.pop();
            gi.pushedPts.push_back(ptIt);
          }
        }
      }
    }
    // send plan whenever group_init is called
    int status = cp_get_group_plan(thd, csep, gi);

    // Never proceed if status != 0 to avoid empty DA
    // crashes on later stages
    if (status != 0)
      goto internal_error;

    // @bug 2547. don't need to send the plan if it's impossible where for all unions.
    // MCOL-2178 commenting the below out since cp_get_group_plan does not modify this variable
    // which has a default value of false
    // if (MIGR::infinidb_vtable.impossibleWhereOnUnion)
    //  return 0;

    string query;
    // Set the query text only once if the server executes
    // subqueries separately.
    if (ci->queryState)
      query.assign("<subquery of the previous>");
    else
      query.assign(thd->query_string.str(), thd->query_string.length());
    csep->data(query);

    try
    {
      csep->priority(ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore User Priority - ") + e.what();
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
    }

#ifdef PLAN_HEX_FILE
    // plan serialization
    string tmpDir = aTmpDir + "/li1-plan.hex";

    ifstream ifs(tmpDir);
    ByteStream bs1;
    ifs >> bs1;
    ifs.close();
    csep->unserialize(bs1);
#endif

    if (ci->traceFlags & 1)
    {
      cerr << "---------------- EXECUTION PLAN ----------------" << endl;
      cerr << *csep << endl;
      cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
    }
    else
    {
      IDEBUG(cout << "---------------- EXECUTION PLAN ----------------" << endl);
      IDEBUG(cerr << *csep << endl);
      IDEBUG(cout << "-------------- EXECUTION PLAN END --------------\n" << endl);
    }
  }  // end of execution plan generation

  {
    ByteStream msg;
    ByteStream emsgBs;

    while (true)
    {
      try
      {
        ByteStream::quadbyte qb = 4;
        msg << qb;
        hndl->exeMgr->write(msg);
        msg.restart();
        csep->rmParms(ci->rmParms);

        // send plan
        csep->serialize(msg);
        hndl->exeMgr->write(msg);

        // get ExeMgr status back to indicate a vtable joblist success or not
        msg.restart();
        emsgBs.restart();
        msg = hndl->exeMgr->read();
        emsgBs = hndl->exeMgr->read();
        string emsg;

        if (msg.length() == 0 || emsgBs.length() == 0)
        {
          emsg = "Lost connection to ExeMgr. Please contact your administrator";
          setError(thd, ER_INTERNAL_ERROR, emsg);
          return ER_INTERNAL_ERROR;
        }

        string emsgStr;
        emsgBs >> emsgStr;
        bool err = false;

        if (msg.length() == 4)
        {
          msg >> qb;

          if (qb != 0)
          {
            err = true;
            // for makejoblist error, stats contains only error code and insert from here
            // because table fetch is not started
            ci->stats.setEndTime();
            ci->stats.fQuery = csep->data();
            ci->stats.fQueryType = csep->queryType();
            ci->stats.fErrorNo = qb;

            try
            {
              ci->stats.insert();
            }
            catch (std::exception& e)
            {
              string msg = string("Columnstore Query Stats - ") + e.what();
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
            }
          }
        }
        else
        {
          err = true;
        }

        if (err)
        {
          setError(thd, ER_INTERNAL_ERROR, emsgStr);
          return ER_INTERNAL_ERROR;
        }

        ci->rmParms.clear();

        ci->queryState = 1;

        break;
      }
      catch (...)
      {
        sm::sm_cleanup(hndl);
        hndl = 0;

        sm::sm_init(sessionID, &hndl, localQuery);
        idbassert(hndl != 0);
        hndl->csc = csc;

        ci->cal_conn_hndl = hndl;
        ci->cal_conn_hndl_st.pop();
        ci->cal_conn_hndl_st.push(ci->cal_conn_hndl);
        try
        {
          hndl->connect();
        }
        catch (...)
        {
          setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
          CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
          goto error;
        }

        msg.restart();
      }
    }
  }

  // set query state to be in_process. Sometimes mysql calls rnd_init multiple
  // times, this makes sure plan only being generated and sent once. It will be
  // reset when query finishes in sm::end_query

  // common path for both vtable select phase and table mode -- open scan handle
  ti = ci->tableMap[table];
  ti.msTablePtr = table;

  {
    // MCOL-1601 Using stacks of ExeMgr conn hndls, table and scan contexts.
    ti.tpl_ctx = new sm::cpsm_tplh_t();
    ti.tpl_ctx_st.push(ti.tpl_ctx);
    ti.tpl_scan_ctx = sm::sp_cpsm_tplsch_t(new sm::cpsm_tplsch_t());
    ti.tpl_scan_ctx_st.push(ti.tpl_scan_ctx);

    // make sure rowgroup is null so the new meta data can be taken. This is for some case mysql
    // call rnd_init for a table more than once.
    ti.tpl_scan_ctx->rowGroup = nullptr;

    try
    {
      tableid = execplan::IDB_VTABLE_ID;
    }
    catch (...)
    {
      string emsg = "No table ID found for table " + string(table->s->table_name.str);
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }

    try
    {
      sm::tpl_open(tableid, ti.tpl_ctx, hndl);
      sm::tpl_scan_open(tableid, ti.tpl_scan_ctx, hndl);
    }
    catch (std::exception& e)
    {
      string emsg = "table can not be opened: " + string(e.what());
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }
    catch (...)
    {
      string emsg = "table can not be opened";
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }

    ti.tpl_scan_ctx->traceFlags = ci->traceFlags;

    if ((ti.tpl_scan_ctx->ctp).size() == 0)
    {
      uint32_t num_attr = table->s->fields;

      for (uint32_t i = 0; i < num_attr; i++)
      {
        CalpontSystemCatalog::ColType ctype;
        ti.tpl_scan_ctx->ctp.push_back(ctype);
      }
    }
  }

  ci->tableMap[table] = ti;
  return 0;

error:

  if (ci->cal_conn_hndl)
  {
    // end_query() should be called here.
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  // do we need to close all connection handle of the table map?
  return ER_INTERNAL_ERROR;

internal_error:

  if (ci->cal_conn_hndl)
  {
    // end_query() should be called here.
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;
}

/*@brief ha_mcs_impl_group_by_next - Return result set for MariaDB group_by
    pushdown handler
*/
/***********************************************************
 * DESCRIPTION:
 * Return a result record for each group_by_handler::next_row() call.
 * PARAMETERS:
 *    group_hand - group by handler, that preserves initial table and items lists. .
 *    table - TABLE pointer The table to save the result set in.
 * RETURN:
 *    0 if success
 *    HA_ERR_END_OF_FILE if the record set has come to an end
 *    others if something went wrong whilst getting the result set
 ***********************************************************/
int ha_mcs_impl_group_by_next(TABLE* table, long timeZone)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return HA_ERR_END_OF_FILE;

  if (isMCSTableUpdate(thd) || isMCSTableDelete(thd))
    return HA_ERR_END_OF_FILE;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  if (ci->alterTableState > 0)
    return HA_ERR_END_OF_FILE;

  cal_table_info ti;
  ti = ci->tableMap[table];
  int rc = HA_ERR_END_OF_FILE;

  if (!ti.tpl_ctx || !ti.tpl_scan_ctx)
  {
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    return ER_INTERNAL_ERROR;
  }

  idbassert(ti.msTablePtr == table);

  try
  {
    // fetchNextRow interface forces to use buf.
    unsigned char buf;
    rc = fetchNextRow(&buf, ti, ci, timeZone, true);
  }
  catch (std::exception& e)
  {
    string emsg = string("Error while fetching from ExeMgr: ") + e.what();
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    return ER_INTERNAL_ERROR;
  }

  ci->tableMap[table] = ti;

  if (rc != 0 && rc != HA_ERR_END_OF_FILE)
  {
    string emsg;

    // remove this check when all error handling migrated to the new framework.
    if (rc >= 1000)
      emsg = ti.tpl_scan_ctx->errMsg;
    else
    {
      logging::ErrorCodes errorcodes;
      emsg = errorcodes.errorString(rc);
    }

    setError(thd, ER_INTERNAL_ERROR, emsg);
    ci->stats.fErrorNo = rc;
    CalpontSystemCatalog::removeCalpontSystemCatalog(tid2sid(thd->thread_id));
    rc = ER_INTERNAL_ERROR;
  }

  return rc;
}

int ha_mcs_impl_group_by_end(TABLE* table)
{
  int rc = 0;
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return 0;

  cal_connection_info* ci = nullptr;

  if (get_fe_conn_info_ptr() != NULL)
    ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (!ci)
  {
    set_fe_conn_info_ptr((void*)new cal_connection_info());
    ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());
  }

  if (((thd->lex)->sql_command == SQLCOM_INSERT) || ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT))
  {
    force_close_fep_conn(thd, ci, true);  // with checking prev command rc
    return rc;
  }

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    // clear querystats because no query stats available for cancelled query
    ci->queryStats = "";
    // Poping next ExeMgr connection out of the stack
    if (ci->cal_conn_hndl_st.size())
    {
      ci->cal_conn_hndl_st.pop();
      if (ci->cal_conn_hndl_st.size())
        ci->cal_conn_hndl = ci->cal_conn_hndl_st.top();
    }

    return 0;
  }

  IDEBUG(cerr << "group_by_end for table " << table->s->table_name.str << endl);

  cal_table_info ti = ci->tableMap[table];
  sm::cpsm_conhdl_t* hndl;
  bool clearScanCtx = false;

  hndl = ci->cal_conn_hndl;

  if (ti.tpl_ctx)
  {
    if (ti.tpl_scan_ctx.get())
    {
      clearScanCtx = ((ti.tpl_scan_ctx.get()->rowsreturned) &&
                      ti.tpl_scan_ctx.get()->rowsreturned == ti.tpl_scan_ctx.get()->getRowCount());
      try
      {
        sm::tpl_scan_close(ti.tpl_scan_ctx);
      }
      catch (...)
      {
        rc = ER_INTERNAL_ERROR;
      }
    }

    ti.tpl_scan_ctx.reset();
    if (ti.tpl_scan_ctx_st.size())
    {
      ti.tpl_scan_ctx_st.pop();
      if (ti.tpl_scan_ctx_st.size())
        ti.tpl_scan_ctx = ti.tpl_scan_ctx_st.top();
    }
    try
    {
      if (hndl)
      {
        {
          bool ask_4_stats = (ci->traceFlags) ? true : false;
          sm::tpl_close(ti.tpl_ctx, &hndl, ci->stats, ask_4_stats, clearScanCtx);
        }
        // Normaly stats variables are set in external_lock method but we set it here
        // since they we pretend we are in vtable_disabled mode and the stats vars won't be set.
        // We sum the stats up here since server could run a number of
        // queries e.g. each for a subquery in a filter.
        if (hndl)
        {
          if (hndl->queryStats.length())
            ci->queryStats += hndl->queryStats;
          if (hndl->extendedStats.length())
            ci->extendedStats += hndl->extendedStats;
          if (hndl->miniStats.length())
            ci->miniStats += hndl->miniStats;
        }
      }

      ci->cal_conn_hndl = hndl;

      ti.tpl_ctx = 0;
    }
    catch (IDBExcept& e)
    {
      if (e.errorCode() == ERR_CROSS_ENGINE_CONNECT || e.errorCode() == ERR_CROSS_ENGINE_CONFIG)
      {
        string msg = string("Columnstore Query Stats - ") + e.what();
        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
      }
      else
      {
        setError(thd, ER_INTERNAL_ERROR, e.what());
        rc = ER_INTERNAL_ERROR;
      }
    }
    catch (std::exception& e)
    {
      setError(thd, ER_INTERNAL_ERROR, e.what());
      rc = ER_INTERNAL_ERROR;
    }
    catch (...)
    {
      setError(thd, ER_INTERNAL_ERROR, "Internal error throwed in group_by_end");
      rc = ER_INTERNAL_ERROR;
    }
  }

  ti.tpl_ctx = 0;

  if (ti.tpl_ctx_st.size())
  {
    ti.tpl_ctx_st.pop();
    if (ti.tpl_ctx_st.size())
      ti.tpl_ctx = ti.tpl_ctx_st.top();
  }

  if (ci->cal_conn_hndl_st.size())
  {
    ci->cal_conn_hndl_st.pop();
    if (ci->cal_conn_hndl_st.size())
      ci->cal_conn_hndl = ci->cal_conn_hndl_st.top();
  }

  ci->tableMap[table] = ti;

  // push warnings from CREATE phase
  if (!ci->warningMsg.empty())
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, ci->warningMsg.c_str());

  ci->warningMsg.clear();
  // reset expressionId just in case
  ci->expressionId = 0;
  return rc;
}

/*@brief  Initiate the query for derived_handler           */
/***********************************************************
 * DESCRIPTION:
 * Execute the query and saves derived table query.
 * There is an extra handler argument so I ended up with a
 * new init function. The code is a copy of
 * ha_mcs_impl_rnd_init() mostly.
 * PARAMETERS:
 * mcs_handler_info* pnt to an envelope struct
 * TABLE* table - dest table to put the results into
 * RETURN:
 *    rc as int
 ***********************************************************/
int ha_mcs_impl_pushdown_init(mcs_handler_info* handler_info, TABLE* table)
{
  IDEBUG(cout << "pushdown_init for table " << endl);
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return 0;

  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
  gp_walk_info gwi(timeZoneOffset);
  gwi.thd = thd;
  bool err = false;

  // check whether the system is ready to process statement.
#ifndef _MSC_VER
  static DBRM dbrm(true);
  int bSystemQueryReady = dbrm.getSystemQueryReady();

  if (bSystemQueryReady == 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "The system is not yet ready to accept queries");
    return ER_INTERNAL_ERROR;
  }
  else if (bSystemQueryReady < 0)
  {
    // Still not ready
    setError(thd, ER_INTERNAL_ERROR, "DBRM is not responding. Cannot accept queries");
    return ER_INTERNAL_ERROR;
  }
#endif

  // Set this to close all outstanding FEP connections on
  // client disconnect in handlerton::closecon_handlerton().
  if (!thd_get_ha_data(thd, mcs_hton))
  {
    thd_set_ha_data(thd, mcs_hton, reinterpret_cast<void*>(0x42));
  }

  if ((thd->lex)->sql_command == SQLCOM_ALTER_TABLE)
  {
    return 0;
  }

  // MCOL-4023 We need to test this code path.
  // Update and delete code
  if (isUpdateOrDeleteStatement(thd->lex->sql_command))
    return doUpdateDelete(thd, gwi, std::vector<COND*>());

  uint32_t sessionID = tid2sid(thd->thread_id);
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(CalpontSystemCatalog::FE);

  if (!get_fe_conn_info_ptr())
    set_fe_conn_info_ptr(reinterpret_cast<void*>(new cal_connection_info(), thd));

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  idbassert(ci != 0);

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    if (ci->cal_conn_hndl)
    {
      // send ExeMgr a signal before closing the connection
      ByteStream msg;
      ByteStream::quadbyte qb = 0;
      msg << qb;

      try
      {
        ci->cal_conn_hndl->exeMgr->write(msg);
      }
      catch (...)
      {
        // canceling query. ignore connection failure.
      }

      sm::sm_cleanup(ci->cal_conn_hndl);
      ci->cal_conn_hndl = 0;
    }

    return 0;
  }

  sm::tableid_t tableid = 0;
  cal_table_info ti;
  sm::cpsm_conhdl_t* hndl;
  SCSEP csep;
  // Declare handlers ptrs in this scope for future use.
  ha_columnstore_select_handler* sh = nullptr;
  ha_columnstore_derived_handler* dh = nullptr;

  // update traceFlags according to the autoswitch state.
  ci->traceFlags = (ci->traceFlags | CalpontSelectExecutionPlan::TRACE_TUPLE_OFF) ^
                   CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;

  bool localQuery = (get_local_query(thd) > 0 ? true : false);

  {
    ci->stats.reset();  // reset query stats
    ci->stats.setStartTime();
    if (thd->main_security_ctx.user)
    {
      ci->stats.fUser = thd->main_security_ctx.user;
    }
    else
    {
      ci->stats.fUser = "";
    }

    if (thd->main_security_ctx.host)
      ci->stats.fHost = thd->main_security_ctx.host;
    else if (thd->main_security_ctx.host_or_ip)
      ci->stats.fHost = thd->main_security_ctx.host_or_ip;
    else
      ci->stats.fHost = "unknown";

    try
    {
      ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser);
    }
    catch (std::exception& e)
    {
      string msg = string("Columnstore User Priority - ") + e.what();
      ci->warningMsg = msg;
    }

    // if the previous query has error, re-establish the connection
    if (ci->queryState != 0)
    {
      sm::sm_cleanup(ci->cal_conn_hndl);
      ci->cal_conn_hndl = 0;
    }

    sm::sm_init(sessionID, &ci->cal_conn_hndl, localQuery);
    idbassert(ci->cal_conn_hndl != 0);
    ci->cal_conn_hndl->csc = csc;
    idbassert(ci->cal_conn_hndl->exeMgr != 0);

    try
    {
      ci->cal_conn_hndl->connect();
    }
    catch (...)
    {
      setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto error;
    }

    hndl = ci->cal_conn_hndl;

    IDEBUG(std::cout << idb_mysql_query_str(thd) << std::endl);

    {
      if (!csep)
        csep.reset(new CalpontSelectExecutionPlan());

      SessionManager sm;
      BRM::TxnID txnID;
      txnID = sm.getTxnID(sessionID);

      if (!txnID.valid)
      {
        txnID.id = 0;
        txnID.valid = true;
      }

      QueryContext verID;
      verID = sm.verID();

      csep->txnID(txnID.id);
      csep->verID(verID);
      csep->sessionID(sessionID);

      if (thd->db.length)
        csep->schemaName(thd->db.str, lower_case_table_names);

      csep->traceFlags(ci->traceFlags);

      // cast the handler and get a plan.
      int status = 42;
      if (handler_info->hndl_type == mcs_handler_types_t::SELECT)
      {
        sh = reinterpret_cast<ha_columnstore_select_handler*>(handler_info->hndl_ptr);
        status = cs_get_select_plan(sh, thd, csep, gwi);
      }
      else if (handler_info->hndl_type == DERIVED)
      {
        dh = reinterpret_cast<ha_columnstore_derived_handler*>(handler_info->hndl_ptr);
        status = cs_get_derived_plan(dh, thd, csep, gwi);
      }

      // Return an error to avoid MDB crash later in end_statement
      if (status != 0)
        goto internal_error;

      string query;
      query.assign(idb_mysql_query_str(thd));
      csep->data(query);

      try
      {
        csep->priority(ci->stats.userPriority(ci->stats.fHost, ci->stats.fUser));
      }
      catch (std::exception& e)
      {
        string msg = string("Columnstore User Priority - ") + e.what();
        push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
      }

// DRRTUY Make this runtime configureable
#ifdef PLAN_HEX_FILE
      // plan serialization
      ifstream ifs("/tmp/li1-plan.hex");
      ByteStream bs1;
      ifs >> bs1;
      ifs.close();
      csep->unserialize(bs1);
#endif

      if (ci->traceFlags & 1)
      {
        cerr << "---------------- EXECUTION PLAN ----------------" << endl;
        cerr << *csep << endl;
        cerr << "-------------- EXECUTION PLAN END --------------\n" << endl;
      }
      else
      {
        IDEBUG(cout << "---------------- EXECUTION PLAN ----------------" << endl);
        IDEBUG(cerr << *csep << endl);
        IDEBUG(cout << "-------------- EXECUTION PLAN END --------------\n" << endl);
      }
    }
  }  // end of execution plan generation

  {
    ByteStream msg;
    ByteStream emsgBs;

    while (true)
    {
      try
      {
        ByteStream::quadbyte qb = 4;
        msg << qb;
        hndl->exeMgr->write(msg);
        msg.restart();
        csep->rmParms(ci->rmParms);

        // send plan
        csep->serialize(msg);
        hndl->exeMgr->write(msg);

        // get ExeMgr status back to indicate a vtable joblist success or not
        msg.restart();
        emsgBs.restart();
        msg = hndl->exeMgr->read();
        emsgBs = hndl->exeMgr->read();
        string emsg;

        if (msg.length() == 0 || emsgBs.length() == 0)
        {
          emsg = "Lost connection to ExeMgr. Please contact your administrator";
          setError(thd, ER_INTERNAL_ERROR, emsg);
          return ER_INTERNAL_ERROR;
        }

        string emsgStr;
        emsgBs >> emsgStr;

        if (msg.length() == 4)
        {
          msg >> qb;

          if (qb != 0)
          {
            err = true;
            // for makejoblist error, stats contains only error code and insert from here
            // because table fetch is not started
            ci->stats.setEndTime();
            ci->stats.fQuery = csep->data();
            ci->stats.fQueryType = csep->queryType();
            ci->stats.fErrorNo = qb;

            try
            {
              ci->stats.insert();
            }
            catch (std::exception& e)
            {
              string msg = string("Columnstore Query Stats - ") + e.what();
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, msg.c_str());
            }
          }
        }
        else
        {
          err = true;
        }

        if (err)
        {
          // CS resets error in create_SH() if fallback is enabled
          setError(thd, ER_INTERNAL_ERROR, emsgStr);
          goto internal_error;
        }

        ci->rmParms.clear();

        // SH will initiate SM in select_next() only
        if (!sh)
          ci->queryState = sm::QUERY_IN_PROCESS;

        break;
      }
      catch (...)
      {
        sm::sm_cleanup(hndl);
        hndl = 0;

        sm::sm_init(sessionID, &hndl, localQuery);
        idbassert(hndl != 0);
        hndl->csc = csc;

        ci->cal_conn_hndl = hndl;

        try
        {
          hndl->connect();
        }
        catch (...)
        {
          setError(thd, ER_INTERNAL_ERROR, IDBErrorInfo::instance()->errorMsg(ERR_LOST_CONN_EXEMGR));
          CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
          goto error;
        }

        msg.restart();
      }
    }
  }

  // set query state to be in_process. Sometimes mysql calls rnd_init multiple
  // times, this makes sure plan only being generated and sent once. It will be
  // reset when query finishes in sm::end_query

  // common path for both vtable select phase and table mode -- open scan handle
  ti = ci->tableMap[table];
  // This is the server's temp table for the result.
  if (sh)
  {
    ti.msTablePtr = sh->table;
  }
  else
  {
    ti.msTablePtr = dh->table;
  }

  // For SH CS creates SM environment inside select_next().
  // This allows us to try and fail with SH.
  if (!sh)
  {
    if (ti.tpl_ctx == 0)
    {
      ti.tpl_ctx = new sm::cpsm_tplh_t();
      ti.tpl_scan_ctx = sm::sp_cpsm_tplsch_t(new sm::cpsm_tplsch_t());
    }

    // make sure rowgroup is null so the new meta data can be taken. This is for some case mysql
    // call rnd_init for a table more than once.
    ti.tpl_scan_ctx->rowGroup = nullptr;

    try
    {
      tableid = execplan::IDB_VTABLE_ID;
    }
    catch (...)
    {
      string emsg = "No table ID found for table " + string(table->s->table_name.str);
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }

    try
    {
      sm::tpl_open(tableid, ti.tpl_ctx, hndl);
      sm::tpl_scan_open(tableid, ti.tpl_scan_ctx, hndl);
    }
    catch (std::exception& e)
    {
      string emsg = "table can not be opened: " + string(e.what());
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }
    catch (...)
    {
      string emsg = "table can not be opened";
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }

    ti.tpl_scan_ctx->traceFlags = ci->traceFlags;

    if ((ti.tpl_scan_ctx->ctp).size() == 0)
    {
      uint32_t num_attr = table->s->fields;

      for (uint32_t i = 0; i < num_attr; i++)
      {
        CalpontSystemCatalog::ColType ctype;
        ti.tpl_scan_ctx->ctp.push_back(ctype);
      }
    }
    ci->tableMap[table] = ti;
  }

  return 0;

error:

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  // do we need to close all connection handle of the table map
  return ER_INTERNAL_ERROR;

internal_error:

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;
}

int ha_mcs_impl_select_next(uchar* buf, TABLE* table, long timeZone)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd) && isDMLStatement(thd->lex->sql_command))
    return HA_ERR_END_OF_FILE;

  int rc = HA_ERR_END_OF_FILE;

  if (get_fe_conn_info_ptr() == nullptr)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (isUpdateOrDeleteStatement(thd->lex->sql_command))
    return rc;

  // @bug 2547
  // MCOL-2178 This variable can never be true in the scope of this function
  //    if (MIGR::infinidb_vtable.impossibleWhereOnUnion)
  //        return HA_ERR_END_OF_FILE;

  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
  {
    force_close_fep_conn(thd, ci);
    return 0;
  }

  if (ci->alterTableState > 0)
    return rc;

  cal_table_info ti;
  ti = ci->tableMap[table];
  // This is the server's temp table for the result.
  ti.msTablePtr = table;
  sm::tableid_t tableid = execplan::IDB_VTABLE_ID;
  sm::cpsm_conhdl_t* hndl = ci->cal_conn_hndl;

  if (!ti.tpl_ctx || !ti.tpl_scan_ctx || (hndl && hndl->queryState == sm::NO_QUERY))
  {
    if (ti.tpl_ctx == 0)
    {
      ti.tpl_ctx = new sm::cpsm_tplh_t();
      ti.tpl_scan_ctx = sm::sp_cpsm_tplsch_t(new sm::cpsm_tplsch_t());
    }

    // make sure rowgroup is null so the new meta data can be taken. This is for some case mysql
    // call rnd_init for a table more than once.
    ti.tpl_scan_ctx->rowGroup = nullptr;

    try
    {
      sm::tpl_open(tableid, ti.tpl_ctx, hndl);
      sm::tpl_scan_open(tableid, ti.tpl_scan_ctx, hndl);
    }
    catch (std::exception& e)
    {
      uint32_t sessionID = tid2sid(thd->thread_id);
      string emsg = "table can not be opened: " + string(e.what());
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }
    catch (...)
    {
      uint32_t sessionID = tid2sid(thd->thread_id);
      string emsg = "table can not be opened";
      setError(thd, ER_INTERNAL_ERROR, emsg);
      CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
      goto internal_error;
    }

    ti.tpl_scan_ctx->traceFlags = ci->traceFlags;

    if ((ti.tpl_scan_ctx->ctp).size() == 0)
    {
      uint32_t num_attr = table->s->fields;

      for (uint32_t i = 0; i < num_attr; i++)
      {
        CalpontSystemCatalog::ColType ctype;
        ti.tpl_scan_ctx->ctp.push_back(ctype);
      }
    }
    ci->tableMap[table] = ti;
    hndl->queryState = sm::QUERY_IN_PROCESS;
  }

  if (!ti.tpl_ctx || !ti.tpl_scan_ctx)
  {
    uint32_t sessionID = tid2sid(thd->thread_id);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return ER_INTERNAL_ERROR;
  }

  idbassert(ti.msTablePtr == table);

  try
  {
    rc = fetchNextRow(buf, ti, ci, timeZone);
  }
  catch (std::exception& e)
  {
    uint32_t sessionID = tid2sid(thd->thread_id);
    string emsg = string("Error while fetching from ExeMgr: ") + e.what();
    setError(thd, ER_INTERNAL_ERROR, emsg);
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    return ER_INTERNAL_ERROR;
  }

  ci->tableMap[table] = ti;

  if (rc != 0 && rc != HA_ERR_END_OF_FILE)
  {
    string emsg;

    // remove this check when all error handling migrated to the new framework.
    if (rc >= 1000)
      emsg = ti.tpl_scan_ctx->errMsg;
    else
    {
      logging::ErrorCodes errorcodes;
      emsg = errorcodes.errorString(rc);
    }

    uint32_t sessionID = tid2sid(thd->thread_id);
    setError(thd, ER_INTERNAL_ERROR, emsg);
    ci->stats.fErrorNo = rc;
    CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);
    rc = ER_INTERNAL_ERROR;
  }

  return rc;

internal_error:

  if (ci->cal_conn_hndl)
  {
    sm::sm_cleanup(ci->cal_conn_hndl);
    ci->cal_conn_hndl = 0;
  }

  return ER_INTERNAL_ERROR;
}

// vim:sw=4 ts=4:
