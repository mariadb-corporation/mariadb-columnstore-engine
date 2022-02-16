/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
 * $Id: ha_mcs_ddl.cpp 9675 2013-07-11 15:38:12Z chao $
 */

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif
#include <utility>
#include <cassert>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>
using namespace boost;

#include "ha_mcs_sysvars.h"
#include "idb_mysql.h"

#include "ha_mcs_impl_if.h"
using namespace cal_impl_if;

#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "idbcompress.h"
using namespace compress;

#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "dbrm.h"
using namespace BRM;

#include "calpontsystemcatalog.h"
#include "expressionparser.h"
#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "expressionparser.h"
#include "constantcolumn.h"
using namespace execplan;

#include "resourcemanager.h"
using namespace joblist;

namespace
{
typedef CalpontSelectExecutionPlan::ColumnMap::value_type CMVT_;
// HDFS is never used nowadays, so don't bother
bool useHdfs = false;  // ResourceManager::instance()->useHdfs();

#include "ha_autoi.cpp"

// convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
  return CalpontSystemCatalog::idb_tid2sid(tid);
}

static void decode_objectname(char* buf, const char* path, size_t buf_size)
{
  size_t new_path_len = filename_to_tablename(path, buf, buf_size);
  buf[new_path_len] = '\0';
}

static void decode_file_path(const char* path, char* decoded_dbname, char* decoded_tbname)
{
  // The format cont ains './' in the beginning of a path.
  char* dbname_start = (char*)path + 2;
  char* dbname_end = dbname_start;
  while (*dbname_end != '/')
    dbname_end++;

  int cnt = dbname_end - dbname_start;
  char* dbname = (char*)my_alloca(cnt + 1);
  memcpy(dbname, dbname_start, cnt);
  dbname[cnt] = '\0';
  decode_objectname(decoded_dbname, dbname, FN_REFLEN);
  my_afree(dbname);

  char* tbname_start = dbname_end + 1;
  decode_objectname(decoded_tbname, tbname_start, FN_REFLEN);
}

CalpontSystemCatalog::ColDataType convertDataType(const ddlpackage::ColumnType& ct)
{
  const datatypes::TypeHandler* h = datatypes::TypeHandler::find_by_ddltype(ct);
  if (!h)
  {
    throw runtime_error("Unsupported datatype!");
    return CalpontSystemCatalog::UNDEFINED;
  }
  return h->code();
}

int parseCompressionComment(std::string comment)
{
  algorithm::to_upper(comment);
  regex compat("[[:space:]]*COMPRESSION[[:space:]]*=[[:space:]]*", regex_constants::extended);
  int compressiontype = 0;
  boost::match_results<std::string::const_iterator> what;
  std::string::const_iterator start, end;
  start = comment.begin();
  end = comment.end();
  boost::match_flag_type flags = boost::match_default;

  if (boost::regex_search(start, end, what, compat, flags))
  {
    // Find the pattern, now get the compression type
    string compType(&(*(what[0].second)));
    //; is the separator between compression and autoincrement comments.
    unsigned i = compType.find_first_of(";");

    if (i <= compType.length())
    {
      compType = compType.substr(0, i);
    }

    i = compType.find_last_not_of(" ");

    if (i <= compType.length())
    {
      compType = compType.substr(0, i + 1);
    }

    errno = 0;
    char* ep = NULL;
    const char* str = compType.c_str();
    compressiontype = strtoll(str, &ep, 10);

    //  (no digits) || (more chars)  || (other errors & value = 0)
    if ((ep == str) || (*ep != '\0') || (errno != 0 && compressiontype == 0))
    {
      compressiontype = -1;
    }
  }
  else
    compressiontype = MAX_INT;

  // MCOL-4685: ignore [COMMENT '[compression=0] option at table or column level (no error
  // messages, just disregard);
  if (compressiontype == 0)
    compressiontype = 2;

  return compressiontype;
}

bool validateAutoincrementDatatype(int type)
{
  bool validAutoType = false;

  switch (type)
  {
    case ddlpackage::DDL_INT:
    case ddlpackage::DDL_INTEGER:
    case ddlpackage::DDL_BIGINT:
    case ddlpackage::DDL_MEDINT:
    case ddlpackage::DDL_SMALLINT:
    case ddlpackage::DDL_TINYINT:
    case ddlpackage::DDL_UNSIGNED_INT:
    case ddlpackage::DDL_UNSIGNED_BIGINT:
    case ddlpackage::DDL_UNSIGNED_MEDINT:
    case ddlpackage::DDL_UNSIGNED_SMALLINT:
    case ddlpackage::DDL_UNSIGNED_TINYINT: validAutoType = true; break;
  }

  return validAutoType;
}

bool validateNextValue(int type, int64_t value)
{
  bool validValue = true;

  switch (type)
  {
    case ddlpackage::DDL_BIGINT:
    {
      if (value > MAX_BIGINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_UNSIGNED_BIGINT:
    {
      if (static_cast<uint64_t>(value) > MAX_UBIGINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_INT:
    case ddlpackage::DDL_INTEGER:
    {
      if (value > MAX_INT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_MEDINT:
    {
      if (value > MAX_MEDINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_UNSIGNED_INT:
    {
      if (static_cast<uint64_t>(value) > MAX_UINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_UNSIGNED_MEDINT:
    {
      if (static_cast<uint64_t>(value) > MAX_UMEDINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_SMALLINT:
    {
      if (value > MAX_SMALLINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_UNSIGNED_SMALLINT:
    {
      if (static_cast<uint64_t>(value) > MAX_USMALLINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_TINYINT:
    {
      if (value > MAX_TINYINT)
        validValue = false;
    }
    break;

    case ddlpackage::DDL_UNSIGNED_TINYINT:
    {
      if (static_cast<uint64_t>(value) > MAX_UTINYINT)
        validValue = false;
    }
    break;
  }

  return validValue;
}

bool anyRowInTable(string& schema, string& tableName, int sessionID)
{
  // find a column in the table
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(execplan::CalpontSystemCatalog::FE);
  CalpontSystemCatalog::TableName aTableName;
  if (lower_case_table_names)
  {
    algorithm::to_lower(schema);
    algorithm::to_lower(tableName);
  }
  aTableName.schema = schema;
  aTableName.table = tableName;

  CalpontSystemCatalog::RIDList ridList = csc->columnRIDs(aTableName, true);
  CalpontSystemCatalog::TableColName tableColName = csc->colName(ridList[0].objnum);

  CalpontSelectExecutionPlan csep;
  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
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

  string firstCol = tableColName.schema + "." + tableColName.table + "." + tableColName.column;
  SimpleColumn* col[1];
  col[0] = new SimpleColumn(firstCol, sessionID);
  SRCP srcp;
  srcp.reset(col[0]);
  colMap.insert(CMVT_(firstCol, srcp));
  csep.columnMapNonStatic(colMap);
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  CalpontSelectExecutionPlan::TableList tablelist;
  tablelist.push_back(make_aliastable(schema, tableName, ""));
  csep.tableList(tablelist);

  boost::shared_ptr<messageqcpp::MessageQueueClient> exemgrClient(
      new messageqcpp::MessageQueueClient("ExeMgr1"));
  ByteStream msg, emsgBs;
  rowgroup::RGData rgData;
  ByteStream::quadbyte qb = 4;
  msg << qb;
  rowgroup::RowGroup* rowGroup = 0;
  bool anyRow = false;

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

      if (rowGroup->getRowCount() > 0)
        anyRow = true;

      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
      return anyRow;
    }
  }
}

bool anyTimestampColumn(string& schema, string& tableName, int sessionID)
{
  boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
  csc->identity(execplan::CalpontSystemCatalog::FE);
  CalpontSystemCatalog::TableName aTableName;
  if (lower_case_table_names)
  {
    algorithm::to_lower(schema);
    algorithm::to_lower(tableName);
  }

  // select columnname from calpontsys.syscolumn
  // where schema = schema and tablename = tableName
  // and datatype = 'timestamp'
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
  SRCP srcp;
  srcp.reset(c1);
  colMap.insert(CMVT_(firstCol, srcp));
  srcp.reset(c2);
  colMap.insert(CMVT_(secondCol, srcp));
  srcp.reset(c3);
  colMap.insert(CMVT_(thirdCol, srcp));
  srcp.reset(c4);
  colMap.insert(CMVT_(fourthCol, srcp));
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
  bool anyRow = false;

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

      if (rowGroup->getRowCount() > 0)
        anyRow = true;

      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
      return anyRow;
    }
  }
}

bool anyNullInTheColumn(THD* thd, string& schema, string& table, string& columnName, int sessionID)
{
  CalpontSelectExecutionPlan csep;
  CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
  CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
  CalpontSelectExecutionPlan::ColumnMap colMap;
  if (lower_case_table_names)
  {
    algorithm::to_lower(schema);
    algorithm::to_lower(table);
  }
  algorithm::to_lower(columnName);

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

  string firstCol = schema + "." + table + "." + columnName;
  SimpleColumn* col[1];
  col[0] = new SimpleColumn(firstCol, sessionID);
  SRCP srcp;
  srcp.reset(col[0]);
  colMap.insert(CMVT_(firstCol, srcp));
  csep.columnMapNonStatic(colMap);
  returnedColumnList.push_back(srcp);
  csep.returnedCols(returnedColumnList);

  SimpleFilter* sf = new SimpleFilter();
  const char* timeZone = thd->variables.time_zone->get_name()->ptr();
  long timeZoneOffset;
  dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
  sf->timeZone(timeZoneOffset);
  boost::shared_ptr<Operator> sop(new PredicateOperator("isnull"));
  sf->op(sop);
  ConstantColumn* rhs = new ConstantColumn("", ConstantColumn::NULLDATA);
  rhs->timeZone(timeZoneOffset);
  sf->lhs(col[0]->clone());
  sf->rhs(rhs);

  filterTokenList.push_back(sf);
  csep.filterTokenList(filterTokenList);

  CalpontSelectExecutionPlan::TableList tablelist;
  tablelist.push_back(make_aliastable(schema, table, ""));
  csep.tableList(tablelist);

  boost::shared_ptr<messageqcpp::MessageQueueClient> exemgrClient(
      new messageqcpp::MessageQueueClient("ExeMgr1"));
  ByteStream msg, emsgBs;
  rowgroup::RGData rgData;
  ByteStream::quadbyte qb = 4;
  msg << qb;
  rowgroup::RowGroup* rowGroup = 0;
  bool anyRow = false;

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
        // msg.advance(amount);
        msg >> emsgStr;
        // exemgrClient->shutdown();
        // delete exemgrClient;
        // exemgrClient = 0;
        throw runtime_error(emsgStr);
      }

      if (rowGroup->getRowCount() > 0)
        anyRow = true;

      // exemgrClient->shutdown();
      // delete exemgrClient;
      // exemgrClient = 0;
      return anyRow;
    }
  }
}

int ProcessDDLStatement(string& ddlStatement, string& schema, const string& table, int sessionID,
                        string& emsg, int compressionTypeIn = 2, bool isAnyAutoincreCol = false,
                        int64_t nextvalue = 1, std::string autoiColName = "",
                        const CHARSET_INFO* default_table_charset = NULL)
{
  SqlParser parser;
  THD* thd = current_thd;
#ifdef MCS_DEBUG
  cout << "ProcessDDLStatement: " << schema << "." << table << ":" << ddlStatement << endl;
#endif

  parser.setDefaultSchema(schema);
  parser.setDefaultCharset(default_table_charset);
  int rc = 0;
  parser.Parse(ddlStatement.c_str());

  if (get_fe_conn_info_ptr() == NULL)
    set_fe_conn_info_ptr((void*)new cal_connection_info());

  cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(get_fe_conn_info_ptr());

  if (parser.Good())
  {
    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
    csc->identity(execplan::CalpontSystemCatalog::FE);
    const ddlpackage::ParseTree& ptree = parser.GetParseTree();

    if (UNLIKELY(ptree.fList.size() == 0))
    {
      // TODO: Once the crash bug is found, this should convert to "return 0"
      cout << "***** ProcessDDLStatement has no stmt *****" << endl;
      setError(thd, ER_CHECK_NOT_IMPLEMENTED, "DDL processed without statement");
      return 1;
    }

    SqlStatement& stmt = *ptree.fList[0];
    bool isVarbinaryAllowed = false;
    std::string valConfig = config::Config::makeConfig()->getConfig("WriteEngine", "AllowVarbinary");
    algorithm::to_upper(valConfig);

    if (valConfig.compare("YES") == 0)
      isVarbinaryAllowed = true;

    const char* timeZone = thd->variables.time_zone->get_name()->ptr();
    long timeZoneOffset;
    dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);

    //@Bug 1771. error out for not supported feature.
    if (typeid(stmt) == typeid(CreateTableStatement))
    {
      CreateTableStatement* createTable = dynamic_cast<CreateTableStatement*>(&stmt);
      if (lower_case_table_names)
      {
        algorithm::to_lower(createTable->fTableDef->fQualifiedName->fSchema);
        algorithm::to_lower(createTable->fTableDef->fQualifiedName->fName);
      }

      bool matchedCol = false;
      bool isFirstTimestamp = true;

      for (unsigned i = 0; i < createTable->fTableDef->fColumns.size(); i++)
      {
        // if there are any constraints other than 'DEFAULT NULL' (which is the default in IDB), kill
        //  the statement
        bool autoIncre = false;
        uint64_t startValue = 1;

        if (createTable->fTableDef->fColumns[i]->fConstraints.size() > 0)
        {
          // support default value and NOT NULL constraint
          for (uint32_t j = 0; j < createTable->fTableDef->fColumns[i]->fConstraints.size(); j++)
          {
            if (createTable->fTableDef->fColumns[i]->fConstraints[j]->fConstraintType != DDL_NOT_NULL)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);

              thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                      (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }
          }
        }

        // check varbinary data type
        if ((createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_VARBINARY) &&
            !isVarbinaryAllowed)
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);

          thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                  "Varbinary is currently not supported by Columnstore.");
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }

        if ((createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_VARBINARY) &&
            ((createTable->fTableDef->fColumns[i]->fType->fLength > 8000) ||
             (createTable->fTableDef->fColumns[i]->fType->fLength < 8)))
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);

          thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED, "Varbinary length has to be between 8 and 8000.");
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }

        // For TIMESTAMP, if no constraint is given, default to NOT NULL
        if (createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_TIMESTAMP &&
            createTable->fTableDef->fColumns[i]->fConstraints.empty())
        {
          createTable->fTableDef->fColumns[i]->fConstraints.push_back(new ColumnConstraintDef(DDL_NOT_NULL));
        }

        if (createTable->fTableDef->fColumns[i]->fDefaultValue)
        {
          if ((!createTable->fTableDef->fColumns[i]->fDefaultValue->fNull) &&
              (createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_VARBINARY))
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);

            thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED, "Varbinary column cannot have default value.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          if (!createTable->fTableDef->fColumns[i]->fDefaultValue->fNull)
          {
            // validate the default value, if out of range, just error out
            CalpontSystemCatalog::ColDataType dataType;
            dataType = convertDataType(createTable->fTableDef->fColumns[i]->fType->fType);
            CalpontSystemCatalog::ColType colType;
            colType.colDataType = dataType;
            colType.colWidth = createTable->fTableDef->fColumns[i]->fType->fLength;
            colType.precision = createTable->fTableDef->fColumns[i]->fType->fPrecision;
            colType.scale = createTable->fTableDef->fColumns[i]->fType->fScale;
            boost::any convertedVal;
            bool pushWarning = false;

            try
            {
              convertedVal =
                  colType.convertColumnData(createTable->fTableDef->fColumns[i]->fDefaultValue->fValue,
                                            pushWarning, timeZoneOffset, false, false, false);
            }
            catch (std::exception&)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);
              thd->raise_error_printf(ER_INTERNAL_ERROR,
                                      "The default value is out of range for the specified data type.");
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if (pushWarning)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);
              thd->raise_error_printf(ER_INTERNAL_ERROR,
                                      "The default value is out of range for the specified data type.");
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }
            if (createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_TIMESTAMP)
            {
              if (createTable->fTableDef->fColumns[i]->fDefaultValue->fValue == "0")
              {
                createTable->fTableDef->fColumns[i]->fDefaultValue->fValue = "0000-00-00 00:00:00";
              }
              if (isFirstTimestamp)
              {
                isFirstTimestamp = false;
              }
            }
          }
        }
        // If no default value exists for TIMESTAMP, we apply
        // automatic TIMESTAMP properties.
        // TODO: If no default value exists but the constraint is NULL,
        // default value should be set to NULL. But this is currently
        // not supported since columnstore does not track whether user
        // specified a NULL or not
        else if (createTable->fTableDef->fColumns[i]->fType->fType == ddlpackage::DDL_TIMESTAMP)
        {
          if (isFirstTimestamp)
          {
            isFirstTimestamp = false;
            createTable->fTableDef->fColumns[i]->fDefaultValue =
                new ColumnDefaultValue("current_timestamp() ON UPDATE current_timestamp()");
          }
          else
          {
            createTable->fTableDef->fColumns[i]->fDefaultValue =
                new ColumnDefaultValue("0000-00-00 00:00:00");
          }
        }

        // Parse the column comment
        string comment = createTable->fTableDef->fColumns[i]->fComment;
        int compressionType = compressionTypeIn;

        if (comment.length() > 0)
        {
          compressionType = parseCompressionComment(comment);

          if (compressionType == MAX_INT)
          {
            compressionType = compressionTypeIn;
          }
          else if (compressionType < 0)
          {
            rc = 1;
            thd->raise_error_printf(
                ER_INTERNAL_ERROR,
                (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          if (compressionType == 1)
            compressionType = 2;

          if ((compressionType > 0) && !(compress::CompressInterface::isCompressionAvail(compressionType)))
          {
            rc = 1;
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
#ifdef SKIP_IDB_COMPRESSION
            Message::Args args;
            thd->get_stmt_da()->set_overwrite_status(true);
            args.add("The compression type");
            thd->raise_error_printf(ER_INTERNAL_ERROR,
                                    (IDBErrorInfo::instance()->errorMsg(ERR_ENTERPRISE_ONLY, args)).c_str());
#else
            thd->raise_error_printf(
                ER_INTERNAL_ERROR,
                (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
#endif
            return rc;
          }

          try
          {
            autoIncre = parseAutoincrementColumnComment(comment, startValue);

            if (autoIncre)
            {
              // Check whether there is a column with autoincrement already
              if ((isAnyAutoincreCol) &&
                  !(boost::iequals(autoiColName, createTable->fTableDef->fColumns[i]->fName)))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_NUMBER_AUTOINCREMENT)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
              else
              {
                isAnyAutoincreCol = true;
                autoiColName = createTable->fTableDef->fColumns[i]->fName;
                matchedCol = true;
              }

              // Check whether the column has default value. If there is, error out
              if (createTable->fTableDef->fColumns[i]->fDefaultValue)
              {
                if (!createTable->fTableDef->fColumns[i]->fDefaultValue->fNull)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "Autoincrement column cannot have a default value.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
            }
          }
          catch (runtime_error& ex)
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }
        }

        if (!autoIncre && isAnyAutoincreCol &&
            (boost::iequals(autoiColName, createTable->fTableDef->fColumns[i]->fName)))
        {
          autoIncre = true;
          matchedCol = true;
          startValue = nextvalue;
        }

        if (startValue <= 0)
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);
          thd->raise_error_printf(ER_INTERNAL_ERROR,
                                  (IDBErrorInfo::instance()->errorMsg(ERR_NEGATIVE_STARTVALUE)).c_str());
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }

        if (autoIncre)
        {
          if (!validateAutoincrementDatatype(createTable->fTableDef->fColumns[i]->fType->fType))
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(
                ER_CHECK_NOT_IMPLEMENTED,
                (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_AUTOINCREMENT_TYPE)).c_str());
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }
        }

        if (!validateNextValue(createTable->fTableDef->fColumns[i]->fType->fType, startValue))
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);
          thd->raise_error_printf(ER_INTERNAL_ERROR,
                                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_STARTVALUE)).c_str());
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }

        // hdfs
        if ((compressionType == 0) && (useHdfs))
        {
          compressionType = 2;
          string errmsg("The table is created with Columnstore compression type 2 under HDFS.");
          push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, errmsg.c_str());
        }

        (createTable->fTableDef->fColumns[i]->fType)->fCompressiontype = compressionType;

        if (autoIncre)
          (createTable->fTableDef->fColumns[i]->fType)->fAutoincrement = "y";
        else
          (createTable->fTableDef->fColumns[i]->fType)->fAutoincrement = "n";

        (createTable->fTableDef->fColumns[i]->fType)->fNextvalue = startValue;
      }

      if (isAnyAutoincreCol && !matchedCol)  //@Bug 3555 error out on invalid column
      {
        rc = 1;
        Message::Args args;
        thd->get_stmt_da()->set_overwrite_status(true);
        args.add(autoiColName);
        thd->raise_error_printf(ER_INTERNAL_ERROR,
                                (IDBErrorInfo::instance()->errorMsg(ERR_UNKNOWN_COL, args)).c_str());
        ci->alterTableState = cal_connection_info::NOT_ALTER;
        ci->isAlter = false;
        return rc;
      }
    }
    else if (typeid(stmt) == typeid(AlterTableStatement))
    {
      AlterTableStatement* alterTable = dynamic_cast<AlterTableStatement*>(&stmt);
      if (lower_case_table_names)
      {
        algorithm::to_lower(alterTable->fTableName->fSchema);
        algorithm::to_lower(alterTable->fTableName->fName);
      }

      alterTable->setTimeZone(timeZoneOffset);

      if (schema.length() == 0)
      {
        schema = alterTable->fTableName->fSchema;

        if (schema.length() == 0)
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);

          thd->raise_error_printf(ER_INTERNAL_ERROR, "No database selected.");
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }
      }

      ddlpackage::AlterTableActionList actionList = alterTable->fActions;

      if (actionList.size() > 1)  //@bug 3753 we don't support multiple actions in alter table statement
      {
        rc = 1;
        thd->get_stmt_da()->set_overwrite_status(true);

        thd->raise_error_printf(
            ER_CHECK_NOT_IMPLEMENTED,
            "Multiple actions in alter table statement is currently not supported by Columnstore.");

        ci->alterTableState = cal_connection_info::NOT_ALTER;
        ci->isAlter = false;
        return rc;
      }

      for (unsigned i = 0; i < actionList.size(); i++)
      {
        if (ddlpackage::AtaAddColumn* addColumnPtr = dynamic_cast<AtaAddColumn*>(actionList[i]))
        {
          // check varbinary data type
          if ((addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_VARBINARY) && !isVarbinaryAllowed)
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                    "Varbinary is currently not supported by Columnstore.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          if ((addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_VARBINARY) &&
              ((addColumnPtr->fColumnDef->fType->fLength > 8000) ||
               (addColumnPtr->fColumnDef->fType->fLength < 8)))
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_INTERNAL_ERROR, "Varbinary length has to be between 8 and 8000.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          uint64_t startValue = 1;
          bool autoIncre = false;

          if ((addColumnPtr->fColumnDef->fConstraints.size() > 0) || addColumnPtr->fColumnDef->fDefaultValue)
          {
            // support default value and NOT NULL constraint
            for (uint32_t j = 0; j < addColumnPtr->fColumnDef->fConstraints.size(); j++)
            {
              if (addColumnPtr->fColumnDef->fConstraints[j]->fConstraintType != DDL_NOT_NULL)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              // if not null constraint, user has to provide a default value
              if ((addColumnPtr->fColumnDef->fConstraints[j]->fConstraintType == DDL_NOT_NULL) &&
                  (!addColumnPtr->fColumnDef->fDefaultValue) &&
                  (addColumnPtr->fColumnDef->fType->fType != ddlpackage::DDL_TIMESTAMP))
              {
                // do select count(*) from the table to check whether there are existing rows. if there is,
                // error out.
                bool anyRow = false;

                try
                {
                  anyRow = anyRowInTable(alterTable->fTableName->fSchema, alterTable->fTableName->fName,
                                         sessionID);
                }
                catch (runtime_error& ex)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
                catch (...)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "Unknown exception caught when checking any rows in the table.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }

                if (anyRow)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(
                      ER_INTERNAL_ERROR,
                      "Table is not empty. New column has to have a default value if NOT NULL required.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
              else if ((addColumnPtr->fColumnDef->fConstraints[j]->fConstraintType == DDL_NOT_NULL) &&
                       (addColumnPtr->fColumnDef->fDefaultValue))
              {
                if (addColumnPtr->fColumnDef->fDefaultValue->fValue.length() ==
                    0)  // empty string is NULL in infinidb
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "New column has to have a default value if NOT NULL required.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
            }

            if (addColumnPtr->fColumnDef->fDefaultValue)
            {
              if ((!addColumnPtr->fColumnDef->fDefaultValue->fNull) &&
                  (addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_VARBINARY))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR, "Varbinary column cannot have default value.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              // validate the default value, if out of range, just error out
              CalpontSystemCatalog::ColDataType dataType;
              dataType = convertDataType(addColumnPtr->fColumnDef->fType->fType);
              CalpontSystemCatalog::ColType colType;
              colType.colDataType = dataType;
              colType.colWidth = addColumnPtr->fColumnDef->fType->fLength;
              colType.precision = addColumnPtr->fColumnDef->fType->fPrecision;
              colType.scale = addColumnPtr->fColumnDef->fType->fScale;
              boost::any convertedVal;
              bool pushWarning = false;

              try
              {
                convertedVal = colType.convertColumnData(addColumnPtr->fColumnDef->fDefaultValue->fValue,
                                                         pushWarning, timeZoneOffset, false, false, false);
              }
              catch (std::exception&)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        "The default value is out of range for the specified data type.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (pushWarning)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        "The default value is out of range for the specified data type.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
              if (addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_TIMESTAMP &&
                  addColumnPtr->fColumnDef->fDefaultValue->fValue == "0")
              {
                addColumnPtr->fColumnDef->fDefaultValue->fValue = "0000-00-00 00:00:00";
              }
            }
          }

          // For TIMESTAMP, if no constraint is given, default to NOT NULL
          if (addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_TIMESTAMP &&
              addColumnPtr->fColumnDef->fConstraints.empty())
          {
            addColumnPtr->fColumnDef->fConstraints.push_back(new ColumnConstraintDef(DDL_NOT_NULL));
          }

          // If no default value exists for TIMESTAMP, we apply
          // automatic TIMESTAMP properties.
          // TODO: If no default value exists but the constraint is NULL,
          // default value should be set to NULL. But this is currently
          // not supported since columnstore does not track whether user
          // specified a NULL or not
          if (addColumnPtr->fColumnDef->fType->fType == ddlpackage::DDL_TIMESTAMP &&
              !addColumnPtr->fColumnDef->fDefaultValue)
          {
            // Query calpontsys.syscolumn to see
            // if a timestamp column already exists in this table
            if (!anyTimestampColumn(alterTable->fTableName->fSchema, alterTable->fTableName->fName,
                                    sessionID))
            {
              addColumnPtr->fColumnDef->fDefaultValue =
                  new ColumnDefaultValue("current_timestamp() ON UPDATE current_timestamp()");
            }
            else
            {
              addColumnPtr->fColumnDef->fDefaultValue = new ColumnDefaultValue("0000-00-00 00:00:00");
            }
          }

          // Handle compression type
          string comment = addColumnPtr->fColumnDef->fComment;
          int compressionType = compressionTypeIn;

          if (comment.length() > 0)
          {
            //@Bug 3782 This is for synchronization after calonlinealter to use
            algorithm::to_upper(comment);
            regex pat("[[:space:]]*SCHEMA[[:space:]]+SYNC[[:space:]]+ONLY", regex_constants::extended);

            if (regex_search(comment, pat))
            {
              return 0;
            }

            compressionType = parseCompressionComment(comment);

            if (compressionType == MAX_INT)
            {
              compressionType = compressionTypeIn;
            }
            else if (compressionType < 0)
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType > 0) && !(compress::CompressInterface::isCompressionAvail(compressionType)))
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType == 0) && (useHdfs))
            {
              compressionType = 2;
              string errmsg("The column is created with Columnstore compression type 2 under HDFS.");
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, errmsg.c_str());
            }

            try
            {
              autoIncre = parseAutoincrementColumnComment(comment, startValue);
            }
            catch (runtime_error& ex)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);
              thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if (autoIncre)
            {
              // Check if the table already has autoincrement column
              boost::shared_ptr<CalpontSystemCatalog> csc =
                  CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
              csc->identity(execplan::CalpontSystemCatalog::FE);
              CalpontSystemCatalog::TableName tableName;
              tableName.schema = alterTable->fTableName->fSchema;
              tableName.table = alterTable->fTableName->fName;
              CalpontSystemCatalog::TableInfo tblInfo;

              try
              {
                tblInfo = csc->tableInfo(tableName);
              }
              catch (runtime_error& ex)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (tblInfo.tablewithautoincr == 1)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_NUMBER_AUTOINCREMENT)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (!validateAutoincrementDatatype(addColumnPtr->fColumnDef->fType->fType))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_AUTOINCREMENT_TYPE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (!validateNextValue(addColumnPtr->fColumnDef->fType->fType, startValue))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_STARTVALUE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (startValue <= 0)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_CHECK_NOT_IMPLEMENTED,
                    (IDBErrorInfo::instance()->errorMsg(ERR_NEGATIVE_STARTVALUE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
            }
          }

          addColumnPtr->fColumnDef->fType->fCompressiontype = compressionType;

          if (autoIncre)
            addColumnPtr->fColumnDef->fType->fAutoincrement = "y";
          else
            addColumnPtr->fColumnDef->fType->fAutoincrement = "n";

          addColumnPtr->fColumnDef->fType->fNextvalue = startValue;
        }
        else if (dynamic_cast<AtaAddTableConstraint*>(actionList[i]))
        {
          rc = 1;
          thd->get_stmt_da()->set_overwrite_status(true);

          thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                  (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str());
          ci->alterTableState = cal_connection_info::NOT_ALTER;
          ci->isAlter = false;
          return rc;
        }
        /*			else if ( dynamic_cast<AtaSetColumnDefault*> (actionList[i]) )
                                {
                                        rc = 1;
                                        thd->get_stmt_da()->set_overwrite_status(true);
                                        thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
           (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str()); ci->alterTableState =
           cal_connection_info::NOT_ALTER; ci->isAlter = false; return rc;
                                }
                                else if ( dynamic_cast<AtaDropColumnDefault*> (actionList[i]) )
                                {
                                        rc = 1;
                                        thd->get_stmt_da()->set_overwrite_status(true);
                                        thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
           (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str()); ci->alterTableState =
           cal_connection_info::NOT_ALTER; ci->isAlter = false; return rc;
                                }
        */
        else if (ddlpackage::AtaAddColumns* addColumnsPtr = dynamic_cast<AtaAddColumns*>(actionList[i]))
        {
          if ((addColumnsPtr->fColumns).size() > 1)
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(
                ER_CHECK_NOT_IMPLEMENTED,
                "Multiple actions in alter table statement is currently not supported by Columnstore.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          // check varbinary data type
          if ((addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_VARBINARY) && !isVarbinaryAllowed)
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                    "Varbinary is currently not supported by Columnstore.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          if ((addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_VARBINARY) &&
              ((addColumnsPtr->fColumns[0]->fType->fLength > 8000) ||
               (addColumnsPtr->fColumns[0]->fType->fLength < 8)))
          {
            rc = 1;
            thd->get_stmt_da()->set_overwrite_status(true);
            thd->raise_error_printf(ER_INTERNAL_ERROR, "Varbinary length has to be between 8 and 8000.");
            ci->alterTableState = cal_connection_info::NOT_ALTER;
            ci->isAlter = false;
            return rc;
          }

          uint64_t startValue = 1;
          bool autoIncre = false;

          if ((addColumnsPtr->fColumns[0]->fConstraints.size() > 0) ||
              addColumnsPtr->fColumns[0]->fDefaultValue)
          {
            //@Bug 5274. support default value and NOT NULL constraint
            for (uint32_t j = 0; j < addColumnsPtr->fColumns[0]->fConstraints.size(); j++)
            {
              if (addColumnsPtr->fColumns[0]->fConstraints[j]->fConstraintType != DDL_NOT_NULL)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              // if not null constraint, user has to provide a default value
              if ((addColumnsPtr->fColumns[0]->fConstraints[j]->fConstraintType == DDL_NOT_NULL) &&
                  (!addColumnsPtr->fColumns[0]->fDefaultValue) &&
                  (addColumnsPtr->fColumns[0]->fType->fType != ddlpackage::DDL_TIMESTAMP))
              {
                // do select count(*) from the table to check whether there are existing rows. if there is,
                // error out.
                bool anyRow = false;

                try
                {
                  anyRow = anyRowInTable(alterTable->fTableName->fSchema, alterTable->fTableName->fName,
                                         sessionID);
                }
                catch (runtime_error& ex)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
                catch (...)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "Unknown exception caught when checking any rows in the table.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }

                if (anyRow)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(
                      ER_INTERNAL_ERROR,
                      "Table is not empty. New column has to have a default value if NOT NULL required.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
              else if ((addColumnsPtr->fColumns[0]->fConstraints[j]->fConstraintType == DDL_NOT_NULL) &&
                       (addColumnsPtr->fColumns[0]->fDefaultValue))
              {
                if (addColumnsPtr->fColumns[0]->fDefaultValue->fValue.length() ==
                    0)  // empty string is NULL in infinidb
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "New column has to have a default value if NOT NULL required.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
            }

            if (addColumnsPtr->fColumns[0]->fDefaultValue)
            {
              if ((!addColumnsPtr->fColumns[0]->fDefaultValue->fNull) &&
                  (addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_VARBINARY))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR, "Varbinary column cannot have default value.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              // validate the default value, if out of range, just error out
              CalpontSystemCatalog::ColDataType dataType;
              dataType = convertDataType(addColumnsPtr->fColumns[0]->fType->fType);
              CalpontSystemCatalog::ColType colType;
              colType.colDataType = dataType;
              colType.colWidth = addColumnsPtr->fColumns[0]->fType->fLength;
              colType.precision = addColumnsPtr->fColumns[0]->fType->fPrecision;
              colType.scale = addColumnsPtr->fColumns[0]->fType->fScale;
              boost::any convertedVal;
              bool pushWarning = false;

              try
              {
                convertedVal = colType.convertColumnData(addColumnsPtr->fColumns[0]->fDefaultValue->fValue,
                                                         pushWarning, timeZoneOffset, false, false, false);
              }
              catch (std::exception&)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        "The default value is out of range for the specified data type.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (pushWarning)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        "The default value is out of range for the specified data type.");
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
              if (addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_TIMESTAMP &&
                  addColumnsPtr->fColumns[0]->fDefaultValue->fValue == "0")
              {
                addColumnsPtr->fColumns[0]->fDefaultValue->fValue = "0000-00-00 00:00:00";
              }
            }
          }

          // For TIMESTAMP, if no constraint is given, default to NOT NULL
          if (addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_TIMESTAMP &&
              addColumnsPtr->fColumns[0]->fConstraints.empty())
          {
            addColumnsPtr->fColumns[0]->fConstraints.push_back(new ColumnConstraintDef(DDL_NOT_NULL));
          }

          // If no default value exists for TIMESTAMP, we apply
          // automatic TIMESTAMP properties.
          // TODO: If no default value exists but the constraint is NULL,
          // default value should be set to NULL. But this is currently
          // not supported since columnstore does not track whether user
          // specified a NULL or not
          if (addColumnsPtr->fColumns[0]->fType->fType == ddlpackage::DDL_TIMESTAMP &&
              !addColumnsPtr->fColumns[0]->fDefaultValue)
          {
            // Query calpontsys.syscolumn to see
            // if a timestamp column already exists in this table
            if (!anyTimestampColumn(alterTable->fTableName->fSchema, alterTable->fTableName->fName,
                                    sessionID))
            {
              addColumnsPtr->fColumns[0]->fDefaultValue =
                  new ColumnDefaultValue("current_timestamp() ON UPDATE current_timestamp()");
            }
            else
            {
              addColumnsPtr->fColumns[0]->fDefaultValue = new ColumnDefaultValue("0000-00-00 00:00:00");
            }
          }

          // Handle compression type
          string comment = addColumnsPtr->fColumns[0]->fComment;
          int compressionType = compressionTypeIn;

          if (comment.length() > 0)
          {
            compressionType = parseCompressionComment(comment);

            if (compressionType == MAX_INT)
            {
              compressionType = compressionTypeIn;
            }
            else if (compressionType < 0)
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType > 0) && !(compress::CompressInterface::isCompressionAvail(compressionType)))
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType == 0) && (useHdfs))
            {
              compressionType = 2;
              string errmsg("The column is created with Columnstore compression type 2 under HDFS.");
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, errmsg.c_str());
            }

            try
            {
              autoIncre = parseAutoincrementColumnComment(comment, startValue);
            }
            catch (runtime_error& ex)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);
              thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if (autoIncre)
            {
              // Check if the table already has autoincrement column
              boost::shared_ptr<CalpontSystemCatalog> csc =
                  CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
              csc->identity(execplan::CalpontSystemCatalog::FE);
              CalpontSystemCatalog::TableName tableName;
              tableName.schema = alterTable->fTableName->fSchema;
              tableName.table = alterTable->fTableName->fName;
              CalpontSystemCatalog::TableInfo tblInfo;

              try
              {
                tblInfo = csc->tableInfo(tableName);
              }
              catch (runtime_error& ex)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (tblInfo.tablewithautoincr == 1)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_NUMBER_AUTOINCREMENT)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (!validateAutoincrementDatatype(addColumnsPtr->fColumns[0]->fType->fType))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_AUTOINCREMENT_TYPE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (!validateNextValue(addColumnsPtr->fColumns[0]->fType->fType, startValue))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_STARTVALUE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (startValue <= 0)
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR, (IDBErrorInfo::instance()->errorMsg(ERR_NEGATIVE_STARTVALUE)).c_str());
                return rc;
              }
            }
          }

          addColumnsPtr->fColumns[0]->fType->fCompressiontype = compressionType;

          if (autoIncre)
            addColumnsPtr->fColumns[0]->fType->fAutoincrement = "y";
          else
            addColumnsPtr->fColumns[0]->fType->fAutoincrement = "n";

          addColumnsPtr->fColumns[0]->fType->fNextvalue = startValue;
        }
        else if (ddlpackage::AtaRenameColumn* renameColumnsPtr =
                     dynamic_cast<AtaRenameColumn*>(actionList[i]))
        {
          uint64_t startValue = 1;
          bool autoIncre = false;
          //@Bug 3746 Handle compression type
          string comment = renameColumnsPtr->fComment;
          int compressionType = compressionTypeIn;

          if (comment.length() > 0)
          {
            compressionType = parseCompressionComment(comment);

            if (compressionType == MAX_INT)
            {
              compressionType = compressionTypeIn;
            }
            else if (compressionType < 0)
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType > 0) && !(compress::CompressInterface::isCompressionAvail(compressionType)))
            {
              rc = 1;
              thd->raise_error_printf(
                  ER_INTERNAL_ERROR,
                  (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE)).c_str());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if ((compressionType == 0) && (useHdfs))
            {
              compressionType = 2;
              string errmsg("The column is created with Columnstore compression type 2 under HDFS.");
              push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, errmsg.c_str());
            }
          }

          // Handle autoincrement
          if (comment.length() > 0)
          {
            try
            {
              autoIncre = parseAutoincrementColumnComment(comment, startValue);
            }
            catch (runtime_error& ex)
            {
              rc = 1;
              thd->get_stmt_da()->set_overwrite_status(true);
              thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
              ci->alterTableState = cal_connection_info::NOT_ALTER;
              ci->isAlter = false;
              return rc;
            }

            if (autoIncre)
            {
              // Check if the table already has autoincrement column
              CalpontSystemCatalog::TableName tableName;
              tableName.schema = alterTable->fTableName->fSchema;
              tableName.table = alterTable->fTableName->fName;
              // CalpontSystemCatalog::TableInfo tblInfo = csc->tableInfo(tableName);

              //@Bug 5444. rename column doen't need to check this.
              /*	if (tblInfo.tablewithautoincr == 1)
                  {
                          rc = 1;
                          thd->get_stmt_da()->set_overwrite_status(true);
                          thd->raise_error_printf(ER_INTERNAL_ERROR,
                 (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_NUMBER_AUTOINCREMENT)).c_str());
                          ci->alterTableState = cal_connection_info::NOT_ALTER;
                          ci->isAlter = false;
                          return rc;
                  } */

              if (!validateAutoincrementDatatype(renameColumnsPtr->fNewType->fType))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(
                    ER_INTERNAL_ERROR,
                    (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_AUTOINCREMENT_TYPE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }

              if (!validateNextValue(renameColumnsPtr->fNewType->fType, startValue))
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_INVALID_STARTVALUE)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
            }
          }

          if (autoIncre)
            renameColumnsPtr->fNewType->fAutoincrement = "y";
          else
            renameColumnsPtr->fNewType->fAutoincrement = "n";

          renameColumnsPtr->fNewType->fNextvalue = startValue;
          renameColumnsPtr->fNewType->fCompressiontype = compressionType;

          if (renameColumnsPtr->fConstraints.size() > 0)
          {
            for (uint32_t j = 0; j < renameColumnsPtr->fConstraints.size(); j++)
            {
              if (renameColumnsPtr->fConstraints[j]->fConstraintType == DDL_NOT_NULL)
              {
                // If NOT NULL constraint is added, check whether the existing rows has null vlues. If there
                // is, error out the query.
                bool anyNullVal = false;

                try
                {
                  anyNullVal =
                      anyNullInTheColumn(thd, alterTable->fTableName->fSchema, alterTable->fTableName->fName,
                                         renameColumnsPtr->fName, sessionID);
                }
                catch (runtime_error& ex)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
                catch (...)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(
                      ER_INTERNAL_ERROR,
                      "Unknown exception caught when checking any existing null values in the column.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }

                if (anyNullVal)
                {
                  rc = 1;
                  thd->get_stmt_da()->set_overwrite_status(true);
                  thd->raise_error_printf(ER_INTERNAL_ERROR,
                                          "The existing rows in this column has null value already.");
                  ci->alterTableState = cal_connection_info::NOT_ALTER;
                  ci->isAlter = false;
                  return rc;
                }
              }
              else
              {
                rc = 1;
                thd->get_stmt_da()->set_overwrite_status(true);
                thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                                        (IDBErrorInfo::instance()->errorMsg(ERR_CONSTRAINTS)).c_str());
                ci->alterTableState = cal_connection_info::NOT_ALTER;
                ci->isAlter = false;
                return rc;
              }
            }
          }
        }
        else
        {
        }
      }
    }
    //@Bug 5923 error out on unsupported statements.
    else if ((typeid(stmt) != typeid(DropTableStatement)) && (typeid(stmt) != typeid(TruncTableStatement)) &&
             (typeid(stmt) != typeid(MarkPartitionStatement)) &&
             (typeid(stmt) != typeid(RestorePartitionStatement)) &&
             (typeid(stmt) != typeid(DropPartitionStatement)))
    {
      rc = 1;
      thd->get_stmt_da()->set_overwrite_status(true);
      thd->raise_error_printf(ER_INTERNAL_ERROR,
                              (IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_SYNTAX)).c_str());
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
      return rc;
    }

    //@Bug 4387
    scoped_ptr<DBRM> dbrmp(new DBRM());
    int rc = dbrmp->isReadWrite();

    if (rc != 0)
    {
      rc = 1;
      thd->get_stmt_da()->set_overwrite_status(true);
      thd->raise_error_printf(ER_INTERNAL_ERROR, "Cannot execute the statement. DBRM is read only!");
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
      return rc;
    }

    stmt.fSessionID = sessionID;
    stmt.fSql = ddlStatement;
    stmt.fOwner = schema;
    stmt.fTableWithAutoi = isAnyAutoincreCol;
    ByteStream bytestream;
    bytestream << stmt.fSessionID;
    stmt.serialize(bytestream);
    MessageQueueClient mq("DDLProc");
    ByteStream::byte b = 0;

    try
    {
      mq.write(bytestream);
      bytestream = mq.read();

      if (bytestream.length() == 0)
      {
        rc = 1;
        thd->get_stmt_da()->set_overwrite_status(true);

        thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DDLProc");
        ci->alterTableState = cal_connection_info::NOT_ALTER;
        ci->isAlter = false;
      }
      else
      {
        bytestream >> b;
        bytestream >> emsg;
        rc = b;
      }
    }
    catch (runtime_error& e)
    {
      rc = 1;
      thd->get_stmt_da()->set_overwrite_status(true);

      thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DDLProc");
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
    }
    catch (...)
    {
      rc = 1;
      thd->get_stmt_da()->set_overwrite_status(true);

      thd->raise_error_printf(ER_INTERNAL_ERROR, "Unknown error caught");
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
    }

    if (b == ddlpackageprocessor::DDLPackageProcessor::DROP_TABLE_NOT_IN_CATALOG_ERROR)
    {
      return ER_NO_SUCH_TABLE_IN_ENGINE;
    }

    if ((b != 0) && (b != ddlpackageprocessor::DDLPackageProcessor::WARNING))
    {
      thd->get_stmt_da()->set_overwrite_status(true);

      thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
    }

    if (b == ddlpackageprocessor::DDLPackageProcessor::WARNING)
    {
      rc = 0;
      string errmsg(
          "Error occured during file deletion. Restart DDLProc or use command tool ddlcleanup to clean up. ");
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, errmsg.c_str());
    }

    return rc;
  }
  else
  {
    rc = 1;
    thd->get_stmt_da()->set_overwrite_status(true);
    //@Bug 3602. Error message for MySql syntax for autoincrement
    algorithm::to_upper(ddlStatement);

    if (ddlStatement.find("AUTO_INCREMENT") != string::npos)
    {
      thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                              "Use of the MySQL auto_increment syntax is not supported in Columnstore. If "
                              "you wish to create an auto increment column in Columnstore, please consult "
                              "the Columnstore SQL Syntax Guide for the correct usage.");
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
    }
    else
    {
      //@Bug 1888,1885. update error message
      thd->raise_error_printf(ER_CHECK_NOT_IMPLEMENTED,
                              "The syntax or the data type(s) is not supported by Columnstore. Please check "
                              "the Columnstore syntax guide for supported syntax or data types.");
      ci->alterTableState = cal_connection_info::NOT_ALTER;
      ci->isAlter = false;
    }
  }

  return rc;
}

}  // namespace

//
// get_field_default_value: Returns the default value as a string value
// NOTE: This is duplicated code copied from show.cc and a MDEV-17006 has
//       been created.
//

static bool get_field_default_value(THD* thd, Field* field, String* def_value, bool quoted)
{
  bool has_default;
  enum enum_field_types field_type = field->type();

  has_default = (field->default_value ||
                 (!(field->flags & NO_DEFAULT_VALUE_FLAG) && field->unireg_check != Field::NEXT_NUMBER));

  def_value->length(0);
  if (has_default)
  {
    StringBuffer<MAX_FIELD_WIDTH> str(field->charset());
    if (field->default_value)
    {
      field->default_value->print(&str);
      if (field->default_value->expr->need_parentheses_in_default())
      {
        def_value->set_charset(&my_charset_utf8mb4_general_ci);
        def_value->append('(');
        def_value->append(str);
        def_value->append(')');
      }
      else
        def_value->append(str);
    }
    else if (!field->is_null())
    {  // Not null by default
      if (field_type == MYSQL_TYPE_BIT)
      {
        str.qs_append('b');
        str.qs_append('\'');
        str.qs_append(field->val_int(), 2);
        str.qs_append('\'');
        quoted = 0;
      }
      else
      {
        field->val_str(&str);
        if (!field->str_needs_quotes())
          quoted = 0;
      }
      if (str.length())
      {
        StringBuffer<MAX_FIELD_WIDTH> def_val;
        uint dummy_errors;
        /* convert to system_charset_info == utf8 */
        def_val.copy(str.ptr(), str.length(), field->charset(), system_charset_info, &dummy_errors);
        if (quoted)
          append_unescaped(def_value, def_val.ptr(), def_val.length());
        else
          def_value->append(def_val);
      }
      else if (quoted)
        def_value->set(STRING_WITH_LEN("''"), system_charset_info);
    }
    else if (field->maybe_null() && quoted)
      def_value->set(STRING_WITH_LEN("NULL"), system_charset_info);  // Null as default
    else
      return 0;
  }
  return has_default;
}

/*
    Utility function search for ZEROFILL
*/

bool hasZerofillDecimal(TABLE* table_arg)
{
  for (Field** field = table_arg->field; *field; field++)
  {
    if (((*field)->flags & ZEROFILL_FLAG) && typeid(**field) == typeid(Field_new_decimal))
      return true;
  }

  return false;
}

int ha_mcs_impl_create_(const char* name, TABLE* table_arg, HA_CREATE_INFO* create_info,
                        cal_connection_info& ci)
{
#ifdef MCS_DEBUG
  cout << "ha_mcs_impl_create_: " << name << endl;
#endif
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return 0;

  char* query = thd->query();

  if (!query)
  {
    setError(thd, ER_INTERNAL_ERROR, "Attempt to create table, but query is NULL");
    return 1;
  }

  string stmt(query);
  stmt += ";";
  algorithm::to_upper(stmt);

  string db, tbl;
  db = table_arg->s->db.str;
  tbl = table_arg->s->table_name.str;

  string tablecomment;
  bool isAnyAutoincreCol = false;
  std::string columnName("");
  uint64_t startValue = 1;

  if (table_arg->s->comment.length > 0)
  {
    tablecomment = table_arg->s->comment.str;

    try
    {
      isAnyAutoincreCol = parseAutoincrementTableComment(tablecomment, startValue, columnName);
    }
    catch (runtime_error& ex)
    {
      setError(thd, ER_INTERNAL_ERROR, ex.what());
      return 1;
    }

    algorithm::to_upper(tablecomment);
  }

  //@Bug 2553 Add a parenthesis around option to group them together
  string alterpatstr(
      "ALTER[[:space:]]+TABLE[[:space:]]+.*[[:space:]]+(((ADD)|(DROP)|(CHANGE)|(ALTER)|(COMMENT))[[:space:]]+"
      "|(COMMENT=))");
  string createpatstr("CREATE[[:space:]]+TABLE[[:space:]]");
  bool schemaSyncOnly = false;
  bool isCreate = true;

  regex pat("[[:space:]]*SCHEMA[[:space:]]+SYNC[[:space:]]+ONLY", regex_constants::extended);

  if (regex_search(tablecomment, pat))
  {
    schemaSyncOnly = true;
    pat = createpatstr;

    if (!regex_search(stmt, pat))
    {
      isCreate = false;
    }

    if (isCreate)
    {
#ifdef MCS_DEBUG
      cout << "ha_mcs_impl_create_: SCHEMA SYNC ONLY found, returning" << endl;
#endif
      return 0;
    }
  }
  else
  {
    if (db == "calpontsys")
    {
      setError(thd, ER_INTERNAL_ERROR, "Calpont system tables can only be created with 'SCHEMA SYNC ONLY'");
      return 1;
    }
    else if (db == "infinidb_vtable")  //@bug 3540. table created in infinidb_vtable schema could be dropped
                                       //when select statement happen to have same tablename.
    {
      setError(thd, ER_INTERNAL_ERROR, "Table creation is not allowed in infinidb_vtable schema.");
      return 1;
    }
  }

  pat = alterpatstr;

  if (regex_search(stmt, pat))
  {
    ci.isAlter = true;
    ci.alterTableState = cal_connection_info::ALTER_FIRST_RENAME;
#ifdef MCS_DEBUG
    cout << "ha_mcs_impl_create_: now in state ALTER_FIRST_RENAME" << endl;
#endif
  }

  string emsg;
  stmt = thd->query();
  stmt += ";";
  int rc = 0;

  // Don't do the DDL (only for create table if this is SSO. Should only get here on ATAC w/SSO.
  if (schemaSyncOnly && isCreate)
    return rc;

  //@bug 5660. Error out REAL DDL/DML on slave node.
  // When the statement gets here, it's NOT SSO or RESTRICT
  if (ci.isSlaveNode)
  {
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
    setError(thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return 1;
  }

  // Send notice if primary key specified that it is not supported
  if (table_arg->key_info && table_arg->key_info->name.length &&
      string(table_arg->key_info->name.str) == "PRIMARY")
  {
    push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, WARN_OPTION_IGNORED, "INDEXES");
  }

  int compressiontype = get_compression_type(thd);

  // MCOL-4685:
  // remove the option to declare uncompressed columns (set infinidb_compression_type = 0).
  if (compressiontype == 1 || compressiontype == 0)
    compressiontype = 2;

  // string tablecomment;
  if (table_arg->s->comment.length > 0)
  {
    tablecomment = table_arg->s->comment.str;
    compressiontype = parseCompressionComment(tablecomment);
  }

  if (compressiontype == MAX_INT)
  {
    compressiontype = get_compression_type(thd);
    // MCOL-4685:
    // remove the option to declare uncompressed columns (set infinidb_compression_type = 0).
    if (compressiontype == 0)
      compressiontype = 2;
  }
  else if (compressiontype < 0)
  {
    string emsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE);
    setError(thd, ER_INTERNAL_ERROR, emsg);
    ci.alterTableState = cal_connection_info::NOT_ALTER;
    ci.isAlter = false;
    return 1;
  }

  // hdfs
  if ((compressiontype == 0) && (useHdfs))
  {
    compressiontype = 2;
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999,
                 "The table is created with Columnstore compression type 2 under HDFS.");
  }

  if (compressiontype == 1)
    compressiontype = 2;

  if ((compressiontype > 0) && !(compress::CompressInterface::isCompressionAvail(compressiontype)))
  {
    string emsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_COMPRESSION_TYPE);
    setError(thd, ER_INTERNAL_ERROR, emsg);
    ci.alterTableState = cal_connection_info::NOT_ALTER;
    ci.isAlter = false;
    return 1;
  }

  // Check if this is one of
  // * "CREATE TABLE ... LIKE "
  // * "ALTER TABLE ... ENGINE=Columnstore"
  // * "CREATE TABLE ... AS ..."
  // If so generate a full create table statement using the properties of
  // the source table. Note that source table has to be a columnstore table and
  // we only check for currently supported options.
  //

  if ((thd->lex->sql_command == SQLCOM_CREATE_TABLE && thd->lex->used_tables) ||
      (thd->lex->sql_command == SQLCOM_ALTER_TABLE && create_info->used_fields & HA_CREATE_USED_ENGINE) ||
      (thd->lex->create_info.like()))
  {
    TABLE_SHARE* share = table_arg->s;
    MY_BITMAP* old_map;  // To save the read_set
    char datatype_buf[MAX_FIELD_WIDTH], def_value_buf[MAX_FIELD_WIDTH];
    String datatype, def_value;
    ostringstream oss;
    string tbl_name = "`" + string(share->db.str) + "`.`" + string(share->table_name.str) + "`";

    // Save the current read_set map and mark it for read
    old_map = tmp_use_all_columns(table_arg, &table_arg->read_set);

    oss << "CREATE TABLE " << tbl_name << " (";

    restore_record(table_arg, s->default_values);
    for (Field** field = table_arg->field; *field; field++)
    {
      uint flags = (*field)->flags;
      datatype.set(datatype_buf, sizeof(datatype_buf), system_charset_info);
      (*field)->sql_type(datatype);
      if (field != table_arg->field)
        oss << ", ";
      oss << "`" << (*field)->field_name.str << "` " << datatype.ptr();

      if ((*field)->has_charset())
      {
        const CHARSET_INFO* field_cs = (*field)->charset();
        if (field_cs && (!share->table_charset || field_cs->number != share->table_charset->number))
        {
          oss << " CHARACTER SET " << field_cs->cs_name.str;
        }
      }

      if (flags & NOT_NULL_FLAG)
        oss << " NOT NULL";

      def_value.set(def_value_buf, sizeof(def_value_buf), system_charset_info);
      if (get_field_default_value(thd, *field, &def_value, true))
      {
        oss << " DEFAULT " << def_value.c_ptr();
      }

      if ((*field)->comment.length)
      {
        String comment;
        append_unescaped(&comment, (*field)->comment.str, (*field)->comment.length);
        oss << " COMMENT ";
        oss << comment.c_ptr();
      }
    }
    // End the list of columns
    oss << ") ENGINE=columnstore ";

    // Process table level options

    /* TODO: uncomment when we support AUTO_INCREMENT
            if (create_info->auto_increment_value > 1)
            {
                    oss << " AUTO_INCREMENT=" << create_info->auto_increment_value;
            }
    */

    if (create_info->auto_increment_value > 1)
    {
      push_warning(thd, Sql_condition::WARN_LEVEL_NOTE, WARN_OPTION_IGNORED, "AUTO INCREMENT");
    }

    if (share->table_charset)
    {
      oss << " DEFAULT CHARSET=" << share->table_charset->cs_name.str;
    }

    // Process table level options such as MIN_ROWS, MAX_ROWS, COMMENT

    if (share->min_rows)
    {
      char buff[80];
      longlong10_to_str(share->min_rows, buff, 10);
      oss << " MIN_ROWS=" << buff;
    }

    if (share->max_rows)
    {
      char buff[80];
      longlong10_to_str(share->max_rows, buff, 10);
      oss << " MAX_ROWS=" << buff;
    }

    if (share->comment.length)
    {
      String comment;
      append_unescaped(&comment, share->comment.str, share->comment.length);
      oss << " COMMENT ";
      oss << comment.c_ptr();
    }

    oss << ";";
    stmt = oss.str();

    tmp_restore_column_map(&table_arg->read_set, old_map);
  }

  rc = ProcessDDLStatement(stmt, db, tbl, tid2sid(thd->thread_id), emsg, compressiontype, isAnyAutoincreCol,
                           startValue, columnName, create_info->default_table_charset);

  if (rc != 0)
  {
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, emsg.c_str());
    // Bug 1705 reset the flag if error occurs
    ci.alterTableState = cal_connection_info::NOT_ALTER;
    ci.isAlter = false;
#ifdef MCS_DEBUG
    cout << "ha_mcs_impl_create_: ProcessDDL error, now in state NOT_ALTER" << endl;
#endif
  }
  else
  {
    if (hasZerofillDecimal(table_arg))
    {
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, WARN_OPTION_IGNORED,
                   "ZEROFILL is ignored in ColumnStore");
    }
  }

  return rc;
}

int ha_mcs_impl_delete_table_(const char* db, const char* name, cal_connection_info& ci)
{
#ifdef MCS_DEBUG
  cout << "ha_mcs_impl_delete_table: " << db << name << endl;
#endif
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return 0;

  char* query = thd->query();

  if (!query)
  {
    setError(thd, ER_INTERNAL_ERROR, "Attempt to drop table, but query is NULL");
    return 1;
  }

  std::string stmt(query);
  algorithm::to_upper(stmt);
  // @bug 4158 allow table name with 'restrict' in it (but not by itself)
  std::string::size_type fpos;
  fpos = stmt.rfind(" RESTRICT");

  if ((fpos != std::string::npos) && ((stmt.size() - fpos) == 9))  // last 9 chars of stmt are " RESTRICT"
  {
    return 0;
  }

  //@bug 5660. Error out REAL DDL/DML on slave node.
  // When the statement gets here, it's NOT SSO or RESTRICT
  if (ci.isSlaveNode)
  {
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
    setError(thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return 1;
  }

  std::string emsg;

  char decodedSchema[FN_REFLEN];
  char decodedTbl[FN_REFLEN];
  decode_file_path(name, decodedSchema, decodedTbl);
  std::string schema(decodedSchema);
  std::string tbl(decodedTbl);

  stmt.clear();
  stmt.assign("DROP TABLE `");
  stmt.append(decodedSchema);
  stmt.append("`.`");
  stmt.append(decodedTbl);
  stmt.append("`;");

  int rc = ProcessDDLStatement(stmt, schema, tbl, tid2sid(thd->thread_id), emsg);

  if (rc != 0 && rc != ER_NO_SUCH_TABLE_IN_ENGINE)
  {
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, emsg.c_str());
  }

  return rc;
}

int ha_mcs_impl_rename_table_(const char* from, const char* to, cal_connection_info& ci)
{
  THD* thd = current_thd;

  if (thd->slave_thread && !get_replication_slave(thd))
    return 0;

  string emsg;

  string tblFrom(from + 2);
  size_t pos = tblFrom.find("/");
  std::string dbFrom = tblFrom.substr(0, pos);
  tblFrom = tblFrom.erase(0, pos + 1);

  string tblTo(to + 2);
  pos = tblTo.find("/");
  std::string dbTo = tblTo.substr(0, pos);
  tblTo = tblTo.erase(0, pos + 1);

  string stmt;

  // This is a temporary table rename, we don't use the temporary table name
  // so this is a NULL op
  if (tblFrom.compare(0, 4, "#sql") == 0)
  {
    return 0;
  }

  //@bug 5660. Error out REAL DDL/DML on slave node.
  // When the statement gets here, it's NOT SSO or RESTRICT
  if (ci.isSlaveNode)
  {
    string emsg = logging::IDBErrorInfo::instance()->errorMsg(ERR_DML_DDL_SLAVE);
    setError(current_thd, ER_CHECK_NOT_IMPLEMENTED, emsg);
    return 1;
  }

  stmt.assign("alter table `");
  stmt.append(dbFrom);
  stmt.append("`.`");
  stmt.append(tblFrom);
  stmt.append("` rename to `");
  stmt.append(dbTo);
  stmt.append("`.`");
  stmt.append(tblTo);
  stmt.append("`;");

  string db;
  if (thd->db.length)
    db = thd->db.str;
  else
    db.assign(dbFrom);

  int rc = ProcessDDLStatement(stmt, db, "", tid2sid(thd->thread_id), emsg);

  if (rc != 0)
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, emsg.c_str());

  return rc;
}

extern "C"
{
#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      long long calonlinealter(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
  {
    string stmt(args->args[0], args->lengths[0]);

    string emsg;
    THD* thd = current_thd;
    string db("");

    if (thd->db.length)
      db = thd->db.str;

    // MCOL-4685:
    // remove the option to declare uncompressed columns (set infinidb_compression_type = 0).
    int compressiontype = get_compression_type(thd);

    if (compressiontype == 1 || compressiontype == 0)
      compressiontype = 2;

    if (compressiontype == MAX_INT)
    {
      // MCOL-4685: remove the option to declare uncompressed columns (set
      // infinidb_compression_type = 0).
      compressiontype = get_compression_type(thd);
      if (compressiontype == 0)
        compressiontype = 2;
    }

    if (compressiontype == 1)
      compressiontype = 2;

    int rc = ProcessDDLStatement(stmt, db, "", tid2sid(thd->thread_id), emsg, compressiontype);

    if (rc != 0)
      push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, emsg.c_str());

    return rc;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool calonlinealter_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT)
    {
      strcpy(message, "CALONLINEALTER() requires one string argument");
      return 1;
    }

    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void calonlinealter_deinit(UDF_INIT* initid)
  {
  }
}

// vim:ts=4 sw=4:
