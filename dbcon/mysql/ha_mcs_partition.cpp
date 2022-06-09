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

/*
 * $Id: ha_mcs_partition.cpp 9642 2013-06-24 14:57:42Z rdempsey $
 */

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
//#include <unistd.h>
#include <iomanip>
using namespace std;

#include <boost/tokenizer.hpp>

#include "idb_mysql.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
using namespace execplan;

#include "mastersegmenttable.h"
#include "extentmap.h"
#include "dbrm.h"
#include "brmtypes.h"
using namespace BRM;

#include "dataconvert.h"
using namespace dataconvert;
#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "bytestream.h"
using namespace messageqcpp;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "errorids.h"
#include "idberrorinfo.h"
#include "exceptclasses.h"
using namespace logging;

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

namespace
{
datatypes::SimpleValue getStartVal(const datatypes::SessionParam& sp, const CalpontSystemCatalog::ColType& ct,
                                   const char* val, datatypes::round_style_t& rfMin)
{
  const datatypes::TypeHandler* h = ct.typeHandler();
  return val ? h->toSimpleValue(sp, ct, val, rfMin) : h->getMinValueSimple();
}

datatypes::SimpleValue getEndVal(const datatypes::SessionParam& sp, const CalpontSystemCatalog::ColType& ct,
                                 const char* val, datatypes::round_style_t& rfMax)
{
  const datatypes::TypeHandler* h = ct.typeHandler();
  return val ? h->toSimpleValue(sp, ct, val, rfMax) : h->getMaxValueSimple();
}

// convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
  return CalpontSystemCatalog::idb_tid2sid(tid);
}

void CHECK(int rc)
{
  if (rc != 0)
  {
    ostringstream oss;
    oss << "Error in DBRM call " << rc << endl;
    throw runtime_error(oss.str());
  }
}

// partition warnings are delimited by '\n'
void push_warnings(THD* thd, string& warnings)
{
  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep("\n");
  tokenizer tokens(warnings, sep);

  for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
  {
    push_warning(thd, Sql_condition::WARN_LEVEL_WARN, 9999, (*tok_iter).c_str());
  }
}

string name(CalpontSystemCatalog::ColType& ct)
{
  const datatypes::TypeHandler* h = ct.typeHandler();
  if (!h)
    return "Unknown Type";
  return h->print(ct);
}

bool CP_type(CalpontSystemCatalog::ColType& ct)
{
  const datatypes::TypeHandler* h = ct.typeHandler();
  if (!h)
    return false;
  return h->CP_type(ct);
}

typedef map<LogicalPartition, datatypes::MinMaxPartitionInfo> PartitionMap;

void parsePartitionString(UDF_ARGS* args, int offset, set<LogicalPartition>& partitionNums, string& errMsg,
                          execplan::CalpontSystemCatalog::TableName tableName)
{
  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(tableName.schema);
  }

  if (tableName.schema == "calpontsys")
  {
    errMsg = IDBErrorInfo::instance()->errorMsg(SYSTABLE_PARTITION);
    return;
  }

  partitionNums.clear();

  string partStr = args->args[offset];
  char* partStrNoSpace = (char*)alloca(partStr.length() + 1);
  char* tmp = partStrNoSpace;

  // trim off space
  for (uint32_t i = 0; i < partStr.length(); i++)
  {
    if (partStr[i] == ' ' || partStr[i] == '\t')
      continue;

    *tmp = partStr[i];
    ++tmp;
  }

  *tmp = 0;
  string parts(partStrNoSpace);

  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep(",");
  boost::char_separator<char> sep1(".");
  tokenizer tokens(parts, sep);
  stringstream ss(stringstream::in | stringstream::out);

  for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
  {
    // cout << "token " << (*tok_iter) << endl;
    LogicalPartition lp;

    tokenizer tokens1((*tok_iter), sep1);
    uint32_t ctn = 0;

    for (tokenizer::iterator tok_iter1 = tokens1.begin(); tok_iter1 != tokens1.end(); ++tok_iter1, ctn++)
    {
      ss << *tok_iter1;

      switch (ctn)
      {
        case 0:
          if (ss >> lp.pp && ss.eof())
            break;

          goto error;

        case 1:
          if (ss >> lp.seg && ss.eof())
            break;

          goto error;

        case 2:
          if (ss >> lp.dbroot && ss.eof())
            break;

          goto error;

        default: goto error;
      }

      ss.clear();
      ss.str("");
    }

    partitionNums.insert(lp);
  }

  if (partitionNums.empty())
  {
    goto error;
  }

  return;

error:
  errMsg = "Invalid partition identifier(s)";
  return;

  // debug
  set<LogicalPartition>::const_iterator it;
  cout << "Partition: ";

  for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
    cout << (*it) << endl;
}

int processPartition(SqlStatement* stmt)
{
  // cout << "Sending to DDLProc" << endl;
  ByteStream bytestream;
  bytestream << stmt->fSessionID;
  stmt->serialize(bytestream);
  MessageQueueClient mq("DDLProc");
  ByteStream::byte b = 0;
  int rc = 0;
  THD* thd = current_thd;
  string emsg;

  try
  {
    mq.write(bytestream);
    bytestream = mq.read();

    if (bytestream.length() == 0)
    {
      rc = 1;
      thd->get_stmt_da()->set_overwrite_status(true);
      thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DDLProc");
    }
    else
    {
      bytestream >> b;
      bytestream >> emsg;
      rc = b;
    }
  }
  catch (runtime_error&)
  {
    rc = 1;
    thd->get_stmt_da()->set_overwrite_status(true);
    thd->raise_error_printf(ER_INTERNAL_ERROR, "Lost connection to DDLProc");
  }
  catch (...)
  {
    rc = 1;
    thd->get_stmt_da()->set_overwrite_status(true);
    thd->raise_error_printf(ER_INTERNAL_ERROR, "Unknown error caught");
  }

  if (b == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
  {
    rc = b;
    push_warnings(thd, emsg);
  }
  else if (b == ddlpackageprocessor::DDLPackageProcessor::PARTITION_WARNING)
  {
    rc = 0;
    push_warnings(thd, emsg);
  }
  else if (b == ddlpackageprocessor::DDLPackageProcessor::WARNING)
  {
    rc = 0;
    string errmsg(
        "Error occured during partitioning operation. Restart DMLProc or use command tool ddlcleanup to "
        "clean up. ");
    push_warnings(thd, errmsg);
  }
  else if (b != 0 && b != ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
  {
    thd->get_stmt_da()->set_overwrite_status(true);
    thd->raise_error_printf(ER_INTERNAL_ERROR, emsg.c_str());
  }

  return rc;
}

static void addPartition(const CalpontSystemCatalog::ColType& ct, DBRM& em, const BRM::EMEntry& entry,
                         PartitionMap& partMap, const LogicalPartition& logicalPartNum)
{
  const datatypes::TypeHandler* h = ct.typeHandler();
  int state;
  datatypes::MinMaxPartitionInfo partInfo = h->getExtentPartitionInfo(ct, em, entry, &state);

  PartitionMap::iterator mapit = partMap.find(logicalPartNum);
  if (mapit == partMap.end())
  {
    if (state != CP_VALID)
      partInfo.set_invalid();

    partMap[logicalPartNum] = partInfo;
  }
  else
  {
    if (mapit->second.is_invalid())
      return;

    mapit->second.MinMaxInfo::operator=(h->widenMinMaxInfo(ct, mapit->second, partInfo));
  }
}

void partitionByValue_common(UDF_ARGS* args,                              // input
                             string& errMsg,                              // output
                             CalpontSystemCatalog::TableName& tableName,  // output
                             set<LogicalPartition>& partSet,              // output
                             string functionName)                         // input
{
  // identify partitions by the range
  BRM::DBRM::refreshShm();
  DBRM em;
  vector<struct EMEntry> entries;
  vector<struct EMEntry>::iterator iter;
  PartitionMap partMap;
  string schema, table, column;

  if (args->arg_count == 5)
  {
    schema = (char*)(args->args[0]);
    table = (char*)(args->args[1]);
    column = (char*)(args->args[2]);
  }
  else
  {
    if (current_thd->db.length)
    {
      schema = current_thd->db.str;
    }
    else
    {
      errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA);
      return;
    }

    table = (char*)(args->args[0]);
    column = (char*)(args->args[1]);
  }

  if (lower_case_table_names)
  {
    boost::algorithm::to_lower(schema);
    boost::algorithm::to_lower(table);
  }
  boost::algorithm::to_lower(column);

  tableName.schema = schema;
  tableName.table = table;

  //@Bug 4695
  if (tableName.schema == "calpontsys")
  {
    errMsg = IDBErrorInfo::instance()->errorMsg(SYSTABLE_PARTITION);
    return;
  }

  try
  {
    boost::shared_ptr<CalpontSystemCatalog> csc =
        CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
    csc->identity(execplan::CalpontSystemCatalog::FE);
    CalpontSystemCatalog::TableColName tcn = make_tcn(schema, table, column, lower_case_table_names);
    csc->identity(CalpontSystemCatalog::FE);
    OID_t oid = csc->lookupOID(tcn);
    CalpontSystemCatalog::ColType ct = csc->colType(oid);
    const char* timeZone = current_thd->variables.time_zone->get_name()->ptr();
    long timeZoneOffset;
    dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
    datatypes::SessionParam sp(timeZoneOffset);
    datatypes::SimpleValue startVal;
    datatypes::SimpleValue endVal;
    datatypes::round_style_t rfMin = datatypes::round_style_t::NONE;
    datatypes::round_style_t rfMax = datatypes::round_style_t::NONE;

    if (oid == -1)
    {
      Message::Args args;
      args.add("'" + schema + string(".") + table + string(".") + column + "'");
      errMsg = IDBErrorInfo::instance()->errorMsg(ERR_TABLE_NOT_IN_CATALOG, args);
      return;
    }

    // check casual partition data type
    if (!CP_type(ct))
    {
      Message::Args args;
      args.add(name(ct));
      args.add(functionName);
      errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_BY_RANGE, args);
      return;
    }

    if (args->arg_count == 4)
    {
      startVal = getStartVal(sp, ct, args->args[2], rfMin);
      endVal = getEndVal(sp, ct, args->args[3], rfMax);
    }
    else
    {
      startVal = getStartVal(sp, ct, args->args[3], rfMin);
      endVal = getEndVal(sp, ct, args->args[4], rfMax);
    }

    CHECK(em.getExtents(oid, entries, false, false, true));

    if (entries.size() > 0)
    {
      for (iter = entries.begin(); iter != entries.end(); ++iter)
      {
        LogicalPartition logicalPartNum;
        logicalPartNum.dbroot = (*iter).dbRoot;
        logicalPartNum.pp = (*iter).partitionNum;
        logicalPartNum.seg = (*iter).segmentNum;
        addPartition(ct, em, *iter, partMap, logicalPartNum);
      }

      // check col value range
      for (PartitionMap::iterator mapit = partMap.begin(); mapit != partMap.end(); ++mapit)
      {
        if (mapit->second.is_invalid())
          continue;

        const datatypes::TypeHandler* h = ct.typeHandler();
        // @bug 4595. check empty/null case
        if (h->isSuitablePartition(ct, mapit->second, startVal, rfMin, endVal, rfMax))
          partSet.insert(mapit->first);
      }
    }
  }
  catch (QueryDataExcept& ex)
  {
    Message::Args args;
    args.add(ex.what());
    errMsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_FUNC_ARGUMENT, args);
    return;
  }
  catch (IDBExcept& ex)
  {
    errMsg = ex.what();
    return;
  }
  catch (...)
  {
    errMsg = string("Error occured when calling ") + functionName;
    return;
  }

  if (partSet.empty())
  {
    errMsg = IDBErrorInfo::instance()->errorMsg(WARN_NO_PARTITION_FOUND);
    return;
  }
}

std::string ha_mcs_impl_markpartitions_(execplan::CalpontSystemCatalog::TableName tableName,
                                        set<LogicalPartition>& partitionNums)
{
  ddlpackage::QualifiedName* qualifiedName = new QualifiedName();
  qualifiedName->fSchema = tableName.schema;
  qualifiedName->fName = tableName.table;
  MarkPartitionStatement* stmt = new MarkPartitionStatement(qualifiedName);
  stmt->fSessionID = tid2sid(current_thd->thread_id);
  stmt->fSql = "caldisablepartitions";
  stmt->fOwner = tableName.schema;
  stmt->fPartitions = partitionNums;
  string msg = "Partitions are disabled successfully";

  int rc = processPartition(stmt);  // warnings will be pushed in the function

  if (rc == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
    msg = "No partitions are disabled";

  delete stmt;
  return msg;
}

std::string ha_mcs_impl_restorepartitions_(execplan::CalpontSystemCatalog::TableName tableName,
                                           set<LogicalPartition>& partitionNums)
{
  ddlpackage::QualifiedName* qualifiedName = new QualifiedName();
  qualifiedName->fSchema = tableName.schema;
  qualifiedName->fName = tableName.table;
  RestorePartitionStatement* stmt = new RestorePartitionStatement(qualifiedName);
  stmt->fSessionID = tid2sid(current_thd->thread_id);
  stmt->fSql = "calenablepartitions";
  stmt->fOwner = tableName.schema;
  stmt->fPartitions = partitionNums;
  string msg;
  int rc = processPartition(stmt);

  if (rc != 0)
    return msg;

  msg = "Partitions are enabled successfully.";

  delete stmt;
  return msg;
}

std::string ha_mcs_impl_droppartitions_(execplan::CalpontSystemCatalog::TableName tableName,
                                        set<LogicalPartition>& partitionNums)
{
  ddlpackage::QualifiedName* qualifiedName = new QualifiedName();
  qualifiedName->fSchema = tableName.schema;
  qualifiedName->fName = tableName.table;
  DropPartitionStatement* stmt = new DropPartitionStatement(qualifiedName);
  stmt->fSessionID = tid2sid(current_thd->thread_id);
  stmt->fSql = "caldroppartitions";
  stmt->fOwner = tableName.schema;
  stmt->fPartitions = partitionNums;
  string msg = "Partitions are dropped successfully";

  int rc = processPartition(stmt);

  if (rc == ddlpackageprocessor::DDLPackageProcessor::WARN_NO_PARTITION)
    msg = "No partitions are dropped";

  delete stmt;
  return msg;
}

extern "C"
{
  /**
   * CalShowPartitions
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool calshowpartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count < 2 || args->arg_count > 3 || args->arg_type[0] != STRING_RESULT ||
        args->arg_type[1] != STRING_RESULT || (args->arg_count == 3 && args->arg_type[2] != STRING_RESULT))
    {
      strcpy(message, "usage: CALSHOWPARTITIONS ([schema], table, column)");
      return 1;
    }

    for (uint32_t i = 0; i < args->arg_count; i++)
    {
      if (!args->args[i])
      {
        strcpy(message, "usage: CALSHOWPARTITIONS ([schema], table, column)");
        return 1;
      }
    }

    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void calshowpartitions_deinit(UDF_INIT* initid)
  {
    delete initid->ptr;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* calshowpartitions(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length,
                                    char* is_null, char* error)
  {
    BRM::DBRM::refreshShm();
    DBRM em;
    vector<struct EMEntry> entries;
    vector<struct EMEntry>::iterator iter;
    vector<struct EMEntry>::iterator end;
    PartitionMap partMap;
    string schema, table, column;
    CalpontSystemCatalog::ColType ct;
    string errMsg;

    try
    {
      PartitionMap::iterator mapit;

      if (args->arg_count == 3)
      {
        schema = (char*)(args->args[0]);
        table = (char*)(args->args[1]);
        column = (char*)(args->args[2]);
      }
      else
      {
        if (current_thd->db.length)
        {
          schema = current_thd->db.str;
        }
        else
        {
          throw IDBExcept(ERR_PARTITION_NO_SCHEMA);
        }

        table = (char*)(args->args[0]);
        column = (char*)(args->args[1]);
      }
      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(schema);
        boost::algorithm::to_lower(table);
      }
      boost::algorithm::to_lower(column);

      boost::shared_ptr<CalpontSystemCatalog> csc =
          CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
      csc->identity(CalpontSystemCatalog::FE);
      CalpontSystemCatalog::TableColName tcn = make_tcn(schema, table, column, lower_case_table_names);
      OID_t oid = csc->lookupOID(tcn);
      ct = csc->colType(oid);

      if (oid == -1)
      {
        Message::Args args;
        args.add("'" + schema + string(".") + table + string(".") + column + "'");
        throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
      }

      CHECK(em.getExtents(oid, entries, false, false, true));

      if (entries.size() > 0)
      {
        iter = entries.begin();
        end = entries.end();
        LogicalPartition logicalPartNum;

        for (; iter != end; ++iter)
        {
          logicalPartNum.dbroot = (*iter).dbRoot;
          logicalPartNum.pp = (*iter).partitionNum;
          logicalPartNum.seg = (*iter).segmentNum;
          addPartition(ct, em, *iter, partMap, logicalPartNum);
        }
      }
    }
    catch (IDBExcept& ex)
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
      return result;
    }
    catch (...)
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, "Error occured when calling CALSHOWPARTITIONS");
      return result;
    }

    const datatypes::TypeHandler* h = ct.typeHandler();
    uint8_t valueCharLength = h->PartitionValueCharLength(ct);
    ostringstream output;
    output.setf(ios::left, ios::adjustfield);

    output << setw(10) << "Part#" << setw(valueCharLength) << "Min" << setw(valueCharLength) << "Max"
           << "Status";

    PartitionMap::const_iterator partIt;

    for (partIt = partMap.begin(); partIt != partMap.end(); ++partIt)
    {
      ostringstream oss;
      oss << partIt->first;
      output << "\n  " << setw(10) << oss.str();

      if (partIt->second.is_invalid())
      {
        output << setw(valueCharLength) << "N/A" << setw(valueCharLength) << "N/A";
      }
      else
      {
        const datatypes::TypeHandler* h = ct.typeHandler();
        output << h->formatPartitionInfo(ct, partIt->second);
      }

      if (partIt->second.is_disabled())
        output << "Disabled";
      else
        output << "Enabled";
    }

    // use our own buffer to make sure it fits.
    initid->ptr = new char[output.str().length() + 1];
    memcpy(initid->ptr, output.str().c_str(), output.str().length());
    *length = output.str().length();
    return initid->ptr;
  }

  /**
   * CalDisablePartitions
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool caldisablepartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 2 || args->arg_count > 3)
      err = true;
    else if (args->arg_count == 3 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
               args->arg_type[2] != STRING_RESULT)))
      err = true;
    else if (args->arg_count == 2 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
      err = true;

    for (uint32_t i = 0; i < args->arg_count; i++)
    {
      if (!args->args[i])
      {
        err = true;
        break;
      }
    }

    if (err)
    {
      strcpy(message, "\nusage: CALDISABLEPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* caldisablepartitions(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length,
                                       char* is_null, char* error)
  {
    CalpontSystemCatalog::TableName tableName;
    set<LogicalPartition> partitionNums;
    string errMsg;

    if (args->arg_count == 3)
    {
      tableName.schema = args->args[0];
      tableName.table = args->args[1];
      parsePartitionString(args, 2, partitionNums, errMsg, tableName);
    }
    else
    {
      tableName.table = args->args[0];

      if (!current_thd->db.length)
      {
        errMsg = "No schema name indicated.";
        memcpy(result, errMsg.c_str(), errMsg.length());
        *length = errMsg.length();
        return result;
      }

      tableName.schema = current_thd->db.str;
      parsePartitionString(args, 1, partitionNums, errMsg, tableName);
    }

    if (errMsg.empty())
      errMsg = ha_mcs_impl_markpartitions_(tableName, partitionNums);

    memcpy(result, errMsg.c_str(), errMsg.length());
    *length = errMsg.length();
    return result;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void caldisablepartitions_deinit(UDF_INIT* initid)
  {
  }

  /**
   * CalEnablePartitions
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool calenablepartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 2 || args->arg_count > 3)
      err = true;
    else if (args->arg_count == 3 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
               args->arg_type[2] != STRING_RESULT)))
      err = true;
    else if (args->arg_count == 2 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
      err = true;

    for (uint32_t i = 0; i < args->arg_count; i++)
    {
      if (!args->args[i])
      {
        err = true;
        break;
      }
    }

    if (err)
    {
      strcpy(message, "\nusage: CALENABLEPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* calenablepartitions(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length,
                                      char* is_null, char* error)
  {
    CalpontSystemCatalog::TableName tableName;
    string errMsg;
    set<LogicalPartition> partitionNums;

    if (args->arg_count == 3)
    {
      tableName.schema = args->args[0];
      tableName.table = args->args[1];
      parsePartitionString(args, 2, partitionNums, errMsg, tableName);
    }
    else
    {
      tableName.table = args->args[0];

      if (!current_thd->db.length)
      {
        current_thd->get_stmt_da()->set_overwrite_status(true);
        current_thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA).c_str());
        return result;
      }

      tableName.schema = current_thd->db.str;
      parsePartitionString(args, 1, partitionNums, errMsg, tableName);
    }

    if (errMsg.empty())
      errMsg = ha_mcs_impl_restorepartitions_(tableName, partitionNums);

    memcpy(result, errMsg.c_str(), errMsg.length());
    *length = errMsg.length();
    return result;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void calenablepartitions_deinit(UDF_INIT* initid)
  {
  }

  /**
   * CalDropPartitions
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool caldroppartitions_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 2 || args->arg_count > 3)
      err = true;
    else if (args->arg_count == 3 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
               args->arg_type[2] != STRING_RESULT)))
      err = true;
    else if (args->arg_count == 2 &&
             ((args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)))
      err = true;

    for (uint32_t i = 0; i < args->arg_count; i++)
    {
      if (!args->args[i])
      {
        err = true;
        break;
      }
    }

    if (err)
    {
      strcpy(message, "\nusage: CALDROPPARTITIONS (['schemaName'], 'tableName', 'partitionList')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* caldroppartitions(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length,
                                    char* is_null, char* error)
  {
    CalpontSystemCatalog::TableName tableName;
    string errMsg;
    set<LogicalPartition> partSet;

    if (args->arg_count == 3)
    {
      tableName.schema = args->args[0];
      tableName.table = args->args[1];
      parsePartitionString(args, 2, partSet, errMsg, tableName);
    }
    else
    {
      tableName.table = args->args[0];

      if (!current_thd->db.length)
      {
        current_thd->get_stmt_da()->set_overwrite_status(true);
        current_thd->raise_error_printf(ER_INTERNAL_ERROR,
                                        IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NO_SCHEMA).c_str());
        return result;
      }

      tableName.schema = current_thd->db.str;
      parsePartitionString(args, 1, partSet, errMsg, tableName);
    }

    if (errMsg.empty())
      errMsg = ha_mcs_impl_droppartitions_(tableName, partSet);

    memcpy(result, errMsg.c_str(), errMsg.length());
    *length = errMsg.length();
    return result;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void caldroppartitions_deinit(UDF_INIT* initid)
  {
  }

  /**
   * CalDropPartitionsByValue
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void caldroppartitionsbyvalue_deinit(UDF_INIT* initid)
  {
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool caldroppartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 4 || args->arg_count > 5)
    {
      err = true;
    }
    else if (args->arg_count == 4)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT)
        err = true;
    }
    else if (args->arg_count == 5)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT ||
          args->arg_type[4] != STRING_RESULT)
        err = true;
    }

    if (err)
    {
      string msg = "\nusage: CALDROPPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')";
      // message = new char[msg.length()+1];
      strcpy(message, msg.c_str());
      message[msg.length()] = 0;
      //*length = msg.length();
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* caldroppartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args, char* result,
                                           unsigned long* length, char* is_null, char* error)
  {
    string msg;
    CalpontSystemCatalog::TableName tableName;
    set<LogicalPartition> partSet;
    partitionByValue_common(args, msg, tableName, partSet, "calDropPartitionsByValue");

    if (!msg.empty())
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, msg.c_str());
      return result;
    }

    msg = ha_mcs_impl_droppartitions_(tableName, partSet);

    memcpy(result, msg.c_str(), msg.length());
    *length = msg.length();
    return result;
  }

  /**
   * CalDisablePartitionsByValue
   */

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void caldisablepartitionsbyvalue_deinit(UDF_INIT* initid)
  {
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool caldisablepartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 4 || args->arg_count > 5)
    {
      err = true;
    }
    else if (args->arg_count == 4)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT)
        err = true;
    }
    else if (args->arg_count == 5)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT ||
          args->arg_type[4] != STRING_RESULT)
        err = true;
    }

    if (err)
    {
      strcpy(message, "\nusage: CALDISABLEPARTITIONS (['schema'], 'table', 'column', 'min', 'max')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* caldisablepartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args, char* result,
                                              unsigned long* length, char* is_null, char* error)
  {
    string msg;
    set<LogicalPartition> partSet;
    CalpontSystemCatalog::TableName tableName;
    partitionByValue_common(args, msg, tableName, partSet, "calDisablePartitionsByValue");

    if (!msg.empty())
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, msg.c_str());
      return result;
    }

    msg = ha_mcs_impl_markpartitions_(tableName, partSet);

    memcpy(result, msg.c_str(), msg.length());
    *length = msg.length();
    return result;
  }

  /**
   * CalEnablePartitionsByValue
   */
#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void calenablepartitionsbyvalue_deinit(UDF_INIT* initid)
  {
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool calenablepartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 4 || args->arg_count > 5)
    {
      err = true;
    }
    else if (args->arg_count == 4)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT)
        err = true;
    }
    else if (args->arg_count == 5)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT ||
          args->arg_type[4] != STRING_RESULT)
        err = true;
    }

    if (err)
    {
      strcpy(message, "\nusage: CALENABLEPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* calenablepartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args, char* result,
                                             unsigned long* length, char* is_null, char* error)
  {
    string msg;
    set<LogicalPartition> partSet;
    CalpontSystemCatalog::TableName tableName;
    partitionByValue_common(args, msg, tableName, partSet, "calEnablePartitionsByValue");

    if (!msg.empty())
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, msg.c_str());
      return result;
    }

    msg = ha_mcs_impl_restorepartitions_(tableName, partSet);

    memcpy(result, msg.c_str(), msg.length());
    *length = msg.length();
    return result;
  }

  /**
   * CalShowPartitionsByValue
   */
#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      my_bool calshowpartitionsbyvalue_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    bool err = false;

    if (args->arg_count < 4 || args->arg_count > 5)
    {
      err = true;
    }
    else if (args->arg_count == 4)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT)
        err = true;
    }
    else if (args->arg_count == 5)
    {
      if (args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT ||
          args->arg_type[2] != STRING_RESULT || args->arg_type[3] != STRING_RESULT ||
          args->arg_type[4] != STRING_RESULT)
        err = true;
    }

    if (err)
    {
      strcpy(message, "\nusage: CALSHOWPARTITIONSBYVALUE (['schema'], 'table', 'column', 'min', 'max')");
      return 1;
    }

    initid->maybe_null = 1;
    initid->max_length = 255;
    return 0;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      void calshowpartitionsbyvalue_deinit(UDF_INIT* initid)
  {
    delete initid->ptr;
  }

#ifdef _MSC_VER
  __declspec(dllexport)
#endif
      const char* calshowpartitionsbyvalue(UDF_INIT* initid, UDF_ARGS* args, char* result,
                                           unsigned long* length, char* is_null, char* error)
  {
    BRM::DBRM::refreshShm();
    DBRM em;
    vector<struct EMEntry> entries;
    vector<struct EMEntry>::iterator iter;
    vector<struct EMEntry>::iterator end;
    PartitionMap partMap;
    PartitionMap::iterator mapit;
    string schema, table, column;
    CalpontSystemCatalog::ColType ct;
    string errMsg;
    const char* timeZone = current_thd->variables.time_zone->get_name()->ptr();
    long timeZoneOffset;
    dataconvert::timeZoneToOffset(timeZone, strlen(timeZone), &timeZoneOffset);
    datatypes::SessionParam sp(timeZoneOffset);
    datatypes::SimpleValue startVal;
    datatypes::SimpleValue endVal;
    datatypes::round_style_t rfMin = datatypes::round_style_t::NONE;
    datatypes::round_style_t rfMax = datatypes::round_style_t::NONE;

    try
    {
      if (args->arg_count == 5)
      {
        schema = (char*)(args->args[0]);
        table = (char*)(args->args[1]);
        column = (char*)(args->args[2]);
      }
      else
      {
        if (current_thd->db.length)
        {
          schema = current_thd->db.str;
        }
        else
        {
          throw IDBExcept(ERR_PARTITION_NO_SCHEMA);
        }

        table = (char*)(args->args[0]);
        column = (char*)(args->args[1]);
      }

      if (lower_case_table_names)
      {
        boost::algorithm::to_lower(schema);
        boost::algorithm::to_lower(table);
      }
      boost::algorithm::to_lower(column);

      boost::shared_ptr<CalpontSystemCatalog> csc =
          CalpontSystemCatalog::makeCalpontSystemCatalog(tid2sid(current_thd->thread_id));
      csc->identity(CalpontSystemCatalog::FE);
      CalpontSystemCatalog::TableColName tcn = make_tcn(schema, table, column, lower_case_table_names);
      OID_t oid = csc->lookupOID(tcn);
      ct = csc->colType(oid);

      if (oid == -1)
      {
        Message::Args args;
        args.add("'" + schema + string(".") + table + string(".") + column + "'");
        throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
      }

      // check casual partition data type
      if (!CP_type(ct))
      {
        Message::Args args;
        args.add(name(ct));
        args.add("calShowPartitionsByValue");
        throw IDBExcept(ERR_PARTITION_BY_RANGE, args);
      }

      if (args->arg_count == 4)
      {
        startVal = getStartVal(sp, ct, args->args[2], rfMin);
        endVal = getEndVal(sp, ct, args->args[3], rfMax);
      }
      else
      {
        startVal = getStartVal(sp, ct, args->args[3], rfMin);
        endVal = getEndVal(sp, ct, args->args[4], rfMax);
      }

      CHECK(em.getExtents(oid, entries, false, false, true));

      if (entries.size() > 0)
      {
        iter = entries.begin();
        end = entries.end();
        LogicalPartition logicalPartNum;

        for (; iter != end; ++iter)
        {
          logicalPartNum.dbroot = (*iter).dbRoot;
          logicalPartNum.pp = (*iter).partitionNum;
          logicalPartNum.seg = (*iter).segmentNum;
          addPartition(ct, em, *iter, partMap, logicalPartNum);
        }
      }
    }
    catch (logging::QueryDataExcept& ex)
    {
      Message::Args args;
      args.add(ex.what());
      errMsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_FUNC_ARGUMENT, args);
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, (char*)errMsg.c_str());
      return result;
    }
    catch (IDBExcept& ex)
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, ex.what());
      return result;
    }
    catch (...)
    {
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, "Error occured when calling CALSHOWPARTITIONS");
      return result;
    }

    // create a set of partInfo for sorting.
    bool noPartFound = true;
    ostringstream output;

    for (mapit = partMap.begin(); mapit != partMap.end(); ++mapit)
    {
      const datatypes::TypeHandler* h = ct.typeHandler();
      uint8_t valueCharLength = h->PartitionValueCharLength(ct);
      if (mapit->second.is_invalid())
      {
        output << setw(valueCharLength) << "N/A" << setw(valueCharLength) << "N/A";
        continue;
      }
      // @bug 4595. check empty/null case
      string tmp = h->PrintPartitionValue(ct, mapit->second, startVal, rfMin, endVal, rfMax);
      if (tmp == "")
        continue;

      // print header
      if (noPartFound)
      {
        output.setf(ios::left, ios::adjustfield);
        output << setw(10) << "Part#" << setw(valueCharLength) << "Min" << setw(valueCharLength) << "Max"
               << "Status";
        noPartFound = false;
      }

      // print part info
      ostringstream oss;
      oss << mapit->first;
      output << "\n  " << setw(10) << oss.str() << tmp;

      if (mapit->second.is_disabled())
        output << "Disabled";
      else
        output << "Enabled";
    }

    if (noPartFound)
    {
      errMsg = IDBErrorInfo::instance()->errorMsg(WARN_NO_PARTITION_FOUND);
      current_thd->get_stmt_da()->set_overwrite_status(true);
      current_thd->raise_error_printf(ER_INTERNAL_ERROR, errMsg.c_str());
      return result;
    }

    // use our own buffer to make sure it fits.
    initid->ptr = new char[output.str().length() + 1];
    memcpy(initid->ptr, output.str().c_str(), output.str().length());
    *length = output.str().length();
    return initid->ptr;
  }
}

}  // namespace
