/* Copyright (C) 2021 MariaDB Corporation

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

#ifndef MCSANALYZETABLEXEXCUTIONPLAN_H
#define MCSANALYZETABLEXEXCUTIONPLAN_H
#include <vector>
#include <map>
#include <iosfwd>

#include <boost/uuid/uuid.hpp>

#include "exp_templates.h"
#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "returnedcolumn.h"
#include "filter.h"
#include "expressionparser.h"
#include "calpontsystemcatalog.h"
#include "brmtypes.h"
#include "objectreader.h"
#include <boost/algorithm/string/case_conv.hpp>

/**
 * Namespace
 */
namespace execplan
{
class MCSAnalyzeTableExecutionPlan : public CalpontExecutionPlan
{
 public:
  typedef std::vector<SRCP> ReturnedColumnList;
  typedef std::multimap<std::string, SRCP> ColumnMap;
  typedef std::vector<RMParam> RMParmVec;

  MCSAnalyzeTableExecutionPlan() : fTraceFlags(TRACE_NONE)
  {
  }

  MCSAnalyzeTableExecutionPlan(const ReturnedColumnList& returnedCols, const ColumnMap& columnMap)
   : fReturnedCols(returnedCols), fColumnMap(columnMap), fTraceFlags(TRACE_NONE)
  {
  }

  virtual ~MCSAnalyzeTableExecutionPlan() = default;

  const ReturnedColumnList& returnedCols() const
  {
    return fReturnedCols;
  }
  ReturnedColumnList& returnedCols()
  {
    return fReturnedCols;
  }

  void returnedCols(const ReturnedColumnList& returnedCols)
  {
    fReturnedCols = returnedCols;
  }

  const ColumnMap& columnMap() const
  {
    return fColumnMap;
  }

  ColumnMap& columnMap()
  {
    return fColumnMap;
  }

  void columnMap(const ColumnMap& columnMap)
  {
    fColumnMap = columnMap;
  }

  const std::string data() const
  {
    return fData;
  }

  void data(const std::string data)
  {
    fData = data;
  }

  uint32_t sessionID() const
  {
    return fSessionID;
  }

  void sessionID(const uint32_t sessionID)
  {
    fSessionID = sessionID;
  }

  int txnID() const
  {
    return fTxnID;
  }

  void txnID(const int txnID)
  {
    fTxnID = txnID;
  }

  const BRM::QueryContext verID() const
  {
    return fVerID;
  }

  void verID(const BRM::QueryContext verID)
  {
    fVerID = verID;
  }

  inline std::string& schemaName()
  {
    return fSchemaName;
  }

  inline void schemaName(const std::string& schemaName, int lower_case_table_names)
  {
    fSchemaName = schemaName;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fSchemaName);
  }

  inline std::string& tableName()
  {
    return fTableName;
  }

  inline void tableName(const std::string& tableName, int lower_case_table_names)
  {
    fTableName = tableName;
    if (lower_case_table_names)
      boost::algorithm::to_lower(fTableName);
  }

  uint32_t statementID() const
  {
    return fStatementID;
  }

  void statementID(const uint32_t statementID)
  {
    fStatementID = statementID;
  }

  void uuid(const boost::uuids::uuid& uuid)
  {
    fUuid = uuid;
  }

  const boost::uuids::uuid& uuid() const
  {
    return fUuid;
  }

  void timeZone(long timezone)
  {
    fTimeZone = timezone;
  }

  long timeZone() const
  {
    return fTimeZone;
  }

  void priority(uint32_t p)
  {
    fPriority = p;
  }

  uint32_t priority() const
  {
    return fPriority;
  }

  const RMParmVec& rmParms()
  {
    return frmParms;
  }

  void rmParms(const RMParmVec& parms)
  {
    frmParms.clear();
    frmParms.assign(parms.begin(), parms.end());
  }

  uint32_t localQuery() const
  {
    return fLocalQuery;
  }

  void localQuery(const uint32_t localQuery)
  {
    fLocalQuery = localQuery;
  }

  inline bool traceOn() const
  {
    return (traceFlags() & TRACE_LOG);
  }

  inline uint32_t traceFlags() const
  {
    return fTraceFlags;
  }

  inline void traceFlags(uint32_t traceFlags)
  {
    fTraceFlags = traceFlags;
  }

  virtual std::string toString() const;

  virtual bool isInternal() const
  {
    return ((fSessionID & 0x80000000) != 0);
  }

  virtual void serialize(messageqcpp::ByteStream& bs) const;

  virtual void unserialize(messageqcpp::ByteStream& bs);

  // TODO: Why do we need this?
  virtual bool operator==(const CalpontExecutionPlan* t) const
  {
    return false;
  }
  virtual bool operator!=(const CalpontExecutionPlan* t) const
  {
    return false;
  }

 private:
  ReturnedColumnList fReturnedCols;
  ColumnMap fColumnMap;
  uint32_t fSessionID;
  int fTxnID;
  BRM::QueryContext fVerID;
  std::string fSchemaName;
  std::string fTableName;
  uint32_t fTraceFlags;
  boost::uuids::uuid fUuid;
  long fTimeZone;
  uint32_t fStatementID;
  uint64_t fStringScanThreshold;
  std::string fData;
  RMParmVec frmParms;
  uint32_t fPriority;
  uint32_t fLocalQuery;
};
}  // namespace execplan
#endif
