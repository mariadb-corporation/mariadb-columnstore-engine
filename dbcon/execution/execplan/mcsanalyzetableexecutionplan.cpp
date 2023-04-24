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

#include <iostream>
#include <algorithm>
using namespace std;

#include <boost/uuid/uuid_io.hpp>

#include "bytestream.h"
using namespace messageqcpp;

#include "mcsanalyzetableexecutionplan.h"
#include "objectreader.h"
#include "filter.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"
#include "querystats.h"

#include "querytele.h"
using namespace querytele;

namespace execplan
{
std::string MCSAnalyzeTableExecutionPlan::toString() const
{
  std::ostringstream output;
  output << ">ANALYZE TABLE " << std::endl;
  output << "Shema: " << fSchemaName << std::endl;
  output << "Table: " << fTableName << std::endl;

  output << ">>Returned Columns" << std::endl;

  for (auto& rColumn : fReturnedCols)
    output << *rColumn << std::endl;

  output << "--- Column Map ---" << std::endl;
  CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;

  for (iter = columnMap().begin(); iter != columnMap().end(); iter++)
    output << (*iter).first << " : " << (*iter).second << endl;

  output << "SessionID: " << fSessionID << std::endl;
  output << "TxnID: " << fTxnID << std::endl;
  output << "VerID: " << fVerID << std::endl;

  return output.str();
}

void MCSAnalyzeTableExecutionPlan::serialize(messageqcpp::ByteStream& bs) const
{
  bs << static_cast<ObjectReader::id_t>(ObjectReader::MCSANALYZETBLEXECUTIONPLAN);

  // Returned columns.
  bs << static_cast<uint32_t>(fReturnedCols.size());
  for (auto& rColumn : fReturnedCols)
    rColumn->serialize(bs);

  // Column map.
  bs << static_cast<uint32_t>(fColumnMap.size());
  for (auto& column : fColumnMap)
  {
    bs << column.first;
    column.second->serialize(bs);
  }

  bs << static_cast<uint32_t>(frmParms.size());

  for (RMParmVec::const_iterator it = frmParms.begin(); it != frmParms.end(); ++it)
  {
    bs << it->sessionId;
    bs << it->id;
    bs << it->value;
  }

  bs << fData;
  bs << static_cast<uint32_t>(fSessionID);
  bs << static_cast<uint32_t>(fTxnID);
  bs << fVerID;
  bs << fStatementID;
  bs << static_cast<uint64_t>(fStringScanThreshold);
  bs << fPriority;
  bs << fSchemaName;
  bs << fTableName;
  bs << fLocalQuery;
  messageqcpp::ByteStream::octbyte timeZone = fTimeZone;
  bs << timeZone;
  bs << fTraceFlags;
}

void MCSAnalyzeTableExecutionPlan::unserialize(messageqcpp::ByteStream& bs)
{
  ObjectReader::checkType(bs, ObjectReader::MCSANALYZETBLEXECUTIONPLAN);
  fReturnedCols.clear();
  fColumnMap.clear();
  uint32_t size;

  bs >> size;
  for (uint32_t i = 0; i < size; ++i)
  {
    auto* returnedColumn = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(bs));
    SRCP srcp(returnedColumn);
    fReturnedCols.push_back(srcp);
  }

  bs >> size;
  for (uint32_t i = 0; i < size; ++i)
  {
    std::string colName;
    bs >> colName;
    auto* returnedColumn = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(bs));
    SRCP srcp(returnedColumn);
    fColumnMap.insert(ColumnMap::value_type(colName, srcp));
  }

  bs >> size;
  messageqcpp::ByteStream::doublebyte id;
  messageqcpp::ByteStream::quadbyte sessionId;
  messageqcpp::ByteStream::octbyte memory;

  for (uint32_t i = 0; i < size; i++)
  {
    bs >> sessionId;
    bs >> id;
    bs >> memory;
    frmParms.push_back(RMParam(sessionId, id, memory));
  }

  bs >> fData;
  bs >> reinterpret_cast<uint32_t&>(fSessionID);
  bs >> reinterpret_cast<uint32_t&>(fTxnID);
  bs >> fVerID;
  bs >> fStatementID;
  bs >> reinterpret_cast<uint64_t&>(fStringScanThreshold);
  bs >> fPriority;
  bs >> fSchemaName;
  bs >> fTableName;
  bs >> fLocalQuery;
  messageqcpp::ByteStream::octbyte timeZone;
  bs >> timeZone;
  fTimeZone = timeZone;
  bs >> fTraceFlags;
}
}  // namespace execplan
