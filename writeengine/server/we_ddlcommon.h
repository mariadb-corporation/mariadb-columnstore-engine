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

/*******************************************************************************
 * $Id$
 *
 *******************************************************************************/

#ifndef WE_DDLCOMMON_H
#define WE_DDLCOMMON_H

#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <stdint.h>

#include <boost/any.hpp>
#include <boost/tuple/tuple.hpp>

#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
#include "ddlpkg.h"
#include "messageobj.h"
#include "we_type.h"
#include "we_define.h"
#include "writeengine.h"
#include "columnresult.h"
#include "brmtypes.h"
#include "joblist.h"

#if defined(_MSC_VER) && defined(xxxDDLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#include <boost/algorithm/string/case_conv.hpp>

template <class T>
bool from_string(T& t, const std::string& s, std::ios_base& (*f)(std::ios_base&))
{
  std::istringstream iss(s);
  return !(iss >> f >> t).fail();
}

namespace WriteEngine
{
struct DDLColumn
{
  execplan::CalpontSystemCatalog::OID oid;
  execplan::CalpontSystemCatalog::ColType colType;
  execplan::CalpontSystemCatalog::TableColName tableColName;
};

typedef std::vector<DDLColumn> ColumnList;

struct DictOID
{
  int dictOID;
  int listOID;
  int treeOID;
  int colWidth;
  int compressionType;
};

struct extentInfo
{
  uint16_t dbRoot;
  uint32_t partition;
  uint16_t segment;
  bool operator==(const extentInfo& rhs) const
  {
    return (dbRoot == rhs.dbRoot && partition == rhs.partition && segment == rhs.segment);
  }
  bool operator!=(const extentInfo& rhs) const
  {
    return !(*this == rhs);
  }
};
inline void getColumnsForTable(uint32_t sessionID, std::string schema, std::string table, ColumnList& colList)
{
  execplan::CalpontSystemCatalog::TableName tableName;
  tableName.schema = schema;
  tableName.table = table;
  std::string err;

  try
  {
    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::EC);

    const execplan::CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName);

    execplan::CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();

    while (rid_iterator != ridList.end())
    {
      execplan::CalpontSystemCatalog::ROPair roPair = *rid_iterator;

      DDLColumn column;
      column.oid = roPair.objnum;
      column.colType = systemCatalogPtr->colType(column.oid);
      column.tableColName = systemCatalogPtr->colName(column.oid);

      colList.push_back(column);

      ++rid_iterator;
    }
  }
  catch (exception& ex)
  {
    err = "DDLPackageProcessor::getColumnsForTable: while reading columns for table " + schema + '.' + table +
          ": " + ex.what();
    throw std::runtime_error(err);
  }
  catch (...)
  {
    err = "DDLPackageProcessor::getColumnsForTable: caught unkown exception!";
    throw std::runtime_error(err);
  }
}

inline int convertDataType(int dataType)
{
  int calpontDataType;

  switch (dataType)
  {
    case ddlpackage::DDL_CHAR: calpontDataType = execplan::CalpontSystemCatalog::CHAR; break;

    case ddlpackage::DDL_VARCHAR: calpontDataType = execplan::CalpontSystemCatalog::VARCHAR; break;

    case ddlpackage::DDL_VARBINARY: calpontDataType = execplan::CalpontSystemCatalog::VARBINARY; break;

    case ddlpackage::DDL_BIT: calpontDataType = execplan::CalpontSystemCatalog::BIT; break;

    case ddlpackage::DDL_REAL:
    case ddlpackage::DDL_DECIMAL:
    case ddlpackage::DDL_NUMERIC:
    case ddlpackage::DDL_NUMBER: calpontDataType = execplan::CalpontSystemCatalog::DECIMAL; break;

    case ddlpackage::DDL_FLOAT: calpontDataType = execplan::CalpontSystemCatalog::FLOAT; break;

    case ddlpackage::DDL_DOUBLE: calpontDataType = execplan::CalpontSystemCatalog::DOUBLE; break;

    case ddlpackage::DDL_INT:
    case ddlpackage::DDL_INTEGER: calpontDataType = execplan::CalpontSystemCatalog::INT; break;

    case ddlpackage::DDL_BIGINT: calpontDataType = execplan::CalpontSystemCatalog::BIGINT; break;

    case ddlpackage::DDL_MEDINT: calpontDataType = execplan::CalpontSystemCatalog::MEDINT; break;

    case ddlpackage::DDL_SMALLINT: calpontDataType = execplan::CalpontSystemCatalog::SMALLINT; break;

    case ddlpackage::DDL_TINYINT: calpontDataType = execplan::CalpontSystemCatalog::TINYINT; break;

    case ddlpackage::DDL_DATE: calpontDataType = execplan::CalpontSystemCatalog::DATE; break;

    case ddlpackage::DDL_DATETIME: calpontDataType = execplan::CalpontSystemCatalog::DATETIME; break;

    case ddlpackage::DDL_TIME: calpontDataType = execplan::CalpontSystemCatalog::TIME; break;

    case ddlpackage::DDL_TIMESTAMP: calpontDataType = execplan::CalpontSystemCatalog::TIMESTAMP; break;

    case ddlpackage::DDL_CLOB: calpontDataType = execplan::CalpontSystemCatalog::CLOB; break;

    case ddlpackage::DDL_BLOB: calpontDataType = execplan::CalpontSystemCatalog::BLOB; break;

    case ddlpackage::DDL_TEXT: calpontDataType = execplan::CalpontSystemCatalog::TEXT; break;

    case ddlpackage::DDL_UNSIGNED_TINYINT: calpontDataType = execplan::CalpontSystemCatalog::UTINYINT; break;

    case ddlpackage::DDL_UNSIGNED_SMALLINT:
      calpontDataType = execplan::CalpontSystemCatalog::USMALLINT;
      break;

    case ddlpackage::DDL_UNSIGNED_MEDINT: calpontDataType = execplan::CalpontSystemCatalog::UMEDINT; break;

    case ddlpackage::DDL_UNSIGNED_INT: calpontDataType = execplan::CalpontSystemCatalog::UINT; break;

    case ddlpackage::DDL_UNSIGNED_BIGINT: calpontDataType = execplan::CalpontSystemCatalog::UBIGINT; break;

    case ddlpackage::DDL_UNSIGNED_DECIMAL:
    case ddlpackage::DDL_UNSIGNED_NUMERIC: calpontDataType = execplan::CalpontSystemCatalog::UDECIMAL; break;

    case ddlpackage::DDL_UNSIGNED_FLOAT: calpontDataType = execplan::CalpontSystemCatalog::UFLOAT; break;

    case ddlpackage::DDL_UNSIGNED_DOUBLE: calpontDataType = execplan::CalpontSystemCatalog::UDOUBLE; break;

    default: throw runtime_error("Unsupported datatype!");
  }

  return calpontDataType;
}

inline void findColumnData(uint32_t sessionID, execplan::CalpontSystemCatalog::TableName& systableName,
                           const std::string& colName, DDLColumn& sysCol)
{
  ColumnList columns;
  ColumnList::const_iterator column_iterator;
  std::string err;

  try
  {
    getColumnsForTable(sessionID, systableName.schema, systableName.table, columns);
    column_iterator = columns.begin();

    while (column_iterator != columns.end())
    {
      sysCol = *column_iterator;
      boost::to_lower(sysCol.tableColName.column);

      if (colName == sysCol.tableColName.column)
      {
        break;
      }

      ++column_iterator;
    }
  }
  catch (exception& ex)
  {
    err = ex.what();
    throw std::runtime_error(err);
  }
  catch (...)
  {
    err = "findColumnData:Unknown exception caught";
    throw std::runtime_error(err);
  }
}

inline void convertRidToColumn(uint64_t& rid, unsigned& dbRoot, unsigned& partition, unsigned& segment,
                               unsigned filesPerColumnPartition, unsigned extentsPerSegmentFile,
                               unsigned extentRows, unsigned startDBRoot, unsigned dbrootCnt)
{
  partition = rid / (filesPerColumnPartition * extentsPerSegmentFile * extentRows);

  segment = (((rid % (filesPerColumnPartition * extentsPerSegmentFile * extentRows)) / extentRows)) %
            filesPerColumnPartition;

  dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;

  // Calculate the relative rid for this segment file
  uint64_t relRidInPartition = rid - ((uint64_t)partition * (uint64_t)filesPerColumnPartition *
                                      (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
  idbassert(relRidInPartition <=
            (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
  uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
  unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
  uint64_t relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
  rid = relRidInThisExtent + numExtentsInThisSegPart * extentRows;
}

}  // namespace WriteEngine
#undef EXPORT
#endif
