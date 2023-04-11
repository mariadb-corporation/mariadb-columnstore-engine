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

/***********************************************************************
 *   $Id: insertdmlpackage.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include <stdexcept>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>
using namespace std;
#define INSERTDMLPKG_DLLEXPORT
#include "insertdmlpackage.h"
#undef INSERTDMLPKG_DLLEXPORT


namespace dmlpackage
{
InsertDMLPackage::InsertDMLPackage()
{
}

InsertDMLPackage::InsertDMLPackage(std::string schemaName, std::string tableName, std::string dmlStatement,
                                   int sessionID)
 : CalpontDMLPackage(schemaName, tableName, dmlStatement, sessionID)
{
}

InsertDMLPackage::~InsertDMLPackage()
{
}

int InsertDMLPackage::write(messageqcpp::ByteStream& bytestream)
{
  int retval = 1;

  messageqcpp::ByteStream::byte package_type = DML_INSERT;
  bytestream << package_type;

  messageqcpp::ByteStream::quadbyte session_id = fSessionID;
  bytestream << session_id;

  bytestream << fUuid;

  bytestream << fDMLStatement;
  bytestream << fDMLStatement;
  bytestream << fSchemaName;
  messageqcpp::ByteStream::octbyte timeZone = fTimeZone;
  bytestream << timeZone;
  bytestream << (uint8_t)fLogging;
  bytestream << (uint8_t)fLogending;

  bytestream << fTableOid;
  bytestream << static_cast<messageqcpp::ByteStream::byte>(fIsInsertSelect);
  bytestream << static_cast<messageqcpp::ByteStream::byte>(fIsBatchInsert);
  bytestream << static_cast<messageqcpp::ByteStream::byte>(fIsCacheInsert);
  bytestream << static_cast<messageqcpp::ByteStream::byte>(fIsAutocommitOn);

  if (fTable != 0)
  {
    retval = fTable->write(bytestream);
  }

  return retval;
}

int InsertDMLPackage::read(messageqcpp::ByteStream& bytestream)
{
  int retval = 1;

  messageqcpp::ByteStream::quadbyte session_id;
  bytestream >> session_id;
  fSessionID = session_id;
  bytestream >> fUuid;

  std::string dmlStatement;
  bytestream >> fDMLStatement;
  bytestream >> fSQLStatement;
  bytestream >> fSchemaName;
  messageqcpp::ByteStream::octbyte timeZone;
  bytestream >> timeZone;
  fTimeZone = timeZone;
  uint8_t logging;
  bytestream >> logging;
  fLogging = (logging != 0);
  uint8_t logending;
  bytestream >> logending;
  fLogending = (logending != 0);

  bytestream >> fTableOid;
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsInsertSelect);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsBatchInsert);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsCacheInsert);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsAutocommitOn);

  fTable = new DMLTable();
  retval = fTable->read(bytestream);
  return retval;
}

void InsertDMLPackage::readMetaData(messageqcpp::ByteStream& bytestream)
{
  messageqcpp::ByteStream::quadbyte session_id;
  bytestream >> session_id;
  fSessionID = session_id;
  bytestream >> fUuid;

  std::string dmlStatement;
  bytestream >> fDMLStatement;
  bytestream >> fSQLStatement;
  bytestream >> fSchemaName;
  messageqcpp::ByteStream::octbyte timeZone;
  bytestream >> timeZone;
  fTimeZone = timeZone;
  uint8_t logging;
  bytestream >> logging;
  fLogging = (logging != 0);
  uint8_t logending;
  bytestream >> logending;
  fLogending = (logending != 0);

  bytestream >> fTableOid;
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsInsertSelect);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsBatchInsert);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsCacheInsert);
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsAutocommitOn);

  fTable = new DMLTable();
  fTable->readMetaData(bytestream);
}

// Has to be called after InsertDMLPackage::readMetaData()
void InsertDMLPackage::readRowData(messageqcpp::ByteStream& bytestream)
{
  fTable->readRowData(bytestream);
}

int InsertDMLPackage::buildFromBuffer(std::string& buffer, int columns, int rows)
{
#ifdef DML_PACKAGE_DEBUG
  // cout << "The data buffer received: " << buffer << endl;
#endif
  int retval = 1;

  initializeTable();

  std::vector<std::string> dataList;
  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep(",");
  tokenizer tokens(buffer, sep);

  for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
  {
    dataList.push_back(StripLeadingWhitespace(*tok_iter));
  }

  int n = 0;

  for (int i = 0; i < rows; i++)
  {
    // get a new row
    Row* aRowPtr = new Row();
    std::string colName;
    std::string colValue;

    for (int j = 0; j < columns; j++)
    {
      // Build a column list
      colName = dataList[n];
      n++;
      colValue = dataList[n];
      n++;
      // XXX check for "null"? what values do we have here?
      utils::NullString nullColValue(colValue);
#ifdef DML_PACKAGE_DEBUG
      // cout << "The column data: " << colName << " " << colValue << endl;
#endif
      DMLColumn* aColumn = new DMLColumn(colName, nullColValue, false);
      (aRowPtr->get_ColumnList()).push_back(aColumn);
    }

    // build a row list for a table
    fTable->get_RowList().push_back(aRowPtr);
  }

  return retval;
}

int InsertDMLPackage::buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap,
                                           int columns, int rows, NullValuesBitset& nullValues)
{
  int retval = 1;

  initializeTable();
  Row* aRowPtr = new Row();
  std::string colName;
  ColValuesList colValList;

  for (int j = 0; j < columns; j++)
  {
    // Build a column list
    colName = colNameList[j];

    colValList = tableValuesMap[j];

    DMLColumn* aColumn = new DMLColumn(colName, colValList, false, 0, nullValues[j]);
    (aRowPtr->get_ColumnList()).push_back(aColumn);
  }

  // build a row list for a table
  fTable->get_RowList().push_back(aRowPtr);
  aRowPtr = NULL;
  delete aRowPtr;
  return retval;
}

int InsertDMLPackage::buildFromSqlStatement(SqlStatement& sqlStatement)
{
  int retval = 1;

  InsertSqlStatement& insertStmt = dynamic_cast<InsertSqlStatement&>(sqlStatement);

  if (!insertStmt.fValuesOrQueryPtr)
    throw runtime_error("insertStmt.fValuesOrQueryPtr == NULL");

  initializeTable();
  bool isNULL = false;

  // only if we don't have a select statement
  if (0 == insertStmt.fValuesOrQueryPtr->fQuerySpecPtr)
  {
    ColumnNameList columnNameList = insertStmt.fColumnList;

    if (columnNameList.size())
    {
      ValuesList valuesList = insertStmt.fValuesOrQueryPtr->fValuesList;

      if (columnNameList.size() != valuesList.size())
      {
        throw logic_error("Column names and values count mismatch!");
      }

      Row* aRow = new Row();

      for (unsigned int i = 0; i < columnNameList.size(); i++)
      {
        // XXX can here be NULLs?
        idbassert(!isNULL);
        utils::NullString ithValue(valuesList[i]);
        DMLColumn* aColumn = new DMLColumn(columnNameList[i], ithValue);
        (aRow->get_ColumnList()).push_back(aColumn);
      }

      fTable->get_RowList().push_back(aRow);
    }
    else
    {
      ValuesList valuesList = insertStmt.fValuesOrQueryPtr->fValuesList;
      ValuesList::const_iterator iter = valuesList.begin();
      Row* aRow = new Row();
      std::string colName = "";
      std::string colValue;

      while (iter != valuesList.end())
      {
        colValue = *iter;
        utils::NullString nullColValue;

        if (strcasecmp(colValue.c_str(), "NULL") == 0)
        {
          isNULL = true;
        }
        else
        {
          nullColValue.assign(colValue);
          isNULL = false;
        }

        DMLColumn* aColumn = new DMLColumn(colName, nullColValue, isNULL);
        (aRow->get_ColumnList()).push_back(aColumn);

        ++iter;
      }

      fTable->get_RowList().push_back(aRow);
    }
  }
  else
  {
    fHasFilter = true;
    fQueryString = insertStmt.getQueryString();
  }

  return retval;
}

}  // namespace dmlpackage
