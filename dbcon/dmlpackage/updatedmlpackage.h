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
 *   $Id: updatedmlpackage.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include "calpontdmlpackage.h"
#include "bytestream.h"

#define EXPORT

namespace dmlpackage
{
/** @brief concrete implementation of a CalpontDMLPackage
 * Specifically for representing UPDATE DML Statements
 */
class UpdateDMLPackage : public CalpontDMLPackage
{
 public:
  /** @brief ctor
   */
  EXPORT UpdateDMLPackage();

  /** @brief ctor
   *
   * @param schemaName the schema of the table being operated on
   * @param tableName the name of the table being operated on
   * @param dmlStatement the dml statement
   * @param sessionID the session ID
   */
  EXPORT UpdateDMLPackage(std::string schemaName, std::string tableName, std::string dmlStatement,
                          int sessionID);

  /** @brief dtor
   */
  EXPORT ~UpdateDMLPackage() override;

  /** @brief write a UpdateDMLPackage to a ByteStream
   *
   * @param bytestream the ByteStream to write to
   */
  EXPORT int write(messageqcpp::ByteStream& bytestream) override;

  /** @brief read a UpdateDMLPackage from a ByteStream
   *
   * @param bytestream the ByteStream to read from
   */
  EXPORT int read(messageqcpp::ByteStream& bytestream) override;

  /** @brief build a UpdateDMLPackage from a string buffer
   *
   * @param buffer
   * @param columns the number of columns in the buffer
   * @param rows the number of rows in the buffer
   */
  EXPORT int buildFromBuffer(std::string& buffer, int columns, int rows) override;

  /** @brief build a UpdateDMLPackage from a parsed UpdateSqlStatement
   *
   * @param sqlStatement the parsed UpdateSqlStatement
   */
  EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement) override;

  /** @brief build a InsertDMLPackage from MySQL buffer
   *
   * @param colNameList, tableValuesMap
   * @param rows the number of rows in the buffer
   */
  EXPORT int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns,
                                  int rows, NullValuesBitset& nullValues) override;
  void buildUpdateFromMysqlBuffer(UpdateSqlStatement& updateStmt);

 protected:
 private:
};
}  // namespace dmlpackage

#undef EXPORT
