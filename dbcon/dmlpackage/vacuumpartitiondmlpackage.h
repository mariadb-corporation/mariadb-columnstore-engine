/* Copyright (C) 2024 MariaDB Corporation

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

#pragma once

#include <string>
#include "calpontdmlpackage.h"
#include "bytestream.h"

#define EXPORT

namespace dmlpackage
{
/** @brief concrete implementation of a CalpontDMLPackage
 * Specifically for representing MCS_VACUUM_PARTITION_BLOAT Statements
 */
class VacuumPartitionDMLPackage : public CalpontDMLPackage
{
 public:
  EXPORT VacuumPartitionDMLPackage() = default;

  /** @brief ctor
   *
   * @param schemaName the schema of the table being operated on
   * @param tableName the name of the table being operated on
   * @param dmlStatement the dml statement
   * @param sessionID the session ID
   * @param partition partition to be vacuumed
   */
  EXPORT VacuumPartitionDMLPackage(std::string schemaName, std::string tableName, std::string dmlStatement,
                                   int sessionID, BRM::LogicalPartition partition);

  /** @brief dtor
   */
  EXPORT virtual ~VacuumPartitionDMLPackage();

  /** @brief write a VacuumPartitionDMLPackage to a ByteStream
   *
   * @param bytestream the ByteStream to write to
   */
  EXPORT int write(messageqcpp::ByteStream& bytestream);

  /** @brief read a VacuumPartitionDMLPackage from a ByteStream
   *
   * @param bytestream the ByteStream to read from
   */
  EXPORT int read(messageqcpp::ByteStream& bytestream);

  /** @brief build a VacuumPartitionDMLPackage from a string buffer
   *
   * @param buffer [rowId, columnName, colValue]
   * @param columns the number of columns in the buffer
   * @param rows the number of rows in the buffer
   */
  EXPORT int buildFromBuffer(std::string& buffer, int columns, int rows);

  /** @brief build a VacuumPartitionDMLPackage from a parsed DeleteSqlStatement
   *
   * @param sqlStatement the parsed DeleteSqlStatement
   */
  EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement);
  /** @brief build a InsertDMLPackage from MySQL buffer
   *
   * @param colNameList, tableValuesMap
   * @param rows the number of rows in the buffer
   */
  EXPORT int buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap, int columns,
                                  int rows, NullValuesBitset& nullValues);

  BRM::LogicalPartition getPartition() const
  {
    return fPartition;
  }

 private:
  BRM::LogicalPartition fPartition;  // The partition number
};
}  // namespace dmlpackage

#undef EXPORT
