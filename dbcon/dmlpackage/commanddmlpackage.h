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
 *   $Id: commanddmlpackage.h 9210 2013-01-21 14:10:42Z rdempsey $
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
 * Specifically for representing COMMAND DML Statements
 */
class CommandDMLPackage : public CalpontDMLPackage
{
 public:
  /** @brief ctor
   */
  EXPORT CommandDMLPackage();

  /** @brief ctor
   */
  EXPORT CommandDMLPackage(std::string dmlStatement, int sessionID);

  /** @brief dtor
   */
  EXPORT ~CommandDMLPackage() override;

  /** @brief write a CommandDMLPackage to a ByteStream
   *
   *  @param bytestream the ByteStream to write to
   */
  EXPORT int write(messageqcpp::ByteStream& bytestream) override;

  /** @brief read CommandDMLPackage from bytestream
   *
   * @param bytestream the ByteStream to read from
   */
  EXPORT int read(messageqcpp::ByteStream& bytestream) override;
  /** @brief do nothing
   *
   * @param buffer
   * @param columns the number of columns in the buffer
   * @param rows the number of rows in the buffer
   */
  inline int buildFromBuffer(std::string& /*buffer*/, int /*columns*/ = 0, int /*rows*/ = 0) override
  {
    return 1;
  };

  /** @brief build a CommandDMLPackage from a CommandSqlStatement
   */
  EXPORT int buildFromSqlStatement(SqlStatement& sqlStatement) override;

  /** @brief build a InsertDMLPackage from MySQL buffer
   *
   * @param colNameList, tableValuesMap
   * @param rows the number of rows in the buffer
   */
  int buildFromMysqlBuffer(ColNameList& /*colNameList*/, TableValuesMap& /*tableValuesMap*/, int /*columns*/, int /*rows*/,
                           NullValuesBitset& /*nullValues*/) override
  {
    return 1;
  };

 protected:
 private:
};

}  // namespace dmlpackage

#undef EXPORT
