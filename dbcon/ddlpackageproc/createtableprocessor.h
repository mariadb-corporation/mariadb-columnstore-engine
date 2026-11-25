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
 *   $Id: createtableprocessor.h 9303 2013-03-07 16:07:12Z chao $
 *
 *
 ***********************************************************************/
/** @file */
#pragma once

#include "ddlpackageprocessor.h"

#define EXPORT

namespace ddlpackageprocessor
{
/** @brief specialization of a DDLPackageProcessor
 * for interacting with the Write Engine
 * to process create table ddl statements.
 */
class CreateTableProcessor : public DDLPackageProcessor
{
 public:
  CreateTableProcessor(BRM::DBRM* aDbrm) : DDLPackageProcessor(aDbrm)
  {
  }

 protected:
  void rollBackCreateTable(const std::string& error, BRM::TxnID txnID, int sessionId,
                           ddlpackage::TableDef& tableDef, DDLResult& result);

 private:
  /** @brief process a create table statement
   *
   * @param createTableStmt the CreateTableStatement
   */
  DDLResult processPackageInternal(ddlpackage::SqlStatement* sqlTableStmt);
};

}  // namespace ddlpackageprocessor

#undef EXPORT
