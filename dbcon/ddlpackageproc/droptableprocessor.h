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
 *   $Id: droptableprocessor.h 9303 2013-03-07 16:07:12Z chao $
 *
 *
 ***********************************************************************/
/** @file */
#pragma once

#include "ddlpackageprocessor.h"

#define EXPORT

namespace ddlpackageprocessor
{
/** @brief specialization of a DDLPacakageProcessor
 * for interacting with the Write Engine to process
 * drop table ddl statements.
 */
class DropTableProcessor : public DDLPackageProcessor
{
 public:
  DropTableProcessor(BRM::DBRM* aDbrm) : DDLPackageProcessor(aDbrm)
  {
  }

 protected:
 private:
  /** @brief process a drop table statement
   *
   *  @param dropTableStmt the drop table statement
   */
  DDLResult processPackageInternal(ddlpackage::SqlStatement* dropTableStmt);
};

/** @brief specialization of a DDLPacakageProcessor
 * for interacting with the Write Engine to process
 * truncate table ddl statements.
 */
class TruncTableProcessor : public DDLPackageProcessor
{
 public:
  TruncTableProcessor(BRM::DBRM* aDbrm) : DDLPackageProcessor(aDbrm)
  {
  }

 protected:
 private:
  /** @brief process a truncate table statement
   *
   *  @param truncTableStmt the truncate table statement
   */
  DDLResult processPackageInternal(ddlpackage::SqlStatement* truncTableStmt);
};

}  // namespace ddlpackageprocessor

#undef EXPORT
