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

#define VACUUMPARTITIONDMLPKG_DLLEXPORT
#include "vacuumpartitiondmlpackage.h"
#undef VACUUMPARTITIONDMLPKG_DLLEXPORT

namespace dmlpackage
{

VacuumPartitionDMLPackage::VacuumPartitionDMLPackage(std::string schemaName, std::string tableName,
                                                     std::string dmlStatement, int sessionID,
                                                     BRM::LogicalPartition partition)
 : CalpontDMLPackage(schemaName, tableName, dmlStatement, sessionID), fPartition(partition)
{
}

VacuumPartitionDMLPackage::~VacuumPartitionDMLPackage() = default;

int VacuumPartitionDMLPackage::write(messageqcpp::ByteStream& bytestream)
{
  bytestream << (uint8_t)DML_VACUUM_PARTITION;
  bytestream << (uint32_t)fSessionID;

  bytestream << fDMLStatement;
  bytestream << fSchemaName;
  bytestream << fTableName;

  fPartition.serialize(bytestream);

  return 1;
}

int VacuumPartitionDMLPackage::read(messageqcpp::ByteStream& bytestream)
{
  bytestream >> fSessionID;

  bytestream >> fDMLStatement;
  bytestream >> fSchemaName;
  bytestream >> fTableName;

  fPartition.unserialize(bytestream);

  return 1;
}

int VacuumPartitionDMLPackage::buildFromSqlStatement(SqlStatement& sqlStatement)
{
  throw std::logic_error("VacuumPartitionDMLPackage::buildFromSqlStatement is not implemented yet.");
  return 1;
}

int VacuumPartitionDMLPackage::buildFromBuffer(std::string& buffer, int columns, int rows)
{
  throw std::logic_error("VacuumPartitionDMLPackage::buildFromBuffer is not implemented yet.");
  return 1;
}

int VacuumPartitionDMLPackage::buildFromMysqlBuffer(ColNameList& colNameList, TableValuesMap& tableValuesMap,
                                                    int columns, int rows, NullValuesBitset& nullValues)
{
  throw std::logic_error("VacuumPartitionDMLPackage::buildFromMysqlBuffer is not implemented yet.");
  return 1;
}

}  // namespace dmlpackage
