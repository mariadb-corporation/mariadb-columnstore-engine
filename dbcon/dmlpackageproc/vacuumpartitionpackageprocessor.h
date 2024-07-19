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

#include <vector>
#include "dmlpackageprocessor.h"
#include "vacuumpartitiondmlpackage.h"

#define EXPORT

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
 * Specifically for interacting with the Write Engine to
 * process VACUUM_PARTITION_BLOAT statements.
 */
class VacuumPartitionPackageProcessor : public DMLPackageProcessor
{
 public:
  VacuumPartitionPackageProcessor(BRM::DBRM* dbrm, uint32_t sid) : DMLPackageProcessor(dbrm, sid)
  {
  }

 private:
  DMLResult processPackageInternal(dmlpackage::CalpontDMLPackage& cpackage) override;

  uint64_t doVacuumRows(dmlpackage::VacuumPartitionDMLPackage& package, DMLResult& result,
                        const uint64_t uniqueId, const uint32_t tableOid);

  bool processMetaRG(messageqcpp::ByteStream& bsRowGroup, DMLResult& result, const uint64_t uniqueId,
                     dmlpackage::VacuumPartitionDMLPackage& package, std::map<unsigned, bool>& pmState,
                     uint32_t dbroot = 1);

  bool processRG(messageqcpp::ByteStream& bsRowGroup, DMLResult& result, const uint64_t uniqueId,
                 dmlpackage::VacuumPartitionDMLPackage& package, std::map<unsigned, bool>& pmState,
                 uint32_t dbroot = 1);

  bool receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs,
                  std::map<unsigned, bool>& pmState, const uint32_t tableOid);
};

}  // namespace dmlpackageprocessor

#undef EXPORT
