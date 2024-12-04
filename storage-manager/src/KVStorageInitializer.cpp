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
#include "Config.h"
#include "SMLogging.h"
#include "KVStorageInitializer.h"

namespace storagemanager
{
static std::shared_ptr<FDBCS::FDBDataBase> fdbDataBaseInstance;
static std::unique_ptr<FDBCS::FDBNetwork> fdbNetworkInstance;
static std::mutex kvStorageLock;

static void logError(SMLogging* logger, const char* errorMsg)
{
  logger->log(LOG_CRIT, errorMsg);
  throw std::runtime_error(errorMsg);
}

std::shared_ptr<FDBCS::FDBDataBase> KVStorageInitializer::getStorageInstance()
{
  if (fdbDataBaseInstance)
    return fdbDataBaseInstance;

  auto* config = Config::get();
  auto* logger = SMLogging::get();

  std::unique_lock<std::mutex> lock(kvStorageLock);
  if (fdbDataBaseInstance)
    return fdbDataBaseInstance;

  if (!FDBCS::setAPIVersion())
    logError(logger, "Ownership: FDB setAPIVersion failed.");

  fdbNetworkInstance = std::make_unique<FDBCS::FDBNetwork>();
  if (!fdbNetworkInstance->setUpAndRunNetwork())
    logError(logger, "Ownership: FDB setUpAndRunNetwork failed.");

  std::string clusterFilePath = config->getValue("ObjectStorage", "fdb_cluster_file_path");
  if (clusterFilePath.empty())
    logError(logger,
             "Ownership: Need to specify `Foundation DB cluster file path` in the storagemanager.cnf file");

  fdbDataBaseInstance = FDBCS::DataBaseCreator::createDataBase(clusterFilePath);
  if (!fdbDataBaseInstance)
    logError(logger, "Ownership: FDB createDataBase failed.");

  return fdbDataBaseInstance;
}

}  // namespace storagemanager
