/* Copyright (C) 2021 MariaDB Corporation

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

#include <iostream>
#include <atomic>
#include <random>
#include <boost/filesystem.hpp>

#include "statistics.h"
#include "IDBPolicy.h"
#include "brmtypes.h"
#include "hasher.h"
#include "messagequeue.h"
#include "configcpp.h"

using namespace idbdatafile;
using namespace logging;

namespace statistics
{
StatisticsManager* StatisticsManager::instance()
{
  static StatisticsManager* sm = new StatisticsManager();
  return sm;
}

void StatisticsManager::collectSample(const rowgroup::RowGroup& rowGroup)
{
  std::lock_guard<std::mutex> lock(mut);
  const auto rowCount = rowGroup.getRowCount();
  const auto columnCount = rowGroup.getColumnCount();
  if (!rowCount || !columnCount)
    return;

  const auto& oids = rowGroup.getOIDs();
  for (const auto oid : oids)
  {
    // Initialize a column data with 0.
    if (!columnGroups.count(oid))
      columnGroups[oid] = std::vector<uint64_t>(maxSampleSize, 0);
  }

  // Initialize a first row from the given `rowGroup`.
  rowgroup::Row r;
  rowGroup.initRow(&r);
  rowGroup.getRow(0, &r);

  // Generate a uniform distribution.
  std::random_device randomDevice;
  std::mt19937 gen32(randomDevice());
  std::uniform_int_distribution<> uniformDistribution(0, currentRowIndex + rowCount - 1);

  for (uint32_t i = 0; i < rowCount; ++i)
  {
    if (currentSampleSize < maxSampleSize)
    {
      for (uint32_t j = 0; j < columnCount; ++j)
      {
        // FIXME: Handle null values as well.
        if (!r.isNullValue(j))
          columnGroups[oids[j]][currentSampleSize] = r.getIntField(j);
      }
      ++currentSampleSize;
    }
    else
    {
      const uint32_t index = uniformDistribution(gen32);
      if (index < maxSampleSize)
      {
        for (uint32_t j = 0; j < columnCount; ++j)
          columnGroups[oids[j]][index] = r.getIntField(j);
      }
    }
    r.nextRow();
    ++currentRowIndex;
  }
}

void StatisticsManager::analyzeSample(bool traceOn)
{
  if (traceOn)
    std::cout << "Sample size: " << currentSampleSize << std::endl;

  // PK_FK statistics.
  for (const auto& [oid, sample] : columnGroups)
    keyTypes[oid] = std::make_pair(KeyType::PK, currentRowIndex);

  for (const auto& [oid, sample] : columnGroups)
  {
    std::unordered_set<uint32_t> columnsCache;
    std::unordered_map<uint64_t, uint32_t> columnMCV;
    for (uint32_t i = 0; i < currentSampleSize; ++i)
    {
      const auto value = sample[i];
      // PK_FK statistics.
      if (columnsCache.count(value) && keyTypes[oid].first == KeyType::PK)
        keyTypes[oid].first = KeyType::FK;
      else
        columnsCache.insert(value);

      // MCV statistics.
      if (columnMCV.count(value))
        columnMCV[value]++;
      else
        columnMCV.insert({value, 1});
    }

    // MCV statistics.
    std::vector<pair<uint64_t, uint32_t>> mcvList(columnMCV.begin(), columnMCV.end());
    std::sort(mcvList.begin(), mcvList.end(),
              [](const std::pair<uint64_t, uint32_t>& a, const std::pair<uint64_t, uint32_t>& b) {
                return a.second > b.second;
              });

    // 200 buckets as Microsoft does.
    const auto mcvSize = std::min(columnMCV.size(), static_cast<uint64_t>(200));
    mcv[oid] = std::unordered_map<uint64_t, uint32_t>(mcvList.begin(), mcvList.begin() + mcvSize);
  }

  if (traceOn)
    output();

  // Clear sample.
  columnGroups.clear();
  currentSampleSize = 0;
  currentRowIndex = 0;
}

void StatisticsManager::output()
{
  std::cout << "Columns count: " << keyTypes.size() << std::endl;

  std::cout << "Statistics type [PK_FK]:  " << std::endl;
  for (const auto& p : keyTypes)
  {
    std::cout << "OID: " << p.first << " ";
    if (static_cast<uint32_t>(p.second.first) == 0)
      std::cout << "PK ";
    else
      std::cout << "FK ";
    std::cout << "row count: " << p.second.second << std::endl;
  }

  std::cout << "Statistics type [MCV]: " << std::endl;
  for (const auto& [oid, columnMCV] : mcv)
  {
    std::cout << "OID: " << oid << std::endl;
    for (const auto& [value, count] : columnMCV)
      std::cout << value << ": " << count << std::endl;
  }
}

// Someday it will be a virtual method, based on statistics type we processing.
std::unique_ptr<char[]> StatisticsManager::convertStatsToDataStream(uint64_t& dataStreamSize)
{
  // Number of pairs.
  uint64_t count = keyTypes.size();
  // count, [[uid, keyType, rows count], ... ]
  dataStreamSize = sizeof(uint64_t) + count * (sizeof(uint32_t) + sizeof(KeyType) + sizeof(uint32_t));

  // Count the size of the MCV.
  for (const auto& [oid, mcvColumn] : mcv)
  {
    // [oid, list size, list [value, count]]
    dataStreamSize +=
        (sizeof(uint32_t) + sizeof(uint32_t) + ((sizeof(uint64_t) + sizeof(uint32_t)) * mcvColumn.size()));
  }

  // Allocate memory for data stream.
  std::unique_ptr<char[]> dataStreamSmartPtr(new char[dataStreamSize]);
  auto* dataStream = dataStreamSmartPtr.get();
  // Initialize the data stream.
  uint64_t offset = 0;
  std::memcpy(dataStream, reinterpret_cast<char*>(&count), sizeof(uint64_t));
  offset += sizeof(uint64_t);

  // For each pair [oid, key type, rows count].
  for (const auto& p : keyTypes)
  {
    uint32_t oid = p.first;
    std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&oid), sizeof(uint32_t));
    offset += sizeof(uint32_t);
    KeyType keyType = p.second.first;
    std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&keyType), sizeof(KeyType));
    offset += sizeof(KeyType);
    uint32_t rowCount = p.second.second;
    std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&rowCount), sizeof(uint32_t));
    offset += sizeof(uint32_t);
  }

  // For each [oid, list size, list [value, count]].
  for (const auto& p : mcv)
  {
    // [oid]
    uint32_t oid = p.first;
    std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&oid), sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // [list size]
    const auto& mcvColumn = p.second;
    uint32_t size = mcvColumn.size();
    std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&size), sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // [list [value, count]]
    for (const auto& mcvPair : mcvColumn)
    {
      uint64_t value = mcvPair.first;
      std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&value), sizeof(uint64_t));
      offset += sizeof(uint64_t);
      uint32_t count = mcvPair.second;
      std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&count), sizeof(uint32_t));
      offset += sizeof(uint32_t);
    }
  }
  return dataStreamSmartPtr;
}

void StatisticsManager::convertStatsFromDataStream(std::unique_ptr<char[]> dataStreamSmartPtr)
{
  auto* dataStream = dataStreamSmartPtr.get();
  uint64_t count = 0;
  std::memcpy(reinterpret_cast<char*>(&count), dataStream, sizeof(uint64_t));
  uint64_t offset = sizeof(uint64_t);

  // For each pair.
  for (uint64_t i = 0; i < count; ++i)
  {
    uint32_t oid, rowCount;
    KeyType keyType;
    std::memcpy(reinterpret_cast<char*>(&oid), &dataStream[offset], sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::memcpy(reinterpret_cast<char*>(&keyType), &dataStream[offset], sizeof(KeyType));
    offset += sizeof(KeyType);
    std::memcpy(reinterpret_cast<char*>(&rowCount), &dataStream[offset], sizeof(uint32_t));
    offset += sizeof(uint32_t);
    // Insert pair.
    keyTypes[oid] = std::make_pair(keyType, rowCount);
  }

  for (uint64_t i = 0; i < count; ++i)
  {
    uint32_t oid;
    std::memcpy(reinterpret_cast<char*>(&oid), &dataStream[offset], sizeof(uint32_t));
    offset += sizeof(uint32_t);

    uint32_t mcvSize;
    std::memcpy(reinterpret_cast<char*>(&mcvSize), &dataStream[offset], sizeof(uint32_t));
    offset += sizeof(uint32_t);

    std::unordered_map<uint64_t, uint32_t> columnMCV;
    for (uint32_t j = 0; j < mcvSize; ++j)
    {
      uint64_t value;
      std::memcpy(reinterpret_cast<char*>(&value), &dataStream[offset], sizeof(uint64_t));
      offset += sizeof(uint64_t);
      uint32_t count;
      std::memcpy(reinterpret_cast<char*>(&count), &dataStream[offset], sizeof(uint32_t));
      offset += sizeof(uint32_t);
      columnMCV[value] = count;
    }
    mcv[oid] = std::move(columnMCV);
  }
}

void StatisticsManager::saveToFile()
{
  std::lock_guard<std::mutex> lock(mut);

  const char* fileName = statsFile.c_str();
  std::unique_ptr<IDBDataFile> out(
      IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "wb", 1));

  if (!out)
  {
    BRM::log_errno("StatisticsManager::saveToFile(): open");
    throw ios_base::failure("StatisticsManager::saveToFile(): open failed.");
  }

  // Compute hash.
  uint64_t dataStreamSize = 0;
  std::unique_ptr<char[]> dataStreamSmartPtr = convertStatsToDataStream(dataStreamSize);
  utils::Hasher128 hasher;

  // Prepare a statistics file header.
  const uint32_t headerSize = sizeof(StatisticsFileHeader);
  StatisticsFileHeader fileHeader;
  std::memset(&fileHeader, 0, headerSize);
  fileHeader.version = version;
  fileHeader.epoch = epoch;
  fileHeader.dataSize = dataStreamSize;
  // Compute hash from the data.
  fileHeader.dataHash = hasher(dataStreamSmartPtr.get(), dataStreamSize);

  // Write statistics file header.
  uint64_t size = out->write(reinterpret_cast<char*>(&fileHeader), headerSize);
  if (size != headerSize)
  {
    auto rc = IDBPolicy::remove(fileName);
    if (rc == -1)
      std::cerr << "Cannot remove file " << fileName << std::endl;

    throw ios_base::failure("StatisticsManager::saveToFile(): write failed. ");
  }

  // Write data.
  size = out->write(dataStreamSmartPtr.get(), dataStreamSize);
  if (size != dataStreamSize)
  {
    auto rc = IDBPolicy::remove(fileName);
    if (rc == -1)
      std::cerr << "Cannot remove file " << fileName << std::endl;

    throw ios_base::failure("StatisticsManager::saveToFile(): write failed. ");
  }
}

void StatisticsManager::loadFromFile()
{
  std::lock_guard<std::mutex> lock(mut);
  // Check that stats file does exist.
  if (!boost::filesystem::exists(statsFile))
    return;

  const char* fileName = statsFile.c_str();
  std::unique_ptr<IDBDataFile> in(
      IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "rb", 1));

  if (!in)
  {
    BRM::log_errno("StatisticsManager::loadFromFile(): open");
    throw ios_base::failure("StatisticsManager::loadFromFile(): open failed. Check the error log.");
  }

  // Read the file header.
  StatisticsFileHeader fileHeader;
  const uint32_t headerSize = sizeof(StatisticsFileHeader);
  int64_t size = in->read(reinterpret_cast<char*>(&fileHeader), headerSize);
  if (size != headerSize)
    throw ios_base::failure("StatisticsManager::loadFromFile(): read failed. ");

  // Initialize fields from the file header.
  version = fileHeader.version;
  epoch = fileHeader.epoch;
  const auto dataHash = fileHeader.dataHash;
  const auto dataStreamSize = fileHeader.dataSize;

  // Allocate the memory for the file data.
  std::unique_ptr<char[]> dataStreamSmartPtr(new char[dataStreamSize]);
  auto* dataStream = dataStreamSmartPtr.get();

  // Read the data.
  uint64_t dataOffset = 0;
  auto sizeToRead = dataStreamSize;
  size = in->read(dataStream, sizeToRead);
  sizeToRead -= size;
  dataOffset += size;

  while (sizeToRead > 0)
  {
    size = in->read(dataStream + dataOffset, sizeToRead);
    if (size < 0)
      throw ios_base::failure("StatisticsManager::loadFromFile(): read failed. ");

    sizeToRead -= size;
    dataOffset += size;
  }

  utils::Hasher128 hasher;
  auto computedDataHash = hasher(dataStream, dataStreamSize);
  if (dataHash != computedDataHash)
    throw ios_base::failure("StatisticsManager::loadFromFile(): invalid file hash. ");

  convertStatsFromDataStream(std::move(dataStreamSmartPtr));
}

uint64_t StatisticsManager::computeHashFromStats()
{
  utils::Hasher128 hasher;
  uint64_t dataStreamSize = 0;
  std::unique_ptr<char[]> dataStreamSmartPtr = convertStatsToDataStream(dataStreamSize);
  return hasher(dataStreamSmartPtr.get(), dataStreamSize);
}

void StatisticsManager::serialize(messageqcpp::ByteStream& bs)
{
  uint64_t count = keyTypes.size();
  bs << version;
  bs << epoch;
  bs << count;

  // PK_FK
  for (const auto& keyType : keyTypes)
  {
    bs << keyType.first;
    bs << (uint32_t)keyType.second.first;
    bs << keyType.second.second;
  }

  // MCV
  for (const auto& p : mcv)
  {
    bs << p.first;
    const auto& mcvColumn = p.second;
    bs << static_cast<uint32_t>(mcvColumn.size());
    for (const auto& mcvPair : mcvColumn)
    {
      bs << mcvPair.first;
      bs << mcvPair.second;
    }
  }
}

void StatisticsManager::unserialize(messageqcpp::ByteStream& bs)
{
  uint64_t count;
  bs >> version;
  bs >> epoch;
  bs >> count;

  // PK_FK
  for (uint32_t i = 0; i < count; ++i)
  {
    uint32_t oid, keyType, rowCount;
    bs >> oid;
    bs >> keyType;
    bs >> rowCount;
    keyTypes[oid] = std::make_pair(static_cast<KeyType>(keyType), rowCount);
  }

  // MCV
  for (uint32_t i = 0; i < count; ++i)
  {
    uint32_t oid, mcvSize;
    bs >> oid;
    bs >> mcvSize;
    std::unordered_map<uint64_t, uint32_t> mcvColumn;

    for (uint32_t j = 0; j < mcvSize; ++j)
    {
      uint64_t value;
      uint32_t count;
      bs >> value;
      bs >> count;
      mcvColumn[value] = count;
    }

    mcv[oid] = std::move(mcvColumn);
  }
}

bool StatisticsManager::hasKey(uint32_t oid)
{
  return keyTypes.count(oid) > 0 ? true : false;
}

KeyType StatisticsManager::getKeyType(uint32_t oid)
{
  return keyTypes[oid].first;
}

StatisticsDistributor* StatisticsDistributor::instance()
{
  static StatisticsDistributor* sd = new StatisticsDistributor();
  return sd;
}

void StatisticsDistributor::distributeStatistics()
{
  countClients();
  {
    std::lock_guard<std::mutex> lock(mut);
    // No clients.
    if (clientsCount == 0)
      return;

#ifdef DEBUG_STATISTICS
    std::cout << "Distribute statistics from ExeMgr(Server) to ExeMgr(Clients) " << std::endl;
#endif

    messageqcpp::ByteStream msg, statsHash, statsBs;
    // Current hash.
    statsHash << statistics::StatisticsManager::instance()->computeHashFromStats();
    // Statistics.
    statistics::StatisticsManager::instance()->serialize(statsBs);

    for (uint32_t i = 0; i < clientsCount; ++i)
    {
      try
      {
        messageqcpp::ByteStream::quadbyte qb = ANALYZE_TABLE_REC_STATS;
        msg << qb;

        auto exeMgrID = "ExeMgr" + std::to_string(i + 2);
        // Create a client.
        std::unique_ptr<messageqcpp::MessageQueueClient> exemgrClient(
            new messageqcpp::MessageQueueClient(exeMgrID));

#ifdef DEBUG_STATISTICS
        std::cout << "Try to connect to " << exeMgrID << std::endl;
#endif
        // Try to connect to the client.
        if (!exemgrClient->connect())
        {
          msg.restart();
#ifdef DEBUG_STATISTICS
          std::cout << "Unable to connect to " << exeMgrID << std::endl;
#endif
          continue;
        }

#ifdef DEBUG_STATISTICS
        std::cout << "Write flag ANALYZE_TABLE_REC_STATS from ExeMgr(Server) to ExeMgr(Clients) "
                  << std::endl;
#endif
        // Write a flag to client ExeMgr.
        exemgrClient->write(msg);

#ifdef DEBUG_STATISTICS
        std::cout << "Write statistics hash from ExeMgr(Server) to ExeMgr(Clients) " << std::endl;
#endif
        // Write a hash of the stats.
        exemgrClient->write(statsHash);

        // Read the state from Client.
        msg.restart();
        msg = exemgrClient->read();
        msg >> qb;

        // Do not need a stats.
        if (qb == ANALYZE_TABLE_SUCCESS)
        {
          msg.restart();
          continue;
        }

#ifdef DEBUG_STATISTICS
        std::cout << "Write statistics bytestream from ExeMgr(Server) to ExeMgr(Clients) " << std::endl;
#endif
        // Write a statistics to client ExeMgr.
        exemgrClient->write(statsBs);

        // Read the flag back from the client ExeMgr.
        msg.restart();
        msg = exemgrClient->read();

        if (msg.length() == 0)
          throw runtime_error("Lost conection to ExeMgr.");
#ifdef DEBUG_STATISTICS
        std::cout << "Read flag on ExeMgr(Server) from ExeMgr(Client) " << std::endl;
#endif
        msg.restart();
      }
      catch (std::exception& e)
      {
        msg.restart();
        std::cerr << "distributeStatistics() failed with error: " << e.what() << std::endl;
      }
      catch (...)
      {
        msg.restart();
        std::cerr << "distributeStatistics() failed with unknown error." << std::endl;
      }
    }
  }
}

void StatisticsDistributor::countClients()
{
#ifdef DEBUG_STATISTICS
  std::cout << "count clients to distribute statistics " << std::endl;
#endif
  auto* config = config::Config::makeConfig();
  // Starting from the ExeMgr2, since the Server starts on the ExeMgr1.
  std::atomic<uint32_t> exeMgrNumber(2);

  try
  {
    while (true)
    {
      auto exeMgrID = "ExeMgr" + std::to_string(exeMgrNumber);
      auto exeMgrIP = config->getConfig(exeMgrID, "IPAddr");
      if (exeMgrIP == "")
        break;
#ifdef DEBUG_STATISTICS
      std::cout << "Client: " << exeMgrID << std::endl;
#endif
      ++exeMgrNumber;
    }
  }
  catch (std::exception& e)
  {
    std::cerr << "countClients() failed with error: " << e.what() << std::endl;
  }
  catch (...)
  {
    std::cerr << "countClients() failed with unknown error: ";
  }

  clientsCount = exeMgrNumber - 2;
#ifdef DEBUG_STATISTICS
  std::cout << "Number of clients: " << clientsCount << std::endl;
#endif
}

}  // namespace statistics
