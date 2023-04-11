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

#pragma once

#include "rowgroup.h"
#include "logger.h"
#include "hasher.h"
#include "IDBPolicy.h"

#include <map>
#include <unordered_set>
#include <mutex>
#include <random>

// Represents a commands for `ExeMgr`.
#define ANALYZE_TABLE_EXECUTE 6
#define ANALYZE_TABLE_REC_STATS 7
#define ANALYZE_TABLE_NEED_STATS 8
#define ANALYZE_TABLE_SUCCESS 9
// #define DEBUG_STATISTICS

using namespace idbdatafile;

namespace statistics
{
// Represents a column key type:
// PK - primary key.
// FK - foreign key.
enum class KeyType : uint32_t
{
  PK,
  FK
};

// Rerpresents types of statistics CS supports.
enum class StatisticsType : uint32_t
{
  // A special statistics type, specifies whether a column a primary key or foreign key.
  PK_FK,
  // Most common values.
  MCV
};

// Represetns a header for the statistics file.
struct StatisticsFileHeader
{
  uint64_t version;
  uint64_t epoch;
  uint64_t dataHash;
  uint64_t dataSize;
  uint8_t offset[1024];
};

using ColumnsCache = std::unordered_map<uint32_t, std::unordered_set<uint64_t>>;
using ColumnGroup = std::unordered_map<uint32_t, std::vector<uint64_t>>;
using KeyTypes = std::unordered_map<uint32_t, KeyType>;
using MCVList = std::unordered_map<uint32_t, std::unordered_map<uint64_t, uint32_t>>;

// This class is responsible for processing and storing statistics.
// On each `analyze table` iteration it increases an epoch and stores
// the updated statistics into the special file.
class StatisticsManager
{
 public:
  // Returns the instance of this class, static initialization happens only once.
  static StatisticsManager* instance();
  // Collect samples from the given `rowGroup`.
  void collectSample(const rowgroup::RowGroup& rowGroup);
  // Analyzes collected samples.
  void analyzeSample(bool traceOn);
  // Ouputs stats to out stream.
  void output();
  // Saves stats to the file.
  void saveToFile();
  // Loads stats from the file.
  void loadFromFile();
  void incEpoch()
  {
    ++epoch;
  }
  // Serialize stats to the given `bs`.
  void serialize(messageqcpp::ByteStream& bs);
  // Unserialize stats from the given `bs`.
  void unserialize(messageqcpp::ByteStream& bs);
  // Computes hash from the current statistics data.
  uint64_t computeHashFromStats();
  // Checks whether statistics is available for the given `oid`.
  bool hasKey(uint32_t oid);
  // Returns a KeyType for the given `oid`.
  KeyType getKeyType(uint32_t oid);

 private:
  StatisticsManager() : currentSampleSize(0), epoch(0), version(1)
  {
    // Initialize plugins.
    IDBPolicy::configIDBPolicy();
    // Generate distibution once in range [0, UINT_MAX].
    gen32 = std::mt19937(randomDevice());
    uniformDistribution = std::uniform_int_distribution<uint32_t>(0, UINT_MAX);
  }

  std::unique_ptr<char[]> convertStatsToDataStream(uint64_t& dataStreamSize);
  void convertStatsFromDataStream(std::unique_ptr<char[]> dataStreamSmartPtr);

  std::random_device randomDevice;
  std::mt19937 gen32;
  std::uniform_int_distribution<uint32_t> uniformDistribution;

  // Internal data represents a sample [OID, vector of values].
  ColumnGroup columnGroups;
  // Internal data for the PK/FK statistics [OID, bool value].
  KeyTypes keyTypes;
  // Internal data for MCV list [OID, list[value, count]]
  MCVList mcv;

  // TODO: Think about sample size.
  const uint32_t maxSampleSize = 64000;
  uint32_t currentSampleSize;
  uint32_t epoch;
  uint32_t version;

  std::mutex mut;
  std::string statsFile = "/var/lib/columnstore/local/statistics";
};

// This class is responsible for distributing the statistics across all `ExeMgr` in a cluster.
class StatisticsDistributor
{
 public:
  // Returns the instance of this class, static initialization happens only once.
  static StatisticsDistributor* instance();

  // Distribute stats across all `ExeMgr` in cluster by connecting to them using config file.
  void distributeStatistics();

 private:
  StatisticsDistributor() : clientsCount(0)
  {
  }
  // Count the number of clients by reading config file and evaluating `ExeMgr` fields.
  void countClients();
  uint32_t clientsCount;
  std::mutex mut;
};

}  // namespace statistics
