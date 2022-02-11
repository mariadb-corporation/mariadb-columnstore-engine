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

#ifndef STATISTICS_H
#define STATISTICS_H

#include "rowgroup.h"
#include "logger.h"
#include "hasher.h"
#include "IDBPolicy.h"

#include <map>
#include <unordered_set>
#include <mutex>

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
  // A special statistics type, made to solve circular inner join problem.
  PK_FK
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

// This class is responsible for processing and storing statistics.
// On each `analyze table` iteration it increases an epoch and stores
// the updated statistics into the special file.
class StatisticsManager
{
 public:
  // Returns the instance of this class, static initialization happens only once.
  static StatisticsManager* instance();
  // Analyzes the given `rowGroup` by processing it row by row and searching for foreign key.
  void analyzeColumnKeyTypes(const rowgroup::RowGroup& rowGroup, bool trace);
  // Ouputs stats to out stream.
  void output(StatisticsType statisticsType = StatisticsType::PK_FK);
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
  std::map<uint32_t, KeyType> keyTypes;
  StatisticsManager() : epoch(0), version(1)
  {
    IDBPolicy::init(true, false, "", 0);
  }
  std::unique_ptr<char[]> convertStatsToDataStream(uint64_t& dataStreamSize);

  std::mutex mut;
  uint32_t epoch;
  uint32_t version;
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
#endif
