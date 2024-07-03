#ifndef __MOCK_H__
#define __MOCK_H__

#include <vector>
#include <random>

#include "logicalpartition.h"
#include "calpontsystemcatalog.h"

namespace execplan
{

static constexpr auto MAX_PARTITION_SIZE = 8 * 1024 * 1024;
inline std::vector<bool> getPartitionDeletedBitmap(BRM::LogicalPartition partitionNum,
                                                   execplan::CalpontSystemCatalog::TableName tableName)
{
  std::vector<bool> auxColMock;
  auxColMock.reserve(MAX_PARTITION_SIZE);

  // Initialize a random number generator
  std::random_device rd;
  std::mt19937 gen(rd());
  std::bernoulli_distribution d(0.5);  // 50% chance for true or false

  for (size_t i = 0; i < MAX_PARTITION_SIZE; ++i)
  {
    auxColMock.push_back(d(gen));
  }

  return auxColMock;
}

}  // namespace
#endif  // __MOCK_H__
