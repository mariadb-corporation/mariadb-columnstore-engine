
#include "mock.h"

namespace execplan
{

std::vector<bool> getPartitionDeletedBitmap(const BRM::LogicalPartition& partitionNum,
                                            const CalpontSystemCatalog::TableName& tableName)
{
  std::vector<bool> auxColMock(MAX_PARTITION_SIZE, true);
  std::fill_n(auxColMock.begin(), MAX_PARTITION_SIZE / 2, false);
  return auxColMock;
}
}  // namespace execplan
