#ifndef __MOCK_H__
#define __MOCK_H__

#include <vector>

#include "logicalpartition.h"
#include "calpontsystemcatalog.h"

namespace execplan
{
static constexpr auto MAX_PARTITION_SIZE = 8388608;

/**
 * @brief Get partition deleted bitmap
 *
 * @param partitionNum
 * @param tableName
 * @return deleted bitmap,`true` means deleted and `false` means not deleted
 */
std::vector<bool> getPartitionDeletedBitmap(const BRM::LogicalPartition& partitionNum,
                                            const CalpontSystemCatalog::TableName& tableName);

}  // namespace execplan
#endif  // __MOCK_H__
