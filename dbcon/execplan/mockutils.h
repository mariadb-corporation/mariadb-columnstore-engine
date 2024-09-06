#ifndef __MOCKUTILS_H__
#define __MOCKUTILS_H__

#include <vector>

#include "logicalpartition.h"
#include "calpontsystemcatalog.h"

namespace mockutils
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
                                            const execplan::CalpontSystemCatalog::TableName& tableName);

}  // namespace mockutils
#endif  // __MOCKUTILS_H__
