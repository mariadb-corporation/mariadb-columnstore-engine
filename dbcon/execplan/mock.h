#ifndef __MOCK_H__
#define __MOCK_H__

#include <vector>

#include "logicalpartition.h"
#include "calpontsystemcatalog.h"

namespace execplan
{
/**
 * @brief Get partition deleted bitmap
 *
 * @param partitionNum
 * @param tableName
 * @return std::vector<bool> deleted bitmap,`true` means deleted and `false` means not deleted
 */
std::vector<bool> getPartitionDeletedBitmap(BRM::LogicalPartition partitionNum,
                                            execplan::CalpontSystemCatalog::TableName tableName);

}  // namespace execplan
#endif  // __MOCK_H__
