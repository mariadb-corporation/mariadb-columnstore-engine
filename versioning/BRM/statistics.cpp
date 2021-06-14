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

#include "statistics.h"

namespace statistics
{

StatisticManager* StatisticManager::instance()
{
    static StatisticManager* sm = new StatisticManager();
    return sm;
}

void StatisticManager::analyzeColumnKeyTypes(const rowgroup::RowGroup& rowGroup, bool trace)
{
    std::lock_guard<std::mutex> lock(mut);
    auto rowCount = rowGroup.getRowCount();
    const auto columnCount = rowGroup.getColumnCount();
    if (!rowCount || !columnCount)
        return;

    auto& oids = rowGroup.getOIDs();

    rowgroup::Row r;
    rowGroup.initRow(&r);
    rowGroup.getRow(0, &r);

    std::vector<std::unordered_set<uint32_t>> columns(columnCount, std::unordered_set<uint32_t>());
    // Init key types.
    for (uint32_t index = 0; index < columnCount; ++index)
        keyTypes[oids[index]] = KeyType::PK;

    const uint32_t maxRowCount = 4096;
    // TODO: We should read just couple of blocks from columns, not all data, but this requires
    // more deep refactoring of column commands.
    rowCount = std::min(rowCount, maxRowCount);
    // This is strange, it's a CS but I'm processing data as row by row, how to fix it?
    for (uint32_t i = 0; i < rowCount; ++i)
    {
        for (uint32_t j = 0; j < columnCount; ++j)
        {
            if (r.isNullValue(j) || columns[j].count(r.getIntField(j)))
                keyTypes[oids[j]] = KeyType::FK;
            else
                columns[j].insert(r.getIntField(j));
        }
        r.nextRow();
    }

    if (trace)
        toStringKeyTypes();
}

bool StatisticManager::hasKey(uint32_t oid) { return keyTypes.count(oid) > 0 ? true : false; }

KeyType StatisticManager::getKeyType(uint32_t oid) { return keyTypes[oid]; }

void StatisticManager::toStringKeyTypes()
{
    for (const auto& p : keyTypes)
        std::cout << p.first << " " << (int) p.second << std::endl;
}
} // namespace statistics
