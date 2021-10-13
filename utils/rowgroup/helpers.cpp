/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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
   MA 02110-1301, USA.
*/

#include "helpers.h"

namespace rowgroup
{
void getLocationFromRid(uint64_t rid, uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                        uint16_t* blockNum)
{
    if (partNum)
        *partNum = rid >> 32;

    if (segNum)
        *segNum = rid >> 16;

    if (extentNum)
        *extentNum = (rid >> 10) & 0x3f;

    if (blockNum)
        *blockNum = rid & 0x3ff;
}

uint64_t getFileRelativeRid(uint64_t baseRid)
{
    uint64_t extentNum = (baseRid >> 10) & 0x3f;
    uint64_t blockNum = baseRid & 0x3ff;
    return (extentNum << 23) | (blockNum << 13);
}

uint64_t getExtentRelativeRid(uint64_t baseRid)
{
    uint64_t blockNum = baseRid & 0x3ff;
    return (blockNum << 13);
}

}  // namespace rowgroup