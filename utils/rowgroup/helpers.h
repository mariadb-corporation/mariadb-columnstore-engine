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

#pragma once

#include <cstdint>

namespace rowgroup
{
void getLocationFromRid(uint64_t rid, uint32_t* partNum, uint16_t* segNum, uint8_t* extentNum,
                        uint16_t* blockNum);

// returns the first RID of the logical block identified by baseRid
uint64_t getExtentRelativeRid(uint64_t baseRid);

// returns the first RID of the logical block identified by baseRid
uint64_t getFileRelativeRid(uint64_t baseRid);

}  // namespace rowgroup