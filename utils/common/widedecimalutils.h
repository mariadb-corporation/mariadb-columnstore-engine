/* Copyright (C) 2020 MariaDB Corporation

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

#ifndef WIDE_DECIMAL_UTILS_H
#define WIDE_DECIMAL_UTILS_H

namespace utils
{

using int128_t = __int128;
using uint128_t = unsigned __int128;

const uint64_t BINARYNULLVALUELOW = 0ULL;
const uint64_t BINARYNULLVALUEHIGH = 0x8000000000000000ULL;
const uint64_t BINARYEMPTYVALUELOW = 1ULL;
const uint64_t BINARYEMPTYVALUEHIGH = 0x8000000000000000ULL;

    inline bool isWideDecimalNullValue(const int128_t val)
    {
        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(&val);
        return (ptr[0] == BINARYNULLVALUELOW && ptr[1] == BINARYNULLVALUEHIGH);
    }

    inline bool isWideDecimalEmptyValue(const int128_t val)
    {
        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(&val);
        return (ptr[0] == BINARYEMPTYVALUELOW && ptr[1] == BINARYEMPTYVALUEHIGH);
    }

    inline void int128Max(int128_t& val)
    {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(&val);
        ptr[0] = 0xFFFFFFFFFFFFFFFF;
        ptr[1] = 0x7FFFFFFFFFFFFFFF;
    }

    inline void int128Min(int128_t& val)
    {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(&val);
        ptr[0] = 0;
        ptr[1] = 0x8000000000000000;
    }

    inline void uint128Max(uint128_t& val)
    {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(&val);
        ptr[0] = 0xFFFFFFFFFFFFFFFF;
        ptr[1] = 0xFFFFFFFFFFFFFFFF;
    }

}

#endif // WIDE_DECIMAL_UTILS_H
