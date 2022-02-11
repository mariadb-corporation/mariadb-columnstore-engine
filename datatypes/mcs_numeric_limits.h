/*
   Copyright (C) 2021 MariaDB Corporation

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
#ifndef MCS_NUMERIC_LIMITS_H_INCLUDED
#define MCS_NUMERIC_LIMITS_H_INCLUDED

#include <limits>

namespace datatypes
{
template <typename T>
struct numeric_limits
{
  static constexpr T min()
  {
    return std::numeric_limits<T>::min();
  }
  static constexpr T max()
  {
    return std::numeric_limits<T>::max();
  }
};

using int128_t = __int128;
using uint128_t = unsigned __int128;

template <>
struct numeric_limits<int128_t>
{
  static constexpr int128_t min()
  {
    return int128_t(0x8000000000000000LL) << 64;
  }
  static constexpr int128_t max()
  {
    return (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;
  }
};

template <>
struct numeric_limits<uint128_t>
{
  static constexpr uint128_t min()
  {
    return uint128_t(0);
  }
  static constexpr uint128_t max()
  {
    return (uint128_t(0xFFFFFFFFFFFFFFFFULL) << 64) + 0xFFFFFFFFFFFFFFFFULL;
  }
};

}  // namespace datatypes

#endif  // MCS_NUMERIC_LIMITS_H_INCLUDED
