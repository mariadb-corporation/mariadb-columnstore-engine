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
#ifndef UTILS_COMMON_CHECKS_H
#define UTILS_COMMON_CHECKS_H

#include <type_traits>
#include "mcs_int128.h"

namespace utils
{
template <typename T>
typename std::enable_if<std::is_unsigned<T>::value || datatypes::is_uint128_t<T>::value, bool>::type
    is_nonnegative(T)
{
  return true;
};

template <typename T>
typename std::enable_if<std::is_signed<T>::value || datatypes::is_int128_t<T>::value, bool>::type
is_nonnegative(T v)
{
  return v >= 0;
};

template <typename T>
typename std::enable_if<std::is_unsigned<T>::value || datatypes::is_uint128_t<T>::value, bool>::type
    is_negative(T)
{
  return false;
};

template <typename T>
typename std::enable_if<std::is_signed<T>::value || datatypes::is_int128_t<T>::value, bool>::type is_negative(
    T v)
{
  return v < 0;
};

}  // namespace utils

#endif  // UTILS_COMMON_CHECKS_H
