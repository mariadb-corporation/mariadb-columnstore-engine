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

#ifndef UTILS_COLWIDTH_H
#define UTILS_COLWIDTH_H

#include "branchpred.h"

namespace utils
{
const uint8_t MAXLEGACYWIDTH = 8ULL;
const uint8_t MAXCOLUMNWIDTH = 16ULL;

inline bool isWide(uint8_t width)
{
  return width > MAXLEGACYWIDTH;
}

inline bool isNarrow(uint8_t width)
{
  return width <= MAXLEGACYWIDTH;
}

/** @brief Map a DECIMAL precision to data width in bytes */
inline uint8_t widthByPrecision(unsigned p)
{
  if (LIKELY(p > 18 && p < 39))
    return 16;

  switch (p)
  {
    case 1:
    case 2: return 1;

    case 3:
    case 4: return 2;

    case 5:
    case 6:
    case 7:
    case 8:
    case 9: return 4;

    case 10:
    case 11:
    case 12:
    case 13:
    case 14:
    case 15:
    case 16:
    case 17:
    case 18: return 8;

    default: return 16;
  }
}

inline uint8_t precisionByWidth(unsigned w)
{
  switch (w)
  {
    case 16: return 38;
    // In case we will support decimals that spans 32 bytes.
    default: return 65;
  }
}

}  // namespace utils

#endif  // UTILS_COLWIDTH_H
