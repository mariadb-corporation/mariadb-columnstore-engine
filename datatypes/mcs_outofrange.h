/* Copyright (C) 2021 MariaDB Corporation.

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

#pragma once

#include <string>

#include "exceptclasses.h"

namespace datatypes
{

inline void throwOutOfRangeExceptionIfNeeded(const bool isNull, const bool isOutOfRange)
{
  if (!isNull && isOutOfRange)
  {
    logging::Message::Args args;
    static const std::string sqlType{"BIGINT UNSIGNED"};
    args.add(sqlType);
    const auto errcode = logging::ERR_MATH_PRODUCES_OUT_OF_RANGE_RESULT;
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
  }
}

}  // namespace datatypes
