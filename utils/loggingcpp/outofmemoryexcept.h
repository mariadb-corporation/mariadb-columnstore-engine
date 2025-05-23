/* Copyright (C) 2024 MariaDB Corporation

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

#include <new>
#include <string>
#include <cstdint>
#include "idberrorinfo.h"

namespace logging
{

/** @brief Exception class for out of memory conditions with custom error message
 *  Extends std::bad_alloc to provide IDB-specific error messages
 */
class OutOfMemoryExcept : public std::bad_alloc
{
 public:
    explicit OutOfMemoryExcept(uint16_t code)
        : fMessage(IDBErrorInfo::instance()->errorMsg(code))
    {
    }

    const char* what() const noexcept override
    {
        return fMessage.c_str();
    }

 private:
    std::string fMessage;
};

} // namespace logging
