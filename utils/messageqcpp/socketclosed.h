/* Copyright (C) 2014 InfiniDB, Inc.

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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/

/** @file */

#include <exception>
#include <string>
#include <utility>

#pragma once

namespace messageqcpp
{
/** @brief A closed socket exception class
 *
 *  Some sort of activity has been requested on a closed socket
 */
class SocketClosed : public std::exception
{
  std::string M_msg;

 public:
  /** Takes a character string describing the error.  */
  explicit SocketClosed(std::string _arg) : M_msg(std::move(_arg))
  {
  }

  ~SocketClosed() noexcept override = default;

  /** Returns a C-style character string describing the general cause of
   *  the current error (the same string passed to the ctor).  */
  const char* what() const noexcept override
  {
    return M_msg.c_str();
  }
};

}  // namespace messageqcpp
