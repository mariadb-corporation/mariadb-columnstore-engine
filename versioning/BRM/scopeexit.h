/* Copyright (C) 2026 MariaDB Corporation

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

#include <functional>
#include <iostream>
#include <utility>

namespace BRM
{
/// @brief Simple utility class to call a function when it goes out of scope
class ScopeExit
{
 public:
  explicit ScopeExit(std::function<void()> undo) : fUndo(std::move(undo))
  {
  }
  ScopeExit(const ScopeExit&) = delete;
  ScopeExit& operator=(const ScopeExit&) = delete;

  ~ScopeExit()
  {
    try
    {
      if (fUndo)
        fUndo();
    }
    catch (std::exception& e)
    {
      std::cerr << "ScopeExit: " << e.what() << std::endl;
    }
    catch (...)
    {
    }
  }

  /// Undoes it now rather than at the end of the scope, for a caller that has
  /// more to do and no longer needs whatever this was holding.
  void releaseNow()
  {
    if (!fUndo)
      return;

    auto undo = std::move(fUndo);
    fUndo = nullptr;
    undo();
  }

 private:
  std::function<void()> fUndo;
};

}  // namespace BRM
