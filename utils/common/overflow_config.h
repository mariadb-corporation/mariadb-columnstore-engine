/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

/**
 * @file overflow_config.h
 * @brief Common utilities for ColumnStore overflow handling configuration
 */

#pragma once

#include <boost/algorithm/string/case_conv.hpp>
#include "configcpp.h"

namespace columnstore
{

// Helper function to check ColumnStore overflow handling configuration
inline bool isStrictOverflowMode()
{
  static bool initialized = false;
  static bool strictMode = false;  // Default to permissive mode

  if (!initialized)
  {
    try
    {
      config::Config* config = config::Config::makeConfig();
      std::string overflowHandling = config->getConfig("ArithmeticOperations", "OverflowHandling");
      boost::algorithm::to_lower(overflowHandling);
      strictMode = (overflowHandling == "strict");
    }
    catch (...)
    {
      // If config reading fails, default to permissive mode
      strictMode = false;
    }
    initialized = true;
  }

  return strictMode;
}

}  // namespace columnstore
