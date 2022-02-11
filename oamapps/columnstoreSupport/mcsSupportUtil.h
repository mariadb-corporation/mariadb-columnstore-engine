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

#ifndef MCS_SUPPORT_UTIL_H_
#define MCS_SUPPORT_UTIL_H_

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include "configcpp.h"
#include "liboamcpp.h"

void getSystemNetworkConfig(FILE* pOutputFile);
void getModuleTypeConfig(FILE* pOutputFile);
void getStorageConfig(FILE* pOutputFile);
void getStorageStatus(FILE* pOutputFile);
bool checkLogStatus(std::string filename, std::string phase);
std::string getIPAddress(std::string hostName);

#endif
