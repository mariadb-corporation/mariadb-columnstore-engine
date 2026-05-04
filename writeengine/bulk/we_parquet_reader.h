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

#include <cstdint>
#include <string>
#include <vector>

namespace WriteEngine
{
class TableInfo;

struct ParquetReadStats
{
  int64_t totalRows{0};
  int64_t batchCount{0};
  int64_t rowGroupCount{0};
  int64_t columnCount{0};
  double elapsedSeconds{0.0};
};

struct ParquetColumnMapping
{
  std::string name;
  std::string arrowType;
  std::string conversion;
};

struct ParquetConversionResult
{
  ParquetReadStats stats;
  std::vector<ParquetColumnMapping> mappings;
  std::string materializedFilePath;
};

class ParquetReader
{
 public:
  static int readFile(const std::string& filePath, ParquetReadStats& stats, std::string& errMsg);
  static int convertToDelimitedFile(const std::string& parquetFilePath, const std::string& outputFilePath,
                                    ParquetConversionResult& result, std::string& errMsg);
  static int importIntoTableDirect(const std::string& parquetFilePath, TableInfo& tableInfo,
                                   ParquetConversionResult& result, std::string& errMsg);
};

}  // namespace WriteEngine
