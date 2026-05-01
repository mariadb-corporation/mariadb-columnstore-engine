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

#include "we_parquet_reader.h"

#include <chrono>
#include <memory>
#include <sstream>

#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>

#include "we_define.h"

namespace WriteEngine
{

namespace
{

int setArrowError(const std::string& prefix, const arrow::Status& st, std::string& errMsg)
{
  std::ostringstream oss;
  oss << prefix << ": " << st.ToString();
  errMsg = oss.str();
  return ERR_FILE_READ;
}

template <typename T>
int setArrowError(const std::string& prefix, const arrow::Result<T>& result, std::string& errMsg)
{
  return setArrowError(prefix, result.status(), errMsg);
}

}  // namespace

int ParquetReader::readFile(const std::string& filePath, ParquetReadStats& stats, std::string& errMsg)
{
  stats = {};
  errMsg.clear();

  const auto start = std::chrono::steady_clock::now();

  auto inputResult = arrow::io::ReadableFile::Open(filePath);
  if (!inputResult.ok())
  {
    return setArrowError("Unable to open parquet file", inputResult, errMsg);
  }
  std::shared_ptr<arrow::io::ReadableFile> input = inputResult.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> reader;
  arrow::Status openStatus = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
  if (!openStatus.ok())
  {
    return setArrowError("Unable to initialize parquet reader", openStatus, errMsg);
  }
  reader->set_use_threads(true);

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status schemaStatus = reader->GetSchema(&schema);
  if (!schemaStatus.ok())
  {
    return setArrowError("Unable to read parquet schema", schemaStatus, errMsg);
  }

  stats.columnCount = schema ? schema->num_fields() : 0;
  stats.rowGroupCount = reader->num_row_groups();

  std::shared_ptr<arrow::Table> table;
  arrow::Status readStatus = reader->ReadTable(&table);
  if (!readStatus.ok())
  {
    return setArrowError("Unable to read parquet table", readStatus, errMsg);
  }

  if (table)
  {
    stats.totalRows = table->num_rows();
    if (table->num_columns() > 0)
    {
      stats.batchCount = table->column(0)->num_chunks();
    }
    else
    {
      stats.batchCount = stats.rowGroupCount;
    }
  }

  const auto end = std::chrono::steady_clock::now();
  stats.elapsedSeconds = std::chrono::duration<double>(end - start).count();

  return NO_ERROR;
}

}  // namespace WriteEngine
