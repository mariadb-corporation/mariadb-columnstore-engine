/*
   Copyright (C) 2024 MariaDB Corporation

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
   MA 02110-1301, USA.
*/
#include "mcs_systemcatalog.h"

namespace datatypes
{
std::ostream& operator<<(std::ostream& os, SystemCatalog::ColDataType type)
{
  switch (type)
  {
    case SystemCatalog::BIT: os << "BIT"; break;
    case SystemCatalog::TINYINT: os << "TINYINT"; break;
    case SystemCatalog::CHAR: os << "CHAR"; break;
    case SystemCatalog::SMALLINT: os << "SMALLINT"; break;
    case SystemCatalog::DECIMAL: os << "DECIMAL"; break;
    case SystemCatalog::MEDINT: os << "MEDINT"; break;
    case SystemCatalog::INT: os << "INT"; break;
    case SystemCatalog::FLOAT: os << "FLOAT"; break;
    case SystemCatalog::DATE: os << "DATE"; break;
    case SystemCatalog::BIGINT: os << "BIGINT"; break;
    case SystemCatalog::DOUBLE: os << "DOUBLE"; break;
    case SystemCatalog::DATETIME: os << "DATETIME"; break;
    case SystemCatalog::VARCHAR: os << "VARCHAR"; break;
    case SystemCatalog::VARBINARY: os << "VARBINARY"; break;
    case SystemCatalog::CLOB: os << "CLOB"; break;
    case SystemCatalog::BLOB: os << "BLOB"; break;
    case SystemCatalog::UTINYINT: os << "UTINYINT"; break;
    case SystemCatalog::USMALLINT: os << "USMALLINT"; break;
    case SystemCatalog::UDECIMAL: os << "UDECIMAL"; break;
    case SystemCatalog::UMEDINT: os << "UMEDINT"; break;
    case SystemCatalog::UINT: os << "UINT"; break;
    case SystemCatalog::UFLOAT: os << "UFLOAT"; break;
    case SystemCatalog::UBIGINT: os << "UBIGINT"; break;
    case SystemCatalog::UDOUBLE: os << "UDOUBLE"; break;
    case SystemCatalog::TEXT: os << "TEXT"; break;
    case SystemCatalog::TIME: os << "TIME"; break;
    case SystemCatalog::TIMESTAMP: os << "TIMESTAMP"; break;
    case SystemCatalog::LONGDOUBLE: os << "LONGDOUBLE"; break;
    case SystemCatalog::STRINT: os << "STRINT"; break;
    case SystemCatalog::UNDEFINED: os << "UNDEFINED"; break;
    default: os << "UNKNOWN TYPE";  // Safety net for unexpected values
  }
  return os;
}
}  // namespace datatypes