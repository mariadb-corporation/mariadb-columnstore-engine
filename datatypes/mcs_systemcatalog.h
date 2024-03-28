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
#pragma once

/*
  This file contains simple definitions that can be
  needed in multiple mcs_TYPE.h files.
*/

#include <cstdint>
#include <iostream>

namespace datatypes
{
class SystemCatalog
{
 public:
  /** the set of Calpont column widths
   *
   */
  enum ColWidth
  {
    ONE_BIT,
    ONE_BYTE,
    TWO_BYTE,
    THREE_BYTE,
    FOUR_BYTE,
    FIVE_BYTE,
    SIX_BYTE,
    SEVEN_BYTE,
    EIGHT_BYTE
  };

  /** the set of Calpont column data types
   *
   */
  enum ColDataType
  {
    BIT,                  /*!< BIT type */
    TINYINT,              /*!< TINYINT type */
    CHAR,                 /*!< CHAR type */
    SMALLINT,             /*!< SMALLINT type */
    DECIMAL,              /*!< DECIMAL type */
    MEDINT,               /*!< MEDINT type */
    INT,                  /*!< INT type */
    FLOAT,                /*!< FLOAT type */
    DATE,                 /*!< DATE type */
    BIGINT,               /*!< BIGINT type */
    DOUBLE,               /*!< DOUBLE type */
    DATETIME,             /*!< DATETIME type */
    VARCHAR,              /*!< VARCHAR type */
    VARBINARY,            /*!< VARBINARY type */
    CLOB,                 /*!< CLOB type */
    BLOB,                 /*!< BLOB type */
    UTINYINT,             /*!< Unsigned TINYINT type */
    USMALLINT,            /*!< Unsigned SMALLINT type */
    UDECIMAL,             /*!< Unsigned DECIMAL type */
    UMEDINT,              /*!< Unsigned MEDINT type */
    UINT,                 /*!< Unsigned INT type */
    UFLOAT,               /*!< Unsigned FLOAT type */
    UBIGINT,              /*!< Unsigned BIGINT type */
    UDOUBLE,              /*!< Unsigned DOUBLE type */
    TEXT,                 /*!< TEXT type */
    TIME,                 /*!< TIME type */
    TIMESTAMP,            /*!< TIMESTAMP type */
    NUM_OF_COL_DATA_TYPE, /* NEW TYPES ABOVE HERE */
    LONGDOUBLE,           /* @bug3241, dev and variance calculation only */
    STRINT,               /* @bug3532, string as int for fast comparison */
    UNDEFINED,            /*!< Undefined - used in UDAF API */
  };
};

std::ostream& operator<<(std::ostream& os, SystemCatalog::ColDataType type);

template <SystemCatalog::ColDataType>
struct ColTypeToIntegral
{
  using type = struct
  {
  };
};

template <>
struct ColTypeToIntegral<SystemCatalog::TINYINT>
{
  using type = int8_t;
  constexpr static std::size_t bitWidth = 8;
};

template <>
struct ColTypeToIntegral<SystemCatalog::UTINYINT>
{
  using type = uint8_t;
  constexpr static std::size_t bitWidth = 8;
};

template <>
struct ColTypeToIntegral<SystemCatalog::SMALLINT>
{
  using type = int16_t;
  constexpr static std::size_t bitWidth = 16;
};

template <>
struct ColTypeToIntegral<SystemCatalog::USMALLINT>
{
  using type = uint16_t;
  constexpr static std::size_t bitWidth = 16;
};

template <>
struct ColTypeToIntegral<SystemCatalog::MEDINT>
{
  using type = int32_t;
  constexpr static std::size_t bitWidth = 32;
};

template <>
struct ColTypeToIntegral<SystemCatalog::UMEDINT>
{
  using type = uint32_t;
  constexpr static std::size_t bitWidth = 32;
};

template <>
struct ColTypeToIntegral<SystemCatalog::INT>
{
  using type = int32_t;
  constexpr static std::size_t bitWidth = 32;
};

template <>
struct ColTypeToIntegral<SystemCatalog::UINT>
{
  using type = uint32_t;
  constexpr static std::size_t bitWidth = 32;
};

template <>
struct ColTypeToIntegral<SystemCatalog::BIGINT>
{
  using type = int64_t;
  constexpr static std::size_t bitWidth = 64;
};

template <>
struct ColTypeToIntegral<SystemCatalog::UBIGINT>
{
  using type = uint64_t;
  constexpr static std::size_t bitWidth = 64;
};
}  // namespace datatypes
