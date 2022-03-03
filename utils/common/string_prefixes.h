/*
   Copyright (C) 2021, 2022 MariaDB Corporation

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

/* handling of the conversion of string prefixes to int64_t for quick range checking */

#pragma once

#include <stdlib.h>
#include <stdint.h>

// Encode string prefix into an int64_t, packing as many chars from string as possible
// into the result and respecting the collation provided by charsetNumber.
//
// For one example, for CI Czech collation, encodeStringPrefix("cz") < encodeStringPrefix("CH").
int64_t encodeStringPrefix(const uint8_t* str, size_t len, int charsetNumber);

int64_t encodeStringPrefix_check_null(const uint8_t* str, size_t len, int charsetNumber);
