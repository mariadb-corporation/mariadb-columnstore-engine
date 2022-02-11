/* Copyright (C) 2017 MariaDB Corporation

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

/***********************************************************************
 *   $Id$
 *
 *   regr_count.h
 ***********************************************************************/

/**
 * Columnstore interface for for the regr_count function
 *
 *
 *    CREATE AGGREGATE FUNCTION regr_count returns INTEGER soname 'libregr_mysql.so';
 *
 */
#ifndef HEADER_regr_count
#define HEADER_regr_count

#include <cstdlib>
#include <string>
#include <vector>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include "mcsv1_udaf.h"
#include "calpontsystemcatalog.h"
#include "windowfunctioncolumn.h"

#if defined(_MSC_VER) && defined(xxxRGNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace mcsv1sdk
{
// Return the regr_count value of the dataset

class regr_count : public mcsv1_UDAF
{
 public:
  // Defaults OK
  regr_count() : mcsv1_UDAF(){};
  virtual ~regr_count(){};

  virtual ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

  virtual ReturnCode reset(mcsv1Context* context);

  virtual ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn);

  virtual ReturnCode subEvaluate(mcsv1Context* context, const UserData* valIn);

  virtual ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);

  virtual ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped);

 protected:
};

};  // namespace mcsv1sdk

#undef EXPORT

#endif  // HEADER_regr_count.h
