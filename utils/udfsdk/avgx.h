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
 *   avgx.h
 ***********************************************************************/

/**
 * Columnstore interface for for the avgx function
 *
 *
 *    CREATE AGGREGATE FUNCTION avgx returns REAL soname
 *    'libudf_mysql.so';
 *
 */
#ifndef HEADER_avgx
#define HEADER_avgx

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
// Override mcsv1_UDAF to build your User Defined Aggregate (UDAF) and/or
// User Defined Analytic Function (UDAnF).
// These will be singleton classes, so don't put any instance
// specific data in here. All instance data is stored in mcsv1Context
// passed to each user function and retrieved by the getUserData() method.
//
// Each API function returns a ReturnCode. If ERROR is returned at any time,
// the query is aborted, getInterrupted() will begin to return true and the
// message set in config->setErrorMessage() is returned to MariaDB.

// Return the avgx value of the dataset

class avgx : public mcsv1_UDAF
{
 public:
  // Defaults OK
  avgx() : mcsv1_UDAF(){};
  virtual ~avgx(){};

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

#endif  // HEADER_.h
