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
 *   regr_avgy.h
 ***********************************************************************/

/**
 * Columnstore interface for for the regr_avgy function
 *
 *
 *    CREATE AGGREGATE FUNCTION regr_avgy returns REAL soname 'libregr_mysql.so';
 *
 */
#pragma once

#include <cstdlib>
#include <string>
#include <vector>
#include <unordered.h>

#include "mcsv1_udaf.h"
#include "calpontsystemcatalog.h"
#include "windowfunctioncolumn.h"

#define EXPORT

namespace mcsv1sdk
{
// Return the regr_avgy value of the dataset

class regr_avgy : public mcsv1_UDAF
{
 public:
  // Defaults OK
  regr_avgy() : mcsv1_UDAF() {};
  ~regr_avgy() override = default;

  ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes) override;

  ReturnCode reset(mcsv1Context* context) override;

  ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn) override;

  ReturnCode subEvaluate(mcsv1Context* context, const UserData* valIn) override;

  ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut) override;

  ReturnCode dropValue(mcsv1Context* context, ColumnDatum* valsDropped) override;

 protected:
};

};  // namespace mcsv1sdk

#undef EXPORT
