/* Copyright (C) 2025 MariaDB Corporation

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


#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include "idb_mysql.h"

#include "constantcolumn.h"
#include "ha_mcs_impl_if.h"

namespace cal_impl_if
{

class ValStrStdString : public std::string
{
  bool mIsNull;

  public:
  ValStrStdString(Item* item)
  {
    String val, *str = item->val_str(&val);
    mIsNull = (str == nullptr);
    DBUG_ASSERT(mIsNull == item->null_value);
    if (!mIsNull)
      assign(str->ptr(), str->length());
  }
  bool isNull() const
  {
    return mIsNull;
  }
};
  
bool nonConstFunc(Item_func* ifp);
// Note: This function might be unused currently but is kept for future compatibility.
execplan::ConstantColumn* buildConstantColumnMaybeNullFromValStr(const Item* item, const ValStrStdString& valStr,
  cal_impl_if::gp_walk_info& gwi);
execplan::ConstantColumn* newConstantColumnNotNullUsingValNativeNoTz(Item* item, gp_walk_info& gwi);
bool isSupportedAggregateWithOneConstArg(const Item_sum* item, Item** orig_args);
execplan::ConstantColumn* buildConstantColumnNotNullUsingValNative(Item* item, gp_walk_info& gwi);
execplan::ConstantColumn* buildConstantColumnMaybeNullUsingValStr(Item* item, gp_walk_info& gwi);
}
