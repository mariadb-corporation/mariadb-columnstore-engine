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

#include "ha_mcs_execplan_helpers.h"
#include "constantcolumn.h"

namespace cal_impl_if
{
bool nonConstFunc(Item_func* ifp)
{
  if (strcasecmp(ifp->func_name(), "rand") == 0 || strcasecmp(ifp->func_name(), "sysdate") == 0 ||
      strcasecmp(ifp->func_name(), "idblocalpm") == 0)
    return true;

  for (uint32_t i = 0; i < ifp->argument_count(); i++)
  {
    if (ifp->arguments()[i]->type() == Item::FUNC_ITEM && nonConstFunc(((Item_func*)ifp->arguments()[i])))
      return true;
    if (ifp->arguments()[i]->type() == Item::CACHE_ITEM)
      return true;
  }

  return false;
}

/*
  Create a ConstantColumn according to cmp_type().
  But do not set the time zone yet.

  Handles NULL and NOT NULL values.

  Uses a simplified logic regarding to data types:
    always extracts the value through val_str().

  Should probably be joined with the previous function, to have
  a single function which can at the same time:
  a. handle both NULL and NOT NULL values
  b. extract values using a native val_xxx() method,
     to avoid possible negative effects mentioned in the comments
     to newConstantColumnNotNullUsingValNativeNoTz().
*/
execplan::ConstantColumn* newConstantColumnMaybeNullFromValStrNoTz(const Item* item,
                                                                          const ValStrStdString& valStr,
                                                                          gp_walk_info& gwi)
{
  if (valStr.isNull())
    return new execplan::ConstantColumnNull();

  switch (item->result_type())
  {
    case STRING_RESULT: return new execplan::ConstantColumnString(valStr);
    case DECIMAL_RESULT: return buildDecimalColumn(item, valStr, gwi);
    case TIME_RESULT:
    case INT_RESULT:
    case REAL_RESULT:
    case ROW_RESULT: return new execplan::ConstantColumnNum(colType_MysqlToIDB(item), valStr);
  }
  return nullptr;
}

// Create a ConstantColumn from a previously evaluated val_str() result,
// Supports both NULL and NOT NULL values.
// Sets the time zone according to gwi.
execplan::ConstantColumn* buildConstantColumnMaybeNullFromValStr(const Item* item, const ValStrStdString& valStr,
                                                              gp_walk_info& gwi)
{
  execplan::ConstantColumn* rc = newConstantColumnMaybeNullFromValStrNoTz(item, valStr, gwi);
  if (rc)
    rc->timeZone(gwi.timeZone);
  return rc;
}

// Create a ConstantColumn by calling val_str().
// Supports both NULL and NOT NULL values.
// Sets the time zone according to gwi.
// Special handling for user variables that store numeric values as binary.

execplan::ConstantColumn* buildConstantColumnMaybeNullUsingValStr(Item* item, gp_walk_info& gwi)
{
  // User variables with numeric values (e.g., SET @v = 0x15) are stored as binary strings.
  // val_str() returns raw binary data, which breaks atoll() in ConstantColumn constructor.
  // For binary charset, interpret the string as a little-endian integer.
  if (item->type() == Item::FUNC_ITEM)
  {
    Item_func* ifp = static_cast<Item_func*>(item);
    if (ifp->functype() == Item_func::GUSERVAR_FUNC)
    {
      Item_func_get_user_var* udf = static_cast<Item_func_get_user_var*>(ifp);
      String buf;
      String* str = udf->val_str(&buf);

      if (udf->null_value)
        return new execplan::ConstantColumnNull();

      // Check if this is a binary string (numeric value stored as binary)
      if (str && str->length() > 0 && str->length() <= 8 &&
          str->charset() == &my_charset_bin)
      {
        // Interpret binary string as little-endian integer
        uint64_t val = 0;
        const unsigned char* p = (const unsigned char*)str->ptr();
        for (size_t i = 0; i < str->length(); i++)
          val |= ((uint64_t)p[i]) << (i * 8);

        execplan::ConstantColumn* cc = new execplan::ConstantColumn((int64_t)val);
        cc->timeZone(gwi.timeZone);
        return cc;
      }
    }
  }

  return buildConstantColumnMaybeNullFromValStr(item, ValStrStdString(item), gwi);
}

// Create a ConstantColumn for a NOT NULL expression.
// Sets the time zone according to gwi.
execplan::ConstantColumn* buildConstantColumnNotNullUsingValNative(Item* item, gp_walk_info& gwi)
{
  execplan::ConstantColumn* rc = newConstantColumnNotNullUsingValNativeNoTz(item, gwi);
  if (rc)
    rc->timeZone(gwi.timeZone);
  return rc;
}

/*
  Create a ConstantColumn according to cmp_type().
  But do not set the time zone yet.

  Handles NOT NULL values.

  Three ways of value extraction are used depending on the data type:
  1. Using a native val_xxx().
  2. Using val_str() with further convertion to the native representation.
  3. Using both val_str() and a native val_xxx().

  We should eventually get rid of N2 and N3 and use N1 for all data types:
  - N2 contains a redundant code for str->native conversion.
    It should be replaced to an existing code (a Type_handler method call?).
  - N3 performs double evalation of the value, which may cause
    various negative effects (double side effects or double warnings).
*/
execplan::ConstantColumn* newConstantColumnNotNullUsingValNativeNoTz(Item* item, gp_walk_info& gwi)
{
  DBUG_ASSERT(item->const_item());

  switch (item->cmp_type())
  {
    case INT_RESULT:
    {
      if (item->unsigned_flag)
        return new execplan::ConstantColumnUInt((uint64_t)item->val_uint(), (int8_t)item->decimal_scale(),
                                      (uint8_t)item->decimal_precision());
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new execplan::ConstantColumnSInt(colType_MysqlToIDB(item), str, (int64_t)item->val_int());
    }
    case STRING_RESULT:
    {
      // Special handling for 0xHHHH literals
      if (item->type_handler() == &type_handler_hex_hybrid)
        return new execplan::ConstantColumn((int64_t)item->val_int(), execplan::ConstantColumn::NUM);
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new execplan::ConstantColumnString(str);
    }
    case REAL_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new execplan::ConstantColumnReal(colType_MysqlToIDB(item), str, item->val_real());
    }
    case DECIMAL_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return buildDecimalColumn(item, str, gwi);
    }
    case TIME_RESULT:
    {
      ValStrStdString str(item);
      DBUG_ASSERT(!str.isNull());
      return new execplan::ConstantColumnTemporal(colType_MysqlToIDB(item), str);
    }
    default:
    {
      gwi.fatalParseError = true;
      gwi.parseErrorText = "Unknown item type";
      break;
    }
  }

  return nullptr;
}

bool isSupportedAggregateWithOneConstArg(const Item_sum* item, Item** orig_args)
{
  if (item->argument_count() != 1 || !orig_args[0]->const_item())
    return false;
  switch (orig_args[0]->cmp_type())
  {
    case INT_RESULT:
    case STRING_RESULT:
    case REAL_RESULT:
    case DECIMAL_RESULT: return true;
    default: break;
  }
  return false;
}

}  // namespace cal_impl_if