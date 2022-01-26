/* Copyright (C) 2019 MariaDB Corporation

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

int is_columnstore_extents_plugin_init(void* p);
int is_columnstore_files_plugin_init(void* p);
int is_columnstore_tables_plugin_init(void* p);
int is_columnstore_columns_plugin_init(void* p);

class InformationSchemaCond
{
  StringBuffer<MAX_FIELD_WIDTH> mBufferTableName;
  StringBuffer<MAX_FIELD_WIDTH> mBufferTableSchema;
  String* mTableName;
  String* mTableSchema;

 public:
  InformationSchemaCond()
   : mBufferTableName(system_charset_info)
   , mBufferTableSchema(system_charset_info)
   , mTableName(nullptr)
   , mTableSchema(nullptr)
  {
  }
  const String* tableName() const
  {
    return mTableName;
  }
  const String* tableSchema() const
  {
    return mTableSchema;
  }
  static bool eqName(const String& a, const std::string& b)
  {
    return a.length() == b.length() && !memcmp(a.ptr(), b.data(), a.length());
  }
  static bool eqName(const String* a, const std::string& b)
  {
    return !a || eqName(*a, b);
  }
  bool match(const std::string& schema, const std::string& table) const
  {
    return eqName(mTableName, table) && eqName(mTableSchema, schema);
  }

  void getCondItem(Item_field* item_field, Item* item_const)
  {
    if (strcasecmp(item_field->field_name.str, "table_name") == 0)
    {
      mTableName = item_const->val_str(&mBufferTableName);
    }
    else if (strcasecmp(item_field->field_name.str, "table_schema") == 0)
    {
      mTableSchema = item_const->val_str(&mBufferTableSchema);
    }
  }

  void getCondItemBoolFunc2(Item_bool_func2* func)
  {
    Item_field* field = dynamic_cast<Item_field*>(func->arguments()[0]->real_item());
    if (field && func->arguments()[1]->const_item())
      getCondItem(field, func->arguments()[1]);
  }

  void getCondItems(COND* cond)
  {
    if (Item_bool_func2* func = dynamic_cast<Item_bool_func2*>(cond))
    {
      getCondItemBoolFunc2(func);
    }
    else if (Item_cond_and* subcond = dynamic_cast<Item_cond_and*>(cond))
    {
      List_iterator<Item> li(*(subcond)->argument_list());
      for (Item* item = li++; item; item = li++)
        getCondItems(item);
    }
  }
};
