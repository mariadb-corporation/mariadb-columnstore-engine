/* c-basic-offset: 4; tab-width: 4; indent-tabs-mode: nil
 * vi: set shiftwidth=4 tabstop=4 expandtab:
 *  :indentSize=4:tabSize=4:noTabs=true:
 *
 * Copyright (C) 2016 MariaDB Corporaton
 *
 * This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

#include "idb_mysql.h"
#include <vector>

#include <boost/shared_ptr.hpp>
#include "calpontsystemcatalog.h"
#include "dataconvert.h"


// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD *thd, TABLE *table);

ST_FIELD_INFO is_columnstore_tables_fields[] =
{
    {"TABLE_SCHEMA", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"TABLE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"CREATION_DATE", 0, MYSQL_TYPE_DATE, 0, 0, 0, 0},
    {"COLUMN_COUNT", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"AUTOINCREMENT", 11, MYSQL_TYPE_LONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {0, 0, MYSQL_TYPE_NULL, 0, 0, 0, 0}
};

static void get_cond_item(Item_func *item, String **table, String **db)
{
    char tmp_char[MAX_FIELD_WIDTH];
    Item_field *item_field = (Item_field*) item->arguments()[0]->real_item();
    if (strcasecmp(item_field->field_name, "table_name") == 0)
    {
        String str_buf(tmp_char, sizeof(tmp_char), system_charset_info);
        *table = item->arguments()[1]->val_str(&str_buf);
        return;
    }
    else if (strcasecmp(item_field->field_name, "table_schema") == 0)
    {
        String str_buf(tmp_char, sizeof(tmp_char), system_charset_info);
        *db = item->arguments()[1]->val_str(&str_buf);
        return;
    }
}

static void get_cond_items(COND *cond, String **table, String **db)
{
    if (cond->type() == Item::FUNC_ITEM)
    {
        Item_func* fitem = (Item_func*) cond;
        if (fitem->arguments()[0]->real_item()->type() == Item::FIELD_ITEM &&
            fitem->arguments()[1]->const_item())
        {
            get_cond_item(fitem, table, db);
        }
    }
    else if ((cond->type() == Item::COND_ITEM) && (((Item_cond*) cond)->functype() == Item_func::COND_AND_FUNC))
    {
        List_iterator<Item> li(*((Item_cond*) cond)->argument_list());
        Item *item;
        while ((item= li++))
        {
            if (item->type() == Item::FUNC_ITEM)
            {
                get_cond_item((Item_func*)item, table, db);
            }
            else
            {
                get_cond_items(item, table, db);
            }
        }
    }
}

static int is_columnstore_tables_fill(THD *thd, TABLE_LIST *tables, COND *cond)
{
    CHARSET_INFO *cs = system_charset_info;
    TABLE *table = tables->table;
    String *table_name = NULL;
    String *db_name = NULL;

    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(execplan::CalpontSystemCatalog::idb_tid2sid(thd->thread_id));

    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::FE);

    if (cond)
    {
        get_cond_items(cond, &table_name, &db_name);
    }

    const std::vector< std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> > catalog_tables
        = systemCatalogPtr->getTables();

    for (std::vector<std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> >::const_iterator it = catalog_tables.begin();
         it != catalog_tables.end(); ++it)
    {
        if (db_name)
        {
            if ((*it).second.schema.compare(db_name->ptr()) != 0)
            {
                continue;
            }
        }
        if (table_name)
        {
            if ((*it).second.table.compare(table_name->ptr()) != 0)
            {
                continue;
            }
        }

        execplan::CalpontSystemCatalog::TableInfo tb_info = systemCatalogPtr->tableInfo((*it).second);
        std::string create_date = dataconvert::DataConvert::dateToString((*it).second.create_date);
        table->field[0]->store((*it).second.schema.c_str(), (*it).second.schema.length(), cs);
        table->field[1]->store((*it).second.table.c_str(), (*it).second.table.length(), cs);
        table->field[2]->store((*it).first);
        table->field[3]->store(create_date.c_str(), create_date.length(), cs);
        table->field[4]->store(tb_info.numOfCols);
        if (tb_info.tablewithautoincr)
        {
            table->field[5]->set_notnull();
            table->field[5]->store(systemCatalogPtr->nextAutoIncrValue((*it).second));
        }
        else
        {
            table->field[5]->set_null();
        }
        table->field[5]->store(tb_info.tablewithautoincr);
        if (schema_table_store_record(thd, table))
            return 1;
    }

    return 0;
}

static int is_columnstore_tables_plugin_init(void *p)
{
    ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
    schema->fields_info = is_columnstore_tables_fields;
    schema->fill_table = is_columnstore_tables_fill;
    return 0;
}

static struct st_mysql_information_schema is_columnstore_tables_plugin_version =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

maria_declare_plugin(is_columnstore_tables_plugin)
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_tables_plugin_version,
    "COLUMNSTORE_TABLES",
    "MariaDB Corporaton",
    "An information schema plugin to list ColumnStore tables",
    PLUGIN_LICENSE_GPL,
    is_columnstore_tables_plugin_init,
    //is_columnstore_tables_plugin_deinit,
    NULL,
    0x0100,
    NULL,
    NULL,
    "1.0",
    MariaDB_PLUGIN_MATURITY_STABLE
}
maria_declare_plugin_end;


