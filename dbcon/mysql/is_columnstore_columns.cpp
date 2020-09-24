/* c-basic-offset: 4; tab-width: 4; indent-tabs-mode: nil
 * vi: set shiftwidth=4 tabstop=4 expandtab:
 *  :indentSize=4:tabSize=4:noTabs=true:
 *
 * Copyright (C) 2016 MariaDB Corporation
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
#include <limits>

#include <boost/shared_ptr.hpp>
#include "calpontsystemcatalog.h"
#include "dataconvert.h"
#include "exceptclasses.h"
#include "is_columnstore.h"
using namespace logging;


// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD* thd, TABLE* table);

ST_FIELD_INFO is_columnstore_columns_fields[] =
{
    Show::Column("TABLE_SCHEMA", Show::Varchar(64), NOT_NULL),
    Show::Column("TABLE_NAME", Show::Varchar(64), NOT_NULL),
    Show::Column("COLUMN_NAME", Show::Varchar(64), NOT_NULL),
    Show::Column("OBJECT_ID", Show::ULong(0), NOT_NULL),
    Show::Column("DICTIONARY_OBJECT_ID", Show::ULong(0), NULLABLE),
    Show::Column("LIST_OBJECT_ID", Show::ULong(0), NULLABLE),
    Show::Column("TREE_OBJECT_ID", Show::ULong(0), NULLABLE),
    Show::Column("DATA_TYPE", Show::Varchar(64), NOT_NULL),
    Show::Column("COLUMN_LENGTH", Show::ULong(0), NOT_NULL),
    Show::Column("COLUMN_POSITION", Show::ULong(0), NOT_NULL),
    Show::Column("COLUMN_DEFAULT", Show::Blob(255), NULLABLE),
    Show::Column("IS_NULLABLE", Show::STiny(0), NOT_NULL),
    Show::Column("NUMERIC_PRECISION", Show::ULong(0), NOT_NULL),
    Show::Column("NUMERIC_SCALE", Show::ULong(0), NOT_NULL),
    Show::Column("IS_AUTOINCREMENT", Show::STiny(0), NOT_NULL),
    Show::Column("COMPRESSION_TYPE", Show::Varchar(64), NOT_NULL),
    Show::CEnd()
};

static void get_cond_item(Item_func* item, String** table, String** db)
{
    char tmp_char[MAX_FIELD_WIDTH];
    Item_field* item_field = (Item_field*) item->arguments()[0]->real_item();

    if (strcasecmp(item_field->field_name.str, "table_name") == 0)
    {
        String str_buf(tmp_char, sizeof(tmp_char), system_charset_info);
        *table = item->arguments()[1]->val_str(&str_buf);
        return;
    }
    else if (strcasecmp(item_field->field_name.str, "table_schema") == 0)
    {
        String str_buf(tmp_char, sizeof(tmp_char), system_charset_info);
        *db = item->arguments()[1]->val_str(&str_buf);
        return;
    }
}

static void get_cond_items(COND* cond, String** table, String** db)
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
        Item* item;

        while ((item = li++))
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

static int is_columnstore_columns_fill(THD* thd, TABLE_LIST* tables, COND* cond)
{
    CHARSET_INFO* cs = system_charset_info;
    TABLE* table = tables->table;
    String* table_name = NULL;
    String* db_name = NULL;

    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(execplan::CalpontSystemCatalog::idb_tid2sid(thd->thread_id));

    const std::vector< std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> > catalog_tables
        = systemCatalogPtr->getTables();

    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::FE);

    if (cond)
    {
        get_cond_items(cond, &table_name, &db_name);
    }

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

        execplan::CalpontSystemCatalog::RIDList column_rid_list;

        // Note a table may get dropped as you iterate over the list of tables.
        // So simply ignore the dropped table.
        try
        {
            column_rid_list = systemCatalogPtr->columnRIDs((*it).second, true, lower_case_table_names);
        }
        catch (IDBExcept& ex)
        {
            if (ex.errorCode() == ERR_TABLE_NOT_IN_CATALOG)
            {
                continue;
            }
            else
            {
                return 1;
            }
        }

        for (size_t col_num = 0; col_num < column_rid_list.size(); col_num++)
        {
            execplan::CalpontSystemCatalog::TableColName tcn = systemCatalogPtr->colName(column_rid_list[col_num].objnum);
            execplan::CalpontSystemCatalog::ColType ct = systemCatalogPtr->colType(column_rid_list[col_num].objnum);

            table->field[0]->store(tcn.schema.c_str(), tcn.schema.length(), cs);
            table->field[1]->store(tcn.table.c_str(), tcn.table.length(), cs);
            table->field[2]->store(tcn.column.c_str(), tcn.column.length(), cs);
            table->field[3]->store(column_rid_list[col_num].objnum);

            if (ct.ddn.dictOID == std::numeric_limits<int32_t>::min())
            {
                table->field[4]->set_null();
            }
            else
            {
                table->field[4]->set_notnull();
                table->field[4]->store(ct.ddn.dictOID);
            }

            if (ct.ddn.listOID == std::numeric_limits<int32_t>::min())
            {
                table->field[5]->set_null();
            }
            else
            {
                table->field[5]->set_notnull();
                table->field[5]->store(ct.ddn.listOID);
            }

            if (ct.ddn.treeOID == std::numeric_limits<int32_t>::min())
            {
                table->field[6]->set_null();
            }
            else
            {
                table->field[6]->set_notnull();
                table->field[6]->store(ct.ddn.treeOID);
            }

            std::string data_type = execplan::colDataTypeToString(ct.colDataType);
            table->field[7]->store(data_type.c_str(), data_type.length(), cs);
            table->field[8]->store(ct.colWidth);
            table->field[9]->store(ct.colPosition);

            if (ct.defaultValue.empty())
            {
                table->field[10]->set_null();
            }
            else
            {
                table->field[10]->set_notnull();
                table->field[10]->store(ct.defaultValue.c_str(), ct.defaultValue.length(), cs);
            }

            table->field[11]->store(ct.autoincrement);
            table->field[12]->store(ct.precision);
            table->field[13]->store(ct.scale);

            if (ct.constraintType != execplan::CalpontSystemCatalog::NOTNULL_CONSTRAINT)
            {
                table->field[14]->store(true);
            }
            else
            {
                table->field[14]->store(false);
            }

            std::string compression_type;

            switch (ct.compressionType)
            {
                case 0:
                    compression_type = "None";
                    break;

                case 2:
                    compression_type = "Snappy";
                    break;

                default:
                    compression_type = "Unknown";
                    break;
            }

            table->field[15]->store(compression_type.c_str(), compression_type.length(), cs);

            if (schema_table_store_record(thd, table))
                return 1;
        }
    }

    return 0;
}

int is_columnstore_columns_plugin_init(void* p)
{
    ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;
    schema->fields_info = is_columnstore_columns_fields;
    schema->fill_table = is_columnstore_columns_fill;
    return 0;
}

