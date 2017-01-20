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
#include <limits>

#include <boost/shared_ptr.hpp>
#include "calpontsystemcatalog.h"
#include "dataconvert.h"


// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD *thd, TABLE *table);

ST_FIELD_INFO is_columnstore_columns_fields[] =
{
    {"TABLE_SCHEMA", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"TABLE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COLUMN_NAME", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"DICTIONARY_OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {"LIST_OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {"TREE_OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {"DATA_TYPE", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COLUMN_LENGTH", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"COLUMN_POSITION", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"COLUMN_DEFAULT", 0, MYSQL_TYPE_LONG_BLOB, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {"IS_NULLABLE", 1, MYSQL_TYPE_TINY, 0, 0, 0, 0},
    {"NUMERIC_PRECISION", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"NUMERIC_SCALE", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"IS_AUTOINCREMENT", 1, MYSQL_TYPE_TINY, 0, 0, 0, 0},
    {"COMPRESSION_TYPE", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_NULL, 0, 0, 0, 0}

};

static int is_columnstore_columns_fill(THD *thd, TABLE_LIST *tables, COND *cond)
{
    CHARSET_INFO *cs = system_charset_info;
    TABLE *table = tables->table;

    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(execplan::CalpontSystemCatalog::idb_tid2sid(thd->thread_id));

    const std::vector< std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> > catalog_tables
        = systemCatalogPtr->getTables();

    systemCatalogPtr->identity(execplan::CalpontSystemCatalog::FE);

    for (std::vector<std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> >::const_iterator it = catalog_tables.begin();
         it != catalog_tables.end(); ++it)
    {
        execplan::CalpontSystemCatalog::RIDList column_rid_list = systemCatalogPtr->columnRIDs((*it).second, true);
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

static int is_columnstore_columns_plugin_init(void *p)
{
    ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
    schema->fields_info = is_columnstore_columns_fields;
    schema->fill_table = is_columnstore_columns_fill;
    return 0;
}

static struct st_mysql_information_schema is_columnstore_columns_plugin_version =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

maria_declare_plugin(is_columnstore_columns_plugin)
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_columns_plugin_version,
    "COLUMNSTORE_COLUMNS",
    "MariaDB Corporaton",
    "An information schema plugin to list ColumnStore columns",
    PLUGIN_LICENSE_GPL,
    is_columnstore_columns_plugin_init,
    //is_columnstore_tables_plugin_deinit,
    NULL,
    0x0100,
    NULL,
    NULL,
    "1.0",
    MariaDB_PLUGIN_MATURITY_STABLE
}
maria_declare_plugin_end;


