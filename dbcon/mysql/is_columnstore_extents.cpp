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

#include "dbrm.h"
#include "objectidmanager.h"


// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD *thd, TABLE *table);

ST_FIELD_INFO is_columnstore_extents_fields[] =
{
    {"OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                     // 0
    {"OBJECT_TYPE", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},                 // 1
    {"LOGICAL_BLOCK_START", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},       // 2
    {"LOGICAL_BLOCK_END", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},         // 3
    {"MIN_VALUE", 19, MYSQL_TYPE_LONGLONG, 0, MY_I_S_MAYBE_NULL, 0, 0}, // 4
    {"MAX_VALUE", 19, MYSQL_TYPE_LONGLONG, 0, MY_I_S_MAYBE_NULL, 0, 0}, // 5
    {"WIDTH", 1, MYSQL_TYPE_TINY, 0, 0, 0, 0},                          // 6
    {"DBROOT", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                        // 7
    {"PARTITION_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                  // 8
    {"SEGMENT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                    // 9
    {"BLOCK_OFFSET", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                  // 10
    {"MAX_BLOCKS", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},                    // 11
    {"HIGH_WATER_MARK", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},               // 12
    {"STATE", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},                       // 13
    {"STATUS", 64, MYSQL_TYPE_STRING, 0, 0, 0, 0},                      // 14
    {"DATA_SIZE", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},                 // 15
    {0, 0, MYSQL_TYPE_NULL, 0, 0, 0, 0}
};

static int generate_result(BRM::OID_t oid, BRM::DBRM *emp, TABLE *table, THD *thd)
{
    CHARSET_INFO *cs = system_charset_info;
    std::vector<struct BRM::EMEntry> entries;
   	std::vector<struct BRM::EMEntry>::iterator iter;
	std::vector<struct BRM::EMEntry>::iterator end;

    emp->getExtents(oid, entries, false, false, true);
    if (entries.size() == 0)
        return 0;

    iter = entries.begin();
    end = entries.end();
    while (iter != end)
    {
        table->field[0]->store(oid);
        if (iter->colWid > 0)
        {
            table->field[1]->store("Column", strlen("Column"), cs);
            if (iter->partition.cprange.lo_val == std::numeric_limits<int64_t>::max() ||
                iter->partition.cprange.lo_val <= (std::numeric_limits<int64_t>::min() + 2))
            {
                table->field[4]->set_null();
            }
            else
            {
                table->field[4]->set_notnull();
                table->field[4]->store(iter->partition.cprange.lo_val);
            }
            if (iter->partition.cprange.hi_val == std::numeric_limits<int64_t>::max() ||
                iter->partition.cprange.hi_val <= (std::numeric_limits<int64_t>::min() + 2))
            {
                table->field[5]->set_null();
            }
            else
            {
                table->field[5]->set_notnull();
                table->field[5]->store(iter->partition.cprange.hi_val);
            }
            table->field[6]->store(iter->colWid);

        }
        else
        {
            table->field[1]->store("Dictionary", strlen("Dictionary"), cs);
            table->field[4]->set_null();
            table->field[5]->set_null();
            table->field[6]->store(8192);
        }
        table->field[2]->store(iter->range.start);
        table->field[3]->store(iter->range.start + (iter->range.size * 1024) - 1);

        table->field[7]->store(iter->dbRoot);
        table->field[8]->store(iter->partitionNum);
        table->field[9]->store(iter->segmentNum);
        table->field[10]->store(iter->blockOffset);
        table->field[11]->store(iter->range.size * 1024);
        table->field[12]->store(iter->HWM);

        switch (iter->partition.cprange.isValid)
        {
            case 0:
                table->field[13]->store("Invalid", strlen("Invalid"), cs);
                break;
            case 1:
                table->field[13]->store("Updating", strlen("Updating"), cs);
                break;
            case 2:
                table->field[13]->store("Valid", strlen("Valid"), cs);
                break;
            default:
                table->field[13]->store("Unknown", strlen("Unknown"), cs);
                break;
        }
        switch (iter->status)
        {
            case BRM::EXTENTAVAILABLE:
                table->field[14]->store("Available", strlen("Available"), cs);
                break;
            case BRM::EXTENTUNAVAILABLE:
                table->field[14]->store("Unavailable", strlen("Unavailable"), cs);
                break;
            case BRM::EXTENTOUTOFSERVICE:
                table->field[14]->store("Out of service", strlen("Out of service"), cs);
                break;
            default:
                table->field[14]->store("Unknown", strlen("Unknown"), cs);
        }
        // MCOL-1016: on multiple segments HWM is set to 0 on the lower
        // segments, we don't want these to show as 8KB. The down side is
        // if the column has less than 1 block it will show as 0 bytes.
        // We have no lookahead without it getting messy so this is the
        // best compromise.
        if (iter->HWM == 0)
        {
            table->field[15]->store(0);
        }
        else
        {
            table->field[15]->store((iter->HWM + 1) * 8192);
        }

        if (schema_table_store_record(thd, table))
        {
            delete emp;
            return 1;
        }

        iter++;

    }
    return 0;
}

static int is_columnstore_extents_fill(THD *thd, TABLE_LIST *tables, COND *cond)
{
    BRM::OID_t cond_oid = 0;
    TABLE *table = tables->table;

    BRM::DBRM *emp = new BRM::DBRM();
    if (!emp || !emp->isDBRMReady())
    {
        return 1;
    }

    if (cond && cond->type() == Item::FUNC_ITEM)
    {
        Item_func* fitem = (Item_func*) cond;
        if ((fitem->functype() == Item_func::EQ_FUNC) && (fitem->argument_count() == 2))
        {
            if(fitem->arguments()[0]->real_item()->type() == Item::FIELD_ITEM &&
               fitem->arguments()[1]->const_item())
            {
                // WHERE object_id = value
                Item_field *item_field = (Item_field*) fitem->arguments()[0]->real_item();
                if (strcasecmp(item_field->field_name, "object_id") == 0)
                {
                    cond_oid = fitem->arguments()[1]->val_int();
                    return generate_result(cond_oid, emp, table, thd);
                }
            }
            else if (fitem->arguments()[1]->real_item()->type() == Item::FIELD_ITEM &&
                fitem->arguments()[0]->const_item())
            {
                // WHERE value = object_id
                Item_field *item_field = (Item_field*) fitem->arguments()[1]->real_item();
                if (strcasecmp(item_field->field_name, "object_id") == 0)
                {
                    cond_oid = fitem->arguments()[0]->val_int();
                    return generate_result(cond_oid, emp, table, thd);
                }
            }
        }
        else if (fitem->functype() == Item_func::IN_FUNC)
        {
            // WHERE object_id in (value1, value2)
            Item_field *item_field = (Item_field*) fitem->arguments()[0]->real_item();
            if (strcasecmp(item_field->field_name, "object_id") == 0)
            {
                for (unsigned int i=1; i < fitem->argument_count(); i++)
                {
                    cond_oid = fitem->arguments()[i]->val_int();
                    int result = generate_result(cond_oid, emp, table, thd);
                    if (result)
                       return 1;
                }
            }
        }
        else if (fitem->functype() == Item_func::UNKNOWN_FUNC &&
                 strcasecmp(fitem->func_name(), "find_in_set") == 0)
        {
            // WHERE FIND_IN_SET(object_id, values)
            String *tmp_var = fitem->arguments()[1]->val_str();
            std::stringstream ss(tmp_var->ptr());
            while (ss >> cond_oid)
            {
                int ret = generate_result(cond_oid, emp, table, thd);
                if (ret)
                    return 1;
                if (ss.peek() == ',')
                    ss.ignore();
            }
        }
    }

    execplan::ObjectIDManager oidm;
    BRM::OID_t MaxOID = oidm.size();

    for(BRM::OID_t oid = 3000; oid <= MaxOID; oid++)
    {
        int result = generate_result(oid, emp, table, thd);
        if (result)
            return 1;
    }
    delete emp;
    return 0;
}

static int is_columnstore_extents_plugin_init(void *p)
{
    ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
    schema->fields_info = is_columnstore_extents_fields;
    schema->fill_table = is_columnstore_extents_fill;
    return 0;
}

static struct st_mysql_information_schema is_columnstore_extents_plugin_version =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

maria_declare_plugin(is_columnstore_extents_plugin)
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_extents_plugin_version,
    "COLUMNSTORE_EXTENTS",
    "MariaDB Corporaton",
    "An information schema plugin to list ColumnStore extents",
    PLUGIN_LICENSE_GPL,
    is_columnstore_extents_plugin_init,
    //is_columnstore_extents_plugin_deinit,
    NULL,
    0x0100,
    NULL,
    NULL,
    "1.0",
    MariaDB_PLUGIN_MATURITY_STABLE
}
maria_declare_plugin_end;


