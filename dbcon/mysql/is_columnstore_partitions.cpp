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

#define PREFER_MY_CONFIG_H
#include "idb_mysql.h"
#include <vector>

#include <boost/shared_ptr.hpp>
#include "calpontsystemcatalog.h"
#include "dataconvert.h"
#include "is_columnstore.h"

#include "idberrorinfo.h"
#include "exceptclasses.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "dbrm.h"
#include "brmtypes.h"
using namespace BRM;

// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD* thd, TABLE* table);

ST_FIELD_INFO is_columnstore_partitions_fields[] = {
    Show::Column("TABLE_SCHEMA", Show::Varchar(64), NOT_NULL),
    Show::Column("TABLE_NAME", Show::Varchar(64), NOT_NULL),
    Show::Column("COLUMN_NAME", Show::Varchar(64), NOT_NULL),
    Show::Column("PARTITION_ID", Show::Varchar(64), NOT_NULL),
    Show::CEnd()};

static int is_columnstore_partitions_fill(THD* thd, TABLE_LIST* tables, COND* cond)
{
  BRM::DBRM::refreshShmWithLock();
  DBRM em;
  CHARSET_INFO* cs = system_charset_info;
  TABLE* table = tables->table;
  InformationSchemaCond isCond;

  execplan::CalpontSystemCatalog csc;
  csc.identity(execplan::CalpontSystemCatalog::FE);

  if (cond)
  {
    isCond.getCondItems(cond);
  }

  const std::vector<
      std::pair<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::TableName> >
      catalog_tables = csc.getTables();

  std::set<LogicalPartition> partitionSet;

  for (std::vector<std::pair<execplan::CalpontSystemCatalog::OID,
                             execplan::CalpontSystemCatalog::TableName> >::const_iterator it =
           catalog_tables.begin();
       it != catalog_tables.end(); ++it)
  {
    if (!isCond.match((*it).second.schema, (*it).second.table))
      continue;

    execplan::CalpontSystemCatalog::RIDList column_rid_list;

    // Note a table may get dropped as you iterate over the list of tables.
    // So simply ignore the dropped table.
    try
    {
      column_rid_list = csc.columnRIDs((*it).second, true, lower_case_table_names);
    }
    catch (logging::IDBExcept& ex)
    {
      if (ex.errorCode() == logging::ERR_TABLE_NOT_IN_CATALOG)
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
      OID_t oid = column_rid_list[col_num].objnum;

      execplan::CalpontSystemCatalog::TableColName tcn = csc.colName(oid);

      if (!isCond.matchColumn(tcn.column))
      {
        continue;
      }

      execplan::CalpontSystemCatalog::ColType ct = csc.colType(oid);

      std::vector<struct EMEntry> entries;

      if (em.getExtents(oid, entries, false, false, true))
      {
        continue;
      }
      for (const auto& entry : entries)
      {
        LogicalPartition logicalPartNum;
        logicalPartNum.dbroot = entry.dbRoot;
        logicalPartNum.pp = entry.partitionNum;
        logicalPartNum.seg = entry.segmentNum;
        if (partitionSet.count(logicalPartNum) > 0)
        {
          // added already.
          continue;
        }
        partitionSet.insert(logicalPartNum);

        table->field[0]->store(tcn.schema.c_str(), tcn.schema.length(), cs);
        table->field[1]->store(tcn.table.c_str(), tcn.table.length(), cs);
        table->field[2]->store(tcn.column.c_str(), tcn.column.length(), cs);

        std::ostringstream oss;

        oss << logicalPartNum;

        std::string lpnStr(oss.str());

        table->field[3]->store(lpnStr.c_str(), lpnStr.length(), cs);

        if (schema_table_store_record(thd, table))
        {
          return 1;
        }
      }


    }
  }

  return 0;
}

int is_columnstore_partitions_plugin_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = is_columnstore_partitions_fields;
  schema->fill_table = is_columnstore_partitions_fill;
  return 0;
}
