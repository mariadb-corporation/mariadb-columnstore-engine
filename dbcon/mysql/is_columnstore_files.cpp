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

#include "dbrm.h"
#include "objectidmanager.h"
#include "we_convertor.h"
#include "we_define.h"
#include "IDBPolicy.h"
#include "configcpp.h"
#include "we_config.h"
#include "we_brm.h"
#include "bytestream.h"
#include "liboamcpp.h"
#include "messagequeue.h"
#include "messagequeuepool.h"
#include "we_messages.h"
#include "is_columnstore.h"
#include "ha_mcs_logging.h"

// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD* thd, TABLE* table);

ST_FIELD_INFO is_columnstore_files_fields[] = {
    Show::Column("OBJECT_ID", Show::ULong(0), NOT_NULL),
    Show::Column("SEGMENT_ID", Show::ULong(0), NOT_NULL),
    Show::Column("PARTITION_ID", Show::ULong(0), NOT_NULL),
    Show::Column("FILENAME", Show::Varchar(1024), NOT_NULL),
    Show::Column("FILE_SIZE", Show::ULonglong(0), NULLABLE),
    Show::Column("COMPRESSED_DATA_SIZE", Show::ULonglong(0), NULLABLE),
    Show::CEnd()};

static bool get_file_sizes(THD* thd, messageqcpp::MessageQueueClient* msgQueueClient, const char* fileName,
                           off_t* fileSize, off_t* compressedFileSize)
{
  messageqcpp::ByteStream bs;
  messageqcpp::ByteStream::byte rc;
  std::string errMsg;

  try
  {
    bs << (messageqcpp::ByteStream::byte)WriteEngine::WE_SVR_GET_FILESIZE;
    // header??
    bs << fileName;
    msgQueueClient->write(bs);
    // namespace??
    messageqcpp::SBS sbs;
    sbs = msgQueueClient->read();

    if (sbs->length() == 0)
    {
      delete msgQueueClient;
      return false;
    }

    *sbs >> rc;
    *sbs >> errMsg;
    if (rc)
    {
      ha_mcs_impl::log_this(thd,
                            "I_S::COLUMNSTORE_FILE::get_file_sizes(): WriteEngineServer returns an error().",
                            logging::LOG_TYPE_ERROR, thd->thread_id);
      return false;
    }
    *sbs >> *fileSize;
    *sbs >> *compressedFileSize;
    return true;
  }
  catch (...)
  {
    return false;
  }
}

static int generate_result(BRM::OID_t oid, BRM::DBRM* emp, TABLE* table, THD* thd)
{
  std::vector<struct BRM::EMEntry> entries;
  CHARSET_INFO* cs = system_charset_info;

  char oidDirName[WriteEngine::FILE_NAME_SIZE];
  char fullFileName[WriteEngine::FILE_NAME_SIZE];
  char dbDir[WriteEngine::MAX_DB_DIR_LEVEL][WriteEngine::MAX_DB_DIR_NAME_SIZE];

  config::Config* config;

  try
  {
    config = config::Config::makeConfig();
  }
  catch (std::runtime_error& e)
  {
    thd->raise_error_printf(ER_INTERNAL_ERROR, e.what());
    return ER_INTERNAL_ERROR;
  }

  WriteEngine::Config we_config;
  off_t fileSize = 0;
  off_t compressedFileSize = 0;
  we_config.initConfigCache();
  messageqcpp::MessageQueueClient* msgQueueClient;
  oam::Oam oam_instance;
  int pmId = 0;
  int rc;

  emp->getExtents(oid, entries, false, false, true);

  if (entries.size() == 0)
    return 0;

  std::vector<struct BRM::EMEntry>::const_iterator iter = entries.begin();

  while (iter != entries.end())  // organize extents into files
  {
    // Don't include files more than once at different block offsets
    if (iter->blockOffset > 0)
    {
      iter++;
      continue;
    }

    try
    {
      oam_instance.getDbrootPmConfig(iter->dbRoot, pmId);
    }
    catch (std::runtime_error&)
    {
      // MCOL-1116: If we are here a DBRoot is offline/missing
      iter++;
      return 0;
    }

    table->field[0]->store(oid);
    table->field[1]->store(iter->segmentNum);
    table->field[2]->store(iter->partitionNum);

    WriteEngine::Convertor::oid2FileName(oid, oidDirName, dbDir, iter->partitionNum, iter->segmentNum);
    std::stringstream DbRootName;
    DbRootName << "DBRoot" << iter->dbRoot;
    std::string DbRootPath = config->getConfig("SystemConfig", DbRootName.str());
    fileSize = compressedFileSize = 0;
    rc = snprintf(fullFileName, WriteEngine::FILE_NAME_SIZE, "%s/%s", DbRootPath.c_str(), oidDirName);

    std::ostringstream oss;
    oss << "pm" << pmId << "_WriteEngineServer";
    std::string client = oss.str();
    msgQueueClient = messageqcpp::MessageQueueClientPool::getInstance(oss.str());

    // snprintf output truncation check
    if (rc == WriteEngine::FILE_NAME_SIZE ||
        !get_file_sizes(thd, msgQueueClient, fullFileName, &fileSize, &compressedFileSize))
    {
      messageqcpp::MessageQueueClientPool::releaseInstance(msgQueueClient);
      delete emp;
      return 1;
    }

    table->field[3]->store(fullFileName, strlen(fullFileName), cs);

    if (fileSize > 0)
    {
      table->field[4]->set_notnull();
      table->field[4]->store(fileSize);

      if (compressedFileSize > 0)
      {
        table->field[5]->set_notnull();
        table->field[5]->store(compressedFileSize);
      }
      else
      {
        table->field[5]->set_null();
      }
    }
    else
    {
      table->field[4]->set_null();
      table->field[5]->set_null();
    }

    if (schema_table_store_record(thd, table))
    {
      messageqcpp::MessageQueueClientPool::releaseInstance(msgQueueClient);
      delete emp;
      return 1;
    }

    iter++;
    messageqcpp::MessageQueueClientPool::releaseInstance(msgQueueClient);
    msgQueueClient = NULL;
  }

  return 0;
}

static int is_columnstore_files_fill(THD* thd, TABLE_LIST* tables, COND* cond)
{
  BRM::DBRM::refreshShm();
  BRM::DBRM* emp = new BRM::DBRM();
  BRM::OID_t cond_oid = 0;
  TABLE* table = tables->table;

  if (!emp || !emp->isDBRMReady())
  {
    return 1;
  }

  if (cond && cond->type() == Item::FUNC_ITEM)
  {
    Item_func* fitem = (Item_func*)cond;

    if ((fitem->functype() == Item_func::EQ_FUNC) && (fitem->argument_count() == 2))
    {
      if (fitem->arguments()[0]->real_item()->type() == Item::FIELD_ITEM &&
          fitem->arguments()[1]->const_item())
      {
        // WHERE object_id = value
        Item_field* item_field = (Item_field*)fitem->arguments()[0]->real_item();

        if (strcasecmp(item_field->field_name.str, "object_id") == 0)
        {
          cond_oid = fitem->arguments()[1]->val_int();
          return generate_result(cond_oid, emp, table, thd);
        }
      }
      else if (fitem->arguments()[1]->real_item()->type() == Item::FIELD_ITEM &&
               fitem->arguments()[0]->const_item())
      {
        // WHERE value = object_id
        Item_field* item_field = (Item_field*)fitem->arguments()[1]->real_item();

        if (strcasecmp(item_field->field_name.str, "object_id") == 0)
        {
          cond_oid = fitem->arguments()[0]->val_int();
          return generate_result(cond_oid, emp, table, thd);
        }
      }
    }
    else if (fitem->functype() == Item_func::IN_FUNC)
    {
      // WHERE object_id in (value1, value2)
      Item_field* item_field = (Item_field*)fitem->arguments()[0]->real_item();

      if (strcasecmp(item_field->field_name.str, "object_id") == 0)
      {
        for (unsigned int i = 1; i < fitem->argument_count(); i++)
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
      String* tmp_var = fitem->arguments()[1]->val_str();
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

  if (!cond_oid)
  {
    for (BRM::OID_t oid = 3000; oid <= MaxOID; oid++)
    {
      int result = generate_result(oid, emp, table, thd);

      if (result)
        return 1;
    }
  }

  delete emp;
  return 0;
}

int is_columnstore_files_plugin_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = is_columnstore_files_fields;
  schema->fill_table = is_columnstore_files_fill;
  return 0;
}
