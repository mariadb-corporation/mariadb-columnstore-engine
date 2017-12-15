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

// Required declaration as it isn't in a MairaDB include
bool schema_table_store_record(THD *thd, TABLE *table);

ST_FIELD_INFO is_columnstore_files_fields[] =
{
    {"OBJECT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"SEGMENT_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"PARTITION_ID", 11, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"FILENAME", 1024, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"FILE_SIZE", 19, MYSQL_TYPE_LONGLONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {"COMPRESSED_DATA_SIZE", 19, MYSQL_TYPE_LONGLONG, 0, MY_I_S_MAYBE_NULL, 0, 0},
    {0, 0, MYSQL_TYPE_NULL, 0, 0, 0, 0}
};

static bool get_file_sizes(messageqcpp::MessageQueueClient *msgQueueClient, const char *fileName, off_t *fileSize, off_t *compressedFileSize)
{
    messageqcpp::ByteStream bs;
    messageqcpp::ByteStream::byte rc;
    std::string errMsg;

    try
    {
        bs << (messageqcpp::ByteStream::byte) WriteEngine::WE_SVR_GET_FILESIZE;
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
        *sbs >> *fileSize;
        *sbs >> *compressedFileSize;
        return true;
    }
    catch (...)
    {
        return false;
    }
}

static int is_columnstore_files_fill(THD *thd, TABLE_LIST *tables, COND *cond)
{
    BRM::DBRM *emp = new BRM::DBRM();
    std::vector<struct BRM::EMEntry> entries;
    CHARSET_INFO *cs = system_charset_info;
    TABLE *table = tables->table;

    char oidDirName[WriteEngine::FILE_NAME_SIZE];
	char fullFileName[WriteEngine::FILE_NAME_SIZE];
    char dbDir[WriteEngine::MAX_DB_DIR_LEVEL][WriteEngine::MAX_DB_DIR_NAME_SIZE];
    config::Config* config = config::Config::makeConfig();
    WriteEngine::Config we_config;
    off_t fileSize = 0;
    off_t compressedFileSize = 0;
    we_config.initConfigCache();
    messageqcpp::MessageQueueClient *msgQueueClient;
    oam::Oam oam_instance;
    int pmId = 0;

    if (!emp || !emp->isDBRMReady())
    {
        return 1;
    }

    execplan::ObjectIDManager oidm;
    BRM::OID_t MaxOID = oidm.size();

    for(BRM::OID_t oid = 3000; oid <= MaxOID; oid++)
    {
        emp->getExtents(oid, entries, false, false, true);
        if (entries.size() == 0)
            continue;

        std::vector<struct BRM::EMEntry>::const_iterator iter = entries.begin();
        while ( iter != entries.end() ) //organize extents into files
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
            catch (std::runtime_error)
            {
                // MCOL-1116: If we are here a DBRoot is offline/missing
                iter++;
                continue;
            }
            table->field[0]->store(oid);
            table->field[1]->store(iter->segmentNum);
            table->field[2]->store(iter->partitionNum);

            WriteEngine::Convertor::oid2FileName(oid, oidDirName, dbDir, iter->partitionNum, iter->segmentNum);
            std::stringstream DbRootName;
            DbRootName << "DBRoot" << iter->dbRoot;
            std::string DbRootPath = config->getConfig("SystemConfig", DbRootName.str());
            fileSize = compressedFileSize = 0;
            snprintf(fullFileName, WriteEngine::FILE_NAME_SIZE, "%s/%s", DbRootPath.c_str(), oidDirName);

            std::ostringstream oss;
            oss << "pm" << pmId << "_WriteEngineServer";
            std::string client = oss.str();
            msgQueueClient = messageqcpp::MessageQueueClientPool::getInstance(oss.str());
        
            if (!get_file_sizes(msgQueueClient, fullFileName, &fileSize, &compressedFileSize))
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
    }
    delete emp;
    return 0;
}

static int is_columnstore_files_plugin_init(void *p)
{
    ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
    schema->fields_info = is_columnstore_files_fields;
    schema->fill_table = is_columnstore_files_fill;
    return 0;
}

static struct st_mysql_information_schema is_columnstore_files_plugin_version =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

maria_declare_plugin(is_columnstore_files_plugin)
{
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &is_columnstore_files_plugin_version,
    "COLUMNSTORE_FILES",
    "MariaDB Corporaton",
    "An information schema plugin to list ColumnStore filess",
    PLUGIN_LICENSE_GPL,
    is_columnstore_files_plugin_init,
    //is_columnstore_files_plugin_deinit,
    NULL,
    0x0100,
    NULL,
    NULL,
    "1.0",
    MariaDB_PLUGIN_MATURITY_STABLE
}
maria_declare_plugin_end;
