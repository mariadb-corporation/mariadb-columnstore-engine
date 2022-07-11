/* Copyright (C) 2022 MariaDB Corporation

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
   MA 02110-1301, USA.
*/

#include <curl/curl.h>

#define NEED_CALPONT_INTERFACE
#define PREFER_MY_CONFIG_H 1
#include "ha_mcs_impl.h"
#include "ha_mcs_impl_if.h"
using namespace cal_impl_if;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "columnstoreversion.h"
#include "ha_mcs_sysvars.h"

#include "utils/json/json.hpp"
#include <regex>

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp)
{
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

std::string parseCMAPIkey()
{
  std::regex pattern("\\s*x-api-key\\s*=\\s*'(.*)'");
  auto configFile = std::string(MCSSYSCONFDIR) + "/columnstore/cmapi_server.conf";
  std::ifstream in(configFile, std::ios::in | std::ios::binary);

  if (!in.good())
    return {};

  std::string contents;
  in.seekg(0, std::ios::end);
  contents.reserve(in.tellg());
  in.seekg(0, std::ios::beg);
  std::copy((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>(),
            std::back_inserter(contents));
  in.close();

  std::smatch matches;

  if (std::regex_search(contents, matches, pattern))
    return matches.str(1);

  return {};
}

extern "C"
{
  struct InitData
  {
    CURL* curl = nullptr;
    char* result = nullptr;
  };

  void columnstore_dataload_deinit(UDF_INIT* initid)
  {
    InitData* initData = (InitData*)(initid->ptr);
    if (!initData)
      return;

    curl_easy_cleanup(initData->curl);
    delete initData->result;
  }

  const char* columnstore_dataload_impl(CURL* curl, char* result, unsigned long* length,
                                        std::string_view bucket, std::string_view table,
                                        std::string_view filename, std::string_view database,
                                        std::string_view secret, std::string_view key,
                                        std::string_view region, std::string_view cmapi_host,
                                        ulong cmapi_port, std::string_view cmapi_version,
                                        std::string_view cmapi_key)

  {
    CURLcode res;
    std::string readBuffer;

    nlohmann::json j;

    j["bucket"] = bucket;
    j["table"] = table;
    j["filename"] = filename;
    j["key"] = key;
    j["secret"] = secret;
    j["region"] = region;
    j["database"] = database;

    std::string param = j.dump();

    struct curl_slist* hs = NULL;
    hs = curl_slist_append(hs, "Content-Type: application/json");
    std::string key_header = std::string("x-api-key:") + std::string(cmapi_key.begin(), cmapi_key.end());
    hs = curl_slist_append(hs, key_header.c_str());

    std::string url = std::string(cmapi_host.begin(), cmapi_host.end()) + "/cmapi/" +
                      std::string(cmapi_version.begin(), cmapi_version.end()) + "/cluster/load_s3data";

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hs);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_PORT, cmapi_port);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, param.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK)
    {
      std::string msg = std::string("CMAPI Remote request failed: ") + curl_easy_strerror(res);
      result = new char[msg.length() + 1];
      memcpy(result, msg.c_str(), msg.length());
      *length = msg.length();
      return result;
    }

    result = new char[readBuffer.length() + 1];
    memcpy(result, readBuffer.c_str(), readBuffer.size());
    *length = readBuffer.size();

    return result;
  }

  const char* columnstore_dataload(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length,
                                   char* /*is_null*/, char* /*error*/)
  {
    InitData* initData = (InitData*)(initid->ptr);

    if (!initData->curl)
    {
      std::string msg("CURL initialization failed, remote execution of dataload error");
      memcpy(result, msg.c_str(), msg.length());
      *length = msg.length();
      return result;
    }

    const char* table = args->args[0];
    const char* filename = args->args[1];
    const char* bucket = args->args[2];

    ulong cmapi_port = get_cmapi_port(_current_thd());
    const char* cmapi_host = get_cmapi_host(_current_thd());
    const char* cmapi_version = get_cmapi_version(_current_thd());
    const char* cmapi_key = get_cmapi_key(_current_thd());

    THD* thd = _current_thd();

    const char* secret = get_s3_secret(thd);
    const char* key = get_s3_key(thd);
    const char* region = get_s3_region(thd);
    const char* database = args->arg_count != 4 ? thd->get_db() : args->args[3];

    return columnstore_dataload_impl(initData->curl, initData->result, length, bucket, table, filename,
                                     database, secret, key, region, cmapi_host, cmapi_port, cmapi_version,
                                     ::strlen(cmapi_key) == 0 ? parseCMAPIkey().c_str() : cmapi_key);
  }

  my_bool columnstore_dataload_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
  {
    if (args->arg_count != 3 && args->arg_count != 4)
    {
      strcpy(message, "COLUMNSTORE_DATALOAD() takes three or four arguments: (table, filename, bucket) or (table, filename, bucket, database)");
      return 1;
    }

    initid->max_length = 1000 * 1000;
    InitData* initData = new InitData;
    initData->curl = curl_easy_init();
    initid->ptr = (char*)(initData);

    return 0;
  }
}