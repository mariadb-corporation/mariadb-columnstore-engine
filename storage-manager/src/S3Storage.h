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

#include <deque>
#include <string>
#include <memory>
#include <optional>
#include <unordered_map>

#include "CloudStorage.h"
#include "libmarias3/marias3.h"
#include <curl/curl.h>

namespace storagemanager
{
class S3Storage : public CloudStorage
{
 public:
  explicit S3Storage(bool skipRetry = false, bool verbose = false);

  ~S3Storage() override;

  int getObject(const std::string& sourceKey, const std::string& destFile, size_t* size = nullptr) override;
  int getObject(const std::string& sourceKey, std::shared_ptr<uint8_t[]>* data,
                size_t* size = nullptr) override;
  int putObject(const std::string& sourceFile, const std::string& destKey) override;
  int putObject(const std::shared_ptr<uint8_t[]> data, size_t len, const std::string& destKey) override;
  int deleteObject(const std::string& key) override;
  int copyObject(const std::string& sourceKey, const std::string& destKey) override;
  int exists(const std::string& key, bool* out) override;

  std::vector<IOTaskData> taskList() const override;
  bool killTask(uint64_t task_id) override;

 private:
  struct Connection;
  bool getIAMRoleFromMetadataEC2();
  bool getCredentialsFromMetadataEC2();
  void testConnectivityAndPerms();
  std::shared_ptr<Connection> getConnection();
  void returnConnection(std::shared_ptr<Connection> conn);

  bool skipRetryableErrors;
  bool verbose_enabled;

  std::string bucket;  // might store this as a char *, since it's only used that way
  std::string prefix;
  std::string region;
  std::string key;
  std::string secret;
  std::string token;
  std::string endpoint;
  std::string IAMrole;
  std::string STSendpoint;
  std::string STSregion;
  bool isEC2Instance;
  bool ec2iamEnabled;
  bool useHTTP;
  bool sslVerify;
  int portNumber;
  std::optional<float> connectTimeout;
  std::optional<float> operationTimeout;

  struct Connection
  {
    explicit Connection(uint64_t id) : id(id)
    {
    }
    uint64_t id;
    ms3_st* conn{nullptr};
    timespec touchedAt{};
    bool terminate{false};
  };
  struct ScopedConnection
  {
    ScopedConnection(S3Storage*, std::shared_ptr<Connection>);
    ~ScopedConnection();
    S3Storage* s3;
    std::shared_ptr<Connection> conn;
  };

  // for sanity checking
  // std::map<ms3_st *, boost::mutex> connMutexes;

  mutable boost::mutex connMutex;
  std::deque<std::shared_ptr<Connection>> freeConns;  // using this as a stack to keep lru objects together
  std::unordered_map<uint64_t, std::shared_ptr<Connection>>
      usedConns;  // using this for displaying and killing tasks
  uint64_t nextConnId = 0;
  const time_t maxIdleSecs = 30;
};

}  // namespace storagemanager
