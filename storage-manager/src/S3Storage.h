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

#ifndef S3STORAGE_H_
#define S3STORAGE_H_

#include <string>
#include <map>
#include "CloudStorage.h"
#include "libmarias3/marias3.h"
#include "Config.h"
#include <curl/curl.h>

namespace storagemanager
{
class S3Storage : public CloudStorage
{
 public:
  S3Storage(bool skipRetry = false);

  virtual ~S3Storage();

  int getObject(const std::string& sourceKey, const std::string& destFile, size_t* size = NULL);
  int getObject(const std::string& sourceKey, boost::shared_array<uint8_t>* data, size_t* size = NULL);
  int putObject(const std::string& sourceFile, const std::string& destKey);
  int putObject(const boost::shared_array<uint8_t> data, size_t len, const std::string& destKey);
  int deleteObject(const std::string& key);
  int copyObject(const std::string& sourceKey, const std::string& destKey);
  int exists(const std::string& key, bool* out);

 private:
  bool getIAMRoleFromMetadataEC2();
  bool getCredentialsFromMetadataEC2();
  void testConnectivityAndPerms();
  ms3_st* getConnection();
  void returnConnection(ms3_st*);

  bool skipRetryableErrors;

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

  struct Connection
  {
    ms3_st* conn;
    timespec idleSince;
  };
  struct ScopedConnection
  {
    ScopedConnection(S3Storage*, ms3_st*);
    ~ScopedConnection();
    S3Storage* s3;
    ms3_st* conn;
  };

  // for sanity checking
  // std::map<ms3_st *, boost::mutex> connMutexes;

  boost::mutex connMutex;
  std::deque<Connection> freeConns;  // using this as a stack to keep lru objects together
  const time_t maxIdleSecs = 30;
};

}  // namespace storagemanager

#endif
