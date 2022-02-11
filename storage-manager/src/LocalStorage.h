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

#ifndef LOCALSTORAGE_H_
#define LOCALSTORAGE_H_

#include <string>
#include "CloudStorage.h"
#include "SMLogging.h"
#include <boost/filesystem/path.hpp>

namespace storagemanager
{
class LocalStorage : public CloudStorage
{
 public:
  LocalStorage();
  virtual ~LocalStorage();

  int getObject(const std::string& sourceKey, const std::string& destFile, size_t* size = NULL);
  int getObject(const std::string& sourceKey, boost::shared_array<uint8_t>* data, size_t* size = NULL);
  int putObject(const std::string& sourceFile, const std::string& destKey);
  int putObject(const boost::shared_array<uint8_t> data, size_t len, const std::string& destKey);
  int deleteObject(const std::string& key);
  int copyObject(const std::string& sourceKey, const std::string& destKey);
  int exists(const std::string& key, bool* out);

  const boost::filesystem::path& getPrefix() const;
  void printKPIs() const;

 protected:
  size_t bytesRead, bytesWritten;

 private:
  boost::filesystem::path prefix;
  int copy(const boost::filesystem::path& sourceKey, const boost::filesystem::path& destKey);

  // stuff for faking the latency on cloud ops
  bool fakeLatency;
  uint64_t usecLatencyCap;
  uint r_seed;
  void addLatency();
};

}  // namespace storagemanager

#endif
