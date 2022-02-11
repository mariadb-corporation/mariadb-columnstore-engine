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

#ifndef CLOUDSTORAGE_H_
#define CLOUDSTORAGE_H_

#include <string>
#include <boost/shared_array.hpp>
#include "SMLogging.h"

namespace storagemanager
{
class CloudStorage
{
 public:
  virtual ~CloudStorage(){};
  /* These behave like syscalls.  return code -1 means an error, and errno is set */
  virtual int getObject(const std::string& sourceKey, const std::string& destFile, size_t* size = NULL) = 0;
  virtual int getObject(const std::string& sourceKey, boost::shared_array<uint8_t>* data,
                        size_t* size = NULL) = 0;
  virtual int putObject(const std::string& sourceFile, const std::string& destKey) = 0;
  virtual int putObject(const boost::shared_array<uint8_t> data, size_t len, const std::string& destKey) = 0;
  virtual int deleteObject(const std::string& key) = 0;
  virtual int copyObject(const std::string& sourceKey, const std::string& destKey) = 0;
  virtual int exists(const std::string& key, bool* out) = 0;

  virtual void printKPIs() const;

  // this will return a CloudStorage instance of the type specified in StorageManager.cnf
  static CloudStorage* get();

 protected:
  SMLogging* logger;
  CloudStorage();

  // some KPIs
  size_t bytesUploaded, bytesDownloaded, objectsDeleted, objectsCopied, objectsGotten, objectsPut,
      existenceChecks;

 private:
};

}  // namespace storagemanager

#endif
