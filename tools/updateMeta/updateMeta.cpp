/* Copyright (C) 2024 MariaDB Corporation

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

#include <iostream>
#include <string>
#include <filesystem>
#include <vector>
#include "fdbcs.hpp"
#include <fstream>
#include <sstream>

using namespace std;
std::unique_ptr<FDBCS::FDBNetwork> fdbNetworkInstance;

static std::shared_ptr<FDBCS::FDBDataBase> getStorageInstance()
{
  if (!FDBCS::setAPIVersion())
  {
    cerr << "Update meta: FDB setAPIVersion failed." << endl;
    return nullptr;
  }

  fdbNetworkInstance = std::make_unique<FDBCS::FDBNetwork>();
  if (!fdbNetworkInstance->setUpAndRunNetwork())
  {
    cerr << "Update meta: FDB setUpAndRunNetwork failed." << endl;
    return nullptr;
  }

  std::string clusterFilePath = "/etc/foundationdb/fdb.cluster";
  auto fdbDataBaseInstance = FDBCS::DataBaseCreator::createDataBase(clusterFilePath);
  if (!fdbDataBaseInstance)
  {
    cerr << "Update meta: FDB createDataBase failed." << endl;
    return nullptr;
  }

  return fdbDataBaseInstance;
}

class MetaCollector
{
 public:
  MetaCollector(const std::string& metaPath) : metaPath(metaPath)
  {
  }

  bool collect(const std::string& dbRoot)
  {
    try
    {
      auto path = metaPath + dbRoot;
      for (auto const& file : std::filesystem::recursive_directory_iterator{path})
      {
        if (std::filesystem::is_regular_file(file))
        {
          files.push_back(file.path());
        }
      }
    }
    catch (std::exception& e)
    {
      std::cerr << e.what() << std::endl;
      return false;
    }
    return true;
  }

  bool commit()
  {
    auto kvStorage = getStorageInstance();
    if (!kvStorage)
    {
      cerr << "Cannot get a storage instance" << endl;
      return false;
    }
    auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();

    for (const auto& file : files)
    {
      auto fileName = file.string();
      cout << fileName << endl;
      ifstream iFile(fileName);
      if (!iFile.is_open())
        return false;

      std::stringstream metaDataStream;
      metaDataStream << iFile.rdbuf();
      FDBCS::BlobHandler blobWriter(keyGen);
      auto metaKey = metaPrefix + fileName;
      if (!blobWriter.writeBlob(kvStorage, metaKey, metaDataStream.str()))
      {
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<filesystem::path> files;
  string metaPrefix = "SM_M_";
  string metaPath;
  string dbRoot;
};

int main(int argc, char** argv)
{
  if (argc != 2)
  {
    cout << "Have to provide dbroot name. For example: updateMeta data1 " << endl;
    return 0;
  }
  std::string metaPath = "/var/lib/columnstore/storagemanager/metadata/";
  MetaCollector collector(metaPath);
  string dbRootName = argv[1];
  if (!collector.collect(dbRootName))
  {
    cerr << "Cannot collect metadata files " << endl;
    return 1;
  }
  if (!collector.commit())
  {
    cerr << "Cannot commit metadata to kv storage " << endl;
    return 1;
  }
  return 0;
}
