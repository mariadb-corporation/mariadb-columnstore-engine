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

#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include "fdbcs.hpp"

namespace FDBCS
{

#define RETURN_ON_ERROR(exp) \
  do                         \
  {                          \
    auto rc = (exp);         \
    if (!rc)                 \
      return rc;             \
  } while (false);

Transaction::Transaction(FDBTransaction* tnx) : tnx_(tnx)
{
}

Transaction::~Transaction()
{
  if (tnx_)
  {
    fdb_transaction_destroy(tnx_);
    tnx_ = nullptr;
  }
}

void Transaction::set(const ByteArray& key, const ByteArray& value) const
{
  if (tnx_)
  {
    fdb_transaction_set(tnx_, (uint8_t*)key.c_str(), key.length(), (uint8_t*)value.c_str(), value.length());
  }
}

std::pair<bool, ByteArray> Transaction::get(const ByteArray& key) const
{
  if (tnx_)
  {
    FDBFuture* future = fdb_transaction_get(tnx_, (uint8_t*)key.c_str(), key.length(), 0);
    auto err = fdb_future_block_until_ready(future);
    if (err)
    {
      fdb_future_destroy(future);
      std::cerr << "fdb_future_block_until_read error, code: " << (int)err << std::endl;
      return {false, {}};
    }

    const uint8_t* outValue;
    int outValueLength;
    fdb_bool_t present;
    err = fdb_future_get_value(future, &present, &outValue, &outValueLength);
    if (err)
    {
      fdb_future_destroy(future);

      std::cerr << "fdb_future_get_value error, code: " << (int)err << std::endl;
      return {false, {}};
    }

    fdb_future_destroy(future);
    if (present)
    {
      return {true, ByteArray(outValue, outValue + outValueLength)};
    }
    else
    {
      return {false, {}};
    }
  }
  return {false, {}};
}

void Transaction::remove(const ByteArray& key) const
{
  if (tnx_)
  {
    fdb_transaction_clear(tnx_, (uint8_t*)key.c_str(), key.length());
  }
}

void Transaction::removeRange(const ByteArray& beginKey, const ByteArray& endKey) const
{
  if (tnx_)
  {
    fdb_transaction_clear_range(tnx_, (uint8_t*)beginKey.c_str(), beginKey.length(), (uint8_t*)endKey.c_str(),
                                endKey.length());
  }
}

bool Transaction::commit() const
{
  if (tnx_)
  {
    FDBFuture* future = fdb_transaction_commit(tnx_);
    auto err = fdb_future_block_until_ready(future);
    if (err)
    {
      fdb_future_destroy(future);
      std::cerr << "fdb_future_block_until_ready error, code: " << (int)err << std::endl;
      return false;
    }
    err = fdb_future_get_error(future);
    if (err)
    {
      fdb_future_destroy(future);
      std::cerr << "fdb_future_get_error(), code: " << (int)err << std::endl;
      return false;
    }
    fdb_future_destroy(future);
  }
  return true;
}

FDBNetwork::~FDBNetwork()
{
  auto err = fdb_stop_network();
  if (err)
    std::cerr << "fdb_stop_network error, code: " << err << std::endl;
  if (netThread.joinable())
    netThread.join();
}

bool FDBNetwork::setUpAndRunNetwork()
{
  auto err = fdb_setup_network();
  if (err)
  {
    std::cerr << "fdb_setup_network error, code: " << (int)err << std::endl;
    return false;
  }

  netThread = std::thread(
      []
      {
        // TODO: Try more than on time.
        auto err = fdb_run_network();
        if (err)
        {
          std::cerr << "fdb_run_network error, code: " << (int)err << std::endl;
          abort();
        }
      });
  return true;
}

FDBDataBase::FDBDataBase(FDBDatabase* database) : database_(database)
{
}

FDBDataBase::~FDBDataBase()
{
  if (database_)
    fdb_database_destroy(database_);
}

std::unique_ptr<Transaction> FDBDataBase::createTransaction() const
{
  FDBTransaction* tnx;
  auto err = fdb_database_create_transaction(database_, &tnx);
  if (err)
  {
    std::cerr << "fdb_database_create_transaction error, code: " << (int)err << std::endl;
    return nullptr;
  }
  return std::make_unique<Transaction>(tnx);
}

bool FDBDataBase::isDataBaseReady() const
{
  FDBTransaction* tnx;
  auto err = fdb_database_create_transaction(database_, &tnx);
  if (err)
  {
    std::cerr << "fdb_database_create_transaction error, code: " << (int)err << std::endl;
    return false;
  }
  ByteArray emptyKey{""};
  FDBFuture* future = fdb_transaction_get(tnx, (uint8_t*)emptyKey.c_str(), emptyKey.length(), 0);

  uint32_t count = 0;
  while (!fdb_future_is_ready(future) && count < secondsToWait_)
  {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ++count;
  }
  bool ready = fdb_future_is_ready(future);
  fdb_future_destroy(future);
  fdb_transaction_destroy(tnx);
  return ready;
}

std::shared_ptr<FDBDataBase> DataBaseCreator::createDataBase(const std::string clusterFilePath)
{
  FDBDatabase* database;
  auto err = fdb_create_database(clusterFilePath.c_str(), &database);
  if (err)
  {
    std::cerr << "fdb_create_database error, code: " << (int)err << std::endl;
    return nullptr;
  }
  return std::make_shared<FDBDataBase>(database);
}

Key BoostUIDKeyGenerator::generateKey()
{
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

uint32_t BoostUIDKeyGenerator::getKeySize()
{
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()()).size();
}

Keys BlobHandler::generateKeys(const uint32_t num)
{
  Keys keys;
  keys.reserve(num);
  for (uint32_t i = 0; i < num; ++i)
    keys.push_back(keyGen_->generateKey());

  return keys;
}
// FIXME: Put it to util?
float BlobHandler::log(const uint32_t base, const uint32_t value)
{
  return std::log(value) / std::log(base);
}

void BlobHandler::insertKey(Block& block, const std::string& value)
{
  if (!block.first)
  {
    block.second.reserve(blockSizeInBytes_);
    block.second.insert(block.second.end(), keyBlockIdentifier.begin(), keyBlockIdentifier.end());
    block.first += keyBlockIdentifier.size();
  }
  block.second.insert(block.second.begin() + block.first, value.begin(), value.end());
  block.first += value.size();
}

size_t BlobHandler::insertData(Block& block, const std::string& blob, const size_t offset)
{
  const size_t endOfBlock = std::min(offset + dataBlockSizeInBytes_, blob.size());
  auto& dataBlock = block.second;
  if (!block.first)
  {
    dataBlock.reserve(blockSizeInBytes_);
    dataBlock.insert(dataBlock.end(), dataBlockIdentifier.begin(), dataBlockIdentifier.end());
    block.first += dataBlockIdentifier.size();
  }
  dataBlock.insert(dataBlock.begin() + block.first, blob.begin() + offset, blob.begin() + endOfBlock);
  return endOfBlock;
}

bool BlobHandler::commitKeys(std::shared_ptr<FDBCS::FDBDataBase> dataBase, KeyBlockMap& keyBlockMap,
                             const Keys& keys)
{
  auto tnx = dataBase->createTransaction();
  if (!tnx)
    return false;

  for (const auto& key : keys)
    tnx->set(key, keyBlockMap[key].second);

  return tnx->commit();
}

bool BlobHandler::commitKey(std::shared_ptr<FDBCS::FDBDataBase> dataBase, const Key& key,
                            const ByteArray& value)
{
  auto tnx = dataBase->createTransaction();
  tnx->set(key, value);
  return tnx->commit();
}

TreeLevelNumKeysMap BlobHandler::computeNumKeysForEachTreeLevel(const int32_t treeLen,
                                                                const uint32_t numBlocks)
{
  TreeLevelNumKeysMap levelMap;
  levelMap[treeLen] = numBlocks;
  if (!treeLen)
    return levelMap;

  for (int32_t level = treeLen - 1; level >= 0; --level)
  {
    if (level + 1 == treeLen)
      levelMap[level] = levelMap[level + 1];
    else
      levelMap[level] = (levelMap[level + 1] + (numKeysInBlock_ - 1)) / numKeysInBlock_;
  }
  return levelMap;
}

bool BlobHandler::writeBlob(std::shared_ptr<FDBCS::FDBDataBase> dataBase, const ByteArray& key,
                            const ByteArray& blob)
{
  const size_t blobSizeInBytes = blob.size();
  if (!blobSizeInBytes)
    return commitKey(dataBase, key, "");

  const uint32_t numDataBlocks = (blobSizeInBytes + (dataBlockSizeInBytes_ - 1)) / dataBlockSizeInBytes_;
  const uint32_t treeLen = std::ceil(log(numKeysInBlock_, numDataBlocks));
  Keys currentKeys{key};
  auto numKeyLevelMap = computeNumKeysForEachTreeLevel(treeLen, numDataBlocks);

  KeyBlockMap keyBlockMap;
  keyBlockMap[key] = {0, std::string()};
  Keys keysInTnx;
  size_t currentTnxSize = 0;

  for (uint32_t currentLevel = 0; currentLevel < treeLen; ++currentLevel)
  {
    const uint32_t nextLevelKeyNum = numKeyLevelMap[currentLevel];
    auto nextLevelKeys = generateKeys(nextLevelKeyNum);
    uint32_t nextKeysIt = 0;
    for (uint32_t i = 0, size = currentKeys.size(); i < size && nextKeysIt < nextLevelKeyNum; ++i)
    {
      const auto& currentKey = currentKeys[i];
      auto& block = keyBlockMap[currentKey];
      for (uint32_t j = 0; j < numKeysInBlock_ && nextKeysIt < nextLevelKeyNum; ++j, ++nextKeysIt)
      {
        const auto& nextKey = nextLevelKeys[nextKeysIt];
        insertKey(block, nextKey);
        keyBlockMap[nextKey] = {0, std::string()};
      }
      if (currentTnxSize + (keySizeInBytes_ + block.second.size()) >= maxTnxSize_)
      {
        RETURN_ON_ERROR(commitKeys(dataBase, keyBlockMap, keysInTnx));
        currentTnxSize = 0;
        keysInTnx.clear();
      }
      currentTnxSize += block.second.size() + keySizeInBytes_;
      keysInTnx.push_back(currentKey);
    }
    currentKeys = std::move(nextLevelKeys);
  }

  size_t offset = 0;
  for (uint32_t i = 0; i < numDataBlocks; ++i)
  {
    const auto& currentKey = currentKeys[i];
    auto& block = keyBlockMap[currentKey];
    offset = insertData(block, blob, offset);
    if (currentTnxSize + (keySizeInBytes_ + block.second.size()) >= maxTnxSize_)
    {
      RETURN_ON_ERROR(commitKeys(dataBase, keyBlockMap, keysInTnx));
      currentTnxSize = 0;
      keysInTnx.clear();
    }
    keysInTnx.push_back(currentKey);
    currentTnxSize += block.second.size() + keySizeInBytes_;
  }

  if (currentTnxSize)
    RETURN_ON_ERROR(commitKeys(dataBase, keyBlockMap, keysInTnx));

  return true;
}

std::pair<bool, Keys> BlobHandler::getKeysFromBlock(const Block& block)
{
  Keys keys;
  const auto& blockData = block.second;
  if (blockData.size() > blockSizeInBytes_)
    return {false, {""}};

  uint32_t offset = 1;
  for (uint32_t i = 0; i < numKeysInBlock_ && offset + keySizeInBytes_ <= blockData.size(); ++i)
  {
    Key key(blockData.begin() + offset, blockData.begin() + offset + keySizeInBytes_);
    keys.push_back(std::move(key));
    offset += keySizeInBytes_;
  }

  return {true, keys};
}

bool BlobHandler::isDataBlock(const Block& block)
{
  return block.second.compare(0, keyBlockIdentifier.size(), keyBlockIdentifier) != 0;
}

std::pair<bool, std::string> BlobHandler::readBlob(std::shared_ptr<FDBCS::FDBDataBase> database,
                                                   const ByteArray& key)
{
  Keys currentKeys{key};
  bool dataBlockReached = false;

  while (!dataBlockReached)
  {
    auto tnx = database->createTransaction();
    if (!tnx)
      return {false, ""};

    std::vector<Block> blocks;
    for (const auto& key : currentKeys)
    {
      auto p = tnx->get(key);
      if (!p.first)
        return {false, ""};

      Block block{0, p.second};
      if (isDataBlock(block))
      {
        dataBlockReached = true;
        break;
      }
      blocks.push_back(block);
    }

    if (dataBlockReached)
      break;

    Keys nextKeys;
    for (const auto& block : blocks)
    {
      auto keysPair = getKeysFromBlock(block);
      if (!keysPair.first)
        return {false, ""};

      auto& keys = keysPair.second;
      nextKeys.insert(nextKeys.end(), keys.begin(), keys.end());
    }
    currentKeys = std::move(nextKeys);
  }

  std::string blob;
  for (const auto& key : currentKeys)
  {
    auto tnx = database->createTransaction();
    if (!tnx)
      return {false, ""};

    auto resultPair = tnx->get(key);
    if (!resultPair.first)
      return {false, ""};

    auto& dataBlock = resultPair.second;
    if (!dataBlock.size())
      return {false, ""};

    blob.insert(blob.end(), dataBlock.begin() + dataBlockIdentifier.size(), dataBlock.end());
  }

  return {true, blob};
}

bool BlobHandler::removeKeys(std::shared_ptr<FDBCS::FDBDataBase> database, const Keys& keys)
{
  for (const auto& key : keys)
  {
    auto tnx = database->createTransaction();
    if (!tnx)
      return false;
    tnx->remove(key);
    RETURN_ON_ERROR(tnx->commit());
  }

  return true;
}

bool BlobHandler::removeBlob(std::shared_ptr<FDBCS::FDBDataBase> database, const Key& key)
{
  std::unordered_map<uint32_t, Keys> treeLevel;
  auto tnx = database->createTransaction();
  if (!tnx)
    return false;

  uint32_t currentLevel = 0;
  treeLevel[0] = {key};

  while (true)
  {
    const auto& currentKeys = treeLevel[currentLevel];
    std::vector<Block> blocks;
    for (const auto& key : currentKeys)
    {
      auto p = tnx->get(key);
      if (!p.first)
        return false;
      blocks.push_back({0, p.second});
    }

    if (isDataBlock(blocks.front()) || !currentKeys.size())
      break;

    Keys nextKeys;
    for (const auto& block : blocks)
    {
      auto keysPair = getKeysFromBlock(block);
      if (!keysPair.first)
        return false;

      auto& keys = keysPair.second;
      nextKeys.insert(nextKeys.end(), keys.begin(), keys.end());
    }

    ++currentLevel;
    treeLevel[currentLevel] = std::move(nextKeys);
  }

  for (uint32_t level = 0; level <= currentLevel; ++level)
    RETURN_ON_ERROR(removeKeys(database, treeLevel[level]));

  return true;
}

bool setAPIVersion()
{
  auto err = fdb_select_api_version(FDB_API_VERSION);
  return err ? false : true;
}
}  // namespace FDBCS
