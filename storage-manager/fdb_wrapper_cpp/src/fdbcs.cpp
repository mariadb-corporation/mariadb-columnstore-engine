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
#include "../include/fdbcs.hpp"

namespace FDBCS
{
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

std::unique_ptr<FDBDataBase> DataBaseCreator::createDataBase(const std::string clusterFilePath)
{
  FDBDatabase* database;
  auto err = fdb_create_database(clusterFilePath.c_str(), &database);
  if (err)
  {
    std::cerr << "fdb_create_database error, code: " << (int)err << std::endl;
    return nullptr;
  }
  return std::make_unique<FDBDataBase>(database);
}

bool setAPIVersion()
{
  auto err = fdb_select_api_version(FDB_API_VERSION);
  return err ? false : true;
}
}  // namespace FDBCS
