/* Copyright (C) 2016-2024 MariaDB Corporation

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

#include <cstdint>
#include "brmtypes.h"
#include "filebuffermgr.h"

using namespace std;

namespace testing_utils
{

inline uint64_t nextRandom(uint64_t seed)
{
  uint64_t z = (seed += UINT64_C(0x9E3779B97F4A7C15));
  z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
  z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
  return z ^ (z >> 31);
}

struct Constants
{
  const size_t bulkInsert = 40;
  const size_t find = 80;
  const size_t flushOne = 90;
  const size_t flushMany = 100;
  const size_t minTDBSize = 5000;
  const size_t maxOps = 16;
  const size_t maxTDBSize = 100000;

  Constants(const size_t id) : id(id), minLbid(maxTDBSize * id){};

  size_t id;
  BRM::LBID_t minLbid;
};

struct ThreadDB
{
  struct LbidVer
  {
    BRM::LBID_t lbid;
    BRM::VER_t ver;
  };
  using LBIDVerVec = std::vector<LbidVer>;

  ThreadDB(const size_t id) : constants(id), id(id)
  {
  }

  void insert(dbbc::CacheInsert_t& op)
  {
    insertedBlocks.push_back({op.lbid, op.ver});
  }

  size_t getIdx(const size_t randNum) const
  {
    return insertedBlocks.size() / randNum % insertedBlocks.size();
  }

  LbidVer getAndRemoveRandomEntry(const size_t randNum)
  {
    auto res = getRandomEntry(getIdx(randNum));
    insertedBlocks.erase(insertedBlocks.begin() + getIdx(randNum));
    return res;
  }

  LbidVer getRandomEntry(const size_t randNum)
  {
    auto idx = insertedBlocks.size() / randNum % insertedBlocks.size();
    return insertedBlocks[idx];
  }

  size_t size() const
  {
    return insertedBlocks.size();
  }

  dbbc::CacheInsertVec generateAndStoreOps(const size_t randNum)
  {
    auto ops = dbbc::CacheInsertVec();
    auto opsNum = randNum % constants.maxOps;
    auto random = randNum;
    for (size_t i = 0; i < opsNum; ++i)
    {
      random = nextRandom(random);
      // WIP need prng func here
      auto lbid = static_cast<BRM::LBID_t>(random % constants.maxTDBSize + constants.minLbid);
      auto ver = static_cast<BRM::VER_t>(nextRandom(lbid) % constants.maxOps);
      auto data = new uint8_t[BLOCK_SIZE];  // don't do this in production
      // std::cout << "new data " << (uint64_t)data << std::endl;
      ops.push_back({lbid, ver, data});
      insert(ops.back());
    }
    return ops;
  }

  void checkConsistency(dbbc::FileBufferMgr& fbm)
  {
    for (auto& block : insertedBlocks)
    {
      auto fb = dbbc::FileBuffer();
      auto idx = dbbc::HashObject_t(block.lbid, block.ver, 0);

      if (!fbm.find(idx, fb))
      {
        std::cerr << "Block " << block.lbid << " not found in cache" << std::endl;
      }
    }
  }

  void removeAllEntriesFromCache(dbbc::FileBufferMgr& fbm)
  {
    for (auto& block : insertedBlocks)
    {
      fbm.flushOne(block.lbid, block.ver);
    }
    insertedBlocks.clear();
  }

  LBIDVerVec insertedBlocks;
  Constants constants;
  size_t id;
};

void bulkInsert(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  auto ops = tdb.generateAndStoreOps(randNum);
  fbm.bulkInsert(ops);
  for (auto& op : ops)
  {
    // std::cout << "del data " << (uint64_t)op.data << std::endl;

    delete[] op.data;
    op.data = nullptr;
  }
}

void find(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  auto block = tdb.getRandomEntry(randNum);
  auto fb = dbbc::FileBuffer();
  auto idx = dbbc::HashObject_t(block.lbid, block.ver, 0);
  if (!fbm.find(idx, fb))
  {
    std::cerr << "Block " << block.lbid << " ver " << block.ver << " not found in cache" << std::endl;
  }
}

void bulkFind(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
}

void flushCache(ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
}

void flushOne(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  auto block = tdb.getAndRemoveRandomEntry(randNum);
  fbm.flushOne(block.lbid, block.ver);
}

void flushMany(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  auto opsNum = randNum % tdb.constants.maxOps;
  std::vector<LbidAtVer> ops;
  for (size_t i = 0; i < opsNum; ++i)
  {
    auto block = tdb.getAndRemoveRandomEntry(randNum);
    ops.push_back({static_cast<uint64_t>(block.lbid), static_cast<uint32_t>(block.ver)});
  }
  fbm.flushMany(ops.data(), ops.size());
}

void exists(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  auto block = tdb.getRandomEntry(randNum);
  fbm.exists(block.lbid, block.ver);
}

}  // namespace testing_utils
