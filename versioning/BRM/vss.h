/* Copyright (C) 2014 InfiniDB, Inc.

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

/******************************************************************************
 * $Id: vss.h 1926 2013-06-30 21:18:14Z wweeks $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#pragma once

#include <cstddef>
#include <set>
// #define NDEBUG
#include <cassert>
#include <boost/thread.hpp>

#include "brmshmimpl.h"

#include "brmtypes.h"
#include "nullstring.h"
#include "undoable.h"
#include "mastersegmenttable.h"
#include "shmkeys.h"
#include "hasher.h"
#include "vbbm.h"

#ifdef NONE
#undef NONE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif

// These config parameters need to be loaded

// will get a small hash function performance boost by using powers of 2

namespace BRM
{
const constexpr static size_t VssFactor = MasterSegmentTable::VssShmemTypes.size();

// struct VBBMEntry;
// class VSS;
class VSSShard;
class VSSCluster;
// using VssPtrVector = std::vector<std::unique_ptr<VSS>>;
using LbidVersionVec = std::vector<std::pair<LBID_t, VER_t>>;
using VssClusterPtr = std::unique_ptr<VSSCluster>;

struct VSSEntry
{
  LBID_t lbid;
  VER_t verID;
  bool vbFlag;
  bool locked;
  int next;
#ifndef __LP64__
  uint32_t pad1;
#endif
  VSSEntry();
  VSSEntry(const LBID_t lbid, const VER_t verID, const bool vbFlag, const bool locked);
};

struct VSSShmsegHeader
{
  int capacity;
  int currentSize;
  int LWM;
  int numHashBuckets;
  int lockedEntryCount;

  //  the rest of the overlay looks like this
  // 	int hashBuckets[numHashBuckets];
  // 	VSSEntry storage[capacity];
};

// #define VSSSTORAGE_INITIAL_COUNT 200000L / VssFactor
// #define VSSSTORAGE_INITIAL_SIZE (VSSSTORAGE_INITIAL_COUNT * sizeof(VSSEntry))
// #define VSSSTORAGE_INCREMENT_COUNT 20000L / VssFactor
// #define VSSSTORAGE_INCREMENT (VSSSTORAGE_INCREMENT_COUNT * sizeof(VSSEntry))

// // (average list length = 4)
// #define VSSTABLE_INITIAL_SIZE (50000 * sizeof(int))
// #define VSSTABLE_INCREMENT (5000 * sizeof(int))

// #define VSS_INITIAL_SIZE (sizeof(VSSShmsegHeader) + VSSSTORAGE_INITIAL_SIZE + VSSTABLE_INITIAL_SIZE)

// #define VSS_INCREMENT (VSSTABLE_INCREMENT + VSSSTORAGE_INCREMENT)

// #define VSS_SIZE(entries) ((entries * sizeof(VSSEntry)) + (entries / 4 * sizeof(int)) +
// sizeof(VSSShmsegHeader))

constexpr size_t VssStorageInitialCount = 200000L / VssFactor;
constexpr size_t VssStorageInitialSize = VssStorageInitialCount * sizeof(VSSEntry);
constexpr size_t VssStorageIncrementCount = 20000L / VssFactor;
constexpr size_t VssStorageIncrement = VssStorageIncrementCount * sizeof(VSSEntry);

// (average list length = 4)
constexpr size_t VssTableInitialSize = 50000 * sizeof(int);
constexpr size_t VssTableIncrement = 5000 * sizeof(int);

constexpr size_t VssInitialSize = sizeof(VSSShmsegHeader) + VssStorageInitialSize + VssTableInitialSize;

constexpr size_t VssIncrement = VssTableIncrement + VssStorageIncrement;

constexpr size_t VssSize(size_t entries)
{
  return (entries * sizeof(VSSEntry)) + (entries / 4 * sizeof(int)) + sizeof(VSSShmsegHeader);
}

class QueryContext_vss
{
 public:
  QueryContext_vss() : currentScn(0)
  {
    txns.reset(new std::set<VER_t>());
  }
  QueryContext_vss(const QueryContext& qc);
  VER_t currentScn;
  boost::shared_ptr<std::set<VER_t>> txns;
};

class VSSImplScaled
{
 public:
  static VSSImplScaled* makeVSSImpl(unsigned key, off_t size, const MasterSegmentTable::ShmemType shmType,
                                    bool readOnly = false);

  inline void grow(unsigned key, off_t size)
#ifndef NDBUG
  {
    fVSS.grow(key, size);
  }
#else
  {
    int rc = fVSS.grow(key, size);
    idbassert(rc == 0);
  }
#endif
  inline void makeReadOnly()
  {
    fVSS.setReadOnly();
  }
  inline void clear(unsigned key, off_t size)
  {
    fVSS.clear(key, size);
  }
  inline void swapout(BRMShmImpl& rhs)
  {
    fVSS.swap(rhs);
    rhs.destroy();
  }
  inline unsigned key() const
  {
    return fVSS.key();
  }

  inline VSSShmsegHeader* get() const
  {
    return reinterpret_cast<VSSShmsegHeader*>(fVSS.fMapreg.get_address());
  }

  static size_t shmemType2ContinuesIdx(const MasterSegmentTable::ShmemType shmemType);

 private:
  VSSImplScaled(unsigned key, off_t size, bool readOnly = false);
  ~VSSImplScaled();
  VSSImplScaled(const VSSImplScaled& rhs);
  VSSImplScaled& operator=(const VSSImplScaled& rhs);

  BRMShmImpl fVSS;

  static std::vector<std::mutex> vssImplMutexes_;
  static std::vector<VSSImplScaled*> vssImplInstances_;
};

class VBBM;
class ExtentMap;

// WIP TBC
/** @brief The Version Substitution Structure (VSS)
 *
 * At a high level, the VSS maintains a table that associates an LBID with
 * a version number and 2 flags that indicate whether or not the block
 * identified by the <LBID, VerID> pair exists in the main database files
 * or the Version Buffer.  The VSS's main purpose is to resolve the version of
 * a specified block the caller can safely use given that there may be concurrent
 * writes to that block.
 *
 * As implemented, it is a hash table and a set of lists that exist in
 * shared memory.  The hash table is keyed by LBID, and
 * each valid entry points to the head of a unique list.  Each list element
 * contains the LBID, VerID, & the two flags that encapsulate "an entry in the
 * VSS table".  Every list contains all elements that collide on that hash table
 * entry that points to it, "load factor" has no bearing on performance,
 * and lists can grow arbitrarily large.
 * Technically lookups are O(n), but in normal circumstances it'll
 * be constant time.  As things are right now, we expect there to be about
 * 200k VSS entries.  The hash table is sized such that on average there will be 4
 * entries per list when it's at capacity.
 *
 * The memory management & structure manipulation code is nearly identical
 * to that in the VBBM, so any bugs found here are likely there as well.
 *
 * Shared memory is managed using code similar to the ExtentMap & VBBM.  When
 * the shared memory segment needs to grow, it is write-locked, a new one
 * is created, the contents are reinserted to the new one, the key is
 * registered, and the old segment is destroyed when the last reference to it
 * is detached.
 */

// class VSS : public Undoable
// {
//  public:
//   enum OPS
//   {
//     NONE,
//     READ,
//     WRITE
//   };

//   VSS(const MasterSegmentTable::ShmemType vssId = MasterSegmentTable::VSSSegment_);
//   ~VSS() = default;

//   bool isLocked(const LBID_t lbid, VER_t txnID) const;
//   void removeEntry(LBID_t lbid, VER_t verID, std::vector<LBID_t>* flushList);

//   // Note, the use_vbbm switch should be used for unit testing the VSS only
//   // void removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm = true);
//   LbidVersionVec removeEntryFromDB(const LBID_t& lbid);
//   int lookup(LBID_t lbid, const QueryContext_vss&, VER_t txnID, VER_t* outVer, bool* vbFlag,
//              bool vbOnly = false) const;
//   /// Returns the version in the main DB files
//   VER_t getCurrentVersion(LBID_t lbid, bool* isLocked) const;  // returns the ver in the main DB files

//   /// Returns the highest version in the version buffer, less than max
//   VER_t getHighestVerInVB(LBID_t lbid, VER_t max) const;

//   /// returns true if that block is in the version buffer, false otherwise
//   bool isVersioned(LBID_t lbid, VER_t version) const;

//   void setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag);
//   void insert_(LBID_t, VER_t, bool vbFlag, bool locked, bool loading = false);

//   void commit(VER_t txnID);
//   void getUncommittedLBIDs(VER_t txnID, std::vector<LBID_t>& lbids);
//   void getUnlockedLBIDs(BlockList_t& lbids);
//   void lock_(OPS op);
//   void release(OPS op);
//   void setReadOnly();

//   int checkConsistency(const VBBM& vbbm, ExtentMap& em) const;
//   int size() const;
//   bool hashEmpty() const;
//   void getCurrentTxnIDs(std::set<VER_t>& txnList) const;

//   void clear_();
//   void load(std::string filename);
//   void save(std::string filename);

//   static size_t partition(const LBID_t v)
//   {
//     utils::Hasher_r h;
//     return h(&v, sizeof(LBID_t)) % VssFactor;
//   }

// #ifdef BRM_DEBUG
//   int getShmid() const;
// #endif

//   bool isEmpty(bool doLock = true);

//   /* Bug 2293.  VBBM will use this fcn to determine whether a block is
//    * currently in use. */
//   bool isEntryLocked(LBID_t lbid, VER_t verID) const;
//   bool isTooOld(LBID_t lbid, VER_t verID) const;

//   // int getCurrentSize() const
//   // {
//   //   assert(vss);
//   //   return vss->currentSize;
//   // }

//  private:
//   VSS(const VSS&) = delete;
//   VSS& operator=(const VSS&);

//   uint32_t vssIdx2ShmkeyBase(const MasterSegmentTable::ShmemType shmemType) const
//   {
//     assert(shmemType <= MasterSegmentTable::extVSS8);
//     switch (shmemType)
//     {
//       case MasterSegmentTable::extVSS1: return fShmKeys.KEYRANGE_VSS_BASE1;
//       case MasterSegmentTable::extVSS2: return fShmKeys.KEYRANGE_VSS_BASE2;
//       case MasterSegmentTable::extVSS3: return fShmKeys.KEYRANGE_VSS_BASE3;
//       case MasterSegmentTable::extVSS4: return fShmKeys.KEYRANGE_VSS_BASE4;
//       case MasterSegmentTable::extVSS5: return fShmKeys.KEYRANGE_VSS_BASE5;
//       case MasterSegmentTable::extVSS6: return fShmKeys.KEYRANGE_VSS_BASE6;
//       case MasterSegmentTable::extVSS7: return fShmKeys.KEYRANGE_VSS_BASE7;
//       case MasterSegmentTable::extVSS8: return fShmKeys.KEYRANGE_VSS_BASE8;

//       default: return fShmKeys.KEYRANGE_VSS_BASE;
//     }
//     return 0;
//   }
//   struct VSSShmsegHeader* vss;
//   int* hashBuckets;
//   VSSEntry* storage;
//   bool r_only;
//   static boost::mutex mutex;  // @bug5355 - made mutex static

//   key_t currentVSSShmkey;
//   int vssShmid;
//   MSTEntry* vssShminfo;
//   MasterSegmentTable mst;

//   uint32_t chooseShmkey_(const MasterSegmentTable::ShmemType shmemType) const;
//   void growVSS_();
//   void growForLoad_(uint32_t elementCount);
//   void initShmseg();
//   void copyVSS(VSSShmsegHeader* dest);

//   int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
//   void _insert(VSSEntry& e, VSSShmsegHeader* dest, int* destTable, VSSEntry* destStorage,
//                bool loading = false);
//   ShmKeys fShmKeys;

//   utils::Hasher hasher;
//   static std::vector<std::mutex> vssMutexes_;
//   const MasterSegmentTable::ShmemType vssShmType_;
//   VSSImplScaled* vssImpl_;
// };

class VSSCluster
{
 public:
  enum OPS
  {
    NONE,
    READ,
    WRITE
  };

  struct Header
  {
    int magic;
    uint32_t entries;
  };

  VSSCluster();

  ~VSSCluster() = default;

  bool isLocked(const LBID_t lbid, VER_t txnID) const;
  void removeEntry(LBID_t lbid, VER_t verID, std::vector<LBID_t>* flushList);

  // Note, the use_vbbm switch should be used for unit testing the VSS only
  // void removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm = true);
  LbidVersionVec removeEntryFromDB(const LBID_t& lbid);
  LbidVersionVec removeEntriesFromDB(const LBIDRange_v& lbidRanges);

  // void confirmChanges();
  void confirmChangesIfNeededAndRelease(const VSSCluster::OPS op);
  void undoChangesIfNeededAndRelease(const VSSCluster::OPS op);
  void releaseIfNeeded(const OPS op);

  void undoChanges();

  int lookup(LBID_t lbid, const QueryContext_vss&, VER_t txnID, VER_t* outVer, bool* vbFlag,
             bool vbOnly = false) const;
  /// Returns the version in the main DB files
  VER_t getCurrentVersion(LBID_t lbid, bool* isLocked) const;          // returns the ver in the main DB files
  VER_t getCurrentVersionWExtLock(LBID_t lbid, bool* isLocked) const;  // returns the ver in the main DB files

  /// Returns the highest version in the version buffer, less than max
  VER_t getHighestVerInVB(LBID_t lbid, VER_t max, const bool lockShard) const;

  /// returns true if that block is in the version buffer, false otherwise
  bool isVersioned(LBID_t lbid, VER_t version) const;

  void setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag);
  void insert_(LBID_t, VER_t, bool vbFlag, bool locked, bool loading = false);

  void commit(VER_t txnID);
  void getUncommittedLBIDs(VER_t txnID, std::vector<LBID_t>& lbids);
  void getUnlockedLBIDs(BlockList_t& lbids);
  void lock_(OPS op);
  void lock_(const LBID_t lbid, const OPS op);

  void release(const LBID_t lbid, const OPS op);
  void releaseIfNeeded(const LBID_t lbid, const OPS op);
  void release(const OPS op);

  // void setReadOnly();

  int checkConsistency(const VBBM& vbbm, ExtentMap& em) const;
  bool hashEmpty() const;
  void getCurrentTxnIDs(std::set<VER_t>& txnList);

  void clear_();
  void load(const std::string& filename);
  void save(const std::string& filenamesPrefix);

  size_t shardCount() const  // vss_.size() replacement
  {
    return vssShards_.size();
  }

  static size_t partition(const LBID_t v)
  {
    utils::Hasher_r h;
    return h(&v, sizeof(LBID_t)) % VssFactor;
  }

#ifdef BRM_DEBUG
  int getShmid() const;
#endif

  bool isEmpty(const LBID_t lbid, const bool lockShard);

  /* Bug 2293.  VBBM will use this fcn to determine whether a block is
   * currently in use. */
  bool isEntryLocked(LBID_t lbid, VER_t verID) const;
  bool isTooOld(const LBID_t lbid, const VER_t verID);

  // int getCurrentSize() const
  // {
  //   assert(vss);
  //   return vss->currentSize;
  // }
  std::vector<std::unique_ptr<VSSShard>> vssShards_;

 private:
  VSSCluster(const VSSCluster&) = delete;
  VSSCluster& operator=(const VSSCluster&);

  ShmKeys fShmKeys;

  utils::Hasher hasher;
  static std::vector<std::mutex> vssMutexes_;
  VSSImplScaled* vssImpl_;  // TBD WIP
};

class VSSShard : public Undoable
{
 public:
  VSSShard(const MasterSegmentTable::ShmemType vssId = MasterSegmentTable::VSSSegment_);
  ~VSSShard() = default;

  bool isLocked(const LBID_t lbid, VER_t txnID) const;
  void removeEntry(LBID_t lbid, VER_t verID, std::vector<LBID_t>* flushList);

  // Note, the use_vbbm switch should be used for unit testing the VSS only
  // void removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm = true);
  LbidVersionVec removeEntryFromDB(const LBID_t& lbid);
  // LbidVersionVec removeEntriesFromDB(const LBIDRange_v& lbidRanges);

  int lookup(LBID_t lbid, const QueryContext_vss&, VER_t txnID, VER_t* outVer, bool* vbFlag,
             bool vbOnly = false) const;
  /// Returns the version in the main DB files
  VER_t getCurrentVersion(LBID_t lbid, bool* isLocked) const;  // returns the ver in the main DB files

  /// Returns the highest version in the version buffer, less than max
  VER_t getHighestVerInVB(LBID_t lbid, VER_t max) const;

  /// returns true if that block is in the version buffer, false otherwise
  bool isVersioned(LBID_t lbid, VER_t version) const;

  void setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag);
  void insert_(LBID_t, VER_t, bool vbFlag, bool locked, bool loading = false);

  void commit(VER_t txnID);
  void getUncommittedLBIDs(VER_t txnID, std::vector<LBID_t>& lbids);
  void getUnlockedLBIDs(BlockList_t& lbids);
  void lock_(const VSSCluster::OPS op, std::mutex& vssMutex);
  void release(const VSSCluster::OPS op);

  // void setReadOnly();

  int checkConsistency(const VBBM& vbbm, ExtentMap& em) const;
  int size() const;
  bool hashEmpty() const;
  void getCurrentTxnIDs(std::set<VER_t>& txnList);

  void clear_();
  // void load(const char* readBufPtr, const uint32_t entries);
  uint32_t save(idbdatafile::IDBDataFile& file);

  static size_t partition(const LBID_t v)
  {
    utils::Hasher_r h;
    return h(&v, sizeof(LBID_t)) % VssFactor;
  }

#ifdef BRM_DEBUG
  int getShmid() const;
#endif

  bool isEmpty(const bool lockShard);

  /* Bug 2293.  VBBM will use this fcn to determine whether a block is
   * currently in use. */
  bool isEntryLocked(LBID_t lbid, VER_t verID) const;
  bool isTooOld(const LBID_t lbid, const VER_t verID) const;

  bool shardIsLocked() const
  {
    return currentOpType_ != VSSCluster::NONE;
  }

  void lockShard(const VSSCluster::OPS op)
  {
    currentOpType_ = op;
  }

  void releaseShard(const VSSCluster::OPS op)
  {
    currentOpType_ = VSSCluster::NONE;
  }

  void growForLoad_(uint32_t elementCount);

 private:
  VSSShard(const VSSShard&) = delete;
  VSSShard& operator=(const VSSShard&);

  struct VSSShmsegHeader* vss = nullptr;
  int* hashBuckets = nullptr;
  VSSEntry* storage = nullptr;
  bool r_only = false;
  static boost::mutex mutex;  // @bug5355 - made mutex static

  key_t currentVSSShmkey = -1;
  // int vssShmid = 0;
  MSTEntry* vssShminfo = nullptr;
  MasterSegmentTable mst;

  uint32_t vssIdx2ShmkeyBase_(const MasterSegmentTable::ShmemType shmemType) const
  {
    assert(shmemType <= MasterSegmentTable::extVSS8);
    switch (shmemType)
    {
      case MasterSegmentTable::extVSS1: return fShmKeys.KEYRANGE_VSS_BASE1;
      case MasterSegmentTable::extVSS2: return fShmKeys.KEYRANGE_VSS_BASE2;
      case MasterSegmentTable::extVSS3: return fShmKeys.KEYRANGE_VSS_BASE3;
      case MasterSegmentTable::extVSS4: return fShmKeys.KEYRANGE_VSS_BASE4;
      case MasterSegmentTable::extVSS5: return fShmKeys.KEYRANGE_VSS_BASE5;
      case MasterSegmentTable::extVSS6: return fShmKeys.KEYRANGE_VSS_BASE6;
      case MasterSegmentTable::extVSS7: return fShmKeys.KEYRANGE_VSS_BASE7;
      case MasterSegmentTable::extVSS8: return fShmKeys.KEYRANGE_VSS_BASE8;

      default: return fShmKeys.KEYRANGE_VSS_BASE;
    }
    return 0;
  }

  uint32_t chooseShmkey_(const MasterSegmentTable::ShmemType shmemType) const;
  void growVSS_();
  void initShmseg();
  void copyVSS(VSSShmsegHeader* dest);

  int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
  void _insert(VSSEntry& e, VSSShmsegHeader* dest, int* destTable, VSSEntry* destStorage,
               bool loading = false);
  ShmKeys fShmKeys;

  utils::Hasher hasher;
  static std::mutex vssMutex_;
  const MasterSegmentTable::ShmemType vssShmType_;
  VSSImplScaled* vssImpl_ = nullptr;
  VSSCluster::OPS currentOpType_ = VSSCluster::NONE;
};

}  // namespace BRM