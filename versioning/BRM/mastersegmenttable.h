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
 * $Id: mastersegmenttable.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file
 *
 * class MasterSegmentTable
 *
 * The MasterSegmentTable regulates access to the shared memory segments
 * used by the BRM classes and provides the means for detecting when to resize
 * a segment and when it has been relocated (due to resizing).
 *
 * XXXPAT: We should make a cleanup class here also.
 */

#pragma once

#include <stdexcept>
#include <sys/types.h>
#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/scoped_ptr.hpp>

#include "rwlock.h"
#include "shmkeys.h"

namespace BRM
{
struct MSTEntry
{
  key_t tableShmkey;
  int allocdSize;
  int currentSize;
  MSTEntry();
};

class MasterSegmentTableImpl
{
 public:
  ~MasterSegmentTableImpl();
  static MasterSegmentTableImpl* makeMasterSegmentTableImpl(int key, int size);

  static void refreshShm()
  {
    if (fInstance)
    {
      delete fInstance;
      fInstance = NULL;
    }
  }

  boost::interprocess::shared_memory_object fShmobj;
  boost::interprocess::mapped_region fMapreg;

 private:
  MasterSegmentTableImpl(int key, int size);
  MasterSegmentTableImpl(const MasterSegmentTableImpl& rhs);
  MasterSegmentTableImpl& operator=(const MasterSegmentTableImpl& rhs);

  static boost::mutex fInstanceMutex;
  static MasterSegmentTableImpl* fInstance;
};

/** @brief This class regulates access to the BRM tables in shared memory
 *
 * This class regulates access to the BRM tables in shared memory
 */
class MasterSegmentTable
{
 public:
  /** @brief Constructor.
   * @note Throws runtime_error on a semaphore-related error.
   */
  MasterSegmentTable();
  ~MasterSegmentTable();

  enum ShmemTypes
  {
    EMTable_ = 0,
    EMFreeList_,
    VBBMSegment_,
    VSSSegment_,
    CLSegment_,
    EMIndex_,
    extVSS1,
    extVSS2,
    extVSS3,
    extVSS4,
    extVSS5,
    extVSS6,
    extVSS7,
    extVSS8,
  };
  /// specifies the Extent Map table
  static const int EMTable = 0;
  /// specifies the Extent Map's Freelist table
  static const int EMFreeList = 1;
  /// specifies the Version Buffer Block Map segment
  static const int VBBMSegment = 2;
  /// specifies the Version Substitution Structure segment
  static const int VSSSegment = 3;
  /// specifies the copy lock segment
  static const int CLSegment = 4;
  /// specifies the EM Index segment
  static const int EMIndex = 5;
  /// the number of tables currently defined
  static const int nTables = 14;

  // static const constexpr int

  /** @brief This function gets the specified table.
   *
   * This function gets the specified table and grabs the
   * associated read lock.
   * @param num EMTable, EMFreeList, or VBBMTable
   * @param block If false, it won't block.
   * @note throws invalid_argument if num is outside the valid range
   * and runtime_error on a semaphore-related error.
   * @return If block == true, it always returns the specified MSTEntry;
   * if block == false, it can also return NULL if it could not grab
   * the table's lock.
   */
  MSTEntry* getTable_read(int num, bool block = true) const;

  /** @brief This function gets the specified table.
   *
   * This function gets the specified table and grabs the
   * associated write lock.
   * @param num EMTable, EMFreeList, or VBBMTable
   * @param block If false, it won't block.
   * @note throws invalid_argument if num is outside the valid range
   * and runtime_error on a semaphore-related error.
   * @return If block == true, it always returns the specified MSTEntry;
   * if block == false, it can also return NULL if it could not grab
   * the table's lock.
   */
  MSTEntry* getTable_write(int num, bool block = true) const;

  /** @brief Upgrade a read lock to a write lock.
   *
   * Upgrade a read lock to a write lock.  This is not an atomic
   * operation.
   * @param num The table the caller holds the read lock to.
   */
  void getTable_upgrade(int num) const;

  /** @brief Downgrade a write lock to a read lock.
   *
   * Downgrade a write lock to a read lock.  This is an atomic
   * operation.
   * @param num The table the caller holds the write lock to.
   */
  void getTable_downgrade(int num) const;

  /** @brief This function unlocks the specified table.
   *
   * This function unlocks the specified table.
   *
   * @param num EMTable, EMFreeList, or VBBMTable
   * @note throws invalid_argument if num is outside the valid range
   * and runtime_error on a semaphore-related error.
   */
  void releaseTable_read(int num) const;

  /** @brief This function unlocks the specified table.
   *
   * This function unlocks the specified table.
   *
   * @param num EMTable, EMFreeList, or VBBMTable
   * @note throws invalid_argument if num is outside the valid range
   * and runtime_error on a semaphore-related error.
   */
  void releaseTable_write(int num) const;

 private:
  MasterSegmentTable(const MasterSegmentTable& mst);
  MasterSegmentTable& operator=(const MasterSegmentTable& mst);

  int shmid;
  mutable boost::scoped_ptr<rwlock::RWLock> rwlock[nTables];

  static const int MSTshmsize = nTables * sizeof(MSTEntry);

  /// indexed by EMTable, EMFreeList, and VBBMTable
  MSTEntry* fShmDescriptors;

  void makeMSTSegment();
  void initMSTData();
  ShmKeys fShmKeys;
  uint32_t RWLockKeys[14] = {fShmKeys.KEYRANGE_EXTENTMAP_BASE, fShmKeys.KEYRANGE_EMFREELIST_BASE,
                             fShmKeys.KEYRANGE_VBBM_BASE,      fShmKeys.KEYRANGE_VSS_BASE,
                             fShmKeys.KEYRANGE_CL_BASE,        fShmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE,
                             fShmKeys.KEYRANGE_VSS_BASE1,      fShmKeys.KEYRANGE_VSS_BASE2,
                             fShmKeys.KEYRANGE_VSS_BASE3,      fShmKeys.KEYRANGE_VSS_BASE4,
                             fShmKeys.KEYRANGE_VSS_BASE5,      fShmKeys.KEYRANGE_VSS_BASE6,
                             fShmKeys.KEYRANGE_VSS_BASE7,      fShmKeys.KEYRANGE_VSS_BASE8};
  MasterSegmentTableImpl* fPImpl;
};

}  // namespace BRM