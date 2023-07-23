/* Copyright (C) 2021 MariaDB Corporation

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

#include <cstdint>
#include <iostream>
#include <gtest/gtest.h>

#include "extentmap.h"
#include "vbbm.h"
#include "vss.h"

using namespace BRM;
using namespace std;

using VssPtrVector = std::vector<std::unique_ptr<VSS>>;

void keepalive(int signum)
{
  cerr << "Yes, it's still going..." << endl;
  alarm(290);
}

class VSSTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    ShmKeys BrmKeys;

    auto brmKeyAttributes = {BrmKeys.KEYRANGE_CL_BASE,
                             BrmKeys.KEYRANGE_EXTENTMAP_BASE,
                             BrmKeys.KEYRANGE_EMFREELIST_BASE,
                             BrmKeys.KEYRANGE_VBBM_BASE,
                             BrmKeys.KEYRANGE_VBBM_BASE,
                             BrmKeys.KEYRANGE_VSS_BASE,
                             BrmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE,
                             BrmKeys.KEYRANGE_VSS_BASE1,
                             BrmKeys.KEYRANGE_VSS_BASE2,
                             BrmKeys.KEYRANGE_VSS_BASE3,
                             BrmKeys.KEYRANGE_VSS_BASE4,
                             BrmKeys.KEYRANGE_VSS_BASE5,
                             BrmKeys.KEYRANGE_VSS_BASE6,
                             BrmKeys.KEYRANGE_VSS_BASE7,
                             BrmKeys.KEYRANGE_VSS_BASE8,
                             BrmKeys.MST_SYSVKEY};
    brmKeyAttributes_ = brmKeyAttributes;
  };

  void TearDown() override
  {
    ShmKeys BrmKeys;

    // clean shmem up
    for (auto base : brmKeyAttributes_)
    {
      for (decltype(BrmKeys.KEYRANGE_VSS_BASE1) i = 0; i < ShmKeys::KEYRANGE_SIZE; ++i)
      {
        bi::shared_memory_object::remove(ShmKeys::keyToName(base + i).c_str());
      }
    }
  }
  std::vector<uint32_t> brmKeyAttributes_;
};

TEST_F(VSSTest, lock)
{
  VSS vss(*MasterSegmentTable::VssShmemTypes.begin());
  vss.lock_(VSS::READ);
}

// TEST_F(VSSTest, lock1)
// {
//   MasterSegmentTable mst;
//   VSS vss(*MasterSegmentTable::VssShmemTypes.begin());
//   vss.lock_(VSS::WRITE);
// }

// TEST_F(VSSTest, overall)
// {
//   VssPtrVector vss;
//   for (auto s : MasterSegmentTable::VssShmemTypes)
//   {
//     vss.emplace_back(std::unique_ptr<VSS>(new VSS(s)));
//   }
//   VBBM vbbm;
//   ExtentMap em;
//   int err;
//   VER_t verID;
//   bool vbFlag;
//   vector<LBID_t> lbids;
//   LBIDRange range;

//   void (*oldsig)(int);

//   oldsig = signal(SIGALRM, keepalive);
//   alarm(290);

//   cerr << endl << "VSS test (this can take awhile)" << endl;

//   for (auto& v : vss)
//   {
//     v->lock_(VSS::READ);
//     v->release(VSS::READ);
//   }

//   size_t iterations = 15;
//   cerr << "step 1/5: insert 2 entries for " << iterations << " LBIDs & test lookup logic" << endl;

//   range.start = 0;
//   range.size = 200000;

//   for (auto& v : vss)
//   {
//     v->lock_(VSS::WRITE);
//   }

//   for (size_t lbid = 0; lbid < iterations; lbid++)
//   {
//     auto bucket = VSS::getBucket(lbid);
//     VER_t ver = lbid + 1;
//     // it is int so watch out for wraparound writing tests
//     auto prevSize = vss[bucket]->getCurrentSize();
//     vss[bucket]->insert_(lbid, ver, (lbid % 2 ? true : false), true);
//     assert(prevSize + 1 == vss[bucket]->getCurrentSize());
//     vss[bucket]->confirmChanges();

//     // if (i == 0)
//     // {
//     //   range.start = 0;
//     //   range.size = 1;
//     //   CPPUNIT_ASSERT(vss.isLocked(range));
//     // }
//     // else if (i == 1)
//     // {
//     //   range.start = 1;
//     //   CPPUNIT_ASSERT(!vss.isLocked(range));
//     // }

//     verID = lbid + 1;
//     BRM::QueryContext versionInfo;
//     VER_t outVer = lbid + 10;
//     err = vss[bucket]->lookup(lbid, versionInfo, verID, &outVer, &vbFlag);
//     cout << "err " << err << " outVer " << outVer << " vbFlag " << vbFlag << endl;
//     // ASSERT_EQ(err, 0);
//     // ASSERT_EQ(outVer, i + 1);
//     // ASSERT_TRUE((vbFlag && (i % 2)) || (!vbFlag && !(i % 2)));
//     verID = lbid + 10;
//     err = vss[bucket]->lookup(lbid, versionInfo, verID, &outVer, &vbFlag);
//     cout << "err " << err << " outVer " << outVer << " vbFlag " << vbFlag << endl;

//     // ASSERT_EQ(err, 0);
//     // ASSERT_EQ(outVer, (i % 2 ? i + 1 : i));
//     // ASSERT_EQ(vbFlag, true);

//     if (lbid > 0)
//     {
//       verID = lbid - 1;
//       err = vss[bucket]->lookup(lbid, versionInfo, verID, &outVer, &vbFlag);
//       cout << "err " << err << " outVer " << outVer << " vbFlag " << vbFlag << endl;
//       // ASSERT_EQ(err, -1);
//     }
//   }

//   range.start = 0;
//   range.size = 200000;
//   // CPPUNIT_ASSERT(vss.isLocked(range));
//   // for (size_t i = 0; auto& v : vss)
//   // {
//   //   v->save(string("VSSImage" + to_string(i)));
//   // }
//   // for (size_t i = 0; auto& v : vss)
//   // {
//   //   v->load(string("VSSImage" + to_string(i)));
//   // }

//   // this loop actually breaks the locked -> !vbFlag invariant
//   // switch it back afterward
//   cerr << "step 2/5: flip the vbFlag on half of the entries & test lookup logic" << endl;

//   // for (i = 0; i < iterations; i++)
//   // {
//   //   vss.setVBFlag(i, i + 1, (i % 2 ? false : true));
//   //   vss.confirmChanges();
//   // }

//   // for (i = 0; i < iterations; i++)
//   // {
//   //   verID = i + 10;
//   //   err = vss.lookup(i, verID, i + 1, vbFlag);
//   //   CPPUNIT_ASSERT(err == 0);
//   //   CPPUNIT_ASSERT(verID == i + 1);
//   //   CPPUNIT_ASSERT((!vbFlag && (i % 2)) || (vbFlag && !(i % 2)));
//   // }

//   // cerr << "step 3/5: some clean up" << endl;

//   // for (i = 0; i < iterations; i++)
//   // {
//   //   vss.setVBFlag(i, i + 1, (i % 2 ? true : false));
//   //   vss.confirmChanges();
//   // }

//   // // there are 2 entries for every txnid at this point, one
//   // // which is locked, one which isn't.  That's technically a violation,
//   // // so we have to get rid of the unlocked ones for the next test.
//   // for (i = 0; i < iterations; i++)
//   // {
//   //   vss.removeEntry(i, i);
//   //   vss.confirmChanges();
//   // }

//   // // speed up the next step!
//   // for (i = iterations / 50; i < iterations; i++)
//   // {
//   //   vss.removeEntry(i, i + 1);
//   //   vss.confirmChanges();
//   // }

//   // cerr << "step 4/5: get \'uncommitted\' LBIDs, commit, and test lookup logic" << endl;

//   // for (i = 1; i < iterations / 50; i++)
//   // {
//   //   bool ex = false;

//   //   if (i % 2 == 0)
//   //   {
//   //   }
//   //   else
//   //   {
//   //     LBID_t lbid;

//   //     vss.getUncommittedLBIDs(i, lbids);
//   //     CPPUNIT_ASSERT(lbids.size() == 1);
//   //     lbid = *(lbids.begin());
//   //     CPPUNIT_ASSERT(lbid == i - 1);
//   //     verID = i + 10;
//   //     err = vss.lookup(i - 1, verID, i, vbFlag);
//   //     CPPUNIT_ASSERT(err == 0);
//   //     CPPUNIT_ASSERT(verID == i);
//   //     CPPUNIT_ASSERT(vbFlag == false);

//   //     err = vss.lookup(i - 1, verID, 0, vbFlag);
//   //     CPPUNIT_ASSERT(err == -1);

//   //     vss.commit(i);
//   //     vss.confirmChanges();
//   //     verID = i + 10;
//   //     err = vss.lookup(i - 1, verID, 0, vbFlag);
//   //     CPPUNIT_ASSERT(err == 0);
//   //     CPPUNIT_ASSERT(verID == i);
//   //     CPPUNIT_ASSERT(vbFlag == false);
//   //   }
//   // }

//   // cerr << "step 5/5: final clean up" << endl;

//   // CPPUNIT_ASSERT(vss.size() == iterations / 50);
//   // range.start = 0;
//   // range.size = iterations / 50;
//   // vss.removeEntriesFromDB(range, vbbm, false);
//   // vbbm.confirmChanges();
//   // vss.confirmChanges();

//   // CPPUNIT_ASSERT(vss.size() == 0);
//   // CPPUNIT_ASSERT(vss.hashEmpty());
//   for (auto& v : vss)
//   {
//     v->release(VSS::WRITE);
//   }

//   alarm(0);
//   signal(SIGALRM, oldsig);
//   // Config::deleteInstanceMap();
// }