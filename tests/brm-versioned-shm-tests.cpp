/* Copyright (C) 2026 MariaDB Corporation

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

/* Tests for BRMVersionedShmImplT, the versioned copy-on-write shared memory
   segment every BRM structure now lives in.

   Both area policies are covered, because they differ in exactly the places
   that are easy to get wrong: a managed segment records its own size inside
   the image and so has to be copied at its source's size and grown afterwards,
   while a raw image records nothing and is created at the size wanted straight
   away. Every test therefore runs twice, once per policy, through a harness
   that says how to stamp a recognizable value into an area and how to read it
   back.

   Nothing here needs a running BRM, or a MasterSegmentTable, or any lock: the
   machinery under test is self-contained, and that is the point of testing it
   directly. What it does need is shared memory objects of its own, so every
   test gets a key range nobody else uses - ctest runs these in parallel
   processes - and takes the leftovers of a previous crashed run off its names
   before it starts.
 */

#include <sys/stat.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <algorithm>

#include <gtest/gtest.h>

#include "brmshmimpl.h"
#include "shmkeys.h"

using namespace BRM;

namespace
{
/* Well clear of everything ShmKeys hands out: the structure ranges are
   0x10000 through 0x60000 within a BRM_UID's block and the system keys are up
   at 0xf8000000 and above, so 0xe0000 within a block nothing else uses is
   free whatever BRM_UID happens to be. */
constexpr unsigned TestKeyBase = 0x7ffe0000;
/* One range per test, each holding a metadata area and every data area slot. */
constexpr unsigned TestKeySpan = 0x10;
static_assert(2 + DataAreaSlots <= TestKeySpan, "a test key range must hold every slot");

enum TestRange
{
  RangePublish,
  RangePinHoldsOldArea,
  RangeGrow,
  RangeDiscard,
  RangeCrash,
  RangeIdWrap,
  RangeSlotReuse,
  RangeConcurrent,
  RangeConcurrentWriters,
  RangeUpdateOwner,
  RangeCrashTimeout,
  RangeNestedOuter,
  RangeNestedInner,
  RangeCount
};

/* Each test-and-policy pair gets its own range, so the two instantiations of a
   typed test do not tread on one another either. */
unsigned keyBaseFor(TestRange range, bool managed)
{
  return TestKeyBase + (static_cast<unsigned>(range) * 2 + (managed ? 0u : 1u)) * TestKeySpan;
}

/* The inode behind a data area slot's object (or 0 if there is none) to
 * check does it was remapped. */
ino_t slotInode(unsigned key)
{
  std::string path = "/dev/shm/" + ShmKeys::keyToName(key);
  struct stat st;

  if (stat(path.c_str(), &st) != 0)
    return 0;

  return st.st_ino;
}

constexpr size_t PayloadWords = 256;

/* A boost managed segment as the data area, as the extent map uses it.
 *
 * The payload is a named object, found the way the real readers find theirs:
 * find_no_lock(), because the lock find() would take lives inside the image
 * and a writer memcpys that image whole.
 */
struct ManagedSeg
{
  using Impl = BRMVersionedShmImpl;
  static constexpr bool Managed = true;
  static constexpr off_t InitialSize = 64 * 1024;
  static constexpr off_t GrowBy = 128 * 1024;
  /* Bigger than an area of InitialSize can possibly hold, so that reading it
     back proves the space growUpdate() added is really usable. */
  static constexpr size_t TailBytes = 96 * 1024;

  struct Payload
  {
    uint32_t words[PayloadWords];
  };

  static void stamp(Impl& shm, uint32_t marker)
  {
    auto* p = shm.segment()->find_or_construct<Payload>("payload")();

    for (auto& w : p->words)
      w = marker;
  }

  static bool marker(const Impl::DataAreaPin& pin, uint32_t& out)
  {
    if (!pin)
      return false;

    auto* p = pin->find_no_lock<Payload>("payload").first;

    if (!p)
      return false;

    out = p->words[0];

    for (auto w : p->words)
      if (w != out)
        return false;

    return true;
  }

  static void stampTail(Impl& shm, uint8_t byte)
  {
    auto* t = shm.segment()->find_or_construct<char>("tail")[TailBytes](char(byte));
    memset(t, byte, TailBytes);
  }

  static bool tail(const Impl::DataAreaPin& pin, uint8_t byte)
  {
    auto found = pin->find_no_lock<char>("tail");

    if (!found.first || found.second != TailBytes)
      return false;

    for (size_t i = 0; i < TailBytes; ++i)
      if (uint8_t(found.first[i]) != byte)
        return false;

    return true;
  }

  static off_t sizeOf(const Impl::DataAreaPin& pin)
  {
    return pin->get_size();
  }
};

/* A raw image as the data area, as the free list, VBBM, VSS and the copy locks use it.
 *
 * The payload is the front of the image and the tail is whatever follows it,
 * which is nothing at all until the area has been grown.
 */
struct RawSeg
{
  using Impl = BRMVersionedRawShmImpl;
  static constexpr bool Managed = false;
  static constexpr off_t InitialSize = PayloadWords * sizeof(uint32_t);
  static constexpr off_t GrowBy = 3 * InitialSize;

  static void stamp(Impl& shm, uint32_t marker)
  {
    auto* words = static_cast<uint32_t*>(shm.image());

    for (size_t i = 0; i < PayloadWords; ++i)
      words[i] = marker;
  }

  static bool marker(const Impl::DataAreaPin& pin, uint32_t& out)
  {
    auto* words = static_cast<uint32_t*>(Impl::imageIn(pin));

    if (!words)
      return false;

    out = words[0];

    for (size_t i = 0; i < PayloadWords; ++i)
      if (words[i] != out)
        return false;

    return true;
  }

  static void stampTail(Impl& shm, uint8_t byte)
  {
    auto* image = static_cast<uint8_t*>(shm.image());
    off_t size = shm.imageSize();

    /* There is no tail to stamp until the area has grown past the size it was
       created at, and imageSize() is 0 when there is no area at all. Saying so
       is what keeps the subtraction below from wrapping - which the compiler
       will not take on trust, and rightly: -Wstringop-overflow rejects the
       memset outright without this. Leaving the tail unstamped rather than
       asserting keeps the test honest, because tail() reports a too-small area
       as a failure. */
    if (!image || size <= InitialSize)
      return;

    memset(image + InitialSize, byte, static_cast<size_t>(size - InitialSize));
  }

  static bool tail(const Impl::DataAreaPin& pin, uint8_t byte)
  {
    auto* image = static_cast<uint8_t*>(Impl::imageIn(pin));
    off_t size = pin->reg.get_size();

    if (size <= InitialSize)
      return false;

    for (off_t i = InitialSize; i < size; ++i)
      if (image[i] != byte)
        return false;

    return true;
  }

  static off_t sizeOf(const Impl::DataAreaPin& pin)
  {
    return pin->reg.get_size();
  }
};

template <typename SegmentType>
class VersionedShmTest : public ::testing::Test
{
 protected:
  using Impl = typename SegmentType::Impl;
  using Pin = typename Impl::DataAreaPin;

  /* Claims this test's key range and makes sure nothing is left on it. A test
     that crashed, or one that deliberately abandoned an update, leaves objects
     behind that a later run would otherwise open and read. */
  unsigned use(TestRange range)
  {
    fKeyBase = keyBaseFor(range, SegmentType::Managed);
    removeNames();
    return fKeyBase;
  }

  // A second segment for a test that needs two of them at once.
  unsigned useAlso(TestRange range)
  {
    fKeyBase2 = keyBaseFor(range, SegmentType::Managed);
    removeNamesIn(fKeyBase2);
    return fKeyBase2;
  }

  void TearDown() override
  {
    removeNames();
  }

  void removeNames()
  {
    removeNamesIn(fKeyBase);
    removeNamesIn(fKeyBase2);
  }

  static void removeNamesIn(unsigned base)
  {
    if (!base)
      return;

    for (unsigned i = 0; i < 2 + DataAreaSlots; ++i)
      bi::shared_memory_object::remove(ShmKeys::keyToName(base + i).c_str());
  }

  // The same arithmetic the segment does, so that a test can look at a slot's
  // object from the outside. Kept here rather than reached for in the segment
  // because it is part of the layout being tested, not an implementation
  // detail the test should be told
  unsigned dataAreaKey(uint64_t id) const
  {
    return dataAreaKeyIn(fKeyBase, id);
  }

  static unsigned dataAreaKeyIn(unsigned base, uint64_t id)
  {
    return base + 2 + unsigned(id % DataAreaSlots);
  }

  static uint32_t markerFor(uint64_t id)
  {
    return 0xc0de0000u | uint32_t(id);
  }

  // One whole write transaction: copy, stamp, publish.
  uint64_t publish(Impl& shm, uint32_t marker)
  {
    shm.beginUpdate(SegmentType::InitialSize);
    SegmentType::stamp(shm, marker);
    return shm.publishUpdate();
  }

  uint32_t readMarker(const Pin& pin)
  {
    uint32_t out = 0;
    EXPECT_TRUE(SegmentType::marker(pin, out)) << "the pinned area holds no single marker value";
    return out;
  }

  // The whole image of a pinned area, for the byte-identical comparisons.
  std::vector<char> snapshot(const Pin& pin)
  {
    off_t size = SegmentType::sizeOf(pin);
    std::vector<char> bytes(static_cast<size_t>(size));
    memcpy(bytes.data(), Impl::imageIn(pin), size_t(size));
    return bytes;
  }

  unsigned fKeyBase = 0;
  unsigned fKeyBase2 = 0;
};

using SegmentType = ::testing::Types<ManagedSeg, RawSeg>;
TYPED_TEST_SUITE(VersionedShmTest, SegmentType);

// 1. The basic transaction: a reader sees what a publication put there.
TYPED_TEST(VersionedShmTest, PublishIsVisibleToAReader)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangePublish);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  EXPECT_EQ(0u, writer.currentId()) << "a fresh segment has nothing published";
  EXPECT_FALSE(writer.pin()) << "and nothing to pin";

  EXPECT_EQ(1u, this->publish(writer, this->markerFor(1)));
  EXPECT_EQ(1u, writer.currentId());

  // A second segment object on the same keys, which is what another process is
  // as far as any of this is concerned
  typename SegmentType::Impl reader(base, SegmentType::InitialSize);
  EXPECT_EQ(1u, reader.mappedId());
  EXPECT_EQ(this->markerFor(1), this->readMarker(reader.pin()));

  EXPECT_EQ(2u, this->publish(writer, this->markerFor(2)));
  EXPECT_EQ(this->markerFor(2), this->readMarker(reader.pin()))
      << "a reader that pins again follows the publication";
}

// 2. A pin taken before a publication keeps reading the area it was taken on,
//    which is the whole reason readers need no lock
TYPED_TEST(VersionedShmTest, APinKeepsTheOldAreaReadable)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangePinHoldsOldArea);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  this->publish(writer, this->markerFor(1));

  typename SegmentType::Impl reader(base, SegmentType::InitialSize);
  const auto old = reader.pin();
  ASSERT_TRUE(old != nullptr);
  ASSERT_EQ(this->markerFor(1), this->readMarker(old));

  this->publish(writer, this->markerFor(2));

  // The publication did not touch the pinned area, and did not unmap it
  // either: both are mapped at once, at different addresses
  EXPECT_EQ(this->markerFor(1), this->readMarker(old)) << "the publication reached into a pinned area";

  const auto fresh = reader.pin();
  EXPECT_EQ(this->markerFor(2), this->readMarker(fresh));
  EXPECT_NE(SegmentType::Impl::imageIn(old), SegmentType::Impl::imageIn(fresh))
      << "the two areas should be two mappings, not one";

  // Still good after the reader has moved on, because the pin is what holds
  // the mapping rather than the segment object's own bookkeeping
  EXPECT_EQ(this->markerFor(1), this->readMarker(old));
}

// 3. Growing the copy before publishing it, which is safe precisely because
//    nobody else can see the copy
TYPED_TEST(VersionedShmTest, GrowUpdateEnlargesTheCopyBeforePublishing)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeGrow);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  this->publish(writer, this->markerFor(1));

  off_t before = SegmentType::sizeOf(writer.pin());

  writer.beginUpdate(SegmentType::InitialSize);
  SegmentType::stamp(writer, this->markerFor(2));
  writer.growUpdate(SegmentType::GrowBy);
  EXPECT_GE(writer.imageSize(), before + SegmentType::GrowBy);

  // Everything derived from the area before the grow is stale now - a managed
  // segment is unmapped and mapped again to be resized - so both of these
  // re-derive their pointers. The real callers do the same
  SegmentType::stampTail(writer, 0xa5);
  SegmentType::stamp(writer, this->markerFor(2));
  uint64_t id = writer.publishUpdate();
  EXPECT_EQ(2u, id);

  typename SegmentType::Impl reader(base, SegmentType::InitialSize);
  const auto pin = reader.pin();
  ASSERT_TRUE(pin != nullptr);
  EXPECT_GE(SegmentType::sizeOf(pin), before + SegmentType::GrowBy);
  EXPECT_EQ(this->markerFor(2), this->readMarker(pin));
  EXPECT_TRUE(SegmentType::tail(pin, 0xa5)) << "the space the grow added did not survive the publication";

  // And the enlarged area is itself copyable: the next transaction starts from
  // it, at its size, with its contents.
  this->publish(writer, this->markerFor(3));
  const auto next = reader.pin();
  EXPECT_GE(SegmentType::sizeOf(next), before + SegmentType::GrowBy);
  EXPECT_EQ(this->markerFor(3), this->readMarker(next));
  EXPECT_TRUE(SegmentType::tail(next, 0xa5)) << "the copy lost the tail of its source";
}

// 4. A discarded update leaves the published area exactly as it was
TYPED_TEST(VersionedShmTest, DiscardUpdateLeavesThePublishedAreaAlone)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeDiscard);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  this->publish(writer, this->markerFor(1));

  const auto published = writer.pin();
  ASSERT_TRUE(published != nullptr);
  const std::vector<char> before = this->snapshot(published);

  writer.beginUpdate(SegmentType::InitialSize);
  EXPECT_TRUE(writer.inUpdate());
  SegmentType::stamp(writer, this->markerFor(99));
  writer.discardUpdate();
  EXPECT_FALSE(writer.inUpdate());

  EXPECT_EQ(1u, writer.currentId()) << "a discarded update must publish nothing";
  EXPECT_EQ(before, this->snapshot(published)) << "the discarded copy was written into the published area";
  EXPECT_EQ(this->markerFor(1), this->readMarker(writer.pin()));

  // The burnt id is simply used again by the next transaction.
  EXPECT_EQ(2u, this->publish(writer, this->markerFor(2)));
  EXPECT_EQ(this->markerFor(2), this->readMarker(writer.pin()));
}

// 5. A writer that dies mid-update. The published area is untouched and stays
//    readable, which is the property the whole scheme exists for
TYPED_TEST(VersionedShmTest, AnAbandonedUpdatePublishesNothing)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeCrash);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  this->publish(writer, this->markerFor(1));

  {
    // A process that dies holding an open update, as far as the metadata area
    // can tell: the copy is written but never published and the destructor
    // that would discard it never runs.
    //
    // Kept reachable rather than leaked outright, so that a sanitizer build
    // does not report the mapping as a leak. The update mutex stays held for
    // the rest of this test, which is why nothing below starts another update
    static std::vector<std::shared_ptr<void>> abandoned;
    auto dead = std::make_shared<typename SegmentType::Impl>(base, SegmentType::InitialSize);
    dead->beginUpdate(SegmentType::InitialSize);
    SegmentType::stamp(*dead, this->markerFor(99));
    abandoned.push_back(dead);
  }

  EXPECT_EQ(1u, writer.currentId()) << "an update nobody published must not be visible";

  typename SegmentType::Impl reader(base, SegmentType::InitialSize);
  EXPECT_EQ(this->markerFor(1), this->readMarker(reader.pin()))
      << "the abandoned copy was written into the published area";
}

// The other half of the same story: the next writer along is told what
// happened rather than waiting for ever on a mutex whose owner is gone.
//
// Disabled because it takes UpdateMutexTimeoutSeconds - a compile-time
// constant with no override - to reach its assertion. Run it by hand with
// --gtest_also_run_disabled_tests after touching the update mutex or the
// timeout
TYPED_TEST(VersionedShmTest, DISABLED_AnAbandonedUpdateMakesTheNextWriterTimeOut)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeCrashTimeout);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  this->publish(writer, this->markerFor(1));

  static std::vector<std::shared_ptr<void>> abandoned;
  auto dead = std::make_shared<typename SegmentType::Impl>(base, SegmentType::InitialSize);
  dead->beginUpdate(SegmentType::InitialSize);
  abandoned.push_back(dead);

  try
  {
    writer.beginUpdate(SegmentType::InitialSize);
    FAIL() << "beginUpdate() returned although the update mutex was held by a dead writer";
  }
  catch (const std::runtime_error& e)
  {
    EXPECT_TRUE(strstr(e.what(), "timed out") != nullptr) << e.what();
  }

  // The timeout left no update open, so the segment is still usable
  EXPECT_FALSE(writer.inUpdate());
  EXPECT_EQ(this->markerFor(1), this->readMarker(writer.pin()));
}

// 6. Ids go round the slots and keep meaning the right area
TYPED_TEST(VersionedShmTest, IdsWrapAroundTheSlots)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeIdWrap);

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);
  typename SegmentType::Impl reader(base, SegmentType::InitialSize);

  uint64_t publications = DataAreaSlots + 2;

  for (uint64_t id = 1; id <= publications; ++id)
  {
    EXPECT_EQ(id, this->publish(writer, this->markerFor(id))) << "ids must be consecutive";
    EXPECT_EQ(this->markerFor(id), this->readMarker(reader.pin())) << "after publication " << id;
    EXPECT_EQ(this->dataAreaKey(id), reader.key()) << "publication " << id << " landed in the wrong slot";
  }

  // Which is to say the ids past the first lap share slots with the first.
  EXPECT_EQ(this->dataAreaKey(1), this->dataAreaKey(1 + DataAreaSlots));
}

// 7. A slot's object is written into again, unless somebody is in it
//
// This is what the announcements are for and the whole reason they exist:
// making a new object costs the kernel a page of allocation and a fault for
// every page of the area, measured at ten times what writing into a resident
// one costs. Neither outcome is visible through the API - both produce a
// correct publication - so this watches the slot's inode, which survives a
// rewrite and changes on a recreate.
///
// A process has a segment for each of the extent map, its index, the free
// list, the VSS, the VBBM and the copy locks, and reading one while holding
// another is ordinary - grabEMAndIndexForRead() pins the extent map and then
// pins its index inside that. Each segment has a reader table of its own, so
// the thread has to be announced in both of them.
//
// While the nesting depth was per thread rather than per segment, the inner
// pin here looked like a nested read of the outer segment, announced nowhere,
// and the inner segment's writer was free to write over the area being read
TYPED_TEST(VersionedShmTest, AReadNestedInsideAnotherSegmentIsStillAnnounced)
{
  using SegmentType = TypeParam;
  using Pin = typename SegmentType::Impl::DataAreaPin;

  unsigned outerBase = this->use(RangeNestedOuter);
  unsigned innerBase = this->useAlso(RangeNestedInner);

  typename SegmentType::Impl outer(outerBase, SegmentType::InitialSize);
  typename SegmentType::Impl inner(innerBase, SegmentType::InitialSize);

  uint64_t outerId = 1;
  uint64_t innerId = 1;
  this->publish(outer, this->markerFor(outerId));
  this->publish(inner, this->markerFor(innerId));

  unsigned watched = this->dataAreaKeyIn(innerBase, innerId);
  ino_t created = slotInode(watched);

  if (created == 0)
    GTEST_SKIP() << "cannot see " << ShmKeys::keyToName(watched)
                 << " in /dev/shm, so slot reuse cannot be observed here";

  // The outer segment is held for all of this, exactly as the extent map is
  // held across a read of its index
  Pin outerPin = outer.pin();
  ASSERT_TRUE(outerPin);

  Pin innerPin = inner.pin();
  ASSERT_TRUE(innerPin);
  uint64_t heldId = innerId;

  // A full lap of the inner segment. The reader inside it must be seen.
  for (unsigned i = 0; i < DataAreaSlots; ++i)
    this->publish(inner, this->markerFor(++innerId));

  EXPECT_NE(created, slotInode(watched))
      << "the inner segment's slot was written into although a reader was in it";
  EXPECT_EQ(this->markerFor(heldId), this->readMarker(innerPin))
      << "the nested reader's area was written into underneath it";

  // And the outer segment's own announcement still works the way it did.
  EXPECT_EQ(this->markerFor(outerId), this->readMarker(outerPin));

  innerPin.reset();
  outerPin.reset();

  // Both let go: every slot of the inner segment goes back to being written
  // into in place, so a whole lap leaves all of their objects where they were.
  ino_t settled[DataAreaSlots];

  for (unsigned i = 0; i < DataAreaSlots; ++i)
    settled[i] = slotInode(innerBase + 2 + i);

  for (unsigned i = 0; i < DataAreaSlots; ++i)
    this->publish(inner, this->markerFor(++innerId));

  for (unsigned i = 0; i < DataAreaSlots; ++i)
    EXPECT_EQ(settled[i], slotInode(innerBase + 2 + i))
        << "slot " << i << " should have been reused once the reader left";

  EXPECT_EQ(this->markerFor(innerId), this->readMarker(inner.pin()));
}

TYPED_TEST(VersionedShmTest, ASlotIsWrittenIntoAgainUnlessAReaderIsInIt)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeSlotReuse);
  using Pin = typename SegmentType::Impl::DataAreaPin;

  typename SegmentType::Impl writer(base, SegmentType::InitialSize);

  uint64_t id = 1;
  this->publish(writer, this->markerFor(id));
  unsigned watched = this->dataAreaKey(id);
  ino_t created = slotInode(watched);

  if (created == 0)
    GTEST_SKIP() << "cannot see " << ShmKeys::keyToName(watched)
                 << " in /dev/shm, so slot reuse cannot be observed here";

  // A whole lap with nothing pinned. The slot comes back round and is written
  // into in place.
  for (unsigned i = 0; i < DataAreaSlots; ++i)
    this->publish(writer, this->markerFor(++id));

  ASSERT_EQ(id, 1 + DataAreaSlots);
  EXPECT_EQ(created, slotInode(watched)) << "an undisturbed slot should have been reused, not recreated";
  EXPECT_EQ(this->markerFor(id), this->readMarker(writer.pin()));

  // Now a reader that stops inside the slot. The next lap finds it announcing
  // the version in there, gives up on the object and makes a fresh one -
  // leaving the reader its own
  Pin stuck = writer.pin();
  uint64_t stuckId = id;

  for (unsigned i = 0; i < DataAreaSlots; ++i)
    this->publish(writer, this->markerFor(++id));

  ino_t recreated = slotInode(watched);
  EXPECT_NE(created, recreated) << "the slot was written into although a reader was announcing it";
  EXPECT_EQ(this->markerFor(stuckId), this->readMarker(stuck))
      << "the stuck reader's area was written into underneath it";
  EXPECT_EQ(this->markerFor(id), this->readMarker(writer.pin()));

  // And once it lets go, the slot goes back to being written into again.
  stuck.reset();
  ino_t last = slotInode(watched);

  for (unsigned i = 0; i < DataAreaSlots; ++i)
    this->publish(writer, this->markerFor(++id));

  EXPECT_EQ(last, slotInode(watched)) << "a released slot should have been reused again";
  EXPECT_EQ(this->markerFor(id), this->readMarker(writer.pin()));
}

// 8. An open update belongs to the thread that opened it
//
// Asserted directly rather than raced for, because the race is hard to hit and
// the property is simple. Every wrapper decides whether to open an update, and
// whether to publish one, by asking inUpdate(). Answered per process, a second
// thread is told yes about the first thread's update: it then skips opening
// one - and so skips the exclusion that opening one takes - works on that
// copy, and publishes it. That is two threads publishing the same update, and
// it crashed PrimProc in adoptLocked() when the second found the handle
// already reset
TYPED_TEST(VersionedShmTest, AnOpenUpdateBelongsToTheThreadThatOpenedIt)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeUpdateOwner);

  typename SegmentType::Impl shm(base, SegmentType::InitialSize);
  this->publish(shm, this->markerFor(1));

  std::mutex m;
  std::condition_variable cv;
  bool opened = false;
  bool release = false;

  std::thread owner(
      [&]()
      {
        shm.beginUpdate(SegmentType::InitialSize);
        SegmentType::stamp(shm, this->markerFor(2));

        {
          std::lock_guard<std::mutex> lk(m);
          opened = true;
        }
        cv.notify_all();

        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return release; });
        lk.unlock();

        shm.publishUpdate();
      });

  {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return opened; });
  }

  // The update is open, and it is not this thread's.
  EXPECT_FALSE(shm.inUpdate()) << "a thread was shown an update another thread opened";

  {
    std::lock_guard<std::mutex> lk(m);
    release = true;
  }
  cv.notify_all();
  owner.join();

  // The owner published it, and this thread is not in an update afterwards.
  EXPECT_FALSE(shm.inUpdate());
  EXPECT_EQ(this->markerFor(2), this->readMarker(shm.pin()));
}

// 9. Writers in several threads of one process
//
// An open update is state on an impl that is a process-wide singleton, so the
// question "am I in an update?" has to be answered per thread. It was not, and
// PrimProc crashed for it: a second thread asked, was told yes about the first
// thread's update, skipped opening one of its own - and so skipped the
// exclusion that opening one takes - worked on that copy, and then published
// it. Two threads publishing the same update, and whichever got to
// adoptLocked() second found the handle already reset.
//
// ioManager keeps one DBRM for its whole pool and every thread of it calls
// DBRM::releaseLBIDRange(), which takes the copy locks for writing, so this is
// the shape that actually happens.
TYPED_TEST(VersionedShmTest, ConcurrentWritersEachGetTheirOwnUpdate)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeConcurrentWriters);

  // One object for every writing thread, as a shared DBRM really holds it.
  typename SegmentType::Impl shm(base, SegmentType::InitialSize);
  this->publish(shm, this->markerFor(1));

  constexpr unsigned Writers = 4;
  constexpr uint64_t Each = 15;

  std::atomic<uint64_t> published{0};
  std::atomic<uint64_t> failures{0};
  std::vector<uint64_t> ids(Writers * Each, 0);
  std::vector<std::thread> writers;

  for (unsigned w = 0; w < Writers; ++w)
  {
    writers.emplace_back(
        [&, w]()
        {
          for (uint64_t i = 0; i < Each; ++i)
          {
            try
            {
              /* Exactly what the wrappers do: ask whether an update is open
                 and only open one if not. Answered per process rather than
                 per thread, this is where the second thread used to walk into
                 the first one's transaction. */
              if (!shm.inUpdate())
                shm.beginUpdate(SegmentType::InitialSize);

              SegmentType::stamp(shm, 0xc0de0000u | uint32_t(w * Each + i + 2));

              /* Held open a moment on purpose. The window between opening an
                 update and publishing it is a few instructions otherwise, and
                 the bug this is here for needs a second thread to look at
                 inUpdate() while the first is inside that window - which it
                 would almost never manage on its own. */
              std::this_thread::sleep_for(std::chrono::microseconds(200));

              ids[w * Each + i] = shm.publishUpdate();
              ++published;
            }
            catch (...)
            {
              ++failures;
            }
          }
        });
  }

  for (auto& t : writers)
    t.join();

  EXPECT_EQ(0u, failures.load()) << "a writer threw; the segment should have made it wait, not fail";
  EXPECT_EQ(Writers * Each, published.load());

  // Every publication got an id of its own, and they run without a gap.
  std::vector<uint64_t> sorted(ids);
  std::sort(sorted.begin(), sorted.end());
  EXPECT_EQ(sorted.end(), std::adjacent_find(sorted.begin(), sorted.end()))
      << "two threads published the same id, so they shared one update";
  EXPECT_EQ(2u, sorted.front());
  EXPECT_EQ(1 + Writers * Each, sorted.back());

  // And the segment is still usable, with the last publication readable.
  uint32_t marker = 0;
  EXPECT_TRUE(SegmentType::marker(shm.pin(), marker));
  EXPECT_FALSE(shm.inUpdate());
}

// 10. Readers pinning while a writer publishes, which is the arrangement the
// whole scheme is for. Every area a reader gets hold of has to be one
// single publication's work: never half of one and half of another, and
// never one being written into
TYPED_TEST(VersionedShmTest, ConcurrentPinsNeverSeeAHalfWrittenArea)
{
  using SegmentType = TypeParam;
  unsigned base = this->use(RangeConcurrent);

  // One segment object shared by the writing thread and the reading ones,
  // which is how a process really holds these: the impls are process-wide and
  // pin() is documented safe against a writer in another thread
  typename SegmentType::Impl shm(base, SegmentType::InitialSize);
  this->publish(shm, this->markerFor(1));

  constexpr uint64_t Publications = 200;
  constexpr unsigned Readers = 4;

  std::atomic<bool> done{false};
  std::atomic<uint64_t> torn{0};
  std::atomic<uint64_t> empty{0};
  std::atomic<uint64_t> reads{0};
  std::vector<std::thread> readers;

  for (unsigned i = 0; i < Readers; ++i)
  {
    readers.emplace_back(
        [&]()
        {
          while (!done.load(std::memory_order_relaxed))
          {
            const auto pin = shm.pin();

            if (!pin)
            {
              ++empty;
              continue;
            }

            uint32_t marker = 0;

            // Uniform says the area is one publication's work, and the marker
            // says which - so a value outside the range ever published is a
            // torn read too
            if (!SegmentType::marker(pin, marker) || marker < 0xc0de0001u || marker > 0xc0de0000u + Publications)
              ++torn;

            ++reads;
          }
        });
  }

  for (uint64_t id = 2; id <= Publications; ++id)
    this->publish(shm, this->markerFor(id));

  done.store(true, std::memory_order_relaxed);

  for (auto& t : readers)
    t.join();

  EXPECT_EQ(0u, torn.load()) << "a reader saw an area that was not one publication's work";
  EXPECT_EQ(0u, empty.load()) << "a reader found nothing published although something was";
  EXPECT_GT(reads.load(), 0u) << "the readers never ran, so this proved nothing";
  EXPECT_EQ(Publications, shm.currentId());
  EXPECT_EQ(this->markerFor(Publications), this->readMarker(shm.pin()));
}

}  // namespace