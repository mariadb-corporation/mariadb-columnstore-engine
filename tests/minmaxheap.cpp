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

#include <gtest/gtest.h>
#include <random>
#include <set>
#include "utils/structs/minmaxheap.h"   // header that contains template class MinMaxHeap

using IntHeap = MinMaxHeap<int>;          // default std::less comparator
using IntInvHeap = MinMaxHeap<int, std::greater<int>>; // inverted ordering

// ---------- Basic behaviour -------------------------------------------------

TEST(MinMaxHeap, InitiallyEmpty) {
    IntHeap h;
    EXPECT_TRUE(h.empty());
    EXPECT_EQ(h.size(), 0);
    EXPECT_THROW(h.getMin(), std::out_of_range);
    EXPECT_THROW(h.getMax(), std::out_of_range);
    EXPECT_THROW(h.popMin(), std::out_of_range);
    EXPECT_THROW(h.popMax(), std::out_of_range);
}

TEST(MinMaxHeap, SingleElement) {
    IntHeap h;
    h.push(42);
    EXPECT_FALSE(h.empty());
    EXPECT_EQ(h.size(), 1);
    EXPECT_EQ(h.getMin(), 42);
    EXPECT_EQ(h.getMax(), 42);
    h.popMin();
    EXPECT_TRUE(h.empty());
}

TEST(MinMaxHeap, OrderAfterInsertions) {
    IntHeap h;
    h.push(10);
    h.push(5);
    h.push(20);
    EXPECT_EQ(h.getMin(), 5);
    EXPECT_EQ(h.getMax(), 20);
}

TEST(MinMaxHeap, PopMinAsc) {
    IntHeap h;
    for (int v : {7, 1, 4, 9, 3}) h.push(v);

    std::vector<int> asc;
    while (!h.empty()) {
        asc.push_back(h.getMin());
        h.popMin();
    }
    EXPECT_EQ(asc, (std::vector<int>{1, 3, 4, 7, 9}));
}

TEST(MinMaxHeap, PopMaxDesc) {
    IntHeap h;
    for (int v : {7, 1, 4, 9, 3}) h.push(v);

    std::vector<int> desc;
    while (!h.empty()) {
        desc.push_back(h.getMax());
        h.popMax();
    }
    EXPECT_EQ(desc, (std::vector<int>{9, 7, 4, 3, 1}));
}

TEST(MinMaxHeap, MixedPops) {
    IntHeap h;
    for (int i = 1; i <= 5; ++i) h.push(i); // heap contains 1..5

    // min(1) → {2,3,4,5}
    EXPECT_EQ(h.getMin(), 1);
    h.popMin();
    // max(5) → {2,3,4}
    EXPECT_EQ(h.getMax(), 5);
    h.popMax();
    // min(2) → {3,4}
    EXPECT_EQ(h.getMin(), 2);
    h.popMin();
    // max(4) → {3}
    EXPECT_EQ(h.getMax(), 4);
    h.popMax();
    // last element
    EXPECT_EQ(h.getMin(), 3);
    EXPECT_EQ(h.getMax(), 3);
    h.popMin();

    EXPECT_TRUE(h.empty());
}

// ---------- Comparator test --------------------------------------------------

TEST(MinMaxHeap, CustomComparator) {
    IntInvHeap h; // std::greater ⇒ heap treats greater elements as *smaller*
    for (int v : {3, 1, 4}) h.push(v);
    // now "minimum" is actually the *largest* integer because of comparator
    EXPECT_EQ(h.getMin(), 4);
    EXPECT_EQ(h.getMax(), 1);
}

// ---------- Robustness & stress ---------------------------------------------

TEST(MinMaxHeap, RandomAgainstMultiset) {
    constexpr int N = 100;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(-100000, 100000);

    IntHeap heap;
    std::multiset<int> ref;

    for (int i = 0; i < N; ++i) {
        if (ref.empty() || dist(rng) % 2) { // 50 % push
            int val = dist(rng);
            heap.push(val);
            ref.insert(val);
        } else if (dist(rng) % 2) {         // 25 % popMin
            int refMin = *ref.begin();
            EXPECT_EQ(heap.getMin(), refMin);
            heap.popMin();
            ref.erase(ref.begin());
        } else {                            // 25 % popMax
            int refMax = *ref.rbegin();
            EXPECT_EQ(heap.getMax(), refMax);
            heap.popMax();
            auto it = std::prev(ref.end());
            ref.erase(it);
        }
        // invariants after each mutation
        if (!ref.empty()) {
            EXPECT_EQ(heap.getMin(), *ref.begin());
            EXPECT_EQ(heap.getMax(), *ref.rbegin());
        } else {
            EXPECT_TRUE(heap.empty());
        }
    }
}
