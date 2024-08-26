/* Copyright (C) 2024 MariaDB Corporation.

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
#include <stdexcept>
#include <iostream>
#include <fstream>
using namespace std;
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>

#include <cppunit/extensions/HelperMacros.h>

#include "bytestream.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;
#include "mcs_decimal.h"

class ByteStreamTestSuite : public CppUnit::TestFixture
{
  CPPUNIT_TEST_SUITE(ByteStreamTestSuite);

  CPPUNIT_TEST(bs_1);
  CPPUNIT_TEST(bs_1_1);
  CPPUNIT_TEST(bs_1_2);
  CPPUNIT_TEST(bs_2);
  CPPUNIT_TEST(bs_3);
  CPPUNIT_TEST(bs_4);
  CPPUNIT_TEST_EXCEPTION(bs_5_1, std::underflow_error);
  CPPUNIT_TEST_EXCEPTION(bs_5_2, std::underflow_error);
  CPPUNIT_TEST_EXCEPTION(bs_5_3, std::underflow_error);
  CPPUNIT_TEST_EXCEPTION(bs_5_4, std::underflow_error);
  CPPUNIT_TEST_EXCEPTION(bs_5_5, std::underflow_error);
  CPPUNIT_TEST_EXCEPTION(bs_5_6, std::underflow_error);
  CPPUNIT_TEST(bs_6);
  CPPUNIT_TEST(bs_7);
  CPPUNIT_TEST(bs_8);
  CPPUNIT_TEST_EXCEPTION(bs_9, std::underflow_error);
  CPPUNIT_TEST(bs_10);
  CPPUNIT_TEST(bs_12);
  CPPUNIT_TEST(bs_13);
  CPPUNIT_TEST(bs_14);
  CPPUNIT_TEST(bs_15);
  CPPUNIT_TEST(bs_16);
  CPPUNIT_TEST_SUITE_END();

 private:
  ByteStream::byte b;
  ByteStream::doublebyte d;
  ByteStream::quadbyte q;
  ByteStream::octbyte o;

  uint8_t u8;
  uint16_t u16;
  uint32_t u32;
  uint64_t u64;
  uint128_t u128;
  int8_t i8;
  int16_t i16;
  int32_t i32;
  int64_t i64;
  int128_t i128;

  ByteStream bs;
  ByteStream bs1;

  ByteStream::byte* bap;
  ByteStream::byte* bap1;

  int len;

 public:
  void setUp()
  {
    bs.reset();
    bs1.reset();
    bap = 0;
    bap1 = 0;
  }

  void tearDown()
  {
    bs.reset();
    bs1.reset();
    delete[] bap;
    bap = 0;
    delete[] bap1;
    bap1 = 0;
  }

  void bs_1()
  {
    bs.reset();

    o = 0xdeadbeefbadc0ffeLL;
    bs << o;
    CPPUNIT_ASSERT(bs.length() == 8);
    o = 0;
    bs >> o;
    CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
    CPPUNIT_ASSERT(bs.length() == 0);

    q = 0xdeadbeef;
    bs << q;
    CPPUNIT_ASSERT(bs.length() == 4);
    q = 0;
    bs >> q;
    CPPUNIT_ASSERT(q == 0xdeadbeef);
    CPPUNIT_ASSERT(bs.length() == 0);

    d = 0xf00f;
    bs << d;
    CPPUNIT_ASSERT(bs.length() == 2);
    d = 0;
    bs >> d;
    CPPUNIT_ASSERT(d == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 0);

    b = 0x0f;
    bs << b;
    CPPUNIT_ASSERT(bs.length() == 1);
    b = 0;
    bs >> b;
    CPPUNIT_ASSERT(b == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 0);

    o = 0xdeadbeefbadc0ffeLL;
    bs << o;
    CPPUNIT_ASSERT(bs.length() == 8);
    o = 0;

    q = 0xdeadbeef;
    bs << q;
    CPPUNIT_ASSERT(bs.length() == 12);
    q = 0;

    d = 0xf00f;
    bs << d;
    CPPUNIT_ASSERT(bs.length() == 14);
    d = 0;

    b = 0x0f;
    bs << b;
    CPPUNIT_ASSERT(bs.length() == 15);
    b = 0;

    bs >> o;
    CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
    CPPUNIT_ASSERT(bs.length() == 7);
    bs >> q;
    CPPUNIT_ASSERT(q == 0xdeadbeef);
    CPPUNIT_ASSERT(bs.length() == 3);
    bs >> d;
    CPPUNIT_ASSERT(d == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 1);
    bs >> b;
    CPPUNIT_ASSERT(b == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 0);
  }

  void bs_1_1()
  {
    bs.reset();

    o = 0xdeadbeefbadc0ffeLL;
    bs << o;
    CPPUNIT_ASSERT(bs.length() == 8);
    o = 0;

    q = 0xdeadbeef;
    bs << q;
    CPPUNIT_ASSERT(bs.length() == 12);
    q = 0;

    d = 0xf00f;
    bs << d;
    CPPUNIT_ASSERT(bs.length() == 14);
    d = 0;

    b = 0x0f;
    bs << b;
    CPPUNIT_ASSERT(bs.length() == 15);
    b = 0;

    ByteStream bbs1;
    bbs1 << bs;
    CPPUNIT_ASSERT(bbs1.length() == bs.length() + sizeof(messageqcpp::BSSizeType));
    bs.reset();
    bbs1 >> bs;
    CPPUNIT_ASSERT(bbs1.length() == 0);
    CPPUNIT_ASSERT(bs.length() == 15);

    bs >> o;
    CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
    CPPUNIT_ASSERT(bs.length() == 7);
    bs >> q;
    CPPUNIT_ASSERT(q == 0xdeadbeef);
    CPPUNIT_ASSERT(bs.length() == 3);
    bs >> d;
    CPPUNIT_ASSERT(d == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 1);
    bs >> b;
    CPPUNIT_ASSERT(b == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 0);
  }

  void bs_1_2()
  {
    bs.reset();

    i64 = -2401053089477160962;
    bs << i64;
    CPPUNIT_ASSERT(bs.length() == 8);
    i64 = 0;

    i32 = -559038737;
    bs << i32;
    CPPUNIT_ASSERT(bs.length() == 12);
    i32 = 0;

    i16 = -4081;
    bs << i16;
    CPPUNIT_ASSERT(bs.length() == 14);
    i16 = 0;

    i8 = 15;
    bs << i8;
    CPPUNIT_ASSERT(bs.length() == 15);
    i8 = 0;

    bs >> i64;
    CPPUNIT_ASSERT(i64 == -2401053089477160962);
    CPPUNIT_ASSERT(bs.length() == 7);

    bs >> i32;
    CPPUNIT_ASSERT(i32 == -559038737);
    CPPUNIT_ASSERT(bs.length() == 3);

    bs >> i16;
    CPPUNIT_ASSERT(i16 == -4081);
    CPPUNIT_ASSERT(bs.length() == 1);

    bs >> i8;
    CPPUNIT_ASSERT(i8 == 15);
    CPPUNIT_ASSERT(bs.length() == 0);
  }

  void bs_2()
  {
    int i;

    bs.reset();
    srand(time(0));

    for (i = 0; i < 10240; i++)
    {
      bs << (uint32_t)rand();
    }

    bs1 = bs;

    uint32_t q1;

    for (i = 0; i < 10240; i++)
    {
      bs >> u32;
      bs1 >> q1;
      CPPUNIT_ASSERT(u32 == q1);
    }

    bs.reset();
    bs1.reset();
  }

  void bs_3()
  {
    uint8_t ba[1024] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
    };

    bs.load(ba, 8);
    CPPUNIT_ASSERT(bs.length() == 8);
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x12);
    bs >> u16;
    CPPUNIT_ASSERT(u16 == 0x5634);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdebc9a78);

    CPPUNIT_ASSERT(bs.length() == 1);

    bs.reset();
    CPPUNIT_ASSERT(bs.length() == 0);

    bs.load(ba, 8);
    len = bs.length();
    CPPUNIT_ASSERT(len == 8);
    bap = new ByteStream::byte[len];
    // bs >> bap;
    memcpy(bap, bs.buf(), len);
    CPPUNIT_ASSERT(memcmp(ba, bap, len) == 0);
    delete[] bap;
    bap = 0;

    bs.reset();

    for (u32 = 0; u32 < 20480; u32++)
    {
      bs << u32;
    }

    len = bs.length();
    CPPUNIT_ASSERT(len == (20480 * sizeof(u32)));
    bap = new ByteStream::byte[len];
    // bs >> bap;
    memcpy(bap, bs.buf(), len);

    bs.reset();

    for (u32 = 0; u32 < 20480; u32++)
    {
      bs << u32;
    }

    len = bs.length();
    CPPUNIT_ASSERT(len == (20480 * sizeof(q)));
    bap1 = new ByteStream::byte[len];
    // bs >> bap1;
    memcpy(bap1, bs.buf(), len);

    CPPUNIT_ASSERT(memcmp(bap1, bap, len) == 0);

    delete[] bap;
    bap = 0;
    delete[] bap1;
    bap1 = 0;
    bs.reset();
  }
  void bs_4()
  {
    for (i32 = 0; i32 < 20480; i32++)
    {
      bs << i32;
    }

    ByteStream bs2(bs);
    len = bs2.length();
    CPPUNIT_ASSERT(len == (20480 * sizeof(i32)));
    bap = new ByteStream::byte[len];
    // bs2 >> bap;
    memcpy(bap, bs2.buf(), len);

    bs1 = bs2;
    len = bs1.length();
    CPPUNIT_ASSERT(len == (20480 * sizeof(i32)));
    bap1 = new ByteStream::byte[len];
    // bs1 >> bap1;
    memcpy(bap1, bs1.buf(), len);

    CPPUNIT_ASSERT(memcmp(bap1, bap, len) == 0);
    delete[] bap;
    bap = 0;
    delete[] bap1;
    bap1 = 0;
    bs.reset();
    bs1.reset();
    bs2.reset();
  }

  void bs_5_1()
  {
    bs.reset();

    u8 = 0x0f;
    bs << u8;

    for (;;)
      bs >> u32;
  }

  void bs_5_2()
  {
    bs.reset();

    u8 = 0x0f;
    bs << u8;

    for (;;)
      bs >> u16;
  }

  void bs_5_3()
  {
    bs.reset();

    u8 = 0x0f;
    bs << u8;

    for (;;)
      bs >> u8;
  }

  void bs_5_4()
  {
    bs.reset();

    i8 = 0x0f;
    bs << i8;

    for (;;)
      bs >> i32;
  }

  void bs_5_5()
  {
    bs.reset();

    i8 = 0x0f;
    bs << i8;

    for (;;)
      bs >> i16;
  }

  void bs_5_6()
  {
    bs.reset();

    i8 = 0x0f;
    bs << i8;

    for (;;)
      bs >> i8;
  }

  void bs_6()
  {
    u8 = 0x1a;
    bs << u8;
    u8 = 0x2b;
    bs << u8;
    u8 = 0x3c;
    bs << u8;

    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x1a);
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x2b);
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x3c);

    bs.reset();

    u8 = 12;
    bs << u8;
    u8 = 3;
    bs << u8;
    u8 = 0;
    bs << u8;
    u8 = 2;
    bs << u8;

    ByteStream bs3(bs);

    bs3 >> u8;
    CPPUNIT_ASSERT(u8 == 12);
    bs3 >> u8;
    CPPUNIT_ASSERT(u8 == 3);
    bs3 >> u8;
    CPPUNIT_ASSERT(u8 == 0);
    bs3 >> u8;
    CPPUNIT_ASSERT(u8 == 2);
  }

  void bs_7()
  {
    size_t i;

    bs.reset();
    bap = new ByteStream::byte[ByteStream::BlockSize * 2];
    ByteStream::byte* bapp;

    for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize; bapp++, i++)
      *bapp = 0xa5;

    bs.append(bap, ByteStream::BlockSize);
    CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 1));

    for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize; bapp++, i++)
      *bapp = 0x5a;

    bs.append(bap, ByteStream::BlockSize);
    CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 2));

    for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize * 2; bapp++, i++)
      *bapp = 0x55;

    bs.append(bap, ByteStream::BlockSize * 2);
    CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 4));
    delete[] bap;
    bap = new ByteStream::byte[bs.length()];
    // bs >> bap;
    memcpy(bap, bs.buf(), bs.length());
    bap1 = new ByteStream::byte[bs.length()];

    for (bapp = &bap1[0], i = 0; i < ByteStream::BlockSize; bapp++, i++)
      *bapp = 0xa5;

    for (i = 0; i < ByteStream::BlockSize; bapp++, i++)
      *bapp = 0x5a;

    for (i = 0; i < ByteStream::BlockSize * 2; bapp++, i++)
      *bapp = 0x55;

    CPPUNIT_ASSERT(memcmp(bap, bap1, bs.length()) == 0);
    delete[] bap;
    bap = 0;
    delete[] bap1;
    bap1 = 0;
  }

  void bs_8()
  {
    bs.reset();
    string s;
    s = "This is a test";
    bs << s;
    string s1;
    bs >> s1;
    CPPUNIT_ASSERT(s == s1);
    CPPUNIT_ASSERT(bs.length() == 0);

    ifstream ifs;
    ifs.open("../CMakeLists.txt");
    int ifs_len;
    ifs.seekg(0, ios::end);
    ifs_len = ifs.tellg();
    ifs.seekg(0, ios::beg);
    boost::scoped_array<char> buf(new char[ifs_len + 1]);
    ifs.read(buf.get(), ifs_len);
    buf[ifs_len] = 0;
    ifs.close();
    bs.reset();
    s = buf.get();
    bs << s;
    bs >> s1;
    CPPUNIT_ASSERT(s == s1);
    CPPUNIT_ASSERT(bs.length() == 0);

    u8 = 0xa5;
    bs << u8;
    u16 = 0x5aa5;
    bs << u16;
    u32 = 0xdeadbeef;
    bs << u32;
    bs << s;
    s += s1;
    bs << s;
    s += s1;
    bs << s;
    bs << u32;
    bs << u16;
    bs << u8;

    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0xa5);
    bs >> u16;
    CPPUNIT_ASSERT(u16 == 0x5aa5);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    bs >> s;
    CPPUNIT_ASSERT(s == s1);
    CPPUNIT_ASSERT(s.length() == (s1.length() * 1));
    bs >> s;
    CPPUNIT_ASSERT(s.length() == (s1.length() * 2));
    bs >> s;
    CPPUNIT_ASSERT(s.length() == (s1.length() * 3));
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    bs >> u16;
    CPPUNIT_ASSERT(u16 == 0x5aa5);
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0xa5);
    CPPUNIT_ASSERT(bs.length() == 0);
  }

  void bs_9()
  {
    bs.reset();
    // Load up a bogus string (too short)
    u32 = 100;
    bs << u32;
    bs.append(reinterpret_cast<const ByteStream::byte*>("This is a test"), 14);
    string s;
    // Should throw underflow
    bs >> s;
  }

  void bs_10()
  {
    bs.reset();
    bs1.reset();
    u32 = 0xdeadbeef;
    bs << u32;
    CPPUNIT_ASSERT(bs.length() == 4);
    CPPUNIT_ASSERT(bs1.length() == 0);
    bs.swap(bs1);
    CPPUNIT_ASSERT(bs1.length() == 4);
    CPPUNIT_ASSERT(bs.length() == 0);
    bs1 >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);

    bs.reset();
    bs1.reset();
    u32 = 0xdeadbeef;
    bs << u32;
    bs1 << u32;
    bs += bs1;
    CPPUNIT_ASSERT(bs.length() == 8);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);

    bs.reset();
    bs1.reset();
    ByteStream bs2;
    u32 = 0xdeadbeef;
    bs1 << u32;
    bs2 << u32;
    bs = bs1 + bs2;
    CPPUNIT_ASSERT(bs.length() == 8);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
  }

  void bs_12()
  {
    bs.reset();

    i128 = 10 * 100000000000000000000000000000000000000_xxl;
    bs << i128;
    CPPUNIT_ASSERT(bs.length() == 16);
    i128 = 0;
    bs >> i128;
    CPPUNIT_ASSERT(i128 == static_cast<int128_t>(10 * 100000000000000000000000000000000000000_xxl));
    CPPUNIT_ASSERT(bs.length() == 0);

    u128 = 10 * 100000000000000000000000000000000000000_xxl;
    bs << u128;
    CPPUNIT_ASSERT(bs.length() == 16);
    u128 = 0;
    bs >> u128;
    CPPUNIT_ASSERT(u128 == 10 * 100000000000000000000000000000000000000_xxl);
    CPPUNIT_ASSERT(bs.length() == 0);

    u64 = 0xdeadbeefbadc0ffeLL;
    bs << u64;
    CPPUNIT_ASSERT(bs.length() == 8);
    u64 = 0;
    bs.peek(u64);
    CPPUNIT_ASSERT(u64 == 0xdeadbeefbadc0ffeLL);
    CPPUNIT_ASSERT(bs.length() == 8);
    u64 = 0;
    bs >> u64;
    CPPUNIT_ASSERT(u64 == 0xdeadbeefbadc0ffeLL);
    CPPUNIT_ASSERT(bs.length() == 0);

    u16 = 0xf00f;
    bs << u16;
    CPPUNIT_ASSERT(bs.length() == 2);
    u16 = 0;
    bs.peek(u16);
    CPPUNIT_ASSERT(u16 == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 2);
    u16 = 0;
    bs >> u16;
    CPPUNIT_ASSERT(u16 == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 0);

    u8 = 0x0f;
    bs << u8;
    CPPUNIT_ASSERT(bs.length() == 1);
    u8 = 0;
    bs.peek(u8);
    CPPUNIT_ASSERT(u8 == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 1);
    u8 = 0;
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 0);

    u32 = 0xdeadbeef;
    bs << u32;
    CPPUNIT_ASSERT(bs.length() == 4);
    u32 = 0;

    u16 = 0xf00f;
    bs << u16;
    CPPUNIT_ASSERT(bs.length() == 6);
    u16 = 0;

    u8 = 0x0f;
    bs << u8;
    CPPUNIT_ASSERT(bs.length() == 7);
    u8 = 0;

    bs.peek(u32);
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    CPPUNIT_ASSERT(bs.length() == 7);
    u32 = 0;
    bs >> u32;
    CPPUNIT_ASSERT(u32 == 0xdeadbeef);
    CPPUNIT_ASSERT(bs.length() == 3);
    u16 = 0;
    bs.peek(u16);
    CPPUNIT_ASSERT(u16 == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 3);
    u16 = 0;
    bs >> u16;
    CPPUNIT_ASSERT(u16 == 0xf00f);
    CPPUNIT_ASSERT(bs.length() == 1);
    u8 = 0;
    bs.peek(u8);
    CPPUNIT_ASSERT(u8 == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 1);
    u8 = 0;
    bs >> u8;
    CPPUNIT_ASSERT(u8 == 0x0f);
    CPPUNIT_ASSERT(bs.length() == 0);

    string s;
    s = "This is a test";
    bs << s;
    string s1;
    bs.peek(s1);
    CPPUNIT_ASSERT(s == s1);
    CPPUNIT_ASSERT(bs.length() == s1.size() + 4);
    CPPUNIT_ASSERT(!s1.empty());
    string s2;
    bs >> s2;
    CPPUNIT_ASSERT(s == s2);
    CPPUNIT_ASSERT(bs.length() == 0);
  }

  void bs_13()
  {
    string s;
    ifstream ifs;
    ifs.open("../CMakeLists.txt");
    int ifs_len;
    ifs.seekg(0, ios::end);
    ifs_len = ifs.tellg();
    ifs.seekg(0, ios::beg);
    boost::scoped_array<char> buf(new char[ifs_len + 1]);
    ifs.read(buf.get(), ifs_len);
    buf[ifs_len] = 0;
    ifs.close();
    bs.reset();
    s = buf.get();
    bs << s;
    ofstream of("bs_13.dat");
    of << bs;
    of.close();
    ifs.open("./bs_13.dat");
    ifs.seekg(0, ios::end);
    int ifs_len1;
    ifs_len1 = ifs.tellg();
    // will be longer than orig file because string length is encoded into stream
    CPPUNIT_ASSERT((ifs_len + (int)sizeof(ByteStream::quadbyte)) == ifs_len1);
    ifs.seekg(0, ios::beg);
    boost::scoped_array<char> buf1(new char[ifs_len1]);
    bs1.reset();
    ifs >> bs1;
    ifs.close();
    CPPUNIT_ASSERT(bs.length() == bs1.length());
    string s1;
    bs1 >> s1;
    CPPUNIT_ASSERT(s == s1);
  }

  void bs_14()
  {
    ByteStream bs1(0);
    ByteStream bs2(bs1);
    CPPUNIT_ASSERT(bs2.fBuf == 0);
    ByteStream bs3(0);
    bs3 = bs1;
    CPPUNIT_ASSERT(bs3.fBuf == 0);
  }

  void bs_15()
  {
    ByteStream b1, b2, empty;
    uint8_t u8;

    CPPUNIT_ASSERT(b1 == b2);
    CPPUNIT_ASSERT(b2 == b1);
    CPPUNIT_ASSERT(b2 == empty);
    CPPUNIT_ASSERT(b1 == empty);

    CPPUNIT_ASSERT(!(b1 != b2));
    CPPUNIT_ASSERT(!(b2 != b1));
    CPPUNIT_ASSERT(!(b2 != empty));
    CPPUNIT_ASSERT(!(b1 != empty));

    b1 << "Woo hoo";

    CPPUNIT_ASSERT(b1 != b2);
    CPPUNIT_ASSERT(b2 != b1);
    CPPUNIT_ASSERT(b1 != empty);

    CPPUNIT_ASSERT(!(b1 == b2));
    CPPUNIT_ASSERT(!(b2 == b1));
    CPPUNIT_ASSERT(!(b1 == empty));

    b2 << "Woo hoo";

    CPPUNIT_ASSERT(b1 == b2);
    CPPUNIT_ASSERT(b2 == b1);
    CPPUNIT_ASSERT(!(b1 != b2));
    CPPUNIT_ASSERT(!(b2 != b1));

    b1 >> u8;

    CPPUNIT_ASSERT(b1 != b2);
    CPPUNIT_ASSERT(b2 != b1);
    CPPUNIT_ASSERT(!(b1 == b2));
    CPPUNIT_ASSERT(!(b2 == b1));

    b1 << u8;

    CPPUNIT_ASSERT(b1 != b2);
    CPPUNIT_ASSERT(b2 != b1);
    CPPUNIT_ASSERT(!(b1 == b2));
    CPPUNIT_ASSERT(!(b2 == b1));

    b2 >> u8;
    b2 << u8;

    CPPUNIT_ASSERT(b1 == b2);
    CPPUNIT_ASSERT(b2 == b1);
    CPPUNIT_ASSERT(!(b1 != b2));
    CPPUNIT_ASSERT(!(b2 != b1));
  }

  void bs_16()
  {
    int i;
    uint32_t len;

    bs.reset();
    srand(time(0));

    for (i = 0; i < 10240; i++)
    {
      bs << (ByteStream::quadbyte)rand();
    }

    boost::scoped_array<ByteStream::byte> bp(new ByteStream::byte[bs.length()]);
    ByteStream::byte* bpp = bp.get();
    boost::scoped_array<ByteStream::byte> bp1(new ByteStream::byte[bs.length()]);
    ByteStream::byte* bpp1 = bp1.get();

    len = bs.length();
    CPPUNIT_ASSERT(len == 10240 * 4);
    bs.peek(bpp);
    CPPUNIT_ASSERT(bs.length() == len);
    CPPUNIT_ASSERT(memcmp(bpp, bs.buf(), len) == 0);

    bs >> bpp1;
    CPPUNIT_ASSERT(bs.length() == 0);
    CPPUNIT_ASSERT(memcmp(bpp, bpp1, len) == 0);

    bs.reset();
  }
};

static string normServ;
static string brokeServ;
static string writeServ;
volatile static bool keepRunning;
volatile static bool isRunning;
volatile static bool leakCheck;

#define TS_NS(x) (x)
#define TS_US(x) ((x) * 1000)
#define TS_MS(x) ((x) * 1000000)

CPPUNIT_TEST_SUITE_REGISTRATION(ByteStreamTestSuite);

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <csignal>

void setupSignalHandlers()
{
  struct sigaction ign;

  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = SIG_IGN;

  sigaction(SIGPIPE, &ign, 0);
}

int main(int argc, char** argv)
{
  setupSignalHandlers();

  leakCheck = false;

  if (argc > 1 && strcmp(argv[1], "--leakcheck") == 0)
    leakCheck = true;

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry& registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest(registry.makeTest());
  bool wasSuccessful = runner.run("", false);
  return (wasSuccessful ? 0 : 1);
}
