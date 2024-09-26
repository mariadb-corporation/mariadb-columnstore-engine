/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019-2024 MariaDB Corporation

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
   MA 02110-1301, USA.
*/

#include <cstring>
#include <stdexcept>
#include <string>
#include <iostream>
#include <fstream>
#include <cassert>
#include <algorithm>
#include <cctype>
#include <inttypes.h>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/version.hpp>
using namespace boost;

#define BYTESTREAM_DLLEXPORT
#include "bytestream.h"
#undef BYTESTREAM_DLLEXPORT
#include "datatypes/mcs_int128.h"

#define DEBUG_DUMP_STRINGS_LESS_THAN 0

namespace messageqcpp
{
/* Copies only the data left to be read */
void ByteStream::doCopy(const ByteStream& rhs)
{
  BSSizeType rlen = rhs.length();

  if (fMaxLen < rlen)
  {
    delete[] fBuf;
    fBuf = new uint8_t[rlen + ISSOverhead];
    fMaxLen = rlen;
  }

  memcpy(fBuf + ISSOverhead, rhs.fCurOutPtr, rlen);
  fCurInPtr = fBuf + ISSOverhead + rlen;
  fCurOutPtr = fBuf + ISSOverhead;
  // Copy `longStrings` as well.
  longStrings = rhs.longStrings;
}

ByteStream::ByteStream(const ByteStream& rhs) : fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
  // don't need to copy an empty ByteStream
  if (rhs.fBuf)
    doCopy(rhs);
}

ByteStream::ByteStream(const SBS& rhs) : fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
  if (rhs->fBuf)
    doCopy(*rhs);
}

ByteStream& ByteStream::operator=(const ByteStream& rhs)
{
  if (this != &rhs)
  {
    if (rhs.fBuf)
      doCopy(rhs);
    else
    {
      delete[] fBuf;
      fBuf = fCurInPtr = fCurOutPtr = 0;
      fMaxLen = 0;
      // Clear `longStrings`.
      longStrings.clear();
    }
  }

  return *this;
}

ByteStream::ByteStream(BSSizeType initSize) : fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
  if (initSize > 0)
    growBuf(initSize);
}

void ByteStream::add(const uint8_t b)
{
  if (fBuf == 0 || (static_cast<BSSizeType>(fCurInPtr - fBuf) == fMaxLen + ISSOverhead))
    growBuf();

  *fCurInPtr++ = b;
}

void ByteStream::growBuf(BSSizeType toSize)
{
  if (fBuf == 0)
  {
    if (toSize == 0)
      toSize = BlockSize;
    else
      toSize = ((toSize + BlockSize - 1) / BlockSize) * BlockSize;

    fBuf = new uint8_t[toSize + ISSOverhead];
#ifdef ZERO_ON_NEW
    memset(fBuf, 0, (toSize + ISSOverhead));
#endif
    fMaxLen = toSize;
    fCurInPtr = fCurOutPtr = fBuf + ISSOverhead;
  }
  else
  {
    if (toSize == 0)
      toSize = fMaxLen + BlockSize;
    else
      toSize = ((toSize + BlockSize - 1) / BlockSize) * BlockSize;

    if (toSize <= fMaxLen)
      return;

    // Make sure we at least double the allocation
    toSize = std::max(toSize, fMaxLen * 2);

    uint8_t* t = new uint8_t[toSize + ISSOverhead];
    BSSizeType curOutOff = fCurOutPtr - fBuf;
    BSSizeType curInOff = fCurInPtr - fBuf;
    memcpy(t, fBuf, fCurInPtr - fBuf);
#ifdef ZERO_ON_NEW
    memset(t + (fCurInPtr - fBuf), 0, (toSize + ISSOverhead) - (fCurInPtr - fBuf));
#endif
    delete[] fBuf;
    fBuf = t;
    fMaxLen = toSize;
    fCurInPtr = fBuf + curInOff;
    fCurOutPtr = fBuf + curOutOff;
  }
}

std::vector<std::shared_ptr<uint8_t[]>>& ByteStream::getLongStrings()
{
  return longStrings;
}

const std::vector<std::shared_ptr<uint8_t[]>>& ByteStream::getLongStrings() const
{
  return longStrings;
}

void ByteStream::setLongStrings(const std::vector<std::shared_ptr<uint8_t[]>>& other)
{
  longStrings = other;
}

ByteStream& ByteStream::operator<<(const int8_t b)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(b) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((int8_t*)fCurInPtr) = b;
  fCurInPtr += 1;

  return *this;
}

ByteStream& ByteStream::operator<<(const uint8_t b)
{
  add(b);

  return *this;
}

ByteStream& ByteStream::operator<<(const int16_t d)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(d) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((int16_t*)fCurInPtr) = d;
  fCurInPtr += 2;

  return *this;
}

ByteStream& ByteStream::operator<<(const uint16_t d)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(d) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((uint16_t*)fCurInPtr) = d;
  fCurInPtr += 2;

  return *this;
}

ByteStream& ByteStream::operator<<(const int32_t q)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(q) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((int32_t*)fCurInPtr) = q;
  fCurInPtr += 4;

  return *this;
}

ByteStream& ByteStream::operator<<(const uint32_t q)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(q) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((uint32_t*)fCurInPtr) = q;
  fCurInPtr += 4;

  return *this;
}

ByteStream& ByteStream::operator<<(const int64_t o)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(o) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((int64_t*)fCurInPtr) = o;
  fCurInPtr += 8;

  return *this;
}

ByteStream& ByteStream::operator<<(const uint64_t o)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(o) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((uint64_t*)fCurInPtr) = o;
  fCurInPtr += 8;

  return *this;
}

ByteStream& ByteStream::operator<<(const uint128_t& h)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(h) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);
  datatypes::TSInt128::storeUnaligned(fCurInPtr, h);
  fCurInPtr += 16;
  return *this;
}

ByteStream& ByteStream::operator<<(const int128_t& h)
{
  if (fBuf == 0 || (fCurInPtr - fBuf + sizeof(h) > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);
  datatypes::TSInt128::storeUnaligned(fCurInPtr, h);
  fCurInPtr += 16;
  return *this;
}

ByteStream& ByteStream::operator<<(const string& s)
{
  int32_t len = s.size();

  *this << len;
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0

  if (len < DEBUG_DUMP_STRINGS_LESS_THAN)
  {
    cerr << "bs: appending string len " << len << ": ";

    for (size_t i = 0; i < len; i++)
    {
      char xxx = s.c_str()[i];

      if (isprint(xxx))
        cerr << xxx << ' ';
      else
        cerr << "0x" << hex << ((unsigned)xxx & 0xff) << dec << ' ';
    }

    cerr << endl;
  }

#endif
  append(reinterpret_cast<const uint8_t*>(s.c_str()), len);

  return *this;
}

ByteStream& ByteStream::operator>>(int8_t& b)
{
  peek(b);
  fCurOutPtr++;
  return *this;
}

ByteStream& ByteStream::operator>>(uint8_t& b)
{
  peek(b);
  fCurOutPtr++;
  return *this;
}

ByteStream& ByteStream::operator>>(int16_t& d)
{
  peek(d);
  fCurOutPtr += 2;
  return *this;
}

ByteStream& ByteStream::operator>>(uint16_t& d)
{
  peek(d);
  fCurOutPtr += 2;
  return *this;
}

ByteStream& ByteStream::operator>>(int32_t& q)
{
  peek(q);
  fCurOutPtr += 4;
  return *this;
}

ByteStream& ByteStream::operator>>(uint32_t& q)
{
  peek(q);
  fCurOutPtr += 4;
  return *this;
}

ByteStream& ByteStream::operator>>(int64_t& o)
{
  peek(o);
  fCurOutPtr += 8;
  return *this;
}

ByteStream& ByteStream::operator>>(uint64_t& o)
{
  peek(o);
  fCurOutPtr += 8;
  return *this;
}

ByteStream& ByteStream::operator>>(uint128_t& o)
{
  peek(o);
  fCurOutPtr += 16;
  return *this;
}

ByteStream& ByteStream::operator>>(int128_t& o)
{
  peek(o);
  fCurOutPtr += 16;
  return *this;
}

ByteStream& ByteStream::operator>>(string& s)
{
  peek(s);
  fCurOutPtr += 4 + s.length();
  return *this;
}

ByteStream& ByteStream::operator>>(uint8_t*& bpr)
{
  peek(bpr);
  restart();
  return *this;
}

void ByteStream::peek(int8_t& b) const
{
  if (length() < 1)
    throw underflow_error("ByteStream::peek(int8_t): not enough data in stream to fill datatype");

  b = *fCurOutPtr;
}

void ByteStream::peek(uint8_t& b) const
{
  if (length() < 1)
    throw underflow_error("ByteStream::peek(uint8_t): not enough data in stream to fill datatype");

  b = *((int8_t*)fCurOutPtr);
}

void ByteStream::peek(int16_t& d) const
{
  if (length() < 2)
    throw underflow_error("ByteStream>int16_t: not enough data in stream to fill datatype");

  d = *((int16_t*)fCurOutPtr);
}

void ByteStream::peek(uint16_t& d) const
{
  if (length() < 2)
    throw underflow_error("ByteStream>uint16_t: not enough data in stream to fill datatype");

  d = *((uint16_t*)fCurOutPtr);
}

void ByteStream::peek(int32_t& q) const
{
  if (length() < 4)
    throw underflow_error("ByteStream>int32_t: not enough data in stream to fill datatype");

  q = *((int32_t*)fCurOutPtr);
}

void ByteStream::peek(uint32_t& q) const
{
  if (length() < 4)
    throw underflow_error("ByteStream>uint32_t: not enough data in stream to fill datatype");

  q = *((uint32_t*)fCurOutPtr);
}

void ByteStream::peek(int64_t& o) const
{
  if (length() < 8)
    throw underflow_error("ByteStream>int64_t: not enough data in stream to fill datatype");

  o = *((int64_t*)fCurOutPtr);
}

void ByteStream::peek(uint64_t& o) const
{
  if (length() < 8)
    throw underflow_error("ByteStream>uint64_t: not enough data in stream to fill datatype");

  o = *((uint64_t*)fCurOutPtr);
}

void ByteStream::peek(uint128_t& h) const
{
  if (length() < 16)
    throw underflow_error("ByteStream>uint128_t: not enough data in stream to fill datatype");
  datatypes::TSInt128::assignPtrPtr(&h, fCurOutPtr);
}

void ByteStream::peek(int128_t& h) const
{
  if (length() < 16)
    throw underflow_error("ByteStream>int128_t: not enough data in stream to fill datatype");
  datatypes::TSInt128::assignPtrPtr(&h, fCurOutPtr);
}

void ByteStream::peek(string& s) const
{
  int32_t len;

  peek(len);
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0

  if (len < DEBUG_DUMP_STRINGS_LESS_THAN)
  {
    cerr << "bs: reading string len " << len << ": ";

    for (size_t i = 0; i < len; i++)
    {
      char xxx = fCurOutPtr[4 + i];

      if (isprint(xxx))
        cerr << xxx << ' ';
      else
        cerr << "0x" << hex << ((unsigned)xxx & 0xff) << dec << ' ';
    }

    cerr << endl;
  }

#endif

  if (len < 0)
    throw logging::ProtocolError("expected a string");

  // we know len >= 0 by now...
  if (length() < static_cast<BSSizeType>(len + 4))
  {
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0
    cerr << "bs: wanted " << len + 4 << " bytes, but there are only " << length() << " remaining" << endl;
#endif
    // "put back" the qbyte we just read for strong exception guarantee
    throw underflow_error("ByteStream>string: not enough data in stream to fill datatype");
  }

  s.assign((char*)&fCurOutPtr[4], len);
}

void ByteStream::load(const uint8_t* bp, BSSizeType len)
{
  // Do all the stuff that could throw an exception first
  if (bp == 0 && len != 0)
    throw invalid_argument("ByteStream::load: bp cannot equal 0 when len is not equal to 0");

  BSSizeType newMaxLen = (len + BlockSize - 1) / BlockSize * BlockSize;

  if (len > fMaxLen)
  {
    delete[] fBuf;
    fBuf = new uint8_t[newMaxLen + ISSOverhead];
    fMaxLen = newMaxLen;
  }

  memcpy(fBuf + ISSOverhead, bp, len);
  fCurOutPtr = fBuf + ISSOverhead;
  fCurInPtr = fBuf + len + ISSOverhead;
}

void ByteStream::append(const uint8_t* bp, BSSizeType len)
{
  if (len == 0)
    return;

  if (bp == 0)
    throw invalid_argument("ByteStream::append: bp cannot equal 0 when len is not equal to 0");

  BSSizeType newSize = static_cast<BSSizeType>(fCurInPtr - fBuf + len);

  if (fBuf == 0 || (newSize > fMaxLen))
    growBuf(newSize);

  memcpy(fCurInPtr, bp, len);
  fCurInPtr += len;
}

void ByteStream::swap(ByteStream& rhs)
{
  std::swap(fBuf, rhs.fBuf);
  std::swap(fCurInPtr, rhs.fCurInPtr);
  std::swap(fCurOutPtr, rhs.fCurOutPtr);
  std::swap(fMaxLen, rhs.fMaxLen);
  std::swap(longStrings, rhs.longStrings);
}

ifstream& operator>>(ifstream& ifs, ByteStream& bs)
{
  int ifs_len;
  ifs.seekg(0, ios::end);
  ifs_len = ifs.tellg();
  ifs.seekg(0, ios::beg);
  boost::scoped_array<char> buf(new char[ifs_len]);
  ifs.read(buf.get(), ifs_len);
  bs.append(reinterpret_cast<const uint8_t*>(buf.get()), ifs_len);
  return ifs;
}

bool ByteStream::operator==(const ByteStream& b) const
{
  if (b.length() != length())
    return false;

  if (memcmp(fCurOutPtr, b.fCurOutPtr, length()) != 0)
    return false;

  // Check the `longString` sizes.
  if (longStrings.size() != b.longStrings.size())
    return false;

  // For each `longString`.
  for (uint32_t i = 0, e = b.longStrings.size(); i < e; ++i)
  {
    const auto* leftMemChunk = reinterpret_cast<MemChunk*>(longStrings[i].get());
    const auto* rightMemChunk = reinterpret_cast<MemChunk*>(b.longStrings[i].get());
    if (leftMemChunk == nullptr || rightMemChunk == nullptr)
      return false;

    const uint32_t leftSize = leftMemChunk->currentSize;
    const uint32_t rightSize = rightMemChunk->currentSize;
    if (leftSize != rightSize)
      return false;

    if (memcmp(leftMemChunk->data, rightMemChunk->data, leftSize) != 0)
      return false;
  }

  return true;
}

bool ByteStream::operator!=(const ByteStream& b) const
{
  return !(*this == b);
}

/* Serializeable interface */
void ByteStream::serialize(ByteStream& bs) const
{
  bs << length();
  bs.append(buf(), length());
}

void ByteStream::deserialize(ByteStream& bs)
{
  BSSizeType len;

  restart();
  bs >> len;
  load(bs.buf(), len);
  bs.advance(len);
}

void ByteStream::needAtLeast(BSSizeType amount)
{
  BSSizeType currentSpace;

  currentSpace = fMaxLen - (fCurInPtr - (fBuf + ISSOverhead));

  if (currentSpace < amount)
    growBuf(fMaxLen + amount);
}


ByteStream& ByteStream::operator<<(const ByteStream& bs)
{
  BSSizeType len = bs.length();

  *this << len;

  append(bs.buf(), len);

  return *this;
}

ByteStream& ByteStream::operator>>(ByteStream& bs)
{
  peek(bs);
  fCurOutPtr += sizeof(BSSizeType) + bs.length();
  return *this;
}

void ByteStream::peek(ByteStream& bs) const
{
  BSSizeType len;

  peek(len);

  if (length() < len)
    throw underflow_error("ByteStream>ByteStream: not enough data in stream to fill datatype");

  bs.load(&fCurOutPtr[sizeof(len)], len);
}

ByteStream& ByteStream::operator<<(const uuid& u)
{
  append(reinterpret_cast<const uint8_t*>(&u.data[0]), uuids::uuid::static_size());
  return *this;
}

ByteStream& ByteStream::operator>>(uuid& u)
{
  peek(u);
  fCurOutPtr += uuids::uuid::static_size();
  return *this;
}

void ByteStream::peek(uuid& u) const
{
  if (length() < uuids::uuid::static_size())
    throw underflow_error("ByteStream>uuid: not enough data in stream to fill datatype");

  memcpy(&u.data[0], fCurOutPtr, uuids::uuid::static_size());
}

ByteStream& ByteStream::operator<<(const float f)
{
  const constexpr BSSizeType sz = sizeof(float);

  if (fBuf == 0 || (fCurInPtr - fBuf + sz > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((float*)fCurInPtr) = f;
  fCurInPtr += sz;

  return *this;
}
ByteStream& ByteStream::operator<<(const double d)
{
  const constexpr BSSizeType sz = sizeof(double);

  if (fBuf == 0 || (fCurInPtr - fBuf + sz > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((double*)fCurInPtr) = d;
  fCurInPtr += sz;

  return *this;
}
ByteStream& ByteStream::operator<<(const long double d)
{
  const constexpr BSSizeType sz = sizeof(long double);

  if (fBuf == 0 || (fCurInPtr - fBuf + sz > fMaxLen + ISSOverhead))
    growBuf(fMaxLen + BlockSize);

  *((long double*)fCurInPtr) = d;
  fCurInPtr += sz;

  return *this;
}
ByteStream& ByteStream::operator>>(float& f)
{
  peek(f);
  fCurOutPtr += sizeof(float);
  return *this;
}
ByteStream& ByteStream::operator>>(double& d)
{
  peek(d);
  fCurOutPtr += sizeof(double);
  return *this;
}
ByteStream& ByteStream::operator>>(long double& d)
{
  peek(d);
  fCurOutPtr += sizeof(long double);
  return *this;
}
void ByteStream::peek(float& f) const
{
  if (length() < sizeof(float))
    throw underflow_error("ByteStream>int64_t: not enough data in stream to fill datatype");

  f = *((float*)fCurOutPtr);
}
void ByteStream::peek(double& d) const
{
  if (length() < sizeof(double))
    throw underflow_error("ByteStream>int64_t: not enough data in stream to fill datatype");

  d = *((double*)fCurOutPtr);
}

void ByteStream::peek(long double& d) const
{
  if (length() < sizeof(long double))
    throw underflow_error("ByteStream>int64_t: not enough data in stream to fill datatype");

  d = *((long double*)fCurOutPtr);
}

}  // namespace messageqcpp
