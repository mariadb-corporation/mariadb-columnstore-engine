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
 * $Id: hasher.h 3843 2013-05-31 13:46:24Z pleblanc $
 *
 *****************************************************************************/

/** @file
 * class Hasher interface
 */

#pragma once

#include <cstddef>
#include <stdint.h>
#include <string.h>
#include <string>
#include "mcs_basic_types.h"

namespace utils
{
/** @brief class Hasher
 *  As of 10/16/12, this implements the Murmur3 hash algorithm.
 */

inline uint32_t rotl32(uint32_t x, int8_t r)
{
  return (x << r) | (x >> (32 - r));
}

inline uint32_t fmix(uint32_t h)
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

inline uint64_t fmix(uint64_t k)
{
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;

  return k;
}

inline uint64_t rotl64(uint64_t x, int8_t r)
{
  return (x << r) | (x >> (64 - r));
}

class Hasher
{
 public:
  inline uint32_t operator()(const std::string& s) const
  {
    return operator()(s.data(), s.length());
  }

  inline uint32_t operator()(const char* data, uint64_t len) const
  {
    const int nblocks = len / 4;

    uint32_t h1 = 0;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    //----------
    // body

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

    for (int i = -nblocks; i; i++)
    {
      uint32_t k1 = blocks[i];

      k1 *= c1;
      k1 = rotl32(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = rotl32(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);

    uint32_t k1 = 0;

    switch (len & 3)
    {
      case 3:
        k1 ^= tail[2] << 16;
        /* fall through */
      case 2:
        k1 ^= tail[1] << 8;
        /* fall through */
      case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len;

    h1 = fmix(h1);

    return h1;
  }
};

class Hasher_r
{
 public:
  inline uint32_t operator()(const char* data, uint64_t len, uint32_t seed) const
  {
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    //----------
    // body

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

    for (int i = -nblocks; i; i++)
    {
      uint32_t k1 = blocks[i];

      k1 *= c1;
      k1 = rotl32(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = rotl32(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);

    uint32_t k1 = 0;

    switch (len & 3)
    {
      case 3:
        k1 ^= tail[2] << 16;
        /* FALLTHRU */

      case 2:
        k1 ^= tail[1] << 8;
        /* FALLTHRU */

      case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    };

    return h1;
  }

  inline uint32_t finalize(uint32_t seed, uint32_t len) const
  {
    seed ^= len;
    seed = fmix(seed);
    return seed;
  }
};

// This stream hasher was borrowed from RobinHood
class Hasher64_r
{
 public:
  inline uint64_t operator()(const void* ptr, uint32_t len, uint64_t x = 0ULL)
  {
    auto const* const data64 = static_cast<uint64_t const*>(ptr);
    uint64_t h = seed ^ (len * m);

    std::size_t const n_blocks = len / 8;
    if (x)
    {
      x *= m;
      x ^= x >> r;
      x *= m;
      h ^= x;
      h *= m;
    }
    for (std::size_t i = 0; i < n_blocks; ++i)
    {
      uint64_t k;
      memcpy(&k, data64 + i, sizeof(k));

      k *= m;
      k ^= k >> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    auto const* const data8 = reinterpret_cast<uint8_t const*>(data64 + n_blocks);
    switch (len & 7U)
    {
      case 7:
        h ^= static_cast<uint64_t>(data8[6]) << 48U;
        // FALLTHROUGH
      case 6:
        h ^= static_cast<uint64_t>(data8[5]) << 40U;
        // FALLTHROUGH
      case 5:
        h ^= static_cast<uint64_t>(data8[4]) << 32U;
        // FALLTHROUGH
      case 4:
        h ^= static_cast<uint64_t>(data8[3]) << 24U;
        // FALLTHROUGH
      case 3:
        h ^= static_cast<uint64_t>(data8[2]) << 16U;
        // FALLTHROUGH
      case 2:
        h ^= static_cast<uint64_t>(data8[1]) << 8U;
        // FALLTHROUGH
      case 1:
        h ^= static_cast<uint64_t>(data8[0]);
        h *= m;
        // FALLTHROUGH
      default: break;
    }
    return h;
  }

  inline uint64_t finalize(uint64_t h, uint64_t len) const
  {
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }

 private:
  static constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
  static constexpr uint64_t seed = 0xe17a1465ULL;
  static constexpr unsigned int r = 47;
};

class Hasher128
{
 public:
  inline uint64_t operator()(const char* data, uint64_t len) const
  {
    const int nblocks = len / 16;

    uint64_t h1 = 0;
    uint64_t h2 = 0;

    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    //----------
    // body

    const uint64_t* blocks = (const uint64_t*)(data);

    for (int i = 0; i < nblocks; i++)
    {
      uint64_t k1 = blocks[i * 2 + 0];
      uint64_t k2 = blocks[i * 2 + 1];

      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15)
    {
      case 15:
        k2 ^= uint64_t(tail[14]) << 48;
        /* FALLTHRU */

      case 14:
        k2 ^= uint64_t(tail[13]) << 40;
        /* FALLTHRU */

      case 13:
        k2 ^= uint64_t(tail[12]) << 32;
        /* FALLTHRU */

      case 12:
        k2 ^= uint64_t(tail[11]) << 24;
        /* FALLTHRU */

      case 11:
        k2 ^= uint64_t(tail[10]) << 16;
        /* FALLTHRU */

      case 10:
        k2 ^= uint64_t(tail[9]) << 8;
        /* FALLTHRU */

      case 9:
        k2 ^= uint64_t(tail[8]) << 0;
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
        /* FALLTHRU */

      case 8:
        k1 ^= uint64_t(tail[7]) << 56;
        /* FALLTHRU */

      case 7:
        k1 ^= uint64_t(tail[6]) << 48;
        /* FALLTHRU */

      case 6:
        k1 ^= uint64_t(tail[5]) << 40;
        /* FALLTHRU */

      case 5:
        k1 ^= uint64_t(tail[4]) << 32;
        /* FALLTHRU */

      case 4:
        k1 ^= uint64_t(tail[3]) << 24;
        /* FALLTHRU */

      case 3:
        k1 ^= uint64_t(tail[2]) << 16;
        /* FALLTHRU */

      case 2:
        k1 ^= uint64_t(tail[1]) << 8;
        /* FALLTHRU */

      case 1:
        k1 ^= uint64_t(tail[0]) << 0;
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len;

    h2 ^= len;

    h1 += h2;

    h2 += h1;

    h1 = fmix(h1);

    h2 = fmix(h2);

    h1 += h2;

    h2 += h1;

    return h1;
  }
};

// TODO a copy of these classes also exists in primitiveprocessor.h; consolidate
class Hash128
{
 public:
  inline size_t operator()(const int128_t i) const
  {
    return (uint64_t)((int64_t)i);
  }
};

class Equal128
{
 public:
  inline bool operator()(const int128_t f1, const int128_t f2) const
  {
    return f1 == f2;
  }
};

//------------------------------------------------------------------------------
/** @brief class TupleHasher
 *
 */
//------------------------------------------------------------------------------
class TupleHasher
{
 public:
  TupleHasher(uint32_t len) : fHashLen(len)
  {
  }
  inline uint64_t operator()(const uint8_t* hashKey) const
  {
    return fHasher(reinterpret_cast<const char*>(hashKey), fHashLen);
  }

 private:
  Hasher fHasher;
  uint32_t fHashLen;
};

//------------------------------------------------------------------------------
/** @brief class TupleComparator
 *
 */
//------------------------------------------------------------------------------
class TupleComparator
{
 public:
  TupleComparator(uint32_t len) : fCmpLen(len)
  {
  }
  inline bool operator()(const uint8_t* hashKey1, const uint8_t* hashKey2) const
  {
    return (memcmp(hashKey1, hashKey2, fCmpLen) == 0);
  }

 private:
  uint32_t fCmpLen;
};

}  // namespace utils
