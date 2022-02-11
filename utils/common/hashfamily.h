/* Copyright (C) 2021 Mariadb Corporation.

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

#ifndef UTILS_HASHFAMILY_H
#define UTILS_HASHFAMILY_H

#include "hasher.h"
#include "collation.h"

namespace utils
{
class HashFamily
{
 public:
  HashFamily(const utils::Hasher_r& h, const uint64_t intermediateHash, const uint64_t len,
             const datatypes::MariaDBHasher& hM)
   : mHasher(h), mMariaDBHasher(hM), mHasher_rHash(intermediateHash), mHasher_rLen(len)
  {
  }

  // Algorithm, seed and factor are taken from this discussion
  // https://stackoverflow.com/questions/1646807/quick-and-simple-hash-code-combinations
  inline uint64_t finalize() const
  {
    return (seed * factor + mHasher.finalize(mHasher_rHash, mHasher_rLen)) * factor +
           mMariaDBHasher.finalize();
  }

 private:
  constexpr static uint64_t seed = 1009ULL;
  constexpr static uint64_t factor = 9176ULL;

  const utils::Hasher_r& mHasher;
  const datatypes::MariaDBHasher& mMariaDBHasher;
  const uint64_t mHasher_rHash;
  const uint32_t mHasher_rLen;
};

}  // namespace utils
#endif
// vim:ts=2 sw=2:
