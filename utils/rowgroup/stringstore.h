/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

#pragma once

#include <boost/smart_ptr/shared_array.hpp>
#include <boost/thread/pthread/mutex.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "conststring.h"
#include "serializeable.h"

namespace rowgroup
{
class StringStore
{
  public:
    StringStore();
    virtual ~StringStore();

    std::string getString(uint64_t offset) const;
    uint64_t storeString(const uint8_t* data, uint32_t length);  // returns the offset
    const uint8_t* getPointer(uint64_t offset) const;
    uint32_t getStringLength(uint64_t offset) const;
    utils::ConstString getConstString(uint64_t offset) const;
    bool isEmpty() const;
    uint64_t getSize() const;
    bool isNullValue(uint64_t offset) const;

    void clear();

    void serialize(messageqcpp::ByteStream&) const;
    void deserialize(messageqcpp::ByteStream&);

    //@bug6065, make StringStore::storeString() thread safe
    void useStoreStringMutex(bool b)
    {
        fUseStoreStringMutex = b;
    }
    bool useStoreStringMutex() const
    {
        return fUseStoreStringMutex;
    }

    // This is an overlay b/c the underlying data needs to be any size,
    // and alloc'd in one chunk.  data can't be a separate dynamic chunk.
    struct MemChunk
    {
        uint32_t currentSize = 0;
        uint32_t capacity = 0;
        uint8_t data[];
    };

  private:
    std::string empty_str;

    StringStore(const StringStore&);
    StringStore& operator=(const StringStore&);
    static constexpr const uint32_t CHUNK_SIZE = 64 * 1024;  // allocators like powers of 2

    std::vector<boost::shared_array<uint8_t>> mem;

    // To store strings > 64KB (BLOB/TEXT)
    std::vector<boost::shared_array<uint8_t>> longStrings;
    bool empty = false;
    bool fUseStoreStringMutex;  //@bug6065, make StringStore::storeString() thread safe
    boost::mutex fMutex;
};

}  // namespace rowgroup
