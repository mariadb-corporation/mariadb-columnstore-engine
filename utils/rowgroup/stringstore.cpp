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

#include "stringstore.h"

#include "branchpred.h"
#include "bytestream.h"
#include "joblisttypes.h"

namespace rowgroup
{
StringStore::StringStore() : empty(true), fUseStoreStringMutex(false)
{
}

StringStore::StringStore(const StringStore&)
{
    throw std::logic_error("Don't call StringStore copy ctor");
}

StringStore& StringStore::operator=(const StringStore&)
{
    throw std::logic_error("Don't call StringStore operator=");
}

StringStore::~StringStore()
{
#if 0
    // for mem usage debugging
    uint32_t i;
    uint64_t inUse = 0, allocated = 0;

    for (i = 0; i < mem.size(); i++)
    {
        MemChunk* tmp = (MemChunk*) mem.back().get();
        inUse += tmp->currentSize;
        allocated += tmp->capacity;
    }

    if (allocated > 0)
        cout << "~SS: " << inUse << "/" << allocated << " = " << (float) inUse / (float) allocated << endl;

#endif
}

uint64_t StringStore::storeString(const uint8_t* data, uint32_t len)
{
    MemChunk* lastMC = NULL;
    uint64_t ret = 0;

    empty = false;  // At least a NULL is being stored.

    // Sometimes the caller actually wants "" to be returned.......   argggghhhh......
    // if (len == 0)
    //	return numeric_limits<uint32_t>::max();

    if ((len == 8 || len == 9) && *((uint64_t*)data) == *((uint64_t*)joblist::CPNULLSTRMARK.c_str()))
        return std::numeric_limits<uint64_t>::max();

    //@bug6065, make StringStore::storeString() thread safe
    boost::mutex::scoped_lock lk(fMutex, boost::defer_lock);

    if (fUseStoreStringMutex)
        lk.lock();

    if (mem.size() > 0)
        lastMC = (MemChunk*)mem.back().get();

    if ((len + 4) >= CHUNK_SIZE)
    {
        boost::shared_array<uint8_t> newOne(new uint8_t[len + sizeof(MemChunk) + 4]);
        longStrings.push_back(newOne);
        lastMC = (MemChunk*)longStrings.back().get();
        lastMC->capacity = lastMC->currentSize = len + 4;
        memcpy(lastMC->data, &len, 4);
        memcpy(lastMC->data + 4, data, len);
        // High bit to mark a long string
        ret = 0x8000000000000000;
        ret += longStrings.size() - 1;
    }
    else
    {
        if ((lastMC == NULL) || (lastMC->capacity - lastMC->currentSize < (len + 4)))
        {
            // mem usage debugging
            // if (lastMC)
            // cout << "Memchunk efficiency = " << lastMC->currentSize << "/" << lastMC->capacity <<
            // endl;
            boost::shared_array<uint8_t> newOne(new uint8_t[CHUNK_SIZE + sizeof(MemChunk)]);
            mem.push_back(newOne);
            lastMC = (MemChunk*)mem.back().get();
            lastMC->currentSize = 0;
            lastMC->capacity = CHUNK_SIZE;
            memset(lastMC->data, 0, CHUNK_SIZE);
        }

        ret = ((mem.size() - 1) * CHUNK_SIZE) + lastMC->currentSize;

        // If this ever happens then we have big problems
        if (ret & 0x8000000000000000)
            throw std::logic_error("StringStore memory exceeded.");

        memcpy(&(lastMC->data[lastMC->currentSize]), &len, 4);
        memcpy(&(lastMC->data[lastMC->currentSize]) + 4, data, len);
        /*
        cout << "stored: '" << hex;
        for (uint32_t i = 0; i < len ; i++) {
                cout << (char) lastMC->data[lastMC->currentSize + i];
        }
        cout << "' at position " << lastMC->currentSize << " len " << len << dec << endl;
        */
        lastMC->currentSize += len + 4;
    }

    return ret;
}

void StringStore::serialize(messageqcpp::ByteStream& bs) const
{
    uint64_t i;
    MemChunk* mc;

    bs << (uint64_t)mem.size();
    bs << (uint8_t)empty;

    for (i = 0; i < mem.size(); i++)
    {
        mc = (MemChunk*)mem[i].get();
        bs << (uint64_t)mc->currentSize;
        // cout << "serialized " << mc->currentSize << " bytes\n";
        bs.append(mc->data, mc->currentSize);
    }

    bs.setLongStrings(longStrings);
}

void StringStore::deserialize(messageqcpp::ByteStream& bs)
{
    uint64_t i;
    uint64_t count;
    uint64_t size;
    uint8_t* buf;
    MemChunk* mc;
    uint8_t tmp8;

    // mem.clear();
    bs >> count;
    mem.resize(count);
    bs >> tmp8;
    empty = (bool)tmp8;

    for (i = 0; i < count; i++)
    {
        bs >> size;
        // cout << "deserializing " << size << " bytes\n";
        buf = bs.buf();
        mem[i].reset(new uint8_t[size + sizeof(MemChunk)]);
        mc = (MemChunk*)mem[i].get();
        mc->currentSize = size;
        mc->capacity = size;
        memcpy(mc->data, buf, size);
        bs.advance(size);
    }

    longStrings = bs.getLongStrings();
    return;
}

void StringStore::clear()
{
    std::vector<boost::shared_array<uint8_t>> emptyv;
    std::vector<boost::shared_array<uint8_t>> emptyv2;
    mem.swap(emptyv);
    longStrings.swap(emptyv2);
    empty = true;
}

std::string StringStore::getString(uint64_t off) const
{
    uint32_t length;

    if (off == std::numeric_limits<uint64_t>::max())
        return joblist::CPNULLSTRMARK;

    MemChunk* mc;

    if (off & 0x8000000000000000)
    {
        // off = off - 0x8000000000000000;
        off &= ~0x8000000000000000;

        if (longStrings.size() <= off)
            return joblist::CPNULLSTRMARK;

        mc = (MemChunk*)longStrings[off].get();
        memcpy(&length, mc->data, 4);
        return std::string((char*)mc->data + 4, length);
    }

    uint64_t chunk = off / CHUNK_SIZE;
    uint64_t offset = off % CHUNK_SIZE;

    // this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
    // what gets returned, it just can't go out of bounds.
    if (mem.size() <= chunk)
        return joblist::CPNULLSTRMARK;

    mc = (MemChunk*)mem[chunk].get();

    memcpy(&length, &mc->data[offset], 4);

    if ((offset + length) > mc->currentSize)
        return joblist::CPNULLSTRMARK;

    return std::string((char*)&(mc->data[offset]) + 4, length);
}

const uint8_t* StringStore::getPointer(uint64_t off) const
{
    if (off == std::numeric_limits<uint64_t>::max())
        return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

    uint64_t chunk = off / CHUNK_SIZE;
    uint64_t offset = off % CHUNK_SIZE;
    MemChunk* mc;

    if (off & 0x8000000000000000)
    {
        // off = off - 0x8000000000000000;
        off &= ~0x8000000000000000;

        if (longStrings.size() <= off)
            return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

        mc = (MemChunk*)longStrings[off].get();
        return mc->data + 4;
    }

    // this has to handle uninitialized data as well.  If it's uninitialized it doesn't matter
    // what gets returned, it just can't go out of bounds.
    if (UNLIKELY(mem.size() <= chunk))
        return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

    mc = (MemChunk*)mem[chunk].get();

    if (offset > mc->currentSize)
        return (const uint8_t*)joblist::CPNULLSTRMARK.c_str();

    return &(mc->data[offset]) + 4;
}

bool StringStore::isNullValue(uint64_t off) const
{
    uint32_t length;

    if (off == std::numeric_limits<uint64_t>::max())
        return true;

    // Long strings won't be NULL
    if (off & 0x8000000000000000)
        return false;

    uint32_t chunk = off / CHUNK_SIZE;
    uint32_t offset = off % CHUNK_SIZE;
    MemChunk* mc;

    if (mem.size() <= chunk)
        return true;

    mc = (MemChunk*)mem[chunk].get();
    memcpy(&length, &mc->data[offset], 4);

    if (length == 0)
        return true;

    if (length < 8)
        return false;

    if ((offset + length) > mc->currentSize)
        return true;

    if (mc->data[offset + 4] == 0)  // "" = NULL string for some reason...
        return true;
    return (memcmp(&mc->data[offset + 4], joblist::CPNULLSTRMARK.c_str(), 8) == 0);
}

uint32_t StringStore::getStringLength(uint64_t off) const
{
    uint32_t length;
    MemChunk* mc;

    if (off == std::numeric_limits<uint64_t>::max())
        return 0;

    if (off & 0x8000000000000000)
    {
        // off = off - 0x8000000000000000;
        off &= ~0x8000000000000000;

        if (longStrings.size() <= off)
            return 0;

        mc = (MemChunk*)longStrings[off].get();
        memcpy(&length, mc->data, 4);
    }
    else
    {
        uint64_t chunk = off / CHUNK_SIZE;
        uint64_t offset = off % CHUNK_SIZE;

        if (mem.size() <= chunk)
            return 0;

        mc = (MemChunk*)mem[chunk].get();
        memcpy(&length, &mc->data[offset], 4);
    }

    return length;
}

bool StringStore::isEmpty() const
{
    return empty;
}

uint64_t StringStore::getSize() const
{
    uint32_t i;
    uint64_t ret = 0;
    MemChunk* mc;

    ret += sizeof(MemChunk) * mem.size();
    for (i = 0; i < mem.size(); i++)
    {
        mc = (MemChunk*)mem[i].get();
        ret += mc->capacity;
    }

    ret += sizeof(MemChunk) * longStrings.size();
    for (i = 0; i < longStrings.size(); i++)
    {
        mc = (MemChunk*)longStrings[i].get();
        ret += mc->capacity;
    }

    return ret;
}

utils::ConstString StringStore::getConstString(uint64_t offset) const
{
    return utils::ConstString((const char*)getPointer(offset), getStringLength(offset));
}

}  // namespace rowgroup