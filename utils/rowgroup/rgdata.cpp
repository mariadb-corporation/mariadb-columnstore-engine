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

#include "rgdata.h"

#include "row.h"
#include "rowgroup.h"
#include "serializeable.h"
#include "stringstore.h"
#include "userdatastore.h"

namespace rowgroup
{
void RGData::getRow(uint32_t num, Row* row)
{
    uint32_t size = row->getSize();
    row->setData(
        Row::Pointer(&rowData[num * size], strings.get(), userDataStore.get()));
}

void RGData::useStoreStringMutex(bool b)
{
    if (strings)
        strings->useStoreStringMutex(b);
}

RGData::RGData(const RowGroup& rg, uint32_t rowCount)
{
    // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
    if (LIKELY(rowCount > 0))
        rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);
    else
        rowData = nullptr;

    if (rg.usesStringTable() && rowCount > 0)
        strings.reset(new StringStore());

#ifdef VALGRIND
    /* In a PM-join, we can serialize entire tables; not every value has been
     * filled in yet.  Need to look into that.  Valgrind complains that
     * those bytes are uninitialized, this suppresses that error.
     */
    memset(rowData.get(), 0, rg.getDataSize(rowCount));  // XXXPAT: make valgrind happy temporarily
#endif
    memset(rowData.get(), 0, rg.getDataSize(rowCount));  // XXXPAT: make valgrind happy temporarily
}

RGData::RGData(const RowGroup& rg)
{
    // cout << "rgdata++ = " << __sync_add_and_fetch(&rgDataCount, 1) << endl;
    rowData.reset(new uint8_t[rg.getMaxDataSize()]);

    if (rg.usesStringTable())
        strings.reset(new StringStore());

#ifdef VALGRIND
    /* In a PM-join, we can serialize entire tables; not every value has been
     * filled in yet.  Need to look into that.  Valgrind complains that
     * those bytes are uninitialized, this suppresses that error.
     */
    memset(rowData.get(), 0, rg.getMaxDataSize());
#endif
}

void RGData::reinit(const RowGroup& rg, uint32_t rowCount)
{
    if(LIKELY(rowCount))
    {
        rowData.reset(new uint8_t[rg.getDataSize(rowCount)]);
    }
    else
    {
        rowData = nullptr;
    }

    if (rg.usesStringTable())
        strings.reset(new StringStore());
    else
        strings.reset();

#ifdef VALGRIND
    /* In a PM-join, we can serialize entire tables; not every value has been
     * filled in yet.  Need to look into that.  Valgrind complains that
     * those bytes are uninitialized, this suppresses that error.
     */
    memset(rowData.get(), 0, rg.getDataSize(rowCount));
#endif
}

void RGData::reinit(const RowGroup& rg)
{
    reinit(rg, rgCommonSize);
}

void RGData::serialize(messageqcpp::ByteStream& bs, uint32_t amount) const
{
    // cout << "serializing!\n";
    bs << (uint32_t)RGDATA_SIG;
    bs << (uint64_t)baseRid;
    bs << (uint32_t)rowCount;
    bs << (uint32_t)dbRoot;
    bs << (uint16_t)status;
    bs << (uint32_t)amount;

    bs.append(rowData.get(), amount);

    if (strings)
    {
        bs << (uint8_t)1;
        strings->serialize(bs);
    }
    else
        bs << (uint8_t)0;

    if (userDataStore)
    {
        bs << (uint8_t)1;
        userDataStore->serialize(bs);
    }
    else
        bs << (uint8_t)0;
}

void RGData::deserialize(messageqcpp::ByteStream& bs, uint32_t defAmount)
{
    uint32_t amount, sig;
    uint8_t* buf;
    uint8_t tmp8;

    bs.peek(sig);

    if (sig == RGDATA_SIG)
    {
        bs >> sig;
        bs >> baseRid;
        bs >> rowCount;
        bs >> dbRoot;
        bs >> status;
        bs >> amount;

        rowData.reset(new uint8_t[std::max(amount, defAmount)]);
        buf = bs.buf();
        memcpy(rowData.get(), buf, amount);
        bs.advance(amount);
        bs >> tmp8;

        if (tmp8)
        {
            strings.reset(new StringStore());
            strings->deserialize(bs);
        }
        else
            strings.reset();

        // UDAF user data
        bs >> tmp8;

        if (tmp8)
        {
            userDataStore.reset(new UserDataStore());
            userDataStore->deserialize(bs);
        }
        else
            userDataStore.reset();
    }

    return;
}

void RGData::clear()
{
    baseRid = 0;
    rowCount = 0;
    dbRoot = 0;
    status = 0;
    rowData.reset();
    strings.reset();
    // TODO: why we skip userDataStore here?
}

// UserDataStore is only used for UDAF.
// Just in time construction because most of the time we don't need one.
UserDataStore* RGData::getUserDataStore()
{
    if (!userDataStore)
    {
        userDataStore.reset(new UserDataStore);
    }

    return userDataStore.get();
}

bool RGData::useStoreStringMutex() const
{
    return (strings ? (strings->useStoreStringMutex()) : false);
}

void RGData::useUserDataMutex(bool b)
{
    if (userDataStore)
        userDataStore->useUserDataMutex(b);
}

bool RGData::useUserDataMutex() const
{
    return (userDataStore ? (userDataStore->useUserDataMutex()) : false);
}

void RGData::setStringStore(boost::shared_ptr<StringStore>& ss)
{
    strings = ss;
}
bool RGData::hasData() const
{
    return rowData.get() != nullptr;
}
}  // namespace rowgroup