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
#include <boost/smart_ptr/shared_ptr.hpp>
#include <cstdint>
#include <vector>

#include "serializeable.h"

namespace rowgroup
{
class RowGroup;
class Row;
class StringStore;
class UserDataStore;

class RGData
{
  public:
    RGData() = default;  // useless unless followed by an = or a deserialize operation
    RGData(const RowGroup& rg, uint32_t rowCount);  // allocates memory for rowData
    explicit RGData(const RowGroup& rg);

    RGData(const RGData&) = default;
    RGData& operator=(const RGData&) = default;
    virtual ~RGData() = default;

    // amount should be the # returned by RowGroup::getDataSize()
    void serialize(messageqcpp::ByteStream&, uint32_t amount) const;

    // the 'hasLengthField' is there b/c PM aggregation (and possibly others) currently sends
    // inline data with a length field.  Once that's converted to string table format, that
    // option can go away.
    void deserialize(messageqcpp::ByteStream&, uint32_t amount = 0);  // returns the # of bytes read
    void clear();
    void reinit(const RowGroup& rg);
    void reinit(const RowGroup& rg, uint32_t rowCount);
    void setStringStore(boost::shared_ptr<StringStore>& ss);
    bool hasData() const;

    // this will use the pre-configured Row to figure out where row # num is, then set the Row
    // to point to it.  It's a shortcut around using a RowGroup to do the same thing for cases
    // where it's inconvenient to instantiate one.
    void getRow(uint32_t num, Row* row);

    //@bug6065, make StringStore::storeString() thread safe
    void useStoreStringMutex(bool b);
    bool useStoreStringMutex() const;

    UserDataStore* getUserDataStore();
    // make UserDataStore::storeData() thread safe
    void useUserDataMutex(bool b);
    bool useUserDataMutex() const;

  private:
    uint64_t getBaseRid() const
    {
        return baseRid;
    }
    void setBaseRid(uint64_t baseRid_)
    {
        baseRid = baseRid_;
    }

    uint32_t getRowCount() const
    {
        return rowCount;
    }
    void setRowCount(uint32_t rowCount_)
    {
        rowCount = rowCount_;
    }

    uint32_t getDbRoot() const
    {
        return dbRoot;
    }
    void setDbRoot(uint32_t dbRoot_)
    {
        dbRoot = dbRoot_;
    }

    uint16_t getStatus() const
    {
        return status;
    }
    void setStatus(uint16_t status_)
    {
        status = status_;
    }

  private:
    uint64_t baseRid = 0;
    uint32_t rowCount = 0;
    uint32_t dbRoot = 0;
    uint16_t status = 0;

    boost::shared_array<uint8_t> rowData;
    boost::shared_ptr<StringStore> strings;
    boost::shared_ptr<UserDataStore> userDataStore;

    // Need sig to support backward compat.  RGData can deserialize both forms.
    static const uint32_t RGDATA_SIG = 0xffffffff;  // won't happen for 'old' Rowgroup data

    friend class RowGroup;
    friend class RowGroupStorage;
};

}  // namespace rowgroup