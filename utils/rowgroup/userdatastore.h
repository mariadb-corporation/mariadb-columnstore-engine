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

#include "mcsv1_udaf.h"

namespace rowgroup
{
// Where we store user data for UDA(n)F
class UserDataStore
{
    // length represents the fixed portion length of userData.
    // There may be variable length data in containers or other
    // user created structures.
    struct StoreData
    {
        int32_t length;
        std::string functionName;
        boost::shared_ptr<mcsv1sdk::UserData> userData;
        StoreData() : length(0)
        {
        }
        StoreData(const StoreData& rhs)
        {
            length = rhs.length;
            functionName = rhs.functionName;
            userData = rhs.userData;
        }
    };

  public:
    UserDataStore() = default;
    virtual ~UserDataStore() = default;

    void serialize(messageqcpp::ByteStream&) const;
    void deserialize(messageqcpp::ByteStream&);

    // Set to make UserDataStore thread safe
    void useUserDataMutex(bool b)
    {
        fUseUserDataMutex = b;
    }
    bool useUserDataMutex() const
    {
        return fUseUserDataMutex;
    }

    // Returns the offset
    uint32_t storeUserData(mcsv1sdk::mcsv1Context& context,
                           boost::shared_ptr<mcsv1sdk::UserData> data, uint32_t length);

    boost::shared_ptr<mcsv1sdk::UserData> getUserData(uint32_t offset) const;

  private:
    UserDataStore(const UserDataStore&);
    UserDataStore& operator=(const UserDataStore&);

    std::vector<StoreData> vStoreData;

    bool fUseUserDataMutex = false;
    boost::mutex fMutex;
};

}  // namespace rowgroup