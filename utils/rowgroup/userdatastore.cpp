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

#include "userdatastore.h"

namespace rowgroup
{
uint32_t UserDataStore::storeUserData(mcsv1sdk::mcsv1Context& context,
                                      boost::shared_ptr<mcsv1sdk::UserData> data, uint32_t len)
{
    uint32_t ret = 0;

    if (len == 0 || data == NULL)
    {
        return numeric_limits<uint32_t>::max();
    }

    boost::mutex::scoped_lock lk(fMutex, boost::defer_lock);

    if (fUseUserDataMutex)
        lk.lock();

    StoreData storeData;
    storeData.length = len;
    storeData.functionName = context.getName();
    storeData.userData = data;
    vStoreData.push_back(storeData);

    ret = vStoreData.size();

    return ret;
}

boost::shared_ptr<mcsv1sdk::UserData> UserDataStore::getUserData(uint32_t off) const
{
    if (off == std::numeric_limits<uint32_t>::max())
        return boost::shared_ptr<mcsv1sdk::UserData>();

    if ((vStoreData.size() < off) || off == 0)
        return boost::shared_ptr<mcsv1sdk::UserData>();

    return vStoreData[off - 1].userData;
}

void UserDataStore::serialize(messageqcpp::ByteStream& bs) const
{
    size_t i;

    bs << (uint32_t)vStoreData.size();

    for (i = 0; i < vStoreData.size(); ++i)
    {
        const StoreData& storeData = vStoreData[i];
        bs << storeData.length;
        bs << storeData.functionName;
        storeData.userData->serialize(bs);
    }
}

void UserDataStore::deserialize(messageqcpp::ByteStream& bs)
{
    size_t i;
    uint32_t cnt;
    bs >> cnt;

    vStoreData.resize(cnt);

    for (i = 0; i < cnt; i++)
    {
        bs >> vStoreData[i].length;
        bs >> vStoreData[i].functionName;

        // We don't have easy access to the context here, so we do our own lookup
        if (vStoreData[i].functionName.length() == 0)
        {
            throw std::logic_error("UserDataStore::deserialize: has empty name");
        }

        mcsv1sdk::UDAF_MAP::iterator funcIter =
            mcsv1sdk::UDAFMap::getMap().find(vStoreData[i].functionName);

        if (funcIter == mcsv1sdk::UDAFMap::getMap().end())
        {
            std::ostringstream errmsg;
            errmsg << "UserDataStore::deserialize: " << vStoreData[i].functionName
                   << " is undefined";
            throw std::logic_error(errmsg.str());
        }

        mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
        mcsv1sdk::UserData* userData = NULL;
        rc = funcIter->second->createUserData(userData, vStoreData[i].length);

        if (rc != mcsv1sdk::mcsv1_UDAF::SUCCESS)
        {
            std::ostringstream errmsg;
            errmsg << "UserDataStore::deserialize: " << vStoreData[i].functionName
                   << " createUserData failed(" << rc << ")";
            throw std::logic_error(errmsg.str());
        }

        userData->unserialize(bs);
        vStoreData[i].userData = boost::shared_ptr<mcsv1sdk::UserData>(userData);
    }

    return;
}

}  // namespace rowgroup