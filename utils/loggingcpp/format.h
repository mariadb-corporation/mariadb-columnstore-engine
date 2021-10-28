/*
   Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2021 MariaDB Corporation

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

#include <cstdint>
#include <stdexcept>
#include <string>


namespace logging
{

template <class T, class Iter>
void formatOne(std::string &errMsg, Iter iter, uint32_t position)
{
    T arg = any_cast<T>(*iter);
    std::string token = std::string("%") + std::to_string(position) + std::string("%");
    size_t index = 0;

    while (true)
    {
        index = errMsg.find(token, index);
        if (index == std::string::npos)
            break;

        if constexpr (std::is_same_v<T, std::string>)
        {
            errMsg.replace(index, token.length(), arg);
        }
        else
        {
            errMsg.replace(index, token.length(), std::to_string(arg));
        }

      index += token.length();
    }
}

template <class T>
void formatMany(std::string& errMsg, const T& args)
{
    auto iter = args.begin();
    auto end = args.end();
    uint32_t position = 1;

    while (iter != end)
    {
        if (iter->type() == typeid(long))
        {
            formatOne<long>(errMsg, iter, position);
        }
        else if (iter->type() == typeid(uint64_t))
        {
            formatOne<uint64_t>(errMsg, iter, position);
        }
        else if (iter->type() == typeid(double))
        {
            formatOne<double>(errMsg, iter, position);
        }
        else if (iter->type() == typeid(std::string))
        {
            formatOne<std::string>(errMsg, iter, position);
        }
        else
        {
            throw std::logic_error("logggin::format: unexpected type in argslist");
        }
        ++iter;
        ++position;
    }
}

} // namespace logging