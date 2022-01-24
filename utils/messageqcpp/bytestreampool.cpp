/* Copyright (C) 2019 MariaDB Corporaton

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

#include "bytestreampool.h"

namespace messageqcpp
{
ByteStreamPool::ByteStreamPool()
{
  maxBufferSize = 1 << 20;  // 1MB
  maxFreeBuffers = 10;
}

ByteStreamPool::ByteStreamPool(uint largeBufferSize)
{
  maxBufferSize = largeBufferSize;
  maxFreeBuffers = 10;
}

ByteStreamPool::ByteStreamPool(uint largeBufferSize, uint freeBufferLimit)
{
  maxBufferSize = largeBufferSize;
  maxFreeBuffers = freeBufferLimit;
}

ByteStreamPool::~ByteStreamPool()
{
  while (!freeByteStreams.empty())
  {
    ByteStream* next = freeByteStreams.front();
    freeByteStreams.pop_front();
    delete next;
  }
}

ByteStream* ByteStreamPool::getByteStream()
{
  boost::mutex::scoped_lock s(mutex);
  ByteStream* ret;

  if (!freeByteStreams.empty())
  {
    ret = freeByteStreams.front();
    freeByteStreams.pop_front();
  }
  else
    ret = new ByteStream();

  return ret;
}

void ByteStreamPool::returnByteStream(ByteStream* bs)
{
  if (bs->getBufferSize() > maxBufferSize)
    delete bs;
  else
  {
    boost::mutex::scoped_lock s(mutex);
    if (freeByteStreams.size() > maxFreeBuffers)
      delete bs;
    else
    {
      bs->restart();
      freeByteStreams.push_back(bs);
    }
  }
}

}  // namespace messageqcpp
