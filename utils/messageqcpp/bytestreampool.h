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

#ifndef BYTESTREAMPOOL_H_
#define BYTESTREAMPOOL_H_

/* This class defines a pool of bytestreams to improve BS reuse, reducing
the need to use the dynamic allocator.  It allocates as many BS's as needed,
and automatically disposes of buffers that are 'large' to reduce mem usage.
Initially, 'large' is defined as 1MB.
*/

#include <deque>
#include <boost/thread/mutex.hpp>
#include "bytestream.h"

namespace messageqcpp
{
class ByteStreamPool
{
 public:
  ByteStreamPool();
  ByteStreamPool(uint largeBufferSize);  // BS's above largeBufferSize get deallocated on return to the pool
  ByteStreamPool(uint largeBufferSize,
                 uint freeBufferLimit);  // freeBufferLimit is the max # of BSs to reserve
  virtual ~ByteStreamPool();

  ByteStream* getByteStream();
  void returnByteStream(ByteStream*);

 private:
  std::deque<ByteStream*> freeByteStreams;
  boost::mutex mutex;
  uint maxBufferSize;
  uint maxFreeBuffers;
};

}  // namespace messageqcpp

#endif
