/* Copyright (C) 2014 InfiniDB, Inc.

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

#pragma once

/***************************************************************************
 *
 *   $Id: fileblockrequestqueue.h 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include <atomic>
#include <iostream>
#include "blockingconcurrentqueue.h"
#include "filerequest.h"

/**
        @author Jason Rodriguez <jrodriguez@calpont.com>
*/

/**
 * @brief definition of the block request queue as stl std::priority_queue
 **/
namespace dbbc
{
/**
 * @brief class to hold requests for disk blocks in a lock-free concurrent queue.
 **/

class fileBlockRequestQueue
{
 public:
  /**
   * @brief default ctor
   **/
  fileBlockRequestQueue();

  /**
   * @brief dtor
   **/
  virtual ~fileBlockRequestQueue();

  /**
   * @brief add a request to the queue
   **/
  int push(fileRequest& blk);

  /**
   * @brief get the next request from the queue and delete it from the queue
   **/
  fileRequest* pop(void);

  /**
   * @brief true if no reuquests are in the queue. false if there are requests in the queue
   **/
  bool empty() const;

  /**
   * @brief approximate number of requests in the queue
   **/
  uint32_t size() const
  {
    return static_cast<uint32_t>(fbQueue.size_approx());
  }

  /**
   * @brief queue will stop accecpting requests in preparation for the dtor
   **/
  void stop();

 private:
  moodycamel::BlockingConcurrentQueue<fileRequest*> fbQueue;

  fileBlockRequestQueue(const fileBlockRequestQueue&) = delete;
  const fileBlockRequestQueue& operator=(const fileBlockRequestQueue&) = delete;
};
}  // namespace dbbc
