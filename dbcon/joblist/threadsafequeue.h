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

/*
* $Id: threadsafequeue.h 9655 2013-06-25 23:08:13Z xlou $
*/

/** @file */
#ifndef THREADSAFEQUEUE_H_
#define THREADSAFEQUEUE_H_

#include <unistd.h>
#include <queue>
#include <stdexcept>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <oneapi/tbb/concurrent_queue.h>

namespace joblist
{

struct TSQSize_t
{
    size_t size;
    uint32_t count;
};

/** @brief A thread-safe queue class
 *
 * Implements most of the std::queue interface. All methods are protected via mutexes. The front() method
 * will block until something is in the queue. The push() method will signal all threads wating in front().
 * @warning This implementation is, despite its generic appearance, intended for JobList <-> PrimProc
 * communication. That is, it is designed to support multiple writers and @em one reader. If multiple readers
 * are needed, external synchronization around front() and pop() will be required.
 */
template <typename T>
class ThreadSafeQueue
{
public:
    typedef T value_type;

    /** @brief constructor
     *
     * @warning this class takes ownership of the passed-in pointers.
     */
    ThreadSafeQueue(std::mutex* pimplLock = 0, std::condition_variable* pimplCond = 0) :
        fShutdown(false), bytes(0), zeroCount(0)
    {
        fPimplLock.reset(pimplLock);
        fPimplCond.reset(pimplCond);
    }
    /** @brief destructor
     *
     */
    ~ThreadSafeQueue()
    { }
    /** @brief get the head of the queue
     *
     * Return a read/write reference to the head of the queue, leaving it there. This method will block
     * until there is something to return.
     */
    /*T& front()
    {
        if (fPimplLock == 0 || fPimplCond == 0)
            throw std::runtime_error("TSQ: front(): no sync!");

        std::unique_lock<std::mutex> lk(*fPimplLock);

        if (fImpl.empty())
        {
            do
            {
                fPimplCond->wait(lk);

                if (fShutdown) return fBs0;
            }
            while (fImpl.empty());
        }

        return fImpl.front();
    }*/

    /** @brief put an item on the end of the queue
     *
     * Signals all threads waiting in front() to continue.
     */
    TSQSize_t push(const T& v)
    {
        TSQSize_t ret = {0, 0};

        if (fPimplLock == 0 || fPimplCond == 0)
            throw std::runtime_error("TSQ: push(): no sync!");

        if (fShutdown)
            return ret;

        std::unique_lock<std::mutex> lk(*fPimplLock);
        fImpl.push(v);
        bytes += v->lengthWithHdrOverhead();
        fPimplCond->notify_one();
        ret.size = bytes;
        ret.count = static_cast<uint32_t>(fImpl.size());
        return ret;
    }
    /** @brief remove the front item in the queue
     *
     */
    TSQSize_t pop(T* out = NULL)
    {
        TSQSize_t ret = {0, 0};

        if (fPimplLock == 0)
            throw std::runtime_error("TSQ: pop(): no sync!");

        if (fShutdown)
        {
            *out = fBs0;
            return ret;
        }

        std::unique_lock<std::mutex> lk(*fPimplLock);

        if (out != NULL)
        {
            if (fImpl.empty())
            {
                do
                {
                    if (fShutdown)
                    {
                        *out = fBs0;
                        return ret;
                    }

                    fPimplCond->wait(lk);

                    if (fShutdown)
                    {
                        *out = fBs0;
                        return ret;
                    }
                }
                while (fImpl.empty());
            }

            *out = fImpl.front();
            bytes -= (*out)->lengthWithHdrOverhead();
        }

        fImpl.pop();
        ret.size = bytes;
        ret.count = static_cast<uint32_t>(fImpl.size());
        return ret;
    }

    std::pair<TSQSize_t, bool> pop_one(T* out, bool& isEmpty)
    {
        TSQSize_t ret = {0, 0};
        bool queueIsEmpty = false; 
        if (fShutdown)
        {
            *out = fBs0;
            return std::make_pair(ret, queueIsEmpty);
        }

        std::unique_lock<std::mutex> lk(*fPimplLock);
        if (fImpl.empty())
        {
            if (fShutdown)
            {
                *out = fBs0;
                return std::make_pair(ret, queueIsEmpty);
            }
            queueIsEmpty = true;
            return std::make_pair(ret, queueIsEmpty);
        }

        // use move semantics here if BS can do it.
        *out = fImpl.front();
        fImpl.pop();
        ret.count = static_cast<uint32_t>(fImpl.size());

        bytes -= (*out)->lengthWithHdrOverhead();
        ret.size = bytes;
        return std::make_pair(ret, queueIsEmpty);
    }

    /* If there are less than min elements in the queue, this fcn will return nothing
     * for up to 10 consecutive calls (poor man's timer).  On the 11th, it will return
     * the entire queue.  Note, the zeroCount var is non-critical.  Not a big deal if
     * it gets fudged now and then. */
    TSQSize_t pop_some(uint32_t divisor, std::vector<T>& t, uint32_t min = 1)
    {
        uint32_t curSize, workSize;
        TSQSize_t ret = {0, 0};

        if (fPimplLock == 0)
            throw std::runtime_error("TSQ: pop_some(): no sync!");

        t.clear();

        if (fShutdown)
            return ret;

        std::unique_lock<std::mutex> lk(*fPimplLock);
        curSize = fImpl.size();

        if (curSize < min)
        {
            workSize = 0;
            zeroCount++;
        }
        else if (curSize / divisor <= min)
        {
            workSize = min;
            zeroCount = 0;
        }
        else
        {
            workSize = curSize / divisor;
            zeroCount = 0;
        }

        if (zeroCount > 10)
        {
            workSize = curSize;
            zeroCount = 0;
        }

        for (uint32_t i = 0; i < workSize; ++i)
        {
            t.push_back(fImpl.front());
            bytes -= fImpl.front()->lengthWithHdrOverhead();
            fImpl.pop();
        }

        ret.count = fImpl.size();
        ret.size = bytes;
        return ret;
    }

    inline void pop_all(std::vector<T>& t)
    {
        pop_some(1, t);
    }

    /** @brief is the queue empty
     *
     */
    bool empty() const
    {
        if (fPimplLock == 0)
            throw std::runtime_error("TSQ: empty(): no sync!");

        std::unique_lock<std::mutex> lk(*fPimplLock);
        return fImpl.empty();
    }
    /** @brief how many items are in the queue
     *
     */
    TSQSize_t size() const
    {
        TSQSize_t ret;

        if (fPimplLock == 0)
            throw std::runtime_error("TSQ: size(): no sync!");

        std::unique_lock<std::mutex> lk(*fPimplLock);
        ret.size = bytes;
        ret.count = fImpl.size();
        return ret;
    }

    /** @brief shutdown the queue
     *
     * cause all readers blocked in front() to return a default-constructed T
     */
    void shutdown()
    {
        fShutdown = true;

        if (fPimplCond != 0)
            fPimplCond->notify_all();

        return;
    }

    void clear()
    {
        if (fPimplLock == 0)
            throw std::runtime_error("TSQ: clear(): no sync!");

        std::unique_lock<std::mutex> lk(*fPimplLock);

        while ( !fImpl.empty() )
            fImpl.pop();

        bytes = 0;
        return;
    }

private:
    using impl_type = std::queue<T>;
    using SPBM = std::shared_ptr<std::mutex>;
    using SPBC = std::shared_ptr<std::condition_variable>;

    //defaults okay
    //ThreadSafeQueue<T>(const ThreadSafeQueue<T>& rhs);
    //ThreadSafeQueue<T>& operator=(const ThreadSafeQueue<T>& rhs);

    impl_type fImpl;
    SPBM fPimplLock;
    SPBC fPimplCond;
    volatile bool fShutdown;
    T fBs0;
#ifdef _MSC_VER
    volatile LONG bytes;
#else
    size_t bytes;
#endif
    uint32_t zeroCount;   // counts the # of times read_some returned 0
};

/** @brief A thread-safe queue class
 *
 * Implements most of the std::queue interface. All methods are protected via mutexes. The front() method
 * will block until something is in the queue. The push() method will signal all threads wating in front().
 * @warning This implementation is, despite its generic appearance, intended for JobList <-> PrimProc
 * communication. That is, it is designed to support multiple writers and @em one reader. If multiple readers
 * are needed, external synchronization around front() and pop() will be required.
 */
template <typename T>
class ThreadSafeQueueV2
{
public:
    typedef T value_type;

    /** @brief constructor
     *
     * @warning this class takes ownership of the passed-in pointers.
     */
    ThreadSafeQueueV2()
    {
        fShutdown.store(false, std::memory_order_relaxed);
        bytes.store(0, std::memory_order_relaxed);
        zeroCount.store(0, std::memory_order_relaxed);
    }
    /** @brief destructor
     *
     */
    ~ThreadSafeQueueV2()
    { }
    /** @brief put an item on the end of the queue
     *
     * Signals all threads waiting in front() to continue.
     */
    TSQSize_t push(const T& v)
    {
        TSQSize_t ret = {0, 0};

        if (fShutdown.load(std::memory_order_acquire))
            return ret;

//        std::unique_lock<std::mutex> lk(*fPimplLock);
        uint32_t length = v->lengthWithHdrOverhead();
        fImpl.push(v);
        size_t queueLengthBytes = bytes.fetch_add(length, std::memory_order_release);
        ret.size = queueLengthBytes + length;
        ret.count = static_cast<uint32_t>(fImpl.unsafe_size());
        return ret;
    }

    std::pair<TSQSize_t, bool> pop_one(T* out)
    {
        TSQSize_t ret = {0, 0};
        bool queueIsEmpty = false; 
        if (fShutdown.load(std::memory_order_acquire))
        {
            *out = fBs0;
            return std::make_pair(ret, queueIsEmpty);
        }

        //std::unique_lock<std::mutex> lk(*fPimplLock);
        /*
        if (fImpl.empty())
        {
            if (fShutdown.load(std::memory_order_acquire))
            {
                *out = fBs0;
                return std::make_pair(ret, queueIsEmpty);
            }
            queueIsEmpty = true;
            return std::make_pair(ret, queueIsEmpty);
        }*/

        // use move semantics here if BS can do it.
        if (fImpl.try_pop(*out))
        {
            ret.count = static_cast<uint32_t>(fImpl.unsafe_size());
            uint32_t length = (*out)->lengthWithHdrOverhead();
            size_t queueLengthBytes = bytes.fetch_sub(length, std::memory_order_release);
            ret.size = queueLengthBytes - length;
        }
        else
        {
            if (fShutdown.load(std::memory_order_acquire))
            {
                *out = fBs0;
                return std::make_pair(ret, queueIsEmpty);
            }
            queueIsEmpty = true;
            return std::make_pair(ret, queueIsEmpty);
        }

        return std::make_pair(ret, queueIsEmpty);
    }

    /* If there are less than min elements in the queue, this fcn will return nothing
     * for up to 10 consecutive calls (poor man's timer).  On the 11th, it will return
     * the entire queue.  Note, the zeroCount var is non-critical.  Not a big deal if
     * it gets fudged now and then. */
    TSQSize_t pop_some(uint32_t divisor, std::vector<T>& t, uint32_t min = 1)
    {
        uint32_t curSize, workSize;
        TSQSize_t ret = {0, 0};

        if (!t.empty())
            t.clear();

        if (fShutdown.load(std::memory_order_seq_cst))
            return ret;

        curSize = fImpl.unsafe_size();

        if (curSize < min)
        {
            workSize = 0;
            zeroCount++;
        }
        else if (curSize / divisor <= min)
        {
            workSize = min;
            zeroCount.store(0, std::memory_order_release); 
        }
        else
        {
            workSize = curSize / divisor;
            zeroCount.store(0, std::memory_order_release); 
        }

        if (zeroCount.load(std::memory_order_acquire) > 10)
        {
            workSize = curSize;
            zeroCount.store(0, std::memory_order_release); 
        }

        for (uint32_t i = 0; i < workSize; ++i)
        {
            T element;
            if (fImpl.try_pop(element))
            {
                t.push_back(element);
                uint32_t length = element->lengthWithHdrOverhead();
                bytes.fetch_sub(length, std::memory_order_release);
            }
            ret.count = fImpl.unsafe_size();
        }
        ret.size = bytes.load(std::memory_order_acquire);

        return ret;
    }

    inline void pop_all(std::vector<T>& t)
    {
        pop_some(1, t);
    }

    /** @brief is the queue empty
     *
     */
    bool empty() const
    {
        return fImpl.empty();
    }
    /** @brief how many items are in the queue
     *
     */
    TSQSize_t size() const
    {
        return { bytes.load(std::memory_order_acquire), (uint32_t)fImpl.unsafe_size()};
    }

    size_t queueSize() const
    {
        return fImpl.unsafe_size();
    }

    /** @brief shutdown the queue
     *
     * cause all readers blocked in front() to return a default-constructed T
     */
    void shutdown()
    {
        fShutdown.store(true, std::memory_order_seq_cst);
        return;
    }

    void clear()
    {
        // use atomic_flag to defend this thread-unsafe method
        fImpl.clear();
        bytes.store(0, std::memory_order_seq_cst);
        return;
    }

private:
    using impl_type = tbb::concurrent_queue<T>;

    impl_type fImpl;
    std::atomic_bool fShutdown;
    T fBs0;
    std::atomic_ullong bytes;
    std::atomic_ulong zeroCount;   // counts the # of times read_some returned 0
};


}

#endif

