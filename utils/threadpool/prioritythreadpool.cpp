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

/***********************************************************************
*   $Id: threadpool.cpp 553 2008-02-27 17:51:16Z rdempsey $
*
*
***********************************************************************/

#include <stdexcept>
#include <unistd.h>
#include <exception>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
using namespace logging;

#include "prioritythreadpool.h"
using namespace boost;

#include "dbcon/joblist/primitivemsg.h"

namespace threadpool
{

PriorityThreadPool::PriorityThreadPool(uint targetWeightPerRun, uint highThreads,
                                       uint midThreads, uint lowThreads, uint ID) :
    _stop(false), weightPerRun(targetWeightPerRun), id(ID)
{
    for (uint32_t i = 0; i < highThreads; i++)
        threads.create_thread(ThreadHelper(this, HIGH));

    for (uint32_t i = 0; i < midThreads; i++)
        threads.create_thread(ThreadHelper(this, MEDIUM));

    for (uint32_t i = 0; i < lowThreads; i++)
        threads.create_thread(ThreadHelper(this, LOW));

    cout << "started " << highThreads << " high, " << midThreads << " med, " << lowThreads
         << " low.\n";
    defaultThreadCounts[HIGH] = threadCounts[HIGH] = highThreads;
    defaultThreadCounts[MEDIUM] = threadCounts[MEDIUM] = midThreads;
    defaultThreadCounts[LOW] = threadCounts[LOW] = lowThreads;
}

PriorityThreadPool::~PriorityThreadPool()
{
    stop();
}

void PriorityThreadPool::addJob(const Job& job, bool useLock)
{
    mutex::scoped_lock lk(mutex, defer_lock_t());

    if (useLock)
        lk.lock();

    // Create any missing threads
    if (defaultThreadCounts[HIGH] != threadCounts[HIGH])
    {
        threads.create_thread(ThreadHelper(this, HIGH));
        threadCounts[HIGH]++;
    }

    if (defaultThreadCounts[MEDIUM] != threadCounts[MEDIUM])
    {
        threads.create_thread(ThreadHelper(this, MEDIUM));
        threadCounts[MEDIUM]++;
    }

    if (defaultThreadCounts[LOW] != threadCounts[LOW])
    {
        threads.create_thread(ThreadHelper(this, LOW));
        threadCounts[LOW]++;
    }

    if (job.priority > 66)
        jobQueues[HIGH].push_back(job);
    else if (job.priority > 33)
        jobQueues[MEDIUM].push_back(job);
    else
        jobQueues[LOW].push_back(job);

    if (useLock)
        newJob.notify_one();
}

void PriorityThreadPool::removeJobs(uint32_t id)
{
    list<Job>::iterator it;

    mutex::scoped_lock lk(mutex);

    for (uint32_t i = 0; i < _COUNT; i++)
        for (it = jobQueues[i].begin(); it != jobQueues[i].end();)
            if (it->id == id)
                it = jobQueues[i].erase(it);
            else
                ++it;
}

PriorityThreadPool::Priority PriorityThreadPool::pickAQueue(Priority preference)
{
    if (!jobQueues[preference].empty())
        return preference;
    else if (!jobQueues[HIGH].empty())
        return HIGH;
    else if (!jobQueues[MEDIUM].empty())
        return MEDIUM;
    else
        return LOW;
}

void PriorityThreadPool::threadFcn(const Priority preferredQueue) throw()
{
    Priority queue;
    uint32_t weight, i;
    vector<Job> runList;
    vector<bool> reschedule;
    uint32_t rescheduleCount;
    uint32_t queueSize;
    bool running = false;

    try
    {
        while (!_stop)
        {

            mutex::scoped_lock lk(mutex);

            queue = pickAQueue(preferredQueue);

            if (jobQueues[queue].empty())
            {
                newJob.wait(lk);
                continue;
            }

            queueSize = jobQueues[queue].size();
            weight = 0;
            // 3 conditions stop this thread from grabbing all jobs in the queue
            //
            // 1: The weight limit has been exceeded
            // 2: The queue is empty
            // 3: It has grabbed more than half of the jobs available &
            //     should leave some to the other threads

            while ((weight < weightPerRun) && (!jobQueues[queue].empty())
                    && (runList.size() <= queueSize / 2))
            {
                runList.push_back(jobQueues[queue].front());
                jobQueues[queue].pop_front();
                weight += runList.back().weight;
            }

            lk.unlock();

            reschedule.resize(runList.size());
            rescheduleCount = 0;

            for (i = 0; i < runList.size() && !_stop; i++)
            {
                reschedule[i] = false;
                running = true;
                reschedule[i] = (*(runList[i].functor))();
                running = false;

                if (reschedule[i])
                    rescheduleCount++;
            }

            // no real work was done, prevent intensive busy waiting
            if (rescheduleCount == runList.size())
                usleep(1000);

            if (rescheduleCount > 0)
            {
                lk.lock();

                for (i = 0; i < runList.size(); i++)
                    if (reschedule[i])
                        addJob(runList[i], false);

                if (rescheduleCount > 1)
                    newJob.notify_all();
                else
                    newJob.notify_one();

                lk.unlock();
            }

            runList.clear();
        }
    }
    catch (std::exception& ex)
    {
        // Log the exception and exit this thread
        try
        {
            threadCounts[queue]--;
#ifndef NOLOGGING
            logging::Message::Args args;
            logging::Message message(5);
            args.add("threadFcn: Caught exception: ");
            args.add(ex.what());

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );
#endif

            if (running)
                sendErrorMsg(runList[i].uniqueID, runList[i].stepID, runList[i].sock);
        }
        catch (...)
        {
        }
    }
    catch (...)
    {

        // Log the exception and exit this thread
        try
        {
            threadCounts[queue]--;
#ifndef NOLOGGING
            logging::Message::Args args;
            logging::Message message(6);
            args.add("threadFcn: Caught unknown exception!");

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );
#endif

            if (running)
                sendErrorMsg(runList[i].uniqueID, runList[i].stepID, runList[i].sock);
        }
        catch (...)
        {
        }
    }
}

void PriorityThreadPool::sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock)
{
    ISMPacketHeader ism;
    PrimitiveHeader ph = {0};

    ism.Status =  logging::primitiveServerErr;
    ph.UniqueID = id;
    ph.StepID = step;
    ByteStream msg(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
    msg.append((uint8_t*) &ism, sizeof(ism));
    msg.append((uint8_t*) &ph, sizeof(ph));

    sock->write(msg);
}

void PriorityThreadPool::stop()
{
    _stop = true;
    threads.join_all();
}

} // namespace threadpool
// vim:ts=4 sw=4:
