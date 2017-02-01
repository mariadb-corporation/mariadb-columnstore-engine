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
*   $Id: threadpool.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
***********************************************************************/
#define NOLOGGING
#include <stdexcept>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
using namespace logging;

#define THREADPOOL_DLLEXPORT
#include "threadpool.h"
#undef THREADPOOL_DLLEXPORT

namespace threadpool
{

ThreadPool::ThreadPool()
        :fMaxThreads( 0 ), fQueueSize( 0 )
{
    init();
}

ThreadPool::ThreadPool( size_t maxThreads, size_t queueSize )
        :fMaxThreads( maxThreads ), fQueueSize( queueSize )
{
    init();

    if (fQueueSize == 0)
        fQueueSize = fMaxThreads*2;
}



ThreadPool::~ThreadPool() throw()
{
// 	delete fThreadCreated;
    try
    {
        stop();
    }
    catch(...)
    {}
}

void ThreadPool::init()
{
    fThreadCount = 0;
    fGeneralErrors = 0;
    fFunctorErrors = 0;
	waitingFunctorsSize = 0;
	issued = 0;
    fStop = false;
//     fThreadCreated = new NoOp();
    fNextFunctor = fWaitingFunctors.end();
	fNextHandle=1;
}

void ThreadPool::setQueueSize(size_t queueSize)
{
    boost::mutex::scoped_lock lock1(fMutex);
    fQueueSize = queueSize;
}


void ThreadPool::setMaxThreads(size_t maxThreads)
{
    boost::mutex::scoped_lock lock1(fMutex);
    fMaxThreads = maxThreads;
}

void ThreadPool::setThreadCreatedListener(const Functor_T &f)
{
//     fThreadCreated = f;
}

void ThreadPool::stop()
{
    boost::mutex::scoped_lock lock1(fMutex);
    fStop = true;
    lock1.unlock();

    fNeedThread.notify_all();
    fThreads.join_all();
}


void ThreadPool::wait()
{
    boost::mutex::scoped_lock lock1(fMutex);

    while (waitingFunctorsSize > 0)
    {
        fThreadAvailable.wait(lock1);
		//cerr << "woke!" << endl;
    }
}

void ThreadPool::join(uint64_t thrHandle)
{
    boost::mutex::scoped_lock lock1(fMutex);

    while (waitingFunctorsSize > 0)
    {
		Container_T::iterator iter;
		Container_T::iterator end = fWaitingFunctors.end();
		bool foundit = false;
		for (iter = fWaitingFunctors.begin(); iter != end; ++iter)
		{
			foundit = false;
			if (iter->first == thrHandle)
			{
				foundit = true;
				break;
			}
		}
		if (!foundit)
		{
			break;
		}
        fThreadAvailable.wait(lock1);
    }
}

void ThreadPool::join(std::vector<uint64_t> thrHandle)
{
    boost::mutex::scoped_lock lock1(fMutex);

    while (waitingFunctorsSize > 0)
    {
		Container_T::iterator iter;
		Container_T::iterator end = fWaitingFunctors.end();
		bool foundit = false;
		for (iter = fWaitingFunctors.begin(); iter != end; ++iter)
		{
			foundit = false;
			std::vector<uint64_t>::iterator thrIter;
			std::vector<uint64_t>::iterator thrEnd = thrHandle.end();
			for (thrIter = thrHandle.begin(); thrIter != thrEnd; ++thrIter)
			{
				if (iter->first == *thrIter)
				{
					foundit = true;
					break;
				}
			}
			if (foundit == true)
			{
				break;
			}
		}
		// If we didn't find any of the handles, then all are complete
		if (!foundit)
		{
			break;
		}
        fThreadAvailable.wait(lock1);
    }
}

int64_t ThreadPool::invoke(const Functor_T &threadfunc)
{
    boost::mutex::scoped_lock lock1(fMutex);
	int64_t thrHandle=0;
    for(;;)
    {

        try
        {
            if ( waitingFunctorsSize < fThreadCount)
            {
                // Don't create a thread unless it's needed.  There
                // is a thread available to service this request.
                thrHandle = addFunctor(threadfunc);
                lock1.unlock();
                break;
            }

            bool bAdded = false;

            if ( waitingFunctorsSize < fQueueSize)
            {
                // Don't create a thread unless you have to
                thrHandle = addFunctor(threadfunc);
                bAdded = true;
            }

            if ( fThreadCount < fMaxThreads)
            {
                ++fThreadCount;

                lock1.unlock();
                fThreads.create_thread(beginThreadFunc(*this));

                if (bAdded)
                    break;

                // If the mutex is unlocked before creating the thread
                // this allows fThreadAvailable to be triggered
                // before the wait below runs.  So run the loop again.
                lock1.lock();
                continue;
            }

            if (bAdded)
            {
                lock1.unlock();
                break;
            }

            fThreadAvailable.wait(lock1);
        }
        catch(...)
        {
            ++fGeneralErrors;
            throw;
        }
    }

    fNeedThread.notify_one();
	return thrHandle;
}

void ThreadPool::beginThread() throw()
{
    try
    {
//         fThreadCreated();

        boost::mutex::scoped_lock lock1(fMutex);

        for(;;)
        {
            if (fStop)
                break;

            if (fNextFunctor == fWaitingFunctors.end())
            {
                // Wait until someone needs a thread
                fNeedThread.wait(lock1);
            }
            else
            {
				/* Need to tune these magic #s */

				vector<Container_T::iterator> todoList;
				int i, num;
				Container_T::const_iterator iter;

				/* Use this to control how many jobs are issued to a single thread */
                num = (waitingFunctorsSize - issued >= 1 ? 1 : 0);

				for (i = 0; i < num; i++)
                	todoList.push_back(fNextFunctor++);

				issued += num;
// 				cerr << "got " << num << " jobs." << endl;
//   				cerr << "got " << num << " jobs. waitingFunctorsSize=" <<
//   					waitingFunctorsSize << " issued=" << issued << " fThreadCount=" <<
//   					fThreadCount << endl;
                lock1.unlock();

				for (i = 0; i < num; i++) {
					try {
	                    (*todoList[i]).second();
					}
					catch(exception &e) {
						++fFunctorErrors;
						cerr << e.what() << endl;
					}
				}
				lock1.lock();

				issued -= num;
				waitingFunctorsSize -= num;
				for (i = 0; i < num; i++)
					fWaitingFunctors.erase(todoList[i]);
/*
				if (waitingFunctorsSize != fWaitingFunctors.size())
					cerr << "size mismatch!  fake size=" << waitingFunctorsSize <<
						" real size=" << fWaitingFunctors.size() << endl;
*/
                fThreadAvailable.notify_all();

            }
        }
    }
    catch (exception& ex)
    {

        ++fGeneralErrors;

        // Log the exception and exit this thread
        try
        {
#ifndef NOLOGGING
            logging::Message::Args args;
            logging::Message message(5);
            args.add("beginThread: Caught exception: ");
            args.add(ex.what());

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );
#endif
        }
        catch(...)
        {
        }

    }
    catch(...)
    {

        ++fGeneralErrors;

        // Log the exception and exit this thread
        try
        {
#ifndef NOLOGGING
            logging::Message::Args args;
            logging::Message message(6);
            args.add("beginThread: Caught unknown exception!");

            message.format( args );

            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);

            ml.logErrorMessage( message );
#endif
        }
        catch(...)
        {
        }
    }
}

int64_t ThreadPool::addFunctor(const Functor_T &func)
{
    bool bAtEnd = false;

    if (fNextFunctor == fWaitingFunctors.end())
        bAtEnd = true;

    fWaitingFunctors.push_back(make_pair(fNextHandle, func));
	waitingFunctorsSize++;
    if (bAtEnd)
    {
        --fNextFunctor;
    }
	return fNextHandle++;
}

void ThreadPool::dump()
{
    std::cout << "General Errors: " << fGeneralErrors << std::endl;
    std::cout << "Functor Errors: " << fFunctorErrors << std::endl;
    std::cout << "Waiting functors: " << fWaitingFunctors.size() << std::endl;
}

} // namespace threadpool
