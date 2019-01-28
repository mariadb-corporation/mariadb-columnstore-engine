


#include "ThreadPool.h"
#include <boost/thread/locks.hpp>

using namespace std;

namespace storagemanager
{

ThreadPool::ThreadPool() : maxThreads(1000), die(false), threadsWaiting(0)
{
    // Using this ctor effectively limits the # of threads here to the natural limit of the 
    // context it's used in.  In the CRP class for example, the # of active threads would be 
    // naturally limited by the # of concurrent operations.
}

ThreadPool::ThreadPool(uint num_threads) : maxThreads(num_threads), die(false), threadsWaiting(0)
{
}

ThreadPool::~ThreadPool()
{
    boost::mutex::unique_lock s(m);
    die = true;
    jobs.clear();
    jobAvailable.notify_all();
    s.unlock();
    
    for (uint i = 0; i < threads.size(); i++)
        threads[i]->join();
}

void ThreadPool::addJob(Job &j)
{
    
    boost::mutex::unique_lock s(m);
    jobs.push_back(j);
    // Start another thread if necessary
    if (threadsWaiting == 0 && threads.size() < maxThreads) {
        boost::scoped_ptr<boost::thread> thread(new boost::thread(processingLoop));
        threads.push_back(thread);
    }
    else
        jobAvailable.notify_one();
}

void ThreadPool::processingLoop()
{
    boost::mutex::scoped_lock s(m, boost::defer_lock_t);
    
    while (!die)
    {
        s.lock();
        while (jobs.empty() && !die) 
        {
            threadsWaiting++;
            jobAvailable.wait(s);
            threadsWaiting--;
        }
        if (die)
            return;
        Job job = jobs.front();
        jobs.pop_front();
        s.unlock();
        
        job();
    }
}

}
