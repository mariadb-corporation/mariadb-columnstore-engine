
#include "ThreadPool.h"
#include <iostream>

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
    pruner = boost::thread([this] { this->pruner_fcn(); } );
}

ThreadPool::~ThreadPool()
{
    boost::unique_lock<boost::mutex> s(mutex);
    die = true;
    jobs.clear();
    jobAvailable.notify_all();
    s.unlock();
    
    threads.join_all();
    pruner.interrupt();
    pruner.join();
}

void ThreadPool::addJob(const boost::shared_ptr<Job> &j)
{
    boost::unique_lock<boost::mutex> s(mutex);
    jobs.push_back(j);
    // Start another thread if necessary
    if (threadsWaiting == 0 && threads.size() < maxThreads) {
        boost::thread *thread = threads.create_thread([this] { this->processingLoop(); });
        s_threads.insert(thread);
    }
    else
        jobAvailable.notify_one();
}

void ThreadPool::pruner_fcn()
{
    while (!die)
    {
        prune();
        try {
            boost::this_thread::sleep(idleThreadTimeout);
        }
        catch(boost::thread_interrupted)
        {}
    }
}

void ThreadPool::prune()
{
    set<ID_Thread>::iterator it;
 
    boost::unique_lock<boost::mutex> s(mutex);
    while (1)
    {
        while (pruneable.empty() && !die)
            somethingToPrune.wait(s);
        if (die)
            return;
        
        for (auto &id : pruneable)
        {
            it = s_threads.find(id);
            assert(it != s_threads.end());
            it->thrd->join();
            threads.remove_thread(it->thrd);
            s_threads.erase(it);
        }
        pruneable.clear();
    }
}

void ThreadPool::setMaxThreads(uint newMax)
{
    maxThreads = newMax;
}

void ThreadPool::processingLoop()
{
    try
    {
        _processingLoop();
    }
    catch (...)
    {}
    boost::unique_lock<boost::mutex> s(mutex);
    pruneable.push_back(boost::this_thread::get_id());
    somethingToPrune.notify_one();
}

void ThreadPool::_processingLoop()
{
    boost::unique_lock<boost::mutex> s(mutex, boost::defer_lock);
    
    while (1)
    {
        s.lock();
        while (jobs.empty() && !die) 
        {
            threadsWaiting++;
            bool timedout = !jobAvailable.timed_wait<>(s, idleThreadTimeout);
            threadsWaiting--;
            if (timedout)
                return;
        }
        if (die)
            return;
        boost::shared_ptr<Job> job = jobs.front();
        jobs.pop_front();
        s.unlock();
        
        (*job)();
    }
}

inline bool ThreadPool::id_compare::operator()(const ID_Thread &t1, const ID_Thread &t2) const
{
    return t1.id < t2.id;
}

ThreadPool::ID_Thread::ID_Thread(boost::thread::id &i): id(i)
{
}

ThreadPool::ID_Thread::ID_Thread(boost::thread *t): id(t->get_id()), thrd(t)
{
}

}
