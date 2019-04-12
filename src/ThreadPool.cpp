
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
    logger = SMLogging::get();
}

ThreadPool::ThreadPool(uint num_threads, bool _processQueueOnExit) : maxThreads(num_threads), die(false), 
    processQueueOnExit(_processQueueOnExit), threadsWaiting(0)
{
    logger = SMLogging::get();
    pruner = boost::thread([this] { this->prune(); } );
}

ThreadPool::~ThreadPool()
{
    boost::unique_lock<boost::mutex> s(mutex);
    die = true;
    if (!processQueueOnExit)
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
    if (die)
        return;
    
    jobs.push_back(j);
    // Start another thread if necessary
    if (threadsWaiting == 0 && threads.size() < maxThreads) {
        boost::thread *thread = threads.create_thread([this] { this->processingLoop(); });
        s_threads.insert(thread);
    }
    else
        jobAvailable.notify_one();
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
    boost::unique_lock<boost::mutex> s(mutex);
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
    boost::unique_lock<boost::mutex> s(mutex);
    
    while (threads.size() - pruneable.size() < maxThreads)  // this is the scale-down mechanism when maxThreads is changed
    {
        while (jobs.empty() && !die) 
        {
            threadsWaiting++;
            bool timedout = !jobAvailable.timed_wait<>(s, idleThreadTimeout);
            threadsWaiting--;
            if (timedout && jobs.empty())
                return;
        }
        if (jobs.empty())
            return;
        boost::shared_ptr<Job> job = jobs.front();
        jobs.pop_front();
        
        s.unlock();
        try
        {
            (*job)();
        }
        catch (exception &e)
        {
            logger->log(LOG_CRIT, "ThreadPool: caught '%s' from a job", e.what());
        }
        s.lock();
    }
    //cout << "threadpool thread exiting" << endl;
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
