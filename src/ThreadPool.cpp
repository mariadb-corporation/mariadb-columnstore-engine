
#include "ThreadPool.h"

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
    boost::unique_lock<boost::mutex> s(m);
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
    boost::unique_lock<boost::mutex> s(m);
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
    boost::unique_lock<boost::mutex> s(m);
    set<boost::thread *>::iterator it, to_remove;
    it = s_threads.begin();
    while (it != s_threads.end())
    {
        if ((*it)->joinable())
        {
            (*it)->join();
            threads.remove_thread(*it);
            to_remove = it++;
            s_threads.erase(to_remove);
        }
        else
            ++it;
    }
}

void ThreadPool::setMaxThreads(uint newMax)
{
    maxThreads = newMax;
}

void ThreadPool::processingLoop()
{
    boost::unique_lock<boost::mutex> s(m, boost::defer_lock);
    
    while (!die)
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

}
