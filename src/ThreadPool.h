
#ifndef SM_THREADPOOL_H_
#define SM_THREADPOOL_H_

#include <deque>
#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

namespace storagemanager
{

class ThreadPool : public boost::noncopyable
{
    public:
        ThreadPool();    // this ctor uses an 'unbounded' # of threads; relies on the context for a natural max.
        ThreadPool(uint num_threads);   // this ctor lets caller specify a limit.
        virtual ~ThreadPool();
        
        // addJob doesn't block
        class Job
        {
            public:
                virtual void operator()() = 0;
        };
        
        void addJob(const boost::shared_ptr<Job> &j);
        void setMaxThreads(uint newMax);

    private:
        struct Runner {
            Runner(ThreadPool *t) : tp(t) { }
            void operator()() { tp->processingLoop(); }
            ThreadPool *tp;
        };
        
        void processingLoop();   // the fcn run by each thread
    
        uint maxThreads;
        bool die;
        int threadsWaiting;
        boost::thread_group threads;
        std::set<boost::thread *> s_threads;
        boost::condition jobAvailable;
        std::deque<boost::shared_ptr<Job> > jobs;
        boost::mutex m;
        
        const boost::posix_time::time_duration idleThreadTimeout = boost::posix_time::seconds(60);
        boost::thread pruner;
        void pruner_fcn();
        void prune();
};


}

#endif
