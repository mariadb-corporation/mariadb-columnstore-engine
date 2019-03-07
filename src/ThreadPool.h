
#ifndef SM_THREADPOOL_H_
#define SM_THREADPOOL_H_

#include <deque>
#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include "SMLogging.h"

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
        void processingLoop();   // the fcn run by each thread
        void _processingLoop();  // processingLoop() wraps _processingLoop() with thread management stuff.
    
        SMLogging *logger;
        uint maxThreads;
        bool die;
        int threadsWaiting;
        boost::thread_group threads;
        
        // the set s_threads below is intended to make pruning idle threads efficient.
        // there should be a cleaner way to do it.
        struct ID_Thread
        {
            ID_Thread(boost::thread::id &);
            ID_Thread(boost::thread *);
            boost::thread::id id;
            boost::thread *thrd;
        };
        
        struct id_compare
        {
            bool operator()(const ID_Thread &, const ID_Thread &) const;
        };
        std::set<ID_Thread, id_compare> s_threads;
        
        boost::condition jobAvailable;
        std::deque<boost::shared_ptr<Job> > jobs;
        boost::mutex mutex;
        
        const boost::posix_time::time_duration idleThreadTimeout = boost::posix_time::seconds(60);
        boost::thread pruner;
        boost::condition somethingToPrune;
        std::vector<boost::thread::id> pruneable;  // when a thread is about to return it puts its id here
        void prune();
};


}

#endif
