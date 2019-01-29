
#ifndef SM_THREADPOOL_H_
#define SM_THREADPOOL_H_

#include <list>
#include <deque>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/utility.hpp>

namespace storagemanager
{

class ThreadPool : public boost::noncopyable
{
    public:
        ThreadPool();    // this ctor uses an 'unbounded' # of threads; relies on the context for a natural max.
        ThreadPool(uint num_threads);   // this ctor lets caller specify a limit.
        virtual ~ThreadPool();
        
        // addJob doesn't block
        typedef boost::function0<void> Job;
        void addJob(Job &j);

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
        std::vector<boost::shared_ptr<boost::thread> > threads;
        boost::condition jobAvailable;
        std::deque<Job> jobs;
        boost::mutex m;
};


}

#endif
