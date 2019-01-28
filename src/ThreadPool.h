
#ifndef SM_THREADPOOL_H_
#define SM_THREADPOOL_H_

#include <list>
#include <deque>
#include <boost/thread.hpp>
//#include <boost/thread/mutex.hpp>
#include <boost/utility.hpp>

namespace storagemanager
{

typedef boost::function0<void> Job;

class ThreadPool : public boost::noncopyable
{
    public:
        ThreadPool();    // this ctor uses an 'unbounded' # of threads; relies on the context for a natural max.
        ThreadPool(uint num_threads);   // this ctor lets caller specify a limit.
        virtual ~Threadpool();
        
        // doesn't block
        void addJob(Job &j);

    private:
        void processingLoop();   // the fcn run by each thread
    
        uint maxThreads;
        bool die;
        int threadsWaiting;
        std::vector<boost::scoped_ptr<boost:thread> > threads;
        boost::condition jobAvailable;
        std::deque<Job> jobs;
        boost::mutex m;
};


}

#endif
