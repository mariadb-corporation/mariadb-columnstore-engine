#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>


/* Quicky impl of a read-write lock that prioritizes writers. */
namespace storagemanager
{
    class RWLock
    {
        public:
            RWLock();
            ~RWLock();
            
            void readLock();
            void readUnlock();
            void writeLock();
            void writeUnlock();
            
        private:
            uint readersWaiting;
            uint readersRunning;
            uint writersWaiting;
            uint writersRunning;
            boost::mutex m;
            boost::condition okToWrite;
            boost::condition okToRead;
    };
}

