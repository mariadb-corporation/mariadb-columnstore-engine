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
            // this version will release the lock in the parameter after locking this instance
            void readLock(boost::unique_lock<boost::mutex> &);
            void readUnlock();
            void writeLock();
            // this version will release the lock in the parameter after locking this instance
            void writeLock(boost::unique_lock<boost::mutex> &);
            void writeUnlock();
            
            // returns true if anything is blocked on or owns this lock instance.
            bool inUse();
            
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

