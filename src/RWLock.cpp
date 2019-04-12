#include "RWLock.h"

namespace storagemanager
{

RWLock::RWLock() : readersWaiting(0), readersRunning(0), writersWaiting(0), writersRunning(0)
{
}

RWLock::~RWLock()
{
    assert(!readersWaiting);
    assert(!readersRunning);
    assert(!writersWaiting);
    assert(!writersRunning);
}

bool RWLock::inUse()
{
    boost::unique_lock<boost::mutex> s(m);
    return readersWaiting || readersRunning || writersWaiting || writersRunning;
}

void RWLock::readLock()
{
    boost::unique_lock<boost::mutex> s(m);
    
    ++readersWaiting;
    while (writersWaiting != 0 || writersRunning != 0)
        okToRead.wait(s);
        
    ++readersRunning;
    --readersWaiting;
}

void RWLock::readLock(boost::unique_lock<boost::mutex> &l)
{
    boost::unique_lock<boost::mutex> s(m);
    l.unlock();
    
    ++readersWaiting;
    while (writersWaiting != 0 || writersRunning != 0)
        okToRead.wait(s);
        
    ++readersRunning;
    --readersWaiting;
}

void RWLock::readUnlock()
{
    boost::unique_lock<boost::mutex> s(m);
    
    assert(readersRunning > 0);
    --readersRunning;
    if (readersRunning == 0 && writersWaiting != 0)
        okToWrite.notify_one();
}

void RWLock::writeLock()
{
    boost::unique_lock<boost::mutex> s(m);
    
    ++writersWaiting;
    while (readersRunning != 0 || writersRunning != 0)
        okToWrite.wait(s);
    
    ++writersRunning;
    --writersWaiting;
}

void RWLock::writeLock(boost::unique_lock<boost::mutex> &l)
{
    boost::unique_lock<boost::mutex> s(m);
    l.unlock();
    
    ++writersWaiting;
    while (readersRunning != 0 || writersRunning != 0)
        okToWrite.wait(s);
    
    ++writersRunning;
    --writersWaiting;
}

void RWLock::writeUnlock()
{
    boost::unique_lock<boost::mutex> s(m);
    
    assert(writersRunning > 0);
    --writersRunning;
    if (writersWaiting != 0)
        okToWrite.notify_one();
    else if (readersWaiting != 0)
        okToRead.notify_all();
}

}
