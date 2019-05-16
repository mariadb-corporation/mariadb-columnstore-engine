#include "Utilities.h"
#include "IOCoordinator.h"

namespace storagemanager
{

ScopedReadLock::ScopedReadLock(IOCoordinator *i, const std::string &k) : ioc(i), locked(false), key(k)
{
    lock();
}
ScopedReadLock::~ScopedReadLock()
{
    unlock();
}

void ScopedReadLock::lock()
{
    assert(!locked);
    ioc->readLock(key);
    locked = true;
}

void ScopedReadLock::unlock()
{
    if (locked)
    {
        ioc->readUnlock(key);
        locked = false;
    }
}

ScopedWriteLock::ScopedWriteLock(IOCoordinator *i, const std::string &k) : ioc(i), locked(false), key(k)
{
    lock();
}
ScopedWriteLock::~ScopedWriteLock()
{
    unlock();
}

void ScopedWriteLock::lock()
{
    assert(!locked);
    ioc->writeLock(key);
    locked = true;
}

void ScopedWriteLock::unlock()
{
    if (locked)
    {
        ioc->writeUnlock(key);
        locked = false;
    }
}

ScopedCloser::ScopedCloser(int f) : fd(f) { assert(f != -1); }
ScopedCloser::~ScopedCloser() { 
    int s_errno = errno;
    ::close(fd);
    errno = s_errno; 
}
    
SharedCloser::SharedCloser(int f)
{ 
    block = new CtrlBlock();
    assert(f >= 0);
    block->fd = f;
    block->refCount = 1;
}

SharedCloser::SharedCloser(const SharedCloser &s) : block(s.block)
{
    block->refCount++;
}

SharedCloser::~SharedCloser()
{
    block->refCount--;
    if (block->refCount == 0)
    {
        int s_errno = errno;
        ::close(block->fd);
        delete block;
        errno = s_errno;
    }
}
    
}
