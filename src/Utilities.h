#ifndef UTILITIES_H_
#define UTILITIES_H_

#include <string>


namespace storagemanager
{

class IOCoordinator;

// a few utility classes we've coded here and there, now de-duped and centralized.
// modify as necessary.

struct ScopedFileLock
{
    ScopedFileLock(IOCoordinator *i, const std::string &k);
    virtual ~ScopedFileLock();
    
    virtual void lock() = 0;
    virtual void unlock() = 0;
    
    IOCoordinator *ioc;
    bool locked;
    const std::string key;
};

struct ScopedReadLock : public ScopedFileLock
{
    ScopedReadLock(IOCoordinator *i, const std::string &k);
    virtual ~ScopedReadLock();
    void lock();
    void unlock();
};

struct ScopedWriteLock : public ScopedFileLock
{
    ScopedWriteLock(IOCoordinator *i, const std::string &k);
    virtual ~ScopedWriteLock();
    void lock();
    void unlock();
};

struct ScopedCloser
{
    ScopedCloser();
    ScopedCloser(int f);
    ~ScopedCloser();
    int fd;
};

class SharedCloser
{
    public: 
        SharedCloser(int f);
        SharedCloser(const SharedCloser &);
        ~SharedCloser();
    
    private:
        struct CtrlBlock
        {
            int fd;
            uint refCount;
        };
        
        CtrlBlock *block;
};

}

#endif
