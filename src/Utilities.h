#ifndef UTILITIES_H_
#define UTILITIES_H_

#include <string>


namespace storagemanager
{

class IOCoordinator;

// a few utility classes we've coded here and there, now de-duped and centralized.
// modify as necessary.
struct ScopedReadLock
{
    ScopedReadLock(IOCoordinator *i, const std::string &k);
    ~ScopedReadLock();
    void lock();
    void unlock();
    
    IOCoordinator *ioc;
    bool locked;
    const std::string key;
};

struct ScopedWriteLock
{
    ScopedWriteLock(IOCoordinator *i, const std::string &k);
    ~ScopedWriteLock();
    void lock();
    void unlock();
    
    IOCoordinator *ioc;
    bool locked;
    const std::string key;
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
