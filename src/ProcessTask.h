

#ifndef PROCESS_TASK_H_
#define PROCESS_TASK_H_

#include "ThreadPool.h"

namespace storagemanager
{

class ProcessTask : public ThreadPool::Job
{
    public:
        ProcessTask(int sock, uint length);   // _sock is the socket to read from
        virtual ~ProcessTask();
        
        void operator()();
        
    private:
        ProcessTask();
        
        void handleError(int errCode);
        int sock;
        uint length;
        bool returnedSock;
};



}




#endif
