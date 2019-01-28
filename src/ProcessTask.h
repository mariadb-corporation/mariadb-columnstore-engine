

#ifndef PROCESS_TASK_H_
#define PROCESS_TASK_H_

namespace storagemanager
{

class ProcessTask
{
    public:
        ProcessTask(int sock, uint length);   // _sock is the socket to read from
        virtual ~ProcessTask();
        
        void operator()();
        
    private:
        ProcessTask();
        
        void handleError();
        int sock;
        uint length;
};



}




#endif
