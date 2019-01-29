

#ifndef PINGTASK_H_
#define PINGTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class PingTask : PosixTask
{
    public:
        PingTask(int sock, uint length);
        virtual ~PingTask();
        
        void run();
    
    private:
        PingTask();
};

}
#endif
