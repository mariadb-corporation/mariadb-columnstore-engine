

#ifndef PINGTASK_H_
#define PINGTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class PingTask : public PosixTask
{
    public:
        PingTask(int sock, uint length);
        virtual ~PingTask();
        
        bool run();
    
    private:
        PingTask();
};

}
#endif
