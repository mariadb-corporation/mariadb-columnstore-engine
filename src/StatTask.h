
#ifndef STATTASK_H_
#define STATTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class StatTask : public PosixTask
{
    public:
        StatTask(int sock, uint length);
        virtual ~StatTask();
        
        void run();
    
    private:
        StatTask();
};


}
#endif
