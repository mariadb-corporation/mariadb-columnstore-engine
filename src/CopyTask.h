
#ifndef COPYTASK_H_
#define COPYTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class CopyTask : public PosixTask
{
    public:
        CopyTask(int sock, uint length);
        virtual ~CopyTask();
        
        bool run();
    
    private:
        CopyTask();
};


}
#endif
