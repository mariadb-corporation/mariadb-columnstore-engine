
#ifndef TRUNCATETASK_H_
#define TRUNCATETASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class TruncateTask : public PosixTask
{
    public:
        TruncateTask(int sock, uint length);
        virtual ~TruncateTask();
        
        void run();
    
    private:
        TruncateTask();
};


}
#endif
