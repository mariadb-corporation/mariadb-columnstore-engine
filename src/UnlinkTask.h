
#ifndef UNLINKTASK_H_
#define UNLINKTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class UnlinkTask : public PosixTask
{
    public:
        UnlinkTask(int sock, uint length);
        virtual ~UnlinkTask();
        
        bool run();
    
    private:
        UnlinkTask();
};


}
#endif
