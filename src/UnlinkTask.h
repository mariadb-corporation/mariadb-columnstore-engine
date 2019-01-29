
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
        
        void run();
    
    private:
        UnlinkTask();
        
        struct cmd_overlay {
            uint flen;
            char filename[];
        };
};


}
#endif
