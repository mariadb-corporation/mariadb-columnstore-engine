

#ifndef READTASK_H_
#define READTASH_H_

#include "PosixTask.h"

namespace storagemanager
{

class ReadTask : PosixTask
{
    public:
        ReadTask(int sock, uint length);
        virtual ~ReadTask();
        
        void run();
    
    private:
        ReadTask();
        
        struct cmd_overlay {
            size_t count;
            off_t offset;
            uint flen;
            char filename[];
        };
};

}
#endif
