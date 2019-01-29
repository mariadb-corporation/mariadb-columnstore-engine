

#ifndef WRITETASK_H_
#define WRITETASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class WriteTask : public PosixTask
{
    public:
        WriteTask(int sock, uint length);
        virtual ~WriteTask();
        
        void run();
    
    private:
        WriteTask();
        
        struct cmd_overlay {
            size_t count;
            off_t offset;
            uint filename_len;
            char filename[];
        };
};

}
#endif
