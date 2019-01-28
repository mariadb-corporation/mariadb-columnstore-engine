

#ifndef APPENDTASK_H_
#define APPENDTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class AppendTask : PosixTask
{
    public:
        AppendTask(int sock, uint length);
        virtual ~AppendTask();
        
        void run();
    
    private:
        AppendTask();
        
        struct cmd_overlay {
            size_t count;
            uint filename_len;
            char filename[];
        };
};

}
#endif
