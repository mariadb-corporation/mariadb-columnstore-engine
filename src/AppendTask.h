

#ifndef APPENDTASK_H_
#define APPENDTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class AppendTask : public PosixTask
{
    public:
        AppendTask(int sock, uint length);
        virtual ~AppendTask();
        
        void run();
    
    private:
        AppendTask();
};

}
#endif
