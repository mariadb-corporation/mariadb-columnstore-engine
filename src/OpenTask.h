
#ifndef OPENTASK_H_
#define OPENTASK_H_

#include "PosixTask.h"

namespace storagemanager
{

class OpenTask : public PosixTask
{
    public:
        OpenTask(int sock, uint length);
        virtual ~OpenTask();
        
        void run();
    
    private:
        OpenTask();
};


}
#endif
