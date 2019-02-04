
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
        
        bool run();
    
    private:
        OpenTask();
};


}
#endif
