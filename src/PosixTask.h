

#ifndef POSIX_TASK_H_
#define POSIX_TASK_H_

#include <vector>

class PosixTask
{
    public:
        PosixTask(int sock, uint length);
        virtual ~PosixTask();
        
        virtual void run() = 0;
        
    protected:
        int read(std::vector<uint8_t> *buf, uint offset, uint length);
        bool write(const std::vector<uint8_t> &buf);
        
    private:
        PosixTask();
        
        void handleError();
        
        int sock;
        uint remainingLength;
        uint8_t buffer[4096];
        uint bufferPos;
        uint bufferLen;
};






#endif
