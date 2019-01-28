

#ifndef POSIX_TASK_H_
#define POSIX_TASK_H_

#include <vector>

class PosixTask
{
    public:
        PosixTask(int sock, uint length);
        virtual ~PosixTask();
        
        virtual void run() = 0;
        void primeBuffer();
        
    protected:
        int read(uint8_t *buf, uint length);
        bool write(const std::vector<uint8_t> &buf);
        bool write(void *buf, uint length);
        uint getLength();  // returns the total length of the msg
        uint getRemainingLength();   // returns the remaining length from the caller's perspective
        void handleError(const char *name, int errCode);
        void returnSocket();
        void socketError();
        
    private:
        PosixTask();
        
        int sock;
        int totalLength;
        uint remainingLengthInStream;
        uint remainingLengthForCaller;
        static const uint bufferSize = 4096;
        uint8_t localBuffer[bufferSize];
        uint bufferPos;
        uint bufferLen;
        bool socketReturned;
};






#endif
