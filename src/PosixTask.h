

#ifndef POSIX_TASK_H_
#define POSIX_TASK_H_

#include <vector>
#include <sys/types.h>
#include <stdint.h>
#include "IOCoordinator.h"

namespace storagemanager
{

class PosixTask
{
    public:
        PosixTask(int sock, uint length);
        virtual ~PosixTask();
        
        // this should return false if there was a network error, true otherwise including for other errors
        virtual bool run() = 0;
        void primeBuffer();
        
    protected:
        bool read(uint8_t *buf, uint length);
        bool write(const std::vector<uint8_t> &buf);
        bool write(const uint8_t *buf, uint length);
        void consumeMsg();   // drains the remaining portion of the message
        uint getLength();  // returns the total length of the msg
        uint getRemainingLength();   // returns the remaining length from the caller's perspective
        void handleError(const char *name, int errCode);
        
        IOCoordinator *ioc;
        
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
};

}
#endif
