#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "IOCoordinator.h"


using namespace std;
using namespace storagemanager;




void usage(const char *progname)
{
    cerr << progname << " reads from stdin and puts it in a file managed by StorageManager" << endl;
    cerr << "Usage: " << progname << " output_file" << endl;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    uint8_t data[8192];
    int read_err, write_err;
    ssize_t count, offset = 0;
    
    IOCoordinator *ioc = IOCoordinator::get();
    struct stat _stat;
    read_err = ioc->open(argv[1], O_CREAT | O_TRUNC | O_WRONLY, &_stat);
    if (read_err < 0)
    {
        int l_errno = errno;
        cerr << "Failed to open/create " << argv[1] << ": " << strerror_r(l_errno, (char *) data, 8192) << endl;
        return 1;
    }
    
    do
    {
        read_err = ::read(STDIN_FILENO, data, 8192);
        if (read_err < 0)
        {
            int l_errno = errno;
            cerr << "Error reading stdin: " << strerror_r(l_errno, (char *) data, 8192) << endl;
            return 1;
        }
        count = 0;
        while (count < read_err)
        {
            write_err = ioc->write(argv[1], &data[count], offset + count, read_err - count);
            if (write_err < 0)
            {
                int l_errno = errno;
                cerr << "Error writing to " << argv[1] << ": " << strerror_r(l_errno, (char *) data, 8192) << endl;
                return 1;
            }
            count += write_err;
        }
        offset += read_err;
    } while (read_err > 0);
    
    return 0;
}
