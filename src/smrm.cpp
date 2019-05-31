#include <iostream>
#include "IOCoordinator.h"

using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'rm -rf' for files managed by StorageManager, but with no options or globbing" << endl;
    cerr << "Usage: " << progname << " file-or-dir1 file-or-dir2 .. file-or-dirN" << endl;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    IOCoordinator *ioc = IOCoordinator::get();
    for (int i = 1; i < argc; i++)
        ioc->unlink(argv[i]);
        
    return 0;
}
