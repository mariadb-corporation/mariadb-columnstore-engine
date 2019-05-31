#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <boost/filesystem.hpp>

#include "IOCoordinator.h"

using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'ls -l' for files managed by StorageManager" << endl;
    cerr << "Usage: " << progname << " directory" << endl;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    IOCoordinator *ioc = IOCoordinator::get();
    vector<string> listing;
    char buf[80];
    int err = ioc->listDirectory(argv[1], &listing);
    if (err)
    {
        int l_errno = errno;
        cerr << strerror_r(l_errno, buf, 80) << endl;
        return 1;
    }
    
    struct stat _stat;
    boost::filesystem::path base(argv[1]);
    boost::filesystem::path p;
    cout.fill(' ');
    for (auto &entry : listing)
    {
        p = base / entry;
        err = ioc->stat(p.string().c_str(), &_stat);
        if (!err)
        {
            if (_stat.st_mode & S_IFDIR)
            {
                cout << "d";
                cout.width(14);
            }
            else
                cout.width(15);
            cout << right << _stat.st_size << left << " " << entry << endl;
        }
        else
        {
            cout.width(15);
            cout << right << "error" << left <<  " " << entry << endl;
        }
    }
    
    return 0;
}

