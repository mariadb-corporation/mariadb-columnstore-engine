# copy licensing stuff here

#ifndef SMFILEFACTORY_H_
#define SMFILEFACTORY_H_

#include "IDBDataFile.h"
#include "FileFactoryBase.h"

namespace idbdatafile
{

class SMFileFactory : public FileFactoryBase
{
    public:
        IDBDataFile* open(const char* fname, const char* mode, unsigned opts, unsigned colWidth);
};

}
#endif
