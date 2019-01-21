#ifndef _SMEXECEPTIONS_H_
#define _SMEXECEPTIONS_H_

#include <exception>

namespace idbdatafile
{

class NotImplementedYet : public std::exception
{
    public:
        NotImplementedYet(const std::string &s);
};

class FailedToSend : public std::runtime_error
{
    public:
        FailedToSend(const std::string &s);
};

class FailedToRecv : public std::runtime_error
{
    public:
        FailedToRecv(const std::string &s);
};
        
        
NotImplementedYet::NotImplementedYet(const std::string &s) :
    std::exception(s + "() isn't implemented yet.")
{
}

FailedToSend::FailedToSend(const std::string &s) :
    std::runtime_error(s)
{
}

FailedToRecv::FailedToRecv(const std::string &s) :
    std::runtime_error(s)
{
}

}
#endif
