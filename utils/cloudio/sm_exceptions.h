#ifndef _SMEXECEPTIONS_H_
#define _SMEXECEPTIONS_H_

#include <exception>

class NotImplementedYet : public std::exception
{
    public:
        NotImplementedYet(const std::string &s);
};

NotImplementedYes::NotImplementedYet(const std::string &s) :
    std::exception(s + "() isn't implemented yet.")
{
}

#endif
