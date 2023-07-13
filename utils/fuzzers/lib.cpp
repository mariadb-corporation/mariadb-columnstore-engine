// my_library.cpp
#include <iostream>

// Function in the shared library
extern "C" void hello()
{
    std::cout << "Hello from MyLibrary!" << std::endl;
}